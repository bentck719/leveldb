#!/usr/bin/env python3
"""
Run commands ([...] = fill in your own value):

  # baseline
  sudo python3 run_benchmark.py --mode baseline --benchmarks "[benchmark]" \
       --num [num] --out-base "[out-base]" \
       --repeats [repeats] --wal-footprint --db-root [db-root]

  # prel0
  sudo python3 run_benchmark.py --mode prel0 --benchmarks "[benchmark]" \
       --num [num] --out-base "[out-base]" \
       --repeats [repeats] --wal-footprint --cxl-compaction --theta [theta] \
       --high-watermark [high-watermark] --low-watermark [low-watermark] \
       --histogram --db-root [db-root]
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass

DEFAULT_DB_BENCH = "./build/db_bench"
DEFAULT_DB_ROOT = "/dev/shm"
DEFAULT_METRICS_DIR = "./metrics"
DEFAULT_OUT_BASE = "./out"

CORES = "10,11"
DEFAULT_NUM = 1_000_000
DEFAULT_VALUE_SIZE = 1024

EXP_DB_NAME = "exp_db"
GOLDEN_PREFIX = "golden"

BENCH_ALIASES = {"fillzipfian": "writezipfian"}

THETA_CHOICES = [0.7, 0.9, 0.99]

BASE_PREL0_FLAGS = [
    "--enable_pre_l0=true",
]

METRIC_CSVS = ("compaction_metrics.csv", "flush_metrics.csv", "flush_hotness.csv",
               "wal_metrics.csv", "key_access.csv", "user_write_bytes.csv",
               "phase_timing.csv", "pre_l0_stats.csv", "pre_l0_flush.csv",
               "latency_percentiles.csv", "wal_footprint.csv")

DRY_RUN = False


@dataclass
class Config:
    db_bench: str
    db_root: str
    metrics_dir: str
    out: str
    cxl: bool
    num: int
    size: str
    value_size: int
    zipfian_theta: float
    exp_db: str
    golden: str
    wal_footprint: bool
    histogram: bool
    wal_snapshot_threshold_mb: int
    prefill: str


@dataclass
class Prel0Params:
    hot_threshold: int
    cxl_size_mb: int
    high_wm: float
    low_wm: float


@dataclass
class Failure:
    """One run that did not produce trustworthy results."""
    tag: str
    detail: str
    reason: str

    def __str__(self):
        return f"{self.tag}  [{self.detail}]  {self.reason}"


def size_label(num):
    return f"{num // 1_000_000}GB"


def golden_dir(size):
    return os.path.join(".", f"{GOLDEN_PREFIX}_{size}")


def _rmtree(path):
    if DRY_RUN:
        print(f"[dry-run] rm -rf {path}")
        return
    if not os.path.exists(path):
        return
    try:
        shutil.rmtree(path)
    except OSError as e:
        print(f"[FATAL] cannot remove {path}: {e}")
        print("        (owned by another user? a previous sudo run may have "
              "left it behind — remove it by hand and re-run)")
        sys.exit(1)
    if os.path.exists(path):
        print(f"[FATAL] {path} still exists after rm -rf; refusing to run on a "
              f"dirty DB")
        sys.exit(1)


def _copytree(src, dst):
    if DRY_RUN:
        print(f"[dry-run] cp -r {src} {dst}")
        return
    shutil.copytree(src, dst)


def _copy(src, dst):
    if DRY_RUN:
        print(f"[dry-run] cp {src} {dst}")
        return
    shutil.copy(src, dst)


def _makedirs(path):
    if DRY_RUN:
        print(f"[dry-run] mkdir -p {path}")
        return
    os.makedirs(path, exist_ok=True)


def fstype_of(path):
    try:
        real = os.path.realpath(path)
        best_mp, best_type = "", None
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 3:
                    continue
                mp, fstype = parts[1], parts[2]
                if (real == mp or real.startswith(mp.rstrip("/") + "/")) \
                        and len(mp) > len(best_mp):
                    best_mp, best_type = mp, fstype
        return best_type
    except OSError:
        return None


def preflight(db_bench, db_root):
    if DRY_RUN:
        print("[dry-run] skip preflight checks")
        return
    warn, fatal = [], []

    if not (os.path.isfile(db_bench) and os.access(db_bench, os.X_OK)):
        fatal.append(
            f"db_bench does not exist or is not executable: {db_bench}")

    ft = fstype_of(db_root)
    if ft is None:
        warn.append(
            f"Cannot determine fstype of DB_ROOT={db_root} — verify it is on RAM yourself")
    elif ft not in ("tmpfs", "ramfs"):
        fatal.append(
            f"Plan A requires DB on RAM-backed fs (injection is the latency truth); "
            f"DB_ROOT={db_root} fstype={ft} is not tmpfs")
    else:
        print(f"  DB_ROOT fstype = {ft} (RAM-backed, OK)")

    try:
        iso = open("/sys/devices/system/cpu/isolated").read().strip()
        if iso == "":
            warn.append(
                "isolcpus not set — CPU pinning is ineffective, injection timing will be disturbed")
        else:
            print(f"  isolated cpus = {iso}")
    except OSError:
        pass

    try:
        gov = open(
            "/sys/devices/system/cpu/cpu10/cpufreq/scaling_governor").read().strip()
        if gov != "performance":
            warn.append(
                f"cpu10 governor = {gov} (not performance) — frequency will drift")
    except OSError:
        pass

    for w in warn:
        print(f"  [PREFLIGHT WARN] {w}")
    for fz in fatal:
        print(f"  [PREFLIGHT FATAL] {fz}")
    if fatal:
        print("preflight has fatal problems, aborting.")
        sys.exit(1)
    if not warn:
        print("  preflight OK")


def build_prel0_flags(cfg, params):
    flags = list(BASE_PREL0_FLAGS)
    flags.append(f"--pre_l0_hot_threshold={params.hot_threshold}")
    flags.append(f"--pre_l0_cxl_size_mb={params.cxl_size_mb}")
    flags.append(f"--pre_l0_evict_high_watermark={params.high_wm}")
    flags.append(f"--pre_l0_evict_low_watermark={params.low_wm}")
    flags.append(
        f"--pre_l0_wal_snapshot_threshold_mb={cfg.wal_snapshot_threshold_mb}")
    flags.append(
        f"--enable_cxl_compaction={'true' if cfg.cxl else 'false'}")
    return flags


def parse_sweep(spec):
    spec = spec.strip()
    if "," in spec:
        return [int(x) for x in spec.split(",") if x.strip() != ""]
    parts = spec.split(":")
    if len(parts) == 3:
        start, end, step = (int(p) for p in parts)
        if step <= 0:
            raise ValueError(f"sweep step must be positive: {spec}")
        return list(range(start, end + 1, step))
    if len(parts) == 1:
        return [int(parts[0])]
    raise ValueError(
        f"Cannot parse sweep spec: {spec} (use comma list or START:END:STEP)")


def parse_watermark_sweep(spec):
    spec = spec.strip()
    if spec.startswith("grid:"):
        parts = spec[len("grid:"):].split(":")
        if len(parts) != 3:
            raise ValueError(
                f"Cannot parse watermark grid spec: {spec} (use grid:START:END:STEP)")
        start, end, step = (float(p) for p in parts)
        if step <= 0:
            raise ValueError(f"watermark grid step must be positive: {spec}")
        axis = []
        i = 0
        while True:
            v = round(start + i * step, 4)
            if v > end + 1e-9:
                break
            axis.append(v)
            i += 1
        pairs = []
        for high in axis:
            for low in axis:
                if low < high:
                    if not (0 < low < high <= 1):
                        raise ValueError(
                            f"watermark grid produced a pair outside 0 < low < high <= 1: "
                            f"(high={high}, low={low})")
                    pairs.append((high, low))
        if not pairs:
            raise ValueError(
                f"watermark grid has no valid pairs (need low < high): {spec}")
        pairs.sort()
        return pairs
    pairs = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if chunk == "":
            continue
        parts = chunk.split(":")
        if len(parts) != 2:
            raise ValueError(
                f"Cannot parse watermark pair: {chunk} (use HIGH:LOW)")
        high, low = float(parts[0]), float(parts[1])
        if not (0 < low < high <= 1):
            raise ValueError(
                f"watermark pair must satisfy 0 < low < high <= 1: {chunk}")
        pairs.append((high, low))
    if not pairs:
        raise ValueError(f"watermark sweep is empty: {spec}")
    return pairs


def _clear_metrics(cfg):
    if DRY_RUN:
        print(f"[dry-run] clear metrics csvs in {cfg.metrics_dir}")
        return
    for f in METRIC_CSVS:
        p = os.path.join(cfg.metrics_dir, f)
        if os.path.exists(p):
            os.remove(p)


def _build_cmd(benchmarks, prel0, db_path, cfg, params, use_existing_db):
    cmd = [
        "taskset", "-c", CORES,
        cfg.db_bench,
    ]
    if benchmarks:
        translated = ",".join(BENCH_ALIASES.get(b.strip(), b.strip())
                              for b in benchmarks.split(","))
        cmd.append(f"--benchmarks={translated}")
    cmd += [
        f"--num={cfg.num}",
        f"--value_size={cfg.value_size}",
        f"--db={db_path}",
        f"--metrics_dir={cfg.metrics_dir}",
        f"--use_existing_db={1 if use_existing_db else 0}",
        "--enable_host_model=true",
        f"--enable_wal_footprint_tracking="
        f"{'true' if cfg.wal_footprint else 'false'}",
        f"--zipfian_theta={cfg.zipfian_theta}",
    ]
    if cfg.histogram:
        cmd.append("--histogram=1")
    if prel0:
        cmd += build_prel0_flags(cfg, params)
    return cmd


def _run_db_bench(cmd, output_path):
    if DRY_RUN:
        print(f"[dry-run] exec: {' '.join(cmd)}")
        print(f"[dry-run] would save to: {output_path}")
        return 0
    with open(output_path, "w") as out:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True,
                                encoding="utf-8", errors="replace")
        for line in proc.stdout:
            print(line, end="")
            out.write(line)
        proc.wait()
    print(f"Saved: {output_path}  (exit={proc.returncode})")
    return proc.returncode


def build_golden(cfg):
    golden = cfg.golden
    if os.path.isdir(golden):
        print(f"\n>>> reuse existing golden DB (prefill cache): {golden} <<<")
        return 0
    print("\n==============================")
    print(
        f"Prefill cache miss — building golden DB (fillseq, baseline): {golden}")
    print("==============================")
    _rmtree(golden)
    _clear_metrics(cfg)
    cmd = _build_cmd("fillseq", prel0=False, db_path=golden, cfg=cfg,
                     params=Prel0Params(0, 0, 0, 0),
                     use_existing_db=False)
    rc = _run_db_bench(cmd, os.path.join(cfg.out, "golden_output.txt"))
    _clear_metrics(cfg)
    return rc


def prefill_exp_db(cfg):
    _rmtree(cfg.exp_db)
    _copytree(cfg.golden, cfg.exp_db)


def is_fill_workload(benchmarks):
    if not benchmarks:
        return False
    first = benchmarks.split(",")[0].strip()
    return first.startswith("fill")


def resolve_prefill(benchmarks, mode):
    if mode == "on":
        return True
    if mode == "off":
        return False
    return not is_fill_workload(benchmarks)


def parse_run_output(output_path):
    if not os.path.exists(output_path):
        return [], 0
    values, skipped = [], 0
    with open(output_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = re.match(r"\s*ops/sec \(measured\)\s*:\s*([0-9.]+)", line)
            if m:
                values.append(float(m.group(1)))
            elif "skipped (--use_existing_db is true)" in line:
                skipped += 1
    return values, skipped


def expected_reports(benchmarks):
    if not benchmarks:
        return None
    return len([b for b in benchmarks.split(",") if b.strip()])


def verify_output(output_path, benchmarks):
    ops, skipped = parse_run_output(output_path)
    want = expected_reports(benchmarks)
    if want is not None:
        want -= skipped
    if not ops:
        return ["no 'ops/sec (measured)' line in output.txt — the run produced "
                "no result"]
    if want is not None and len(ops) != want:
        return [f"expected {want} 'ops/sec (measured)' report(s) for "
                f"--benchmarks={benchmarks}"
                + (f" ({skipped} skipped by db_bench)" if skipped else "")
                + f", found {len(ops)} — some benchmark in the list did not "
                  f"finish"]
    if min(ops) <= 0:
        return [f"ops/sec (measured) = {min(ops):.0f}"]
    print(f"  throughput: {', '.join(f'{v:.0f}' for v in ops)} ops/sec"
          + ("" if want is None else f"  ({len(ops)}/{want} reports"
                                     + (f", {skipped} skipped" if skipped else "")
                                     + ")"))
    return []


def collect_metrics(cfg, tag):
    if DRY_RUN:
        for csv in METRIC_CSVS:
            print(f"[dry-run] cp {os.path.join(cfg.metrics_dir, csv)} "
                  f"{os.path.join(cfg.out, f'{tag}_{csv}')}  (if present)")
        return
    collected = 0
    for csv in METRIC_CSVS:
        src = os.path.join(cfg.metrics_dir, csv)
        if os.path.exists(src) and os.path.getsize(src) > 0:
            _copy(src, os.path.join(cfg.out, f"{tag}_{csv}"))
            collected += 1
    print(f"  metrics: collected {collected}/{len(METRIC_CSVS)} CSVs")


def run_benchmark(name, benchmarks, prel0, rep, cfg, params):
    tag = f"{name}_r{rep}"

    bench_desc = benchmarks if benchmarks else "(db_bench default full suite)"
    print()
    print("==============================")
    ht_desc = (f", hot_threshold={params.hot_threshold}, cxl_size_mb={params.cxl_size_mb}"
               f", high_wm={params.high_wm}, low_wm={params.low_wm}"
               f", wal_snapshot_threshold_mb={cfg.wal_snapshot_threshold_mb}"
               if prel0 else "")
    print(
        f"Running: {tag}  (prel0={prel0}, cxl_compaction={cfg.cxl}{ht_desc})")
    print(f"Benchmarks: {bench_desc}")
    print("==============================")

    detail = (f"bench={benchmarks or 'default-suite'} prel0={prel0} "
              f"theta={cfg.zipfian_theta} ht={params.hot_threshold} "
              f"cxl_size_mb={params.cxl_size_mb} "
              f"wm={params.high_wm}:{params.low_wm}")

    prefill = resolve_prefill(benchmarks, cfg.prefill)

    db_path = cfg.exp_db
    if prefill:
        print(f"\n--- {tag} Stage 1: prefill {db_path} from the golden cache "
              f"({cfg.golden}) ---")
        prefill_exp_db(cfg)
    else:
        print(f"\n--- {tag} Stage 1: fresh DB {db_path} (no prefill) ---")
        _rmtree(db_path)
    _clear_metrics(cfg)

    print(
        f"\n--- {tag} Stage 2: workload (--use_existing_db={1 if prefill else 0}) ---")
    cmd = _build_cmd(benchmarks, prel0, db_path, cfg, params,
                     use_existing_db=prefill)

    output_path = os.path.join(cfg.out, f"{tag}_output.txt")
    rc = _run_db_bench(cmd, output_path)

    if not DRY_RUN:
        time.sleep(1)
    collect_metrics(cfg, tag)

    failures = []
    if rc != 0:
        print(f"[FAIL] rc={rc}  {tag}  ({detail})")
        failures.append(Failure(tag, detail, f"db_bench exited rc={rc}"))
    if not DRY_RUN:
        for reason in verify_output(output_path, benchmarks):
            print(f"[FAIL] {tag}  ({detail})\n       {reason}")
            failures.append(Failure(tag, detail, reason))
    return failures


def run_with_retries(name, benchmarks, prel0, rep, cfg, params, retries):
    for attempt in range(retries + 1):
        if attempt:
            print(f"\n>>> RETRY {attempt}/{retries} for {name}_r{rep} "
                  f"(previous attempt failed) <<<")
        failures = run_benchmark(name, benchmarks, prel0, rep, cfg, params)
        if not failures:
            if attempt:
                print(f">>> retry {attempt} succeeded for {name}_r{rep}; "
                      f"its results replace the failed attempt <<<")
            return []
    attempts = retries + 1
    return [Failure(f.tag, f.detail, f"{f.reason}  (failed {attempts}/{attempts} "
                                     f"attempts)") for f in failures]


def build_parser():
    parser = argparse.ArgumentParser(
        description="LevelDB pre-L0 benchmark runner (post-Twin-refactor).")
    parser.add_argument(
        "--mode", choices=["baseline", "prel0", "all"], default="all")
    parser.add_argument("--repeats", type=int, default=3,
                        help="repeat count per group")
    parser.add_argument("--retries", type=int, default=1,
                        help="how many times to re-run a group that failed verification "
                             "(default 1; 0 disables). A retry redoes Stage 1 and Stage 2 and "
                             "overwrites that group's files, so a successful retry leaves clean "
                             "results behind")
    parser.add_argument("--cxl-compaction", action="store_true",
                        help="enable enable_cxl_compaction for the design group (only affects prel0)")
    parser.add_argument("--theta", type=float, default=0.99,
                        choices=THETA_CHOICES,
                        help="Zipfian skew theta, passed directly to db_bench --zipfian_theta. "
                             "Options 0.7 / 0.9 / 0.99, default 0.99 (= db_bench's original hardcoded value). "
                             "Larger theta → more concentrated access. Only effective for *zipfian / ycsb* workloads")
    parser.add_argument("--hot-threshold", type=int, default=1,
                        help="pre_l0_hot_threshold value (used when not sweeping), default 0")
    parser.add_argument("--hot-threshold-sweep", nargs="?", const="0:100:10",
                        default=None,
                        help="threshold sensitivity sweep. Bare flag = 0:100:10; "
                             "or specify START:END:STEP (inclusive), or comma list 0,5,10. "
                             "When enabled the prel0 group sweeps each threshold, baseline runs only once")
    parser.add_argument("--cxl-size", type=int, default=256,
                        help="pre_l0_cxl_size_mb value (MB, used when not sweeping), default 256")
    parser.add_argument("--cxl-size-sweep", nargs="?", const="8,16,32,64,128,256",
                        default=None,
                        help="CXL size sensitivity sweep (MB). Bare flag = "
                             "8,16,32,64,128,256; or comma list, or START:END:STEP. "
                             "When enabled the prel0 group sweeps each size, baseline runs only once")
    parser.add_argument("--high-watermark", type=float, default=0.90,
                        help="pre_l0_evict_high_watermark value (used when not sweeping), default 0.90")
    parser.add_argument("--low-watermark", type=float, default=0.70,
                        help="pre_l0_evict_low_watermark value (used when not sweeping), default 0.70")
    parser.add_argument("--wal-snapshot-threshold-mb", type=int, default=2048,
                        help="MB value for pre_l0_wal_snapshot_threshold_bytes: "
                             "triggers a pre-L0 WAL snapshot when live WAL exceeds this amount. "
                             "0 = disabled (old retain-until-referenced path). Default 2048 (2 GB)")
    parser.add_argument("--watermark-sweep", nargs="?",
                        const="1.0:0.8,0.9:0.7,0.8:0.6,0.7:0.5,0.6:0.4,0.5:0.3,0.4:0.2,0.3:0.1",
                        default=None,
                        help="eviction watermark sensitivity sweep. Bare flag = "
                             "0.90:0.70,0.80:0.60,0.70:0.50; or custom comma-separated "
                             "HIGH:LOW pairs (e.g. 0.85:0.65,0.75:0.55). high/low are dependent "
                             "(low < high), so sweep as pairs. When enabled the prel0 group sweeps each pair, "
                             "baseline runs only once")
    parser.add_argument("--out", default=None,
                        help="output folder; if unspecified, auto-named as "
                             "<benchmarks>_<cxl state>_theta<theta>_<size>")
    parser.add_argument("--out-base", default=DEFAULT_OUT_BASE,
                        help=f"root directory for auto-named output folders; default {DEFAULT_OUT_BASE}")
    parser.add_argument("--benchmarks", default=None,
                        help="custom benchmark string; if unspecified, runs db_bench's default full suite "
                             "(= plain ./db_bench). Supports the script-level alias fillzipfian "
                             "(= writezipfian but builds a fresh empty DB, use_existing_db=0)")
    parser.add_argument("--num", type=int, default=DEFAULT_NUM,
                        help=f"db_bench --num (operation count = total write data budget); default "
                             f"{DEFAULT_NUM}. Also determines the size label for the output folder and golden DB "
                             "(num//1M → '{n}GB', e.g. 10M → '10GB')")
    parser.add_argument("--db-bench", default=DEFAULT_DB_BENCH)
    parser.add_argument("--db-root", default=DEFAULT_DB_ROOT,
                        help="directory the DB lands in. Must be RAM-backed (tmpfs/ramfs, e.g. "
                             f"{DEFAULT_DB_ROOT}) — preflight aborts otherwise, because the "
                             "injection model, not the real device, is the latency truth")
    parser.add_argument("--metrics-dir", default=DEFAULT_METRICS_DIR)
    parser.add_argument("--wal-footprint", action="store_true", default=False,
                        help="enable enable_wal_footprint_tracking (records wal_footprint.csv on each "
                             "cleanup; off by default, adds extra stat/CSV overhead)")
    parser.add_argument("--histogram", action="store_true", default=False,
                        help="enable db_bench --histogram=1; each benchmark outputs "
                             "latency_percentiles.csv (collected per-run into the results folder)")
    parser.add_argument("--prefill", choices=["auto", "on", "off"], default="auto",
                        help="Stage 1 DB preparation, which also decides db_bench's use_existing_db. "
                             "on = prefill exp_db from the golden cache (./golden_<size>, "
                             "built once with fillseq if missing) and run the workload with "
                             "use_existing_db=1. off = empty exp_db, use_existing_db=0, Stage 2 "
                             "fills it itself. auto (default) = off for fill* workloads, on for "
                             "everything else.")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="print the commands and file operations without executing them")
    return parser


def build_config(args):
    size = size_label(args.num)
    if args.out:
        out_dir = args.out
    else:
        cxl_tag = "cxlcomp" if args.cxl_compaction else "nocxlcomp"
        out_dir = os.path.join(args.out_base,
                               f"{args.benchmarks}_{cxl_tag}_theta{args.theta}_{size}")
    return Config(
        db_bench=args.db_bench,
        db_root=args.db_root,
        metrics_dir=args.metrics_dir,
        out=out_dir,
        cxl=args.cxl_compaction,
        num=args.num,
        size=size,
        value_size=DEFAULT_VALUE_SIZE,
        zipfian_theta=args.theta,
        exp_db=os.path.join(args.db_root, EXP_DB_NAME),
        golden=golden_dir(size),
        wal_footprint=args.wal_footprint,
        histogram=args.histogram,
        wal_snapshot_threshold_mb=args.wal_snapshot_threshold_mb,
        prefill=args.prefill,
    )


def main():
    args = build_parser().parse_args()

    global DRY_RUN
    DRY_RUN = args.dry_run

    workload = args.benchmarks

    sweep = parse_sweep(
        args.hot_threshold_sweep) if args.hot_threshold_sweep else None
    cxl_sweep = parse_sweep(
        args.cxl_size_sweep) if args.cxl_size_sweep else None
    wm_sweep = (parse_watermark_sweep(args.watermark_sweep)
                if args.watermark_sweep else None)

    print(f">>> zipfian theta={args.theta} <<<")

    cfg = build_config(args)
    size = cfg.size

    _makedirs(cfg.out)
    _makedirs(cfg.db_root)
    _makedirs(cfg.metrics_dir)

    print(">>> preflight checks <<<")
    preflight(cfg.db_bench, cfg.db_root)
    prefill = resolve_prefill(workload, cfg.prefill)

    print(f">>> output dir: {cfg.out} <<<")
    print(f">>> num: {args.num}  (size label: {size}) <<<")
    if prefill:
        print(
            f">>> exp_db: {cfg.exp_db}  golden (prefill cache): {cfg.golden} <<<")
        stage1 = "rm -rf exp_db then prefill it from the golden cache"
    else:
        print(f">>> exp_db: {cfg.exp_db}  golden: (unused — "
              f"prefill off, Stage 2 builds the DB itself) <<<")
        stage1 = "rm -rf exp_db (Stage 2 fills it itself)"
    print(
        f">>> workload: {workload if workload else '(db_bench default full suite)'} <<<")
    print(f">>> Before each run, Stage 1: {stage1}; "
          f"Stage 2: run workload (collect stats only) <<<")
    if workload is None:
        print("  [WARN] --benchmarks not specified; Stage 2 will run db_bench's default full suite "
              "(includes its own fill), you should usually pair it with a single workload (e.g. writezipfian)")
    if sweep is not None:
        print(f">>> hot_threshold sweep: {sweep} <<<")
    if cxl_sweep is not None:
        print(f">>> cxl_size_mb sweep: {cxl_sweep} <<<")
    if wm_sweep is not None:
        print(f">>> watermark (high:low) sweep: {wm_sweep} <<<")

    thresholds = sweep if sweep is not None else [args.hot_threshold]
    sizes = cxl_sweep if cxl_sweep is not None else [args.cxl_size]
    watermarks = (wm_sweep if wm_sweep is not None
                  else [(args.high_watermark, args.low_watermark)])

    if prefill:
        rc = build_golden(cfg)
        if rc != 0:
            print(f"\n[FATAL] golden DB build failed (rc={rc}); aborting before "
                  f"any run, since every run would start from it.")
            sys.exit(1)
    else:
        print(">>> prefill off: no golden DB, each run builds its own fresh DB <<<")

    failed = []
    for rep in range(1, args.repeats + 1):
        if args.mode in ("baseline", "all"):
            print(f"\n>>> BASELINE (rep {rep}/{args.repeats}) <<<")
            failed += run_with_retries(
                "baseline", workload, prel0=False, rep=rep, cfg=cfg,
                params=Prel0Params(args.hot_threshold, args.cxl_size,
                                   args.high_watermark, args.low_watermark),
                retries=args.retries)
        if args.mode in ("prel0", "all"):
            for thr in thresholds:
                for sz in sizes:
                    for high_wm, low_wm in watermarks:
                        name = "design"
                        if sweep is not None:
                            name += f"_ht{thr}"
                        if cxl_sweep is not None:
                            name += f"_cxl{sz}"
                        if wm_sweep is not None:
                            name += f"_wm{high_wm}-{low_wm}"
                        print(f"\n>>> DESIGN pre-L0 ht={thr} "
                              f"cxl_size_mb={sz} high_wm={high_wm} "
                              f"low_wm={low_wm} (rep {rep}/{args.repeats}) <<<")
                        failed += run_with_retries(
                            name, workload, prel0=True, rep=rep, cfg=cfg,
                            params=Prel0Params(thr, sz, high_wm, low_wm),
                            retries=args.retries)
    print("\n==============================")
    print(f"Done (mode={args.mode}, repeats={args.repeats}, "
          f"cxl_compaction={cfg.cxl}). Results in {cfg.out}")
    if not DRY_RUN:
        subprocess.run(["ls", "-lh", cfg.out])
    print("==============================")

    if failed:
        print(f"\n=== {len(failed)} run(s) FAILED (retries exhausted) ===")
        for f in failed:
            print(f"   {f}")
        print("Re-run only the groups listed above.")
        sys.exit(1)
    print("\nAll runs completed with usable results.")


if __name__ == "__main__":
    main()
