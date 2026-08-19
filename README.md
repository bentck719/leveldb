# Pre-L0 Benchmark 使用文檔

本文件說明如何跑 Pre-L0 的效能實驗，涵蓋三件事。
以下指令的相對路徑都**假設在 `leveldb/` 資料夾裡**（`/sys`、`/proc`、`/dev/shm` 這類系統路徑除外）。

1. [CPU 時脈控制的設定](#1-cpu-時脈控制的設定)
2. [編譯的指令](#2-編譯的指令)
3. [`run_benchmark.py` 的使用](#3-run_benchmarkpy-的使用)

## 1. CPU 時脈控制的設定

### 1.1 開機參數：隔離核心（一次性設定，需重開機）

編輯 `/etc/default/grub`，在 `GRUB_CMDLINE_LINUX_DEFAULT` 加上：

```
isolcpus=10,11 nohz_full=10,11 rcu_nocbs=10,11
```

三個參數各自的作用：

| 參數 | 作用 |
|---|---|
| `isolcpus=10,11` | 把 10/11 移出預設排程域，一般行程不會被排上去，只有明確 `taskset` 才進得來 |
| `nohz_full=10,11` | 關掉這兩顆核心的週期性 timer tick，消除每個 tick 的中斷抖動 |
| `rcu_nocbs=10,11` | 把 RCU callback 的處理搬到別的核心，不讓 GC 類工作污染實驗核心 |

套用並重開機：

```bash
sudo update-grub
sudo reboot
```

驗證（本機目前已經設好）：

```bash
cat /proc/cmdline                          # 應含 isolcpus=10,11 nohz_full=10,11 rcu_nocbs=10,11
cat /sys/devices/system/cpu/isolated       # → 10-11
cat /sys/devices/system/cpu/nohz_full      # → 10-11
nproc                                      # → 14（= 16 - 2，代表隔離確實生效）
```

`nproc` 從 16 掉到 14 是隔離有效的旁證：`nproc` 讀的是行程的 CPU affinity mask，
`isolcpus` 已經把 10/11 從預設 mask 拿掉了。

### 1.2 時脈鎖定

sysfs 的設定 **不會跨重開機保留**，每次開機後、跑實驗前都要重新套用。

`intel_pstate` 的 **active mode** 只有 `performance` 和 `powersave` 兩種 governor，
且不存在 `/sys/devices/system/cpu/cpufreq/boost`（要用 `intel_pstate/no_turbo` 控 turbo）。

**步驟一 — governor 切成 performance：**

```bash
sudo cpupower -c 10,11 frequency-set -g performance
```

或直接寫 sysfs：

```bash
for c in 10 11; do
  echo performance | sudo tee /sys/devices/system/cpu/cpu$c/cpufreq/scaling_governor
done
```

**步驟二 — 關掉 Turbo Boost（全域設定，非 per-core）：**

```bash
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
```

這兩步合起來的效果：`performance` governor 在 HWP 下會把 min/max 效能請求都拉到頂，
而 `no_turbo=1` 把上限壓在 **base frequency 3.7 GHz**。結果就是核心穩定跑在 3.7 GHz，
不會因為熱、功耗或負載變化而在 0.8–4.9 GHz 之間漂移。

### 1.3 驗證

```bash
cat /sys/devices/system/cpu/cpu10/cpufreq/scaling_governor   # → performance
cat /sys/devices/system/cpu/intel_pstate/no_turbo            # → 1
cat /sys/devices/system/cpu/cpu1{0,1}/cpufreq/scaling_cur_freq   # → 3700000 左右
```

如果 `scaling_cur_freq` 還在 800000，代表 governor 沒切成功。

`run_benchmark.py` 的 preflight 也會幫你檢查其中兩項，但只會發 **WARN 不會擋**
（`run_benchmark.py:179-195`）：

- `isolcpus` 為空 → `isolcpus not set — CPU pinning is ineffective`
- `cpu10` 的 governor 不是 `performance` → `cpu10 governor = <x> (not performance) — frequency will drift`

**看到這兩個 WARN 就不要收該次數據**，回來補上面的設定。

### 1.4 還原成日常使用

```bash
sudo cpupower -c 10,11 frequency-set -g powersave
echo 0 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
```

---

## 2. 編譯的指令

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DLEVELDB_BUILD_BENCHMARKS=ON \
      -DCMAKE_CXX_FLAGS="-Wno-unused-variable" ..
make -j"$(nproc)"
```

之後改了程式碼要重編，要：

```bash
cd ./build && make -j"$(nproc)"
```

---

## 3. `run_benchmark.py` 的使用

### 3.1 兩個範例指令

**baseline：**

```bash
sudo python3 run_benchmark.py --mode baseline \
  --benchmarks ycsba --num 10_000_000 \
  --db-bench ./build/db_bench \
  --out-base ./out \
  --repeats 3 --wal-footprint
```

**prel0：**

```bash
sudo python3 run_benchmark.py --mode prel0 \
  --benchmarks ycsba --num 10_000_000 \
  --db-bench ./build/db_bench \
  --out-base ./out \
  --repeats 3 --wal-footprint --cxl-compaction \
  --theta 0.99 --high-watermark 1.0 --low-watermark 0.9 --histogram
```

### 3.2 完整流程速查

```bash
# 1. 鎖定 CPU（每次開機後一次）
sudo cpupower -c 10,11 frequency-set -g performance
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo

# 2. 確認鎖定成功
cat /sys/devices/system/cpu/cpu10/cpufreq/scaling_cur_freq   # → 3700000 左右

# 3. 編譯
cd ./build && make -j"$(nproc)" && cd ..

# 4. 跑實驗
sudo python3 run_benchmark.py --mode prel0 \
  --benchmarks ycsba --num 10_000_000 \
  --db-bench ./build/db_bench \
  --out-base ./out \
  --repeats 3 --wal-footprint --cxl-compaction --theta 0.99 --histogram

# 5. 跑完後還原
sudo cpupower -c 10,11 frequency-set -g powersave
echo 0 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
```
