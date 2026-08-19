# Pre-L0 Snapshot-Based WAL Compaction — Code Agent Prompts (leveldb)

## Context (give this to the agent first)

Repo: `leveldb`. This is a modified LevelDB with a **Pre-L0** stage
(`db/pre_l0/`) that buffers keys in CXL/volatile memory as a set of B+ trees
(`PseudoSST`) before they are flushed to L0. Pre-L0 durability currently relies on
**retaining the main WAL**: each live key records the WAL (`log`) number that holds
its latest version (`KeyLoc.latest_log`), `log_refcount_` maps log-number ->
live-key count, and `PreL0Manager::MinLogNumber()` returns the smallest WAL still
referenced. `DBImpl::CompactMemTable()` sets `edit.SetLogNumber(MinLogNumber())`,
and `RemoveObsoleteFiles()` deletes any WAL with `number < versions_->LogNumber()`.

**Problem:** this per-key WAL-number pairing forces retention of every WAL that
still holds a live Pre-L0 key. Measured up to ~4GB of retained WALs, dominated by
stale versions of hot keys (dead data), inflating storage and recovery replay cost.

**Goal:** replace the retention-until-referenced scheme with a **threshold-driven
snapshot**. When total live WAL bytes cross a user-configurable threshold, write a
compact snapshot of Pre-L0's current resident state (in WAL/log format), repoint
retention past the superseded WALs, and let `RemoveObsoleteFiles` delete them.
Measured compaction target: ~4GB -> ~100MB. Pre-L0 must keep serving reads/writes
during the snapshot.

## Confirmed facts about the existing code (rely on these — do not re-derive from scratch)

- **MVCC sequence numbers already exist.** Pre-L0 B+ tree values carry `seq` and
  `type` (`BPlusTreeValue::seq`, `::type`); `InsertOrUpdate` takes
  `SequenceNumber seq, ValueType type`. No new versioning is needed.
- **A non-blocking point-in-time snapshot primitive already exists:**
  `PreL0Manager::NewIterator()` (in `db/pre_l0/pre_l0_manager.cc`). Under
  `db_mutex_` (NOT pre-L0 `mutex_`) it walks `active_hot_`, `active_cold_`, and
  every sealed PST in `lru_`, emits each live key exactly once as a TRUE internal
  key `(user_key, seq, type)` including `kTypeDeletion` tombstones, sorted in
  internal-key order, and returns an `OwningVectorIterator` that owns a copied-out
  snapshot of the data. This is exactly the full resident-state snapshot we need.
- **WAL format = log records wrapping a `WriteBatch`.** `log::Writer::AddRecord`
  (`db/log_writer.h`) appends a record; content is `WriteBatchInternal::Contents`.
  `WriteBatchInternal::SetSequence` sets a batch's base sequence. Snapshot output
  written this way is replayable by the stock `DBImpl::RecoverLogFile`.
- **Retention floor is manifest-controlled and already crash-safe.**
  `versions_->LogNumber()` (set via a `VersionEdit` + `LogAndApply`) is the floor;
  `RemoveObsoleteFiles` deletes `kLogFile` with `number < LogNumber` (except
  `PrevLogNumber`). We reuse this as the atomic "publish" — no new manifest needed.
- **Recovery is implicit.** `PreL0Manager::Recover()` only opens the CXL region;
  Pre-L0 state is rebuilt by the normal WAL replay path (`RecoverLogFile` ->
  memtable -> `CompactMemTable` -> `FlushMemTable`). A snapshot written as a
  numbered log file is therefore replayed automatically on open.
- **Per-WAL bookkeeping exists.** `DBImpl` tracks `wal_create_times_` and computes
  per-WAL sizes in `RemoveObsoleteFiles` (see `wal_gap.csv` / `WalDeleteInfo`).
- **Scope decision (confirmed): the snapshot covers the entire Pre-L0 content
  still resident in memory and not yet in L0** — i.e. exactly what
  `NewIterator()` returns.
- **CRITICAL — `NewIterator()` does NOT include the memtables.** `mem_` (active)
  and `imm_` (immutable, flush pending) hold data that is durable only in their
  WALs and not yet in Pre-L0, and their sequence numbers can be `<= S`. The
  retention floor must therefore NEVER be advanced past any WAL still backing a
  live memtable, or that data is lost on crash. This is handled by the memtable
  clamp in the Target design below (confirmed necessary by P0).

## Target design (implement this)

On snapshot trigger, under `db_mutex_`:
1. Capture `S = versions_->LastSequence()` (the point-in-time cut).
2. Allocate `snapshot_num = versions_->NewFileNumber()`, then rotate the active
   WAL via the existing `mem_ -> imm_` swap path (as in `MakeRoomForWrite`, which
   allocates the next number `N+1 > snapshot_num`) so subsequent writes go to WAL
   `N+1` unblocked (their seq `> S`, not part of the snapshot). A *pure* WAL-file
   rotation that leaves `mem_` in place is NOT safe — `mem_`'s existing bytes would
   remain in a WAL `< snapshot_num` and be lost when it is deleted; the `mem_ ->
   imm_` swap moves those bytes into `imm_`/its retained WAL. If an `imm_` is
   already pending at trigger, let it drain first (or defer this snapshot cycle) —
   `MakeRoomForWrite` already blocks a second rotation while `imm_ != nullptr`.
3. Build the snapshot from `pre_l0_->NewIterator()` (filter to `seq <= S` for
   safety). For each entry emit a single-op `WriteBatch`
   (`SetSequence(entry.seq)`, `Put`/`Delete` by `type`) via a `log::Writer` into a
   temp file; keep only the latest version per key (the iterator already yields one
   per key). `fsync` the file and its directory, then atomically rename to
   `LogFileName(dbname, snapshot_num)`.
4. Repoint Pre-L0 retention: set every `KeyLoc.latest_log = snapshot_num` and
   rebuild `log_refcount_ = { snapshot_num : hashmap_.size() }`, so
   `MinLogNumber()` returns `snapshot_num`.
5. Compute the **clamped floor** `new_floor = min(snapshot_num, imm_log_number_)`
   where `imm_log_number_` is the WAL of any not-yet-flushed `imm_` (use
   `snapshot_num` when no `imm_` is pending). The active `mem_`'s WAL is `N+1 >
   snapshot_num` so it is never at risk. `LogAndApply` a `VersionEdit` with
   `SetLogNumber(new_floor)` — the atomic commit point. This is the always-safe
   correctness rule: it can lose no data regardless of memtable state.
6. `RemoveObsoleteFiles` deletes all superseded WALs (`number < new_floor`). The at
   most one retained WAL (`imm_log_number_`, a single write-buffer, ~MB) is folded
   into Pre-L0 by the normal `imm_` flush and reclaimed at the next snapshot cycle
   — negligible against a GB-scale threshold.

Crash safety falls out of the ordering: temp+fsync+rename before `LogAndApply`;
`LogAndApply` is the commit; deletions happen only after. Because every snapshot
record preserves its original `seq`, replay is idempotent across any crash window
(re-applying an already-present `key@seq` is a no-op), so no double-apply.

> Optional future optimization (do NOT implement now): "fold `imm_`" — force-flush
> `imm_` into Pre-L0 before the cut so it is captured by the snapshot, letting the
> floor advance to `snapshot_num` and reclaim that last WAL immediately. It buys a
> single write-buffer of space at the cost of coupling the snapshot to a
> synchronous memtable flush. The clamp above already keeps the floor safe, so
> folding is a pure optimization layered on top — leave a TODO, ship the clamp.

---

## Kickoff prompt (give this to the code agent first, before P0)

> You are implementing a feature in the `leveldb` codebase: replacing Pre-L0's
> "retain WALs until no live key references them" durability scheme with a
> **threshold-driven snapshot**. When total live WAL bytes cross a user-configured
> threshold, write a compact snapshot of Pre-L0's current resident state in
> LevelDB log format, repoint the WAL-retention floor past the superseded WALs,
> and let `RemoveObsoleteFiles` delete them. This bounds WAL growth (currently
> unbounded, measured ~4GB) to roughly the threshold, with the snapshot itself
> being working-set sized (measured ~102MB). Pre-L0 must keep serving reads and
> writes throughout — the snapshot is a point-in-time cut and later writes flow
> into a freshly rotated WAL.
>
> Read the "Context", "Confirmed facts", and "Target design" sections of this
> document before writing any code — they tell you which existing mechanisms to
> reuse (`PreL0Manager::NewIterator()` for the non-blocking snapshot, `log::Writer`
> + `WriteBatchInternal::SetSequence` for the log format, and
> `LogAndApply(SetLogNumber(...))` as the atomic commit point — do NOT invent a new
> manifest). Do not re-derive facts already stated there.
>
> Work through the phases **P0 -> P7 strictly in order**. P0 is read-only: produce
> the confirmation note and stop for my review before touching code. For each
> subsequent phase: implement it, add the tests named in that phase, build, and run
> those tests plus the relevant existing suites (`db_test`, `recovery_test`,
> `fault_injection_test`, `pre_l0_test`); do not start the next phase until the
> current one builds clean and its tests pass. The whole feature is gated behind
> `enable_pre_l0`, and behaves as the old code when
> `pre_l0_wal_snapshot_threshold_bytes == 0`, so it must be able to land
> incrementally without regressing existing behavior. Report at the end of each
> phase: what changed (files + functions), what tests were added, and their result.
> Flag any deviation from the Target design before implementing it — especially the
> WAL-number ordering constraint noted in P0/P3.

## P0 — Confirm the integration points (read-only)

> Read `db/pre_l0/pre_l0_manager.{h,cc}`, `db/db_impl.cc`
> (`CompactMemTable`, `RemoveObsoleteFiles`, `MakeRoomForWrite`, `RecoverLogFile`,
> `Open`), `db/log_writer.h`, `db/write_batch_internal.h`, `db/version_edit.h`,
> `db/version_set.{h,cc}` (`LogNumber`, `SetLogNumber`, `NewFileNumber`,
> `LastSequence`, `LogAndApply`), and `include/leveldb/options.h`. Confirm and
> report, with line numbers: (a) exactly where the active WAL is created/rotated
> and how `logfile_number_` is assigned; (b) how `versions_->LogNumber()` gates
> WAL deletion in `RemoveObsoleteFiles`; (c) the API to write a log record
> (`log::Writer::AddRecord`) and to build a single-op `WriteBatch` with a chosen
> sequence (`WriteBatchInternal::SetSequence`, `WriteBatch::Put/Delete`); (d) that
> `PreL0Manager::NewIterator()` yields internal keys `(user_key, seq, type)` incl.
> tombstones and owns its data. Do not modify anything. Produce a short note
> confirming the "Target design" above fits, and flag any mismatch (esp. WAL
> rotation ordering vs. `snapshot_num` allocation).

## P1 — Configurable WAL threshold + hysteresis (options)

> In `include/leveldb/options.h`, next to the other `pre_l0_*` options, add
> `uint64_t pre_l0_wal_snapshot_threshold_bytes` (bytes; `0` = disabled; pick and
> document a default, e.g. 2GB) and `double pre_l0_wal_snapshot_release_ratio`
> (default `0.5`, range `(0,1]`). Follow how existing `pre_l0_*` options are
> declared/defaulted/threaded through `Open`. These are configured at DB open
> only — read once from `Options` when the DB is opened and held immutable for the
> lifetime of the DB. Do NOT add any runtime-mutation path. Add a unit test
> covering defaults and bounds.

## P2 — Live WAL byte accounting + trigger

> In `DBImpl`, maintain a running total of live WAL bytes = sum of sizes of WAL
> files with `number >= versions_->LogNumber()`. Reuse existing per-WAL tracking
> (`wal_create_times_` and the size computation in `RemoveObsoleteFiles`); update
> the total when a WAL is created, appended, and deleted. Do NOT stat the
> filesystem on the write hot path. Evaluate the threshold only at WAL-rotation
> boundaries (where a new WAL is created in `MakeRoomForWrite`) or a low-frequency
> background check. When the total crosses `pre_l0_wal_snapshot_threshold_bytes`
> (and the feature is enabled), schedule exactly one snapshot job; do not schedule
> another until the total falls below `threshold * pre_l0_wal_snapshot_release_ratio`
> (hysteresis). Guard against concurrent/duplicate jobs. Unit-test the trigger by
> driving the counter across the threshold and asserting a single trigger with
> hysteresis respected.

## P3 — Snapshot cut + non-blocking WAL rotation

> Implement the snapshot entry point (a `PreL0Manager` method invoked from
> `DBImpl` under `db_mutex_`, mirroring how `CompactMemTable` calls into
> `pre_l0_`). Under `db_mutex_`: capture `S = versions_->LastSequence()`; allocate
> `snapshot_num = versions_->NewFileNumber()`; then rotate the active WAL using the
> existing `mem_ -> imm_` swap path (as in `MakeRoomForWrite`, which allocates the
> next number `> snapshot_num`) so subsequent writes go to the new WAL unblocked.
> Reuse that path rather than duplicating it, and do NOT do a pure WAL-file rotation
> that leaves `mem_` in place — that would strand `mem_`'s bytes in a WAL below the
> floor (see Target design step 2). If an `imm_` is already pending, let it drain
> (or defer this cycle). Record `imm_log_number_` for the clamp in P5.
> Do not hold locks across the file I/O in P4 longer than necessary — the actual
> snapshot bytes are produced from the `OwningVectorIterator` (which already
> copied data out), so foreground reads/writes stay unblocked. Add a concurrency
> test: drive writes while snapshotting and assert the snapshot content equals
> exactly the resident state at `seq <= S`.

## P4 — Snapshot writer in log format

> Write the captured entries to a snapshot file readable by the stock recovery
> path. Obtain entries from `pre_l0_->NewIterator()`; for each internal key with
> `seq <= S`, build a single-op `WriteBatch` (`WriteBatchInternal::SetSequence(seq)`,
> then `WriteBatch::Put(user_key, value)` for `kTypeValue` or `WriteBatch::Delete`
> for `kTypeDeletion` — preserve tombstones) and append it with `log::Writer::AddRecord`.
> Only the latest version per key is emitted (the iterator already guarantees one
> entry per key). Write to a temp filename, `fsync` the file and the directory,
> then atomically `rename` to `LogFileName(dbname_, snapshot_num)`. Add a test that
> replays the produced file through `RecoverLogFile` (or decodes it with the log
> reader + `WriteBatchInternal`) and verifies key set, values, tombstones, and
> exact sequence numbers.

## P5 — Commit + repoint retention + delete superseded WALs

> After the snapshot file is durable: (1) in `PreL0Manager`, repoint retention —
> set every `KeyLoc.latest_log = snapshot_num` and rebuild
> `log_refcount_ = { snapshot_num : hashmap_.size() }` so `MinLogNumber()` returns
> `snapshot_num` (keep the `DebugCheckRefcountInvariant` passing); (2) in `DBImpl`,
> compute the **clamped floor** `new_floor = min(snapshot_num, imm_log_number_)`
> (use `snapshot_num` when no `imm_` is pending) and `LogAndApply` a `VersionEdit`
> with `SetLogNumber(new_floor)` as the atomic commit point — this clamp is the
> correctness guarantee that no un-flushed memtable's WAL is ever deleted; (3) call
> `RemoveObsoleteFiles`, which will delete every WAL with `number < new_floor`.
> Ensure ordering: file durable -> repoint -> LogAndApply -> delete. Never delete
> the freshly-rotated active WAL (number `> snapshot_num`) or the retained
> `imm_log_number_` WAL. Do NOT implement the "fold `imm_`" optimization here —
> leave a TODO; the clamp is sufficient and always safe. Add crash-injection tests
> (reuse
> `fault_injection_test`/`recovery_test` style) at each boundary: crash before
> `LogAndApply` recovers from old WALs; crash after `LogAndApply` but before
> deletion recovers from the snapshot and ignores superseded WALs; both yield the
> identical, correct state with no double-apply.

## P6 — Recovery + retire per-key pairing reliance

> Verify recovery needs no special-casing: because the snapshot is a numbered log
> file `>= LogNumber`, `RecoverLogFile` replays it (rebuilding Pre-L0 via
> `FlushMemTable`) followed by newer WALs, applied by sequence so later seqs win
> and duplicates are no-ops. Add recovery tests: snapshot-only; snapshot + trailing
> WAL; tombstone survives; interleave with keys already in L0 (no resurrection).
> Confirm the old per-key WAL-number retention still functions but is now driven by
> the snapshot floor (post-snapshot `MinLogNumber() == snapshot_num`); document
> that `KeyLoc.latest_log` now tracks the snapshot rather than the original write
> WAL.

## P7 — End-to-end + regression + metrics

> Integration tests: (a) drive writes until the WAL threshold triggers a snapshot,
> then kill/restart at randomized points and assert the recovered state is exactly
> correct; (b) assert total live WAL bytes drop sharply after a snapshot (the
> compaction reclaims space — mirror the ~4GB->~100MB expectation on a scaled-down
> workload, e.g. many updates to a small hot-key set); (c) assert foreground
> read/write latency is not stalled during snapshotting; (d) assert changing
> `pre_l0_wal_snapshot_threshold_bytes` changes trigger timing. Emit a metrics CSV
> (alongside the existing `wal_gap.csv`) reporting pre/post live WAL bytes,
> snapshot duration, and entries written. Run the full existing test suite
> (`db_test`, `recovery_test`, `fault_injection_test`, `pre_l0_test`) and fix any
> regressions.

---

## Notes / open items

- WAL rotation vs. number allocation ordering is the one subtlety: `snapshot_num`
  must sort BELOW the WAL that receives concurrent writes but ABOVE the superseded
  WALs, so the delete step (`number < snapshot_num`) never removes the active WAL.
  Confirm in P0 and assert it in P3.
- Threshold is configured at DB open only (read once from `Options`, immutable
  for the DB lifetime); no runtime-mutation path.
- Feature is gated behind `enable_pre_l0`; when
  `pre_l0_wal_snapshot_threshold_bytes == 0` behavior is unchanged (old
  retain-until-referenced path), so this can land incrementally.