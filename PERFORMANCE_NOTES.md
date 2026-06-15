# Keratin Performance Notes

Status: investigative

Date: 2026-06-14

These notes track performance baselines and optimization ideas. Do not commit
performance-focused implementation changes unless a before/after run shows the
change is actually faster for the target workload.

## Current Baseline

Machine context:

- `/home/george/code/keratin` is on ext4 over `/dev/sdb4`
- `/tmp` is tmpfs
- Results below are quick local runs, not a rigorous benchmark suite

### Existing Binaries

| Command | Storage | Result |
| --- | --- | ---: |
| `keratin_test` | repo disk | passed basic write/read sanity |
| `keratin_bench` | repo disk | `338,608 msg/s` |
| `keratin_bench_auto_batch` | repo disk | `234,358 msg/s` |

`keratin_bench_auto_batch` also emitted a normal-looking drop-time shutdown
notification warning. That warning is likely log-noise from polling the shutdown
oneshot before the writer responds.

### Configurable Throughput Utility

Added local utility: `keratin-log/src/bin/keratin_throughput_bench.rs`

Representative config:

```text
--messages 1000000
--payload 1024
--max-batch-records 4096
--max-batch-mb 8
--fsync-ms 20
--linger-ms 20
--flush-mb 32
--durability fsync
```

| Mode | Producers | Storage | Result |
| --- | ---: | --- | ---: |
| batch | 8 | tmpfs | `1,368,087 msg/s` |
| batch | 8 | repo disk | `352,980 msg/s` |
| enqueue | 8 | tmpfs | `461,972 msg/s` |
| enqueue | 8 | repo disk | `218,044 msg/s` |
| batch | 1 | tmpfs | `344,052 msg/s` |
| enqueue | 1 | tmpfs | `341,455 msg/s` |

Interpretation:

- The disk path is a large limiter on this machine.
- The enqueue/completion path is the main target workload because it is closer
  to how Fibril feeds Keratin: work is accepted, later completed, and callers
  observe confirmation.
- Direct batched appends can exceed 1M msg/s on tmpfs. Treat that as a useful
  upper-bound/reference path, not the primary optimization target.
- Enqueue currently tops out around `460k-470k msg/s` on tmpfs for this quick
  run.
- One producer underfeeds the writer or spends too much time constructing work.
  Multiple producers are needed to approach the current ceiling.
- Larger `max_batch_records=8192`, `max_batch_mb=12`, `fsync_ms=25`, and
  `linger_ms=25` did not improve the tested tmpfs runs.
- Larger enqueue confirmation windows did not improve the tested tmpfs runs.
- Writer-path benchmarks should use longer runs when comparing changes. Short
  `1M` runs were useful for fast iteration but hid important batching behavior.
- Isolated Keratin throughput is not enough to accept a writer change. Compare
  promising writer changes against Fibril's steady-state benchmark too,
  especially around the latency knee.

### Configurable Open Utility

Added local utility: `keratin-log/src/bin/keratin_open_bench.rs`

| Records | Payload | Segment size | Segments | Storage | Clean reopen |
| ---: | ---: | ---: | ---: | --- | ---: |
| 200k | 1KB | 16MB | 14 | tmpfs | `76.169 ms` |
| 1M | 1KB | 16MB | 82 | tmpfs | `297.440 ms` |
| 1M | 1KB | 16MB | 82 | tmpfs | `1.000 ms` after clean-fast-open |
| 1M | 1KB | 16MB | 82 | tmpfs | `293.590 ms` with `--force-recovery-scan` |

Interpretation:

- Clean-fast-open removes the scan after an orderly shutdown.
- `force_recovery_scan=true` keeps the full scan/repair path available.
- This is primarily a user-visible startup latency issue, not steady-state
  throughput.
- It still matters for sparse workloads with many queues, and for restart or
  failover paths.

### Configurable Read Utility

Added local utility: `keratin-log/src/bin/keratin_read_bench.rs`

Representative config:

```text
--messages 1000000
--payload 1024
--batch 4096
--page 4096
--segment-mb 16
--fetches 100000
```

| Records | Payload | Segment size | Segments | Storage | Sequential scan | Sparse fetch |
| ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 1M | 1KB | 16MB | 82 | tmpfs | `2,847,153 msg/s` after cursor scan | `29,556 fetch/s` |
| 1M | 1KB | 16MB | 82 | repo disk, warm cache | `3,352,117 msg/s` after cursor scan | `28,815 fetch/s` |
| 500k | 1KB | 1MB | 651 | tmpfs | `2,633,570 msg/s` after cursor scan | `33,250 fetch/s` after bounded segment lookup |

Interpretation:

- The reader API is intentionally synchronous. Higher layers can wrap larger
  read loops in `spawn_blocking` where appropriate, without forcing Keratin to
  expose an async facade over sync filesystem calls.
- Sequential scan is already much faster than point lookup in this quick run.
- The repo-disk read result is page-cache-warm because the benchmark writes the
  dataset and immediately reads it back. A colder read benchmark needs a
  read-only mode over a prebuilt dataset, or explicit OS cache control.
- Sparse fetch currently opens the index/log path and seeks per call. Treat it
  as an optimization candidate only if message inspection or replication starts
  depending on many isolated point reads.
- For higher-level inspection paths, prefer filtering the target offsets first
  and then reading the smallest useful ranges. Many isolated point fetches are
  usually the wrong shape when the caller can ask state for relevant offsets up
  front.

## Candidate Improvements

### 1. Clean-Shutdown Fast Open

Current behavior:

- `Log::open` discovers segments and scans every segment after dirty shutdown,
  forced recovery scan, or incompatible manifest metadata.
- The scan is required after crash or dirty shutdown.
- After a known clean shutdown, the manifest carries enough state to skip full
  recovery scanning.

Possible direction:

- Store a clean/dirty flag in the manifest. Implemented with the existing
  manifest header flags field.
- On open, mark the manifest dirty before returning a writable handle.
- On clean shutdown, force-store final `next_offset`, active base, head, epoch,
  and clean=true.
- On next open, if clean=true and manifest/config invariants match, trust the
  manifest and skip full segment scan.
- If clean=false, manifest is missing, validation fails, or
  `force_recovery_scan=true`, use the current full scan path.

Tests needed:

- Clean shutdown manifest lifecycle is covered.
- Forced recovery scan repairing a corrupted tail after clean shutdown is
  covered.
- Dirty crash recovery tests still cover truncated tails.
- Clean fast path refusing obviously inconsistent metadata still needs a focused
  test.

### 2. Writer Pipeline

Current behavior:

- The writer loop receives work, batches it, encodes records into the write
  buffer, writes/flushed buffers, fsyncs when due, then notifies completions.
- Encoding and write/fsync scheduling are serial in one writer thread.
- `Log` currently owns both logical append state and file handles. That is the
  main obstacle to a real pipeline.

Ownership split:

- Record encoding itself is stateless.
- Append staging is not stateless. It assigns offsets, decides segment rolls,
  emits sparse index entries, and advances logical watermarks.
- The safe split is to extract an ordered logical planner from `Log`, not to
  make the whole log stateless.
- The planner owns:
  - next offset
  - active segment base
  - scheduled active segment byte position, including bytes not written yet
  - last sparse index position
  - segment size and index stride rules
- The file writer owns:
  - segment and index file handles
  - actual `write_all`, `flush`, segment creation, and manifest persistence
- The fsync gate owns:
  - the last file-write fence known to be durable
  - completion release for `AfterFsync` requests only after the matching bytes
    have been written and synced

Possible direction:

- Split the pipeline into ordered stages:
  1. batch/admission stage groups `AppendReq`s and preserves caller order
  2. planner/encode stage assigns offsets, emits segment-roll commands, encoded
     log bytes, encoded index bytes, and completion fences
  3. file writer stage applies those commands in order
  4. fsync stage receives durable fences when a flush/fsync boundary is due
  5. notifier stage completes callers after the required durability point

Risks:

- Segment rolling and index emission must stay single-writer ordered.
- Fsync acknowledgements must correspond exactly to bytes already written. A
  fence can be completed only after the writer has applied all commands through
  that fence and the fsync stage has synced the relevant files.
- The writer cannot let an encoder get ahead in ways that force invalid segment
  decisions.
- Error handling must fail the right completions without losing later work.
- Duplicating file handles for a separate fsync thread needs care. It may be
  safer to first build a two-stage pipeline, then split fsync once write fences
  are explicit and tested.
- `AfterWrite` currently means accepted by the writer path, not necessarily
  kernel-flushed to disk. Before a pipeline rewrite, decide whether to preserve
  that behavior or strengthen it to mean file-write applied. Pre-0.1 lets us
  change this, but the contract should be explicit.

First refactor target:

- Introduce a private logical append planner with no file handles.
- Keep the existing single writer loop at first.
- Make the current `Log` call the planner internally and apply the resulting
  commands immediately.
- Benchmark this no-pipeline refactor. If it regresses, revert before adding
  threads.
- Once neutral, move planner/encode work behind a channel and keep file
  write/fsync together until fences are explicit and covered by tests.

Result:

- A private append-planning helper was extracted inside `Log`. It owns no file
  handles and writes encoded records plus sparse index entries into caller-owned
  buffers.
- The first shape regressed direct enqueue from the prior `482,374 msg/s`
  sample to roughly `453k msg/s`. Forced inlining of the planner helpers
  recovered the loss, with follow-up samples at `491,775 msg/s` and
  `475,508 msg/s`.
- Treat this as a boundary refactor, not a throughput win. The key value is
  that planner and file-handle responsibilities are now easier to separate
  without adding threads yet.

Static linger experiment:

- The old adaptive linger loop appears to defeat configured linger under the
  isolated enqueue workload. With `--messages 5000000`, `--fsync-ms 20`,
  `--linger-ms 20`, and tmpfs storage, static configured linger measured about
  `906k-908k msg/s`. The adaptive shape measured about `446k msg/s` on the
  same benchmark shape. Static linger also measured `974,070 msg/s` on a later
  `10M` run.
- The IO log showed much smaller adaptive batches, roughly `10k-12k` records
  per batch versus roughly `25k-31k` for the static shape.
- Small fsync windows were not inherently a writer scheduling problem on tmpfs:
  one `10M` static-linger run measured `1,018,540 msg/s` at `fsync-ms=1`,
  and another measured `966,478 msg/s` at `fsync-ms=20`. Tmpfs reports near
  zero fsync time, so these numbers are mainly about batching and scheduling.
- Disk-backed durability still pays for aggressive fsync. On the repo disk with
  `2M` messages, `fsync-ms=1` measured `213,084 msg/s`, `fsync-ms=5` measured
  `242,288 msg/s`, and `fsync-ms=20` measured `287,633 msg/s`. Treat this as
  the expected operational tradeoff: smaller fsync windows reduce potential
  loss after a crash but spend more time syncing.
- Despite the isolated win, static linger was not kept as a global writer
  change. Fibril's top-level `throughput-1k` steady-state benchmark regressed
  in the low and medium rate cases: at 250k/s p95 publish-to-deliver latency
  moved from `17ms` adaptive to `41ms` static, and at 350k/s from `800ms` to
  `958ms`. Static was roughly tied at 400k/s and modestly better only in the
  already-backlogged 500k/s case.
- The next useful direction is not a blunt static/adaptive linger swap. It is a
  measured pipeline experiment with stage overlap instrumentation, plus payload
  size sweeps.

Fibril comparison against the benchmark docs:

- The June 7 docs table remains directionally useful, but current branch runs
  should not be assumed to match it exactly. In the adaptive comparison run,
  250k/s still looked close to the docs, but 350k/s was much worse
  (`592/800/840ms` publish-to-deliver p50/p95/p99 instead of
  `79/114/122ms`). That could be current branch behavior, environment variance,
  storage state, or benchmark noise. Do not update the public docs from this
  single WIP-branch run.

Observability needed for real pipelining:

- The current `KERATIN IO` line reports cumulative serial work per writer loop:
  encode, log write, index, fsync, manifest, bytes, and records per batch.
- That is enough to see where serial time goes, but it cannot prove overlap
  because the current design has no overlapping stages.
- When a pipeline is introduced, add per-stage interval metrics around:
  admission/batch wait, planner/encode, file write, fsync, and notify release.
  Treat each channel item as the measured work unit. Each item should carry a
  stable work id or fence id across stages, and each stage should emit
  `(id, stage, start, end, bytes, records)` to a collector.
- The collector should sit off the hot path. A bounded crossbeam channel and a
  dedicated collector thread are a reasonable first shape. If the collector
  falls behind, prefer dropping or sampling instrumentation records over
  blocking the writer pipeline being measured.
- The benchmark can then report stage utilization and adjacent-stage overlap by
  matching ids between consecutive stages. That is more useful than only
  comparing aggregate stage totals.
- Keep these metrics optional or sampled. Per-message timing on the hot path is
  too expensive for the question we need answered.
- Test pipeline experiments with larger payloads too. Tiny messages mainly
  stress scheduling, batching, and channel overhead. Encoding and copy overlap
  should matter more with larger payloads, so small-message wins or losses are
  not enough to accept or reject a pipeline design.

Current writer instrumentation plan:

- Add stage timing behind a compile-time `writer-stage-trace` feature. Default
  builds must not compile or execute tracing work in the writer hot path.
- When enabled, write CSV trace output by setting
  `KERATIN_WRITER_STAGE_TRACE=/path/to/trace.csv`.
- Treat each channel item or flushed batch as the measured work unit. Assign a
  stable id that can survive across future pipeline stages.
- Emit stage intervals as `(id, stage, start, end, bytes, records)` to an
  off-path collector. The collector can use a bounded channel and may drop
  samples rather than blocking the writer being measured.
- Keep the first implementation small: current serial writer intervals are a
  baseline for future overlap checks, not the pipeline itself.
- Acceptance gate for instrumentation:
  - default build still passes `keratin-log` checks and tests
  - default-build isolated enqueue benchmark stays within noise
  - feature-enabled tracing is usable for experiments, but not a production
    default
- Acceptance gate for future pipeline changes:
  - isolated Keratin benchmark must show the intended stage overlap or
    throughput effect
  - Fibril steady-state benchmark must not regress low-rate latency or the
    practical latency knee
  - payload sweeps must include larger messages because encode/copy overlap may
    matter more there than in 1KB runs

Instrumentation validation result:

- Default `keratin-log` checks and tests passed after adding the feature-gated
  tracing path.
- Feature-enabled `keratin-log` checks and tests passed.
- A traced smoke run wrote CSV rows like
  `id,stage,start_ns,end_ns,duration_ns,records,bytes`. The 100k-message smoke
  produced about 200k rows because the current writer often stages one
  single-message unit plus one post-stage interval per id.
- Default-build isolated enqueue samples with tracing disabled measured
  `460,082 msg/s` and `474,371 msg/s`, matching the old adaptive-linger range
  closely enough for this local check.
- With the trace feature compiled in but `KERATIN_WRITER_STAGE_TRACE` unset, a
  longer 1M-message enqueue sample measured `585,339 msg/s`. This is a useful
  sanity check that the compiled-in feature path stays close to normal
  variation when the collector is disabled.
- With CSV tracing enabled, the matching 1M-message sample measured
  `493,217 msg/s` and wrote about 2M rows, 95MB. That overhead is acceptable for
  local diagnostics, but it is too invasive for normal benchmark comparisons.
- Fibril's default-build baseline steady-state sweep also looked normal:
  50k/s measured `14/20/23/58ms` publish-to-deliver p50/p95/p99/max, and
  150k/s measured `12/16/24/58ms`, with zero missing messages and zero publish
  errors.
- The trace feature is suitable for local experiments. Do not enable it in
  production-like performance runs unless the goal is specifically to collect
  timing intervals.

Correctness tests needed before splitting fsync:

- Torn or corrupted active-segment tail truncates cleanly, then later good
  writes continue from the repaired offset. A regression test now covers CRC
  corruption in the tail record, then appending after repair.
- Corruption around a segment roll either repairs the old segment tail or moves
  to a new segment without exposing an ambiguous middle state. A regression
  test now covers garbage appended to the latest segment after crossing at
  least one segment boundary, then appending after repair.
- Manifest metadata cannot claim durable offsets past what recovery can prove.
  This found a real bug: full recovery scan used
  `computed_next.max(manifest.next_offset)`, which allowed a clean manifest to
  keep an offset past the verified tail. Full scan now trusts the verified scan
  result.
- If a write-stage command fails, only the affected completions fail and later
  accepted work is either not written or is recovered from a clear durable
  boundary.
- A true mid-write failure test still needs an explicit fault-injection seam in
  the writer or segment layer. Filesystem tricks like chmod or unlinking are not
  reliable once the segment file is already open.
- The future fsync stage must release `AfterFsync` completions only for fences
  whose bytes were already written and whose relevant files were synced.
- The future write stage should emit explicit durable fences. A fence should
  identify the ordered write boundary, the highest covered offset, and the
  segment/index files that must be synced.
- The fsync stage must not infer durability from planned bytes. It can only
  advance a fence after the file writer confirms all commands through that fence
  were applied in order.
- Segment roll and manifest updates need their own fence rules. A completion
  that depends on a newly created segment cannot be released until the log file,
  index file, manifest, and required parent directory syncs have reached the
  agreed durability point.
- `AfterWrite` and `AfterFsync` contracts should be rechecked before splitting
  fsync. Pre-0.1 lets us tighten the contract, but the public meaning should be
  explicit before the pipeline is built around it.

Benchmark target:

- Improve enqueue tmpfs from roughly `460k-470k msg/s` toward `500k-600k msg/s`
  without reducing correctness. This is the main throughput target for Fibril.
- Improve disk-backed enqueue only if the disk is not already the dominant cap.

### 3. Append Path Allocation and Repeated Accounting

Current behavior:

- `stage_reqs` computes payload byte totals.
- `stage_append_batch` recomputes estimated bytes and stats totals.
- Encoding loops over records again.

Possible direction:

- Carry precomputed byte totals from batcher/stage into the log append path.
- Avoid repeated `bytes_len()` passes where it does not improve safety.
- Measure carefully. The disk path is fsync-limited here, but tmpfs enqueue
  numbers suggest CPU/accounting overhead matters.

Investigation note:

- Removing unused per-entry accounting fields from `BatcherCore` was tested as
  a tiny enqueue-path cleanup.
- Results were not stable enough to justify keeping it: one run improved, later
  sequential runs regressed.
- Reusing the already-computed `estimated` byte total for `Log` stats was also
  tested and reverted. Enqueue throughput dropped from `468,169 msg/s` to
  roughly `422k msg/s` in two sequential runs. This is counterintuitive, but the
  measurement was clear enough to avoid keeping the change.
- Pending fsync detection was changed from scanning the pending ack queue to an
  O(1) emptiness check. This is valid because `AfterWrite` completions are
  answered immediately and only `AfterFsync` completions enter `pending`.
  Enqueue throughput moved from `468,169 msg/s` to `471,858 msg/s` and
  `477,139 msg/s` in two post-change runs.
- A direct oneshot completion path was added for enqueue callers that do not
  need a boxed completion trait object. In the throughput utility,
  `--mode enqueue` now uses the direct path, while `--mode enqueue-boxed` keeps
  the old boxed path. With the same fsync durability settings, direct enqueue
  measured `482,374 msg/s` and boxed enqueue measured `459,155 msg/s`.
- Batching `AfterWrite` notifications until the end of `stage_reqs` was tested
  and reverted. It reduced notifier channel sends, but delayed immediate
  completions enough to lower write-durability enqueue throughput from
  `447,207 msg/s` to `407,554 msg/s`.
- Coalescing flushed single-message `AppendReq`s into one `stage_append_batch`
  call was tested and reverted. A fresh baseline after rebuild was
  `460,156 msg/s`; the coalesced shape measured `491,462`, `450,933`, and
  `468,951 msg/s`. Reusing scratch vectors did not make it stable, with samples
  at `457,544` and `441,875 msg/s`. The added completion-splitting complexity
  is not justified by these results.
- The batcher currently has more API surface than active use justifies. Consider
  either removing it or simplifying it around the concrete writer-loop use case
  before spending more time on micro-optimizations inside it.

### 4. Reader Path Cleanup

Current behavior:

- `find_segment_base` uses a bounded `BTreeMap` lookup.
- `scan_forward_exact` drains from the front of a `Vec` per decoded record.
- `fetch` and `scan_from` open index/log files per call.

Possible direction:

- Segment lookup was changed from reverse key iteration to
  `range(..=offset).next_back()`. On the 651-segment sparse fetch run, this
  improved point fetch from `32,467 fetch/s` to `33,160 fetch/s`. This is a
  modest cleanup, not a major bottleneck removal.
- Repeated front-drain was replaced with a cursor and compact-on-read. On the
  82-segment sequential scan run, this improved scan from `2,103,122 msg/s` to
  `2,847,153 msg/s`. On the 651-segment run, scan improved from
  `2,044,344 msg/s` to `2,633,570 msg/s`.
- Consider lightweight per-reader segment/index caching for sequential scans.
- Consider a sequential read-ahead mode that keeps the next segment or next slab
  in flight while the caller decodes the current one. This should be opt-in or
  tied to a streaming scan API, because it can waste I/O for small inspections
  and random access.
- Add a cold-read benchmark mode before optimizing disk read-ahead.

### 5. Shutdown Warning Noise

Current behavior:

- `Drop` waits briefly for the writer shutdown notification.
- Normal "not ready yet" polling no longer emits a warning.

Possible direction:

- Keep warnings for closed notification channels and timeout only.
- Not performance-sensitive, but cleaner for benchmarks and operator logs.

### 6. Configuration Builder

Current behavior:

- `KeratinConfig` is a public struct with many direct literal initializers in
  tests and benchmark binaries.
- Adding one setting requires a broad initializer sweep.

Possible direction:

- Add a small builder or fluent helpers for common settings.
- Keep `Default` for simple setup, but make benchmark and application configs
  easier to evolve.
- Do this as an API cleanup, not as part of a specific performance experiment.
