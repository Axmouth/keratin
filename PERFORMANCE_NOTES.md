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

Possible direction:

- Split the pipeline into ordered stages:
  1. encode stage builds immutable encoded batches and records completion fences
  2. writer stage appends encoded bytes to log/index files in order
  3. fsync stage receives durable fences when a flush/fsync boundary is due
  4. notifier stage completes callers after the required durability point

Risks:

- Segment rolling and index emission must stay single-writer ordered.
- Fsync acknowledgements must correspond exactly to bytes already written.
- The writer cannot let an encoder get ahead in ways that force invalid segment
  decisions.
- Error handling must fail the right completions without losing later work.

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
- Batching `AfterWrite` notifications until the end of `stage_reqs` was tested
  and reverted. It reduced notifier channel sends, but delayed immediate
  completions enough to lower write-durability enqueue throughput from
  `447,207 msg/s` to `407,554 msg/s`.
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
