# Batch tracing

Keratin can capture staging, buffer writes, commit preparation, physical fsync
jobs and durable-frontier publication on a shared process timeline. This is
optional diagnostic instrumentation. It changes no record, header or disk format.

Build with `keratin-log/writer-stage-trace` in an embedding workspace, or
`--features writer-stage-trace` when building this package. Set
`KERATIN_WRITER_STAGE_TRACE` to a new output file. `%p` expands to the process ID.
The feature adds observation to the normal worker loops, including normal fsync
fusion. With the variable absent, observation is disabled. Ordinary builds omit
the instrumentation entirely. The environment is read when each writer starts.
Changing it does not toggle already-running writers.

The dedicated collector works in release builds even when an embedding binary
uses `tracing/release_max_level_info`. Runtime TRACE targets can be added later
once release filtering is addressed across the embedding workspace.

## Capture and inspect

From the repository root, choose persistent output paths outside the benchmark's
storage directory. Both directories in this example must be fresh.

```sh
cargo build --release -p keratin-log --features writer-stage-trace --example batch_trace
KERATIN_WRITER_STAGE_TRACE=/path/to/capture-%p.jsonl \
  KERATIN_TRACE_MEASURE_EMISSION=1 \
  target/release/examples/batch_trace /path/to/fresh-data 131072
# Use the same binary with tracing unset to verify every persisted record.
target/release/examples/batch_trace /path/to/fresh-data 131072 --validate
python3 tools/batch_trace.py /path/to/capture-PID.jsonl /path/to/timeline.json
```

The example runs two independent logs concurrently. Its labels are illustrative,
and it does not model the broker's payload/event dependency. The converter emits
Chrome trace-event JSON with separate writer, fsync and notifier lanes and
explicit commit-to-fsync flow links. The converter handles one process per file.
It does not infer cross-process clock alignment.

Call `flush_writer_stage_trace()` from a control thread to flush a capture while
writers remain live. After stopping all writers, call
`finish_writer_stage_trace()` to drain and close the collector. It returns
`WouldBlock` while writer threads still hold it. The example handles this brief
shutdown handoff. Captures also flush periodically every 250 ms. A killed process
can lose buffered diagnostics. A capture without a final closed summary is
incomplete.

## Reading the events

The process header supplies a monotonic-clock origin and a wall-clock anchor.
Writer registrations identify the log path and its opening epoch. IDs are unique
across all logs within that process capture. Offset fields use half-open bounds.
A commit's `[0, next)` describes its requested durability frontier, including
already durable records. It does not assert that the entire prefix remains
retained on disk.

| Event | Meaning |
| --- | --- |
| `stage_begin`, `stage` | A collected batch enters and completes staging, with its assigned offset range |
| `encode` | An append request's encoding work inside that batch |
| `log_write`, `index_write` | Buffered log and index writes |
| `commit_begin`, `commit_prepare` | Preparing a commit, including any wait for fsync capacity |
| `commit_queued`, `sync_queued` | A normal commit or explicit sync is about to enter the worker channel |
| `fsync_member` | A commit belongs to the physical group identified by `parent` |
| `fsync_begin`, `fsync` | A coalesced job begins and completes syncing its log and index files |
| `fsync_success`, `fsync_error` | The physical group's result |
| `completion_received` | The writer receives that commit's completion |
| `manifest_write` | A periodic manifest write during completion handling |
| `durable_published` | The writer publishes the successful durable frontier |
| `notify` | The notifier processes a batch of completion callbacks |
| `history_control` | An epoch change, checkpoint reset or suffix repair is being attempted |
| `retention` | Writer-thread prefix retention, including its synchronous durability barrier and any segment deletion |

Staging IDs can also identify a directly following commit. Other commits get
fresh IDs, with offset bounds available for inspection. Multiple commits can map
to one physical fsync group. The converter only creates arrows from explicit
relationships. Offset-only correlation must account for history-control events,
log reopenings and epoch changes. Opening epochs are registration metadata, not
a claim that a writer's epoch never changes.

The `notify` operation has a separate ID and is currently an independent timing
span. Broker publish IDs, payload/event dependencies, network transfers, follower
application phases and delivery correlation remain follow-up work. The current
`replicated_append` span is an aggregate operation. It does not expose all of its
internal stages. Record counts on queue/group events count requests, while the
`stage` event counts newly staged records.

A failed operation can have a completed duration span. Use the corresponding
error marker and normal error logs when interpreting it. An unmatched begin can
also mean interrupted work. Rollovers can perform synchronous durability work
inside staging, which is included in `stage` but is not separately broken down.

## Measurement costs and completeness

A single bounded collector serves every log in the process. Producer events use
fixed-size metadata. Serialization and file writes happen on the collector
thread. A full channel drops diagnostic events rather than blocking storage.
Summary rows report drops, and the converter warns about incomplete captures.
The destination is opened exclusively, so an existing file is never truncated.
Use a distinct path per process and per run.

`KERATIN_TRACE_MEASURE_EMISSION=1` adds timing around bounded-channel emission
and accumulates total and maximum emission nanoseconds. This measures enqueue
and accounting work, excluding event construction, other instrumentation calls,
serialization and collector I/O. That timing mode adds its own overhead.
Summaries during activity are concurrent snapshots. The closed summary is final.

Compare tracing-off and tracing-on runs of the same executable. Add a separate
run with emission measurement enabled if useful. Keep workloads and storage
placement matched and record collector CPU and file volume when assessing cost.
Prefer a different device for diagnostics. Timing the emission call alone cannot
measure scheduler interference or changes in subsequent batching, so its cost
cannot simply be subtracted from observed latency.

The output is versioned JSON Lines. It replaces the earlier per-writer CSV trace.
Earlier CSV consumers need updating to the new schema.

## Combined-candidate substeps

The immediate-staging path also traces its bounded buffer flushes. Physical
worker jobs and direct follower fsyncs expose `log_fsync` and `index_fsync` under
the encompassing operation ID. Follower application exposes `replicated_stage`,
`replicated_flush`, buffer writes, direct fsync and `replicated_done` with the
requested offset range. All observations wrap the existing operation bodies.
`manifest_write` measures the periodic manifest handoff when background manifests
are enabled. It does not measure the separate manifest worker's disk I/O.

Broker captures terminated by SIGTERM lack a final closed summary. Preserve that
incomplete status, verify periodic drop counters and completed operation links,
and restrict conclusions to observed work. Cross-process wall-clock anchors can
illustrate approximate overlap on one host, but do not establish RPC causality.

## Broker batch overlap view

For normal-operation captures from the same-host broker harness, use the
[batch timeline renderer](../tools/BATCH_TIMELINE.md). A case manifest selects
trace files, node labels and payload offset windows. One command generates an
interactive lane view plus a capture audit, with separate controls for runs,
windows and payload/event logs. The raw Perfetto converter remains useful for
arbitrary logs, failure traces and detailed event inspection.
