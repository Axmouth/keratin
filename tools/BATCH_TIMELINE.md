# Repeatable batch timelines

Render same-host broker captures into aligned staging, buffered write, sync-worker
queue, fsync and durable-publication lanes. This complements `batch_trace.py`,
which exports raw single-process events to Chrome/Perfetto without correlating
storage batches. Both read the same capture format. The timeline uses only the
Python standard library and loads pinned D3 7.9.0 when the page opens.

```sh
python3 tools/batch_timeline.py cases.json batch-overlap.html
```

Open `batch-overlap.html` in a browser. The command also writes an inline HTML
fragment and a capture audit beside it. Change the case, payload offset window
or log using the selectors. Hover for intervals, fsync substeps and offset
ranges. Click to retain the closest operation in the detail line.

A minimal `cases.json` follows. Trace globs resolve relative to that file.
Node labels follow the harness's zero-based `node-N` directories. Node 0 is the
owner. Each selected topic needs one active payload writer and event writer per
node, under `node-N/data/{messages,events}/TOPIC/`.

```json
{
  "clock": "same-host",
  "topic": "topic1",
  "payload_bytes": 1024,
  "nodes": ["Owner · SATA", "Follower 1 · NVMe", "Follower 2 · NVMe"],
  "windows": [{"first": 600, "count": 200}, {"first": 800, "count": 100}],
  "cases": [{"id": "control", "name": "Control", "traces": "captures/control-*.jsonl"}]
}
```

The tool expects version-1 captures with zero dropped events and a matching
emission count. A missing final close marker normally stops rendering. If the
broker was stopped with SIGTERM after workload settlement, use
`--allow-open-capture` to inspect the observed intervals. This prints a warning
and preserves the open-capture status in the audit and chart. It does not prove
that the entire process lifetime was captured.

Choose windows that are fully covered by completed durability groups on all
nodes. Keep windows near batch boundaries. A window starts at the first owner
staging batch whose first offset lies inside it. The event-log view uses that
payload window through all-node payload completion plus 1 ms. Later ACK work
may lie outside this window. Empty, missing or unlinked work is rejected.
Choose fewer windows if the interactive fragment exceeds 1 MB.

## Reading the lanes

- Each process has a monotonic trace clock and a wall-clock anchor. Integer
  nanoseconds are rebased before conversion to milliseconds. This provides
  approximate same-host alignment. It does not establish network causality or
  justify aligning unsynchronised hosts.
- Stage and Write can overlap because writes occur within staging. Durations
  across lanes must not be added together.
- Queued starts at the explicit commit/sync enqueue marker and ends at physical
  fsync start. It includes the handoff and worker wait.
- Fsync includes the log and index sync calls. Hover shows their separate times.
- Durable is a log-frontier publication, before any unmeasured actor, protocol
  reply or consumer delivery work. It is not a publish confirmation timestamp.
- O1, O2 and F1.1 are local display labels for physical durability groups. They
  restart in each window and log. Equal colours across nodes do not establish a
  shared batch. Half-open offset ranges identify the covered records within the
  same log history.

Worker groups are connected with explicit `fsync_member` parent IDs. A direct
follower group requires same-ID fsync and `replicated_done`. When sync runs in a
worker, `replicated_done` only indicates staging completion. The renderer waits
for `durable_published` to mark durability. Staging-to-group association uses
same-writer offset coverage and time ordering, so the renderer rejects multiple
active writer incarnations, observed operation errors and history changes after
the selected work starts. Bootstrap history before the last history-control
marker is discarded before associating operations. Use the raw Perfetto exporter for recovery or failure traces.

There are no inferred transport, batch-collection, actor-application or
client-delivery spans. The renderer is a focused normal-operation harness tool.
It is not yet a general distributed tracing system. Use clean untraced runs for
performance comparisons because tracing can change timing and batch boundaries.
