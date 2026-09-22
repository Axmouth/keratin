# Staging capacity and residency experiment

Build with `staging-buffer-experiment` (forwarded through Stroma and Fibril).
This adds a capacity observer to reusable buffers only in the experiment build.
`RUST_LOG=warn,keratin_buffer=info` records initial capacity and subsequent
reserve, edit-completion and maintenance changes. Lines include the log path,
write/index identity, current and historical peak bytes, and growth/shrink/release
counts. No payloads are logged. Initial allocations are separate from growth counts.

Set ordinary `storage.keratin.adaptive_staging = true`, then select:

- `KERATIN_BUFFER_EXPERIMENT_MODE=small-retained`: eager configured floors,
  ordinary Vec growth and no maintenance.
- `adaptive-eager-reallocate`: adaptive retention, eager configured floors,
  ordinary Vec reallocation.
- `adaptive-lazy-reallocate`: adaptive retention, empty initial vectors,
  ordinary Vec reallocation.
- `adaptive-lazy-replace`: adaptive retention, empty initial vectors,
  replacement on empty-buffer resize (Fibril's adopted opt-in policy).
- `normal` or unset: normal configured policy with capacity observation.

The four variants permit adjacent comparisons of retention, initial allocation
and resize strategy. The experimental mode is fixed on first log initialization
for the process. An unknown mode fails fast. Ordinary builds contain neither
these switches nor observer fields/checks.

`cargo test -p keratin-log --lib --features staging-buffer-experiment` checks the
observer as well as buffer/log behavior. The Linux full-broker screen used sixteen
queues, 1 MiB payloads, three nine-message bursts and 25/75-second idle gaps, in
forward/reverse policy order. Current capacity is distinct from RSS and from the
sum of per-buffer historical peaks; observations alone do not establish allocator
fragmentation or page-retention costs. Raw capacity logs, scripts and results are
preserved in the external handoff's `integration/staging-residency-20260922/`.
