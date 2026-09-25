# Changelog

All notable changes to the Keratin repo (the Keratin append-only log store and
the Stroma queue and stream state layer built on it) are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet. Earlier history predates this changelog.

## [Unreleased]

### Breaking changes

- Recovery forks that share closed payload segments persist manifest version 3
  before linking. Older binaries reject participating logs; this implementation
  continues to read version 2. Unshared logs retain version 2. See
  [recovery segment reuse](experiments/RECOVERY_SEGMENT_REUSE.md).

### Added

- Recovery limit errors carry bounded numeric diagnostics for inspection, replay,
  snapshot output and staging budgets. Reports identify accepted work and refused
  work without payloads. The limits and recovery authority remain unchanged.

- Read-only, bounded node storage accounting distinguishes active files,
  agreed-checkpoint artifacts, inactive generations, recovery staging and other
  files. Unix counts allocated blocks and deduplicates hard links. Traversal runs
  on a blocking worker with one process-wide permit and never admits a queue.

- Added non-materializing checkpoint activity hints (log ranges and per-open local
  append content bytes). Hints are approximate scheduling/diagnostic values and
  grant no durability or history authority.

- Durable queue checkpoint pins and canonical same-cut capsules with bounded
  replay/physical verification, compaction retention and atomic replacement.
  Externally authorized acceptance persists a covering restart snapshot before
  exact logical log floors; sealed recovery can use the accepted capsule plus
  retained suffix. Missing capsules use ordinary verified snapshots, while corrupt
  evidence is refused. Publication boundaries have fault and SIGKILL tests.
- Strict indexed cursors for externally retained durable prefixes and frozen
  logical ranges. Every retained record still requires CRC and contiguous offsets;
  sparse indexes are seek hints and bounded scan costs include skipped records.

- Frozen-log forks with private active tails and metadata, immutable closed-segment
  sharing on Unix, and copy fallback when linking is unavailable. Repair and
  unclean-open truncation privatize shared files before mutation. Stroma recovery
  reuses exact, independently verified local payloads and completed stages while
  retaining the existing seal, selected snapshot and quorum-admission boundaries.

- Bounded sequential frozen-log cursors and tentative sealed-history inspection
  sessions. CRC, offset and final canonical-digest verification remain mandatory;
  errors poison cursors, cancellation retains owned I/O guards, and admission is
  separate from strict read admission. Existing strict recovery reads and the
  storage format are unchanged.

- Opt-in `KeratinConfig::adaptive_staging` and reusable, caller-maintained buffers
  with lazy growth, configurable empty-buffer decay and full idle release.
  Retained staging remains the default; durable completion and the on-disk format
  are unchanged. See `experiments/ADAPTIVE_STAGING.md` for policy and tradeoffs.

- Externally authorized non-voting queue learner preparation, exact-history
  checkpoint installation and durable/applied readiness checks. Partial learners
  resume across restart; older local histories are sealed and retained while a
  separate learner generation becomes active. Checkpoint journals bind their
  storage history, and ordinary bound-replica resets remain blocked. Linux tests
  cover interrupted checkpoint backfill and learner route publication under SIGKILL.

- Initial-history preparation can resume pristine, never-admitted storage after
  fresh external consensus authorization. Durable preparation intent precedes
  log creation; renewed receipts retain history IDs and bind the new storage
  instance. Admission persists a marker that prevents later empty preparation.

- Startup `KeratinConfig.writer_buffer_factor` controls the eagerly allocated
  writer input and notification channels: 64 slots per factor unit, range
  1–128, default 128 (8,192 slots each). Smaller factors apply backpressure
  earlier; fsync pipeline capacity remains independent.

- Bounded, payload-free replication conflict diagnostics: recent control
  operations, record identities and offset context help reconstruct overlap
  failures without retaining message bodies. Diagnostics do not alter repair
  policy or add ordinary append-path history recording.

### Changed

- Frozen recovery scans use buffered sequential reads and reuse bounded record
  scratch space while retaining CRC, offset and full-history verification.
  Varied-size records, buffer boundaries and post-scan corruption are covered
  by regression tests.

### Fixed

- Checkpoint reset drains outstanding fsync completions before replacing log
  files and resetting durability. An older completion can no longer republish
  its previous frontier after the reset or overwrite the new manifest boundary.

- Fsync jobs retain an exclusive durable boundary. An empty job can no longer
  persist a manifest claiming offset zero exists or mark a later offset-zero
  append durable. Regression coverage includes delayed completion after a new
  append and strict recovery of empty logs.

- Remote checkpoint resets must match the writer's exact epoch. Stroma validates
  both source epochs before resetting either log, and each writer checks again
  in command order. Stale or future checkpoints cannot replace fenced history.
  The multi-log/state installation is not yet interruption-safe as a whole.

- A queue publish routed at a plexus stream partition is refused before
  anything is appended. It used to append the message and a queue-kind
  enqueue event durably and only then fail the in-memory apply, leaving
  events in the stream's log that every later recovery tripped over - the
  partition could never open again. Recovery now also skips such
  wrong-kind events with an error log instead of refusing the partition,
  so logs poisoned by older brokers heal on the next boot (the skipped
  events were never applied or confirmed to any producer).
- The per-second IO stats line is a tracing event instead of a raw print,
  so embedders can filter it. It prints once a second per active writer,
  which adds up fast with many partitions.

- A stray directory in a topic's on-disk tree no longer refuses to open the
  whole storage engine. The partition scan used to fail on any name that
  did not parse as a partition number - including the `.trash-` leftovers a
  destroy that crashed mid-delete leaves behind. The scan now finishes
  interrupted destroys and skips unknown directories with a warning,
  leaving them in place for a human.

### Added

- Stream durable cursors are enumerable: `cursors_snapshot()` on the stream
  state, a `ListCursors` stream command, `cursors()` on the queue handle's
  stream view, and `stream_cursors(tp, part)` on Stroma - all (name, offset)
  pairs, name-sorted. Feeds the admin dashboard's per-stream subscriber
  view.
- A `min_fsync_interval_ms` config floor on group-commit cadence, for storage
  where per-fsync cost dominates (consumer SATA class). Default `0` keeps the
  self-clocking behavior.
- Parallel durable publish (Stroma): the message-log and event-log fsyncs
  overlap instead of serializing (append the enqueue off the message staging
  offset, confirm and deliver only when both logs are durable), roughly halving
  the single-node durable publish latency. A message fsync failure annihilates
  the durable enqueue with a new `CancelEnqueueMany` event so live and recovered
  state stay consistent (including dropping a not-yet-fired delayed enqueue).
  Both immediate and delayed publishes take this path. Event-log appends are
  serialized per partition so a crash can never strand a confirmed publish behind
  a non-durable one.
- Adaptive fsync fusion (Keratin writer): when recent commits are small
  (fsync-count-bound) the writer pipelines several and the fsync worker coalesces
  them into one fdatasync, lifting small-batch durable throughput several-fold
  without touching latency. Fat, bandwidth-bound commits keep a single fsync in
  flight. The pipeline depth and the small-vs-fat threshold are configurable via
  `max_inflight_fsyncs` and `pipeline_commit_records`.
- Optional segment preallocation (`segment_preallocate_bytes`, off by default):
  preallocate space ahead of the write cursor so durable fsyncs hit
  already-allocated blocks in place instead of extending the file, which cuts the
  low-load durable-publish latency floor on consumer NVMe. The durable watermark
  is published as an unambiguous exclusive frontier so reads stop cleanly at the
  durable end and never touch the preallocated padding.
- A per-queue disk-use breakdown (`estimate_disk_used_breakdown` on Stroma):
  each queue's on-disk footprint split into message-log and event-log bytes,
  from the same walk as the existing total. Feeds the Fibril dashboard's
  storage breakdown. Covers every partition on disk: unloaded (evicted)
  queues are measured straight from their directories without waking them,
  so their bytes stay visible - this also corrects the pre-existing total,
  which only counted loaded queues.

### Changed

- Self-clocking group commit: a staged append commits as soon as the fsync
  worker is idle instead of waiting out the fsync interval tick, so the
  durability latency floor is no longer interval-bound. The interval remains
  the ceiling while an fsync is in flight.
- Recovery folds the event log to its net state (an enqueue annihilated by a
  later cancel is dropped) and auto-truncates a dangling forward reference (the
  expected, always-unconfirmed artifact of the parallel-append path) instead of
  quarantining the partition. A genuinely corrupt record still follows the
  mismatch policy (quarantine by default).
- Ack tracking uses a settled `RangeSet` instead of a bounded bitset, removing
  the ack-window size limit.
- Work-queue and stream partition handles are split at the type level, so a
  stream partition cannot be driven through queue-only operations.

### Fixed

- A rare deadlock between the writer thread and the fsync worker under
  saturated storage. The interval-due commit path could push fsync requests
  past the bounded pipeline and park the writer in the send, while the fused
  fsync drain could owe more completions than the equally bounded done channel
  accepts - each thread then waited on the other forever, wedging every
  subsequent append (publishes stalled until restart while the control plane
  stayed healthy). Scheduled commits now respect the pipeline capacity as a
  hard cap, and every fsync handoff drains completions until a slot is free,
  so neither side can ever block the other. Reproduced and verified with the
  new `wedge_stress` example under drive saturation.
- The partition kind marker write used one shared temp file, so concurrent
  identical stream declares raced renames and failed spuriously. The temp
  name is unique per writer and a matching marker short-circuits.
