# Changelog

All notable changes to the Keratin repo (the Keratin append-only log store and
the Stroma queue and stream state layer built on it) are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet. Earlier history predates this changelog.

## [Unreleased]

### Added

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
