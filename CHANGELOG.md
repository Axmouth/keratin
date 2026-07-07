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
  state stay consistent. Immediate publishes take this path. Delayed publishes
  keep the serial path for now.
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

- The partition kind marker write used one shared temp file, so concurrent
  identical stream declares raced renames and failed spuriously. The temp
  name is unique per writer and a matching marker short-circuits.
