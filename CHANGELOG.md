# Changelog

All notable changes to the Keratin repo (the Keratin append-only log store and
the Stroma queue and stream state layer built on it) are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet. Earlier history predates this changelog.

## [Unreleased]

### Breaking changes

- `QueuePublishCommit` gains `buffered`, true when the publication completed
  once written rather than once synced. Struct literals need the field.

- A queue partition declared buffered holds a `buffered` marker file, which
  earlier builds refuse as an unexpected entry. Turn the mode off before
  downgrading.

- The `early-replication-experiment` feature is removed. The written-tail
  reads for replication (`scan_for_replication*`, `subscribe_written` and the
  Stroma `*_early` and `subscribe_owner_*written` reads) are always built.
  Callers decide when to use them.

- The two-lane queue adapters are gone. `read_owner_message_records*`,
  `read_owner_event_records*`, `queue_replication_next_offsets`,
  `partition_payloads_in_event_log`, `QueueLogLayout` and the handle
  accessors `message_lane_next` and `message_lane_epoch` are removed. Queues
  use `read_owner_queue_records*` and `queue_replication_next_offset`, and
  streams use their own record and cursor event reads. A queue follower frame
  that carries a record batch is refused. `QueueReplicationRetention` has one
  required method, `retained_next`. `RecoveryPairInspector::new` loses its
  `stream` flag and `RecoveryJournalIdentity` loses its `stream` field, since
  the seal history already carries the resource kind. A recovery stage keeps
  its staged log under `log/` and its intent header moves to a new version,
  so stages from earlier builds are refused.
  `stream_replication_next_offsets` refuses queue partitions.

- Queue recovery evidence, inspection and staging describe one log.
  `RecoveryQueueReplayEvidence` carries the retained queue log, the applied
  `next`, `required_next` and `checkpoint_next`, and drops the layout and
  message lane fields. Artifacts and summaries drop `message_head`.
  `RecoveryPairInspection` reports one `log` comparison and reference gaps
  name a `payload_offset`. `QueueRecoveryStageSpec` carries the source `log`,
  whose tail is the applied boundary, and the stage receipt reports one
  `next`. The recovery phase store semantics and the stage intent header
  move to new versions, so completed phases and stages from earlier builds
  are refused.

- Recovery evidence names its physical logs by resource kind.
  `RetainedHistoryIdentity` carries `logs: RetainedLogs`, either one queue
  log or a stream record log plus a cursor event log, and the variant is part
  of the history id. The id tag moves to `fibril-retained-history-v2`.
  `SealedReplicaFrontiers` is `{ request, history }` and bounds come from the
  history. The seal intent records one fencing epoch for a queue and two for
  a stream. `RecoveryReadSource` is `StreamRecords`, `Log` or `Snapshot`, and
  a queue history refuses `StreamRecords` instead of returning an empty page.
  Recovery inspection and replay accept queue histories only. History receipt
  and seal intent files use new headers, so seals from earlier builds are
  refused.

- Agreed queue checkpoints describe one log. Pins carry one epoch, the
  applied boundary, the payload floor and the physical log tail. Targets and
  contents carry `next` and `payload_head`, and the payload digest replaces
  the message digest. `queue_checkpoint_target` takes the minimum cut and the
  minimum payload floor. `QueueCheckpointActivity` reports one log head, tail
  and epoch. Retention, base and capsule files use a new header and the
  capsule digest tag moves to `stroma-queue-checkpoint-capsule-v2`, so files
  and receipts from earlier builds are refused.

- Stroma storage uses layout version 3. The learner checkpoint journal and
  the queue reseed completion record now hold single queue log offsets.
  Version 2 roots are refused with a clear error, with no migration. The
  queue role API speaks one log: `QueuePublishCommit` is `{ next, epoch }`,
  queue promotion and demotion report one log position, a stream promotes
  through its own `StreamPromotionOutcome`, and `QueueStateCheckpoint`
  replaces the owner export and learner install types. Queue follower apply
  takes one log batch, and queue log truncation is split from stream record
  truncation. Each of these refuses the other partition kind.

- Stroma storage uses layout version 2. Each partition lives in one
  directory, `partitions/<group>/<topic>/<part>/`, with a kind marker and a
  `current` pointer to its active generation. Recovery generations, staging
  and bookkeeping live inside the partition. Queues have no message log
  directory. Roots without the `stroma-layout` marker, or with unknown
  entries, are refused, with no migration. The recovery inspection cache is
  no longer inside the storage root.

- `install_follower_state_checkpoint` is removed. Queue checkpoints install
  only through the authorized learner path.

- Keratin refuses to open a log directory that has segments but no manifest,
  instead of starting it as an empty log.

- Recovery staging stores snapshot bytes in a separate resumable artifact file.
  Stage specifications bind the snapshot length as well as its digest. Previous
  private staging intents are incompatible and have no migration support.

- Work queues store payload-bearing enqueues and state events in one ordered
  log. Queue layout and unordered-application switches are removed. Existing
  storage roots without the single-log format marker require fresh storage,
  with no migration support. Streams retain separate payload and cursor logs.

- Recovery forks that share closed payload segments persist manifest version 3
  before linking. Older binaries reject participating logs; this implementation
  continues to read version 2. Unshared logs retain version 2. See
  [recovery segment reuse](experiments/RECOVERY_SEGMENT_REUSE.md).

### Added

- Written frontier. `Keratin::written_next` is the exclusive end of the
  records written through Keratin's buffers to the OS, where an `AfterFlush`
  append completes, and `watch_written_next` wakes a reader when it moves.
  Read after `durable_next` it is never lower, and it never passes
  `next_offset`. `durable_next` and every durable read, scan and recovery
  path are unchanged.

- Background sync. The log writer syncs records that are written but not
  durable once `background_sync_interval` has passed since the last sync of
  the log started. It keeps at most one such sync in flight, skips missed
  ticks instead of catching up, never runs on an idle or fully synced log,
  and completes waiting `AfterFsync` appends that it covers. A failure
  latches the log like any other failed sync. The interval is a target, not
  a bound on loss.

- `Keratin::sync_status` reports the written and durable frontiers, the
  durable lag in records, the time since the last successful sync, the
  applied background sync interval, whether a background sync is in flight,
  how many have started, and the storage failure that stopped the log.

- Stroma `queue_log_sync_status` reports that status for each log behind a
  queue partition, and `set_queue_background_sync_interval` changes their
  cadence through the live writer settings. Queue behavior is unchanged.

- `LogReader::scan_written_with_byte_budget` reads up to the written
  frontier instead of the durable one, from the tail cache or the segment
  files, with the usual offset, CRC and byte budget checks. A log that stops
  after a failed sync moves its written frontier back to the durable end at
  once, so written reads never serve records the next open cuts.

- Buffered queues in Stroma. `set_queue_buffered` declares a queue partition
  buffered. The declaration is stored as a `buffered` marker file in the
  partition directory, loaded when the queue log opens, and removed with the
  partition. `queue_is_buffered` reports it once the log is open.
  Owner publishes, acks, nacks, releases, discards and dead-letter markers of
  a buffered queue complete at `AfterFlush`, delivery and dead-letter payload
  reads use the written frontier, and the queue log does not expect every
  write synced. Writes that request `AfterFsync` explicitly keep it. Turning
  the mode off pauses owner operations, waits for those in flight and syncs
  the log before reads and writes move back to the durable frontier. Queues
  on the two-log layout and stream partitions are refused. Queues not
  declared buffered behave as before.

- Stroma `set_queue_background_sync_default` sets the background sync
  interval of every open queue log and of queue logs opened later, so an
  embedder can drive it from a runtime setting.

- The Stroma debug snapshot reports per queue whether it is buffered and,
  for open queue logs, the written and durable frontiers, durable lag, time
  since the last sync and the background sync interval.

- Unexpected background sync warning. A log with the new
  `expect_every_write_synced` setting declares that every append waits for
  an fsync. When a background sync on such a log covers records written by
  an `AfterWrite` or `AfterFlush` append that no sync had covered yet, the
  writer logs a warning with the log root, the number of those records and
  the time since the last sync, at most once a minute per log, and counts it
  in `SyncStatus::unexpected_background_syncs`. Waiting `AfterFsync`
  appends that the background sync covers do not count. The setting only
  applies while the log is in the owner role, records written as a follower
  never count, and sync behavior is unchanged. `SyncStatus` also reports
  `every_write_sync_expected`.

- Stroma sets `expect_every_write_synced` on queue logs whenever the event
  log default durability is `AfterFsync`, which makes every owner publish
  and settlement wait for an fsync. Streams opt in with
  `set_stream_writes_synced`, meant for tiers whose every append waits for
  an fsync. Role changes need no extra step, since Stroma already moves each
  physical log to the follower role with its partition.

- Live writer settings. `Keratin::writer_settings`,
  `replace_writer_settings` and the closure form `update_writer_settings`
  read and change one open log's default durability, batch limits, linger
  ceiling and fsync intervals, checked against a revision. A change applies
  in order with appends after the writer fsyncs everything written so far. A
  failed sync stops the log through the storage failure stop. Settings reset
  on reopen. Logs that never call these behave as before.

- `KDurability::AfterFlush` and Stroma's `Durability::AfterFlush`, an opt-in
  completion mode that acknowledges once a batch is written to the OS,
  without fsync. It is not durable across power loss until a later sync, does
  not advance the durable frontier, and ordinary scans do not see the record
  until then. A failed write stops the log through the same storage failure
  stop as a failed fsync. Follower retries under `AfterFlush` verify an
  overlap against written records byte for byte. The default stays
  `AfterFsync` and the other modes are unchanged.

- `Keratin::durable_next` returns the exclusive durable frontier, which is
  unambiguous where the inclusive `durable_offset` is not. `next_offset` is
  documented as counting staged records that may not be durable or readable
  yet. Stroma exposes the stream record log frontier as `stream_durable_next`.

- Persist completed local recovery inspection and comparison phases with exact
  source/configuration binding and a versioned recipe. Restored artifacts verify
  their snapshot and state digests. Namespace leases and bounded retirement keep
  inspection-cache cleanup separate from active or installed storage.
- Expose immutable completed queue-seal and stage receipts for lost-response
  retries. These receipts preserve the original evidence. Strict inspection and
  installation still validate the selected retained records.

- Optional batch timelines share one bounded collector and clock across log writers,
  with commit-to-fsync links, offset bounds, drop counters and emission timing.
  Instrumented and ordinary builds now use the same fsync coalescing loop.
  The diagnostic output changes from per-writer CSV to versioned JSON Lines.
  See [batch tracing](experiments/BATCH_TRACING.md).

- Add compaction retention constraints for queue replication and a separate
  replacement-generation path for admitted follower repair. Original storage stays
  available as recovery evidence until a durable, fully applied replacement covers
  both original log tails. A completion receipt resumes interrupted admission
  without resetting the installed copy.

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

- Segment rollover syncs only the outgoing segment's log data on the writer.
  The new segment's header, its directory entry and the sealed index sync
  with the next fsync, before any record in the new segment counts as
  durable. At 10k msg/s with three replicas on one drive, durable p99 went
  from about 75 ms to 28-33 ms with `KERATIN_BACKGROUND_MANIFEST=1` and to
  45-47 ms without it, and p50 from about 30 ms to 9 ms.

- Truncation moves the head in memory and removes the files on the manifest
  worker, after that log's outstanding manifest store. The caller hears back
  once the files are gone. A crash before then only keeps more history.

- Freed segments are kept as spares, up to 4 per log, and reused at
  rollover. Appends then overwrite existing blocks, so their data syncs skip
  the journal commit an extending write pays. A segment still linked from a
  frozen fork is never reused.

- The active index syncs at most every 500 ms. Recovery rebuilds a lost or
  short index from the log, and drops entries past the recovered end.

- Recovery stops a segment's scan at a record from below its base, which is
  how a reused file's older records end. A trailing segment whose header was
  never written, or still names an older segment, is removed. Readers stop a
  segment on an offset jump.

- The periodic snapshot runs every 8 to 12 s instead of exactly every 10 s,
  so replicas stop compacting at the same moment.

- `KeratinConfig` and `WriterSettings` gain `background_sync_interval`,
  100 ms by default and adjustable live. Zero turns the background sync off
  and leaves written records to the operating system with no bound on how
  long they stay unsynced. With the default, logs that complete appends at
  `AfterFlush`, or write `AfterWrite` records to the OS, now become durable
  about one interval later without an explicit `sync`. Struct literals that
  list every field need the new one.

- `KeratinConfig` and `WriterSettings` gain `expect_every_write_synced`, off
  by default and adjustable live, and `SyncStatus` gains
  `every_write_sync_expected` and `unexpected_background_syncs`. Struct
  literals that list every field need the new ones.

- Queue checkpoints sync the log up to the captured boundary before the
  snapshot becomes recovery authority: the local periodic checkpoint, the
  agreed checkpoint pin and the owner checkpoint exported to learners. This
  only costs a sync when applied records are not durable yet, as on a
  buffered queue.

- `queue_replication_next_offset` syncs a follower log that holds records
  not yet durable and then answers, instead of refusing. The answer is still
  durable progress.

- Frozen recovery scans use buffered sequential reads and reuse bounded record
  scratch space while retaining CRC, offset and full-history verification.
  Varied-size records, buffer boundaries and post-scan corruption are covered
  by regression tests.

### Fixed

- An epoch advance, a frozen fork and a retained head advance sync every
  written record before they proceed, also when no append waits for an
  fsync. Records written by `AfterFlush` appends were left unsynced, so a
  recovery seal of a log holding them refused with "retained scan exceeds
  durable history".

- A log that stops after a failed sync records a cut back to the end of its
  last successful sync (`latched-cut.pending`), and the next open carries it
  out before anything reads the tail. Linux reports a writeback error once,
  so the reopen scan or a later sync could otherwise accept records whose
  pages never reached the disk. Only records that were never confirmed as
  durable are dropped, which matches a crash for every durability tier. Logs
  bound to a replicated history are cut the same way: confirms, follower
  progress and recovery seals all lie at or below the last successful sync.

- A command sent to a log while or after it shuts down always gets an
  answer. One that reached the writer channel after the writer handled
  `Shutdown` could wait forever, since a bounded crossbeam channel keeps its
  buffered messages while a sender exists. The writer refuses every later
  command with "log is shut down" (or the latched storage failure) until the
  last handle drops, and `Keratin` methods fail at once after `shutdown`.

- A dirty reopen, a forced recovery scan and `open_preserving_history` sync
  the active segment, its index and the segments directory before they
  publish the recovered tail as the durable frontier. Records written but not
  synced before a crash were treated as durable after reopen, so a follower
  could resume past them and an `AfterFsync` retry could return
  `AlreadyPresent` for data that power loss would still drop. If that sync
  fails the open fails. A clean reopen is unchanged. A reopen after a latched
  sync failure is still not fully healed, see the storage failure notes in
  the README.

- `stream_offset_at_or_after_time` no longer starts past staged stream records
  that are not written yet. A missing probe at or above the durable frontier
  counts as newer, so a by-time start at the live edge cannot skip records a
  broker already delivers from memory. The `io-fault-injection` feature gains
  `Fault::WritePause` to hold a segment write in tests.

- Log, recovery and cache directory locks are POSIX record locks owned by the
  process, managed by the new `keratin_log::lock` module. A child process
  spawned while a lock was held could keep it looking held after release,
  which made opens fail with "Resource temporarily unavailable". Lock files
  are opened only through that module. `lock_existing_log` returns a
  `FileLock`, and `create_log_lock_file` prepares a log lock file.

- Avoid scanning every TTL range on each enqueue. Deadline updates notify the
  coalescing expiry worker, keeping fragmented TTL publication and recovery replay
  independent of a growing per-message minimum search. Expiry selection still
  computes the exact earliest deadline.

- Single-log queues can share storage with streams. Stream reads, cursor replay,
  retention, follower application and recovery seals retain their separate logs.

- The single-log queue experiment rejects forward payload references before
  owner or follower append, including cancellation directives. Rejected frames
  leave the apply fence clean unless an earlier frame already mutated an open
  replication group.
- Direct NACK, inflight-release and discard paths now settle their metadata log
  positions before advancing ordered application. Their live settled frontier
  matches follower replay and cold recovery.

- Ordered queue application wakes only ranges that can proceed at the current
  frontier. Reset and failure still wake all invalidated waiters, and cancellation
  removes its registration. This prevents concurrent publishers from creating a
  wake-up storm that can stall delivery and heartbeat processing under saturation.

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
