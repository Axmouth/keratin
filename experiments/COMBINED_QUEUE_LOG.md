# Combined queue log experiment

This worktree is adopting a single log for every work queue. Queue storage has
no layout feature or environment switch. Ordered application is mandatory.
Existing roots without the single-log format marker are rejected. There is no
migration from the previous two-log queue format. Streams keep separate payload
and cursor logs within new roots.

The adoption branch is still under validation and has not been merged into main.
A pinned pre-adoption binary retains both layouts for benchmark controls.

Original enqueues contain their headers and payload. Their event offsets identify
the payload. Subsequent activation, lease and settlement events reference those
positions. Delivery scans and filters requested offsets. Queue actor application
is ordered, and non-payload positions are settled internally. Retention includes
live payloads, the persisted snapshot replay boundary and existing pins.

Combined queues have one physical writer. The optional separate message log is
absent. `QueueHandleInner::msg_log` returns a Result, with Unsupported for combined
queues. Physical lifecycle operations enumerate actual logs. The compatibility
wire message lane has cursor zero and takes its fencing epoch from the event log.
It never carries records. Nonzero cursors or malformed empty-lane frames fail.
The event writer inherits the message-sized storage profile. Metadata-only logs
retain their original profile. No byte/count or durability cap is relaxed.

## Early replication

Compile the `early-replication-experiment` feature. Fibril forwards it through
its facade. Set `FIBRIL_EXPERIMENT_EARLY_REPLICATION=1` to
opt in on admitted, history-bound replicated queues. Leave it unset or zero for
replication after owner fsync. This is a process-start experiment setting, with
no dashboard control. It does not enable speculative consumer delivery.

Early reads may use written-but-not-yet-durable tail-cache bytes. Cache misses
retain the durable disk path. Owner completion and state application still wait
for owner fsync. Broker confirmations also wait for the required replica durable
frontier. A follower's earlier successful persistence cannot substitute for a
failed owner fsync. Followers can retain an unconfirmed suffix after that failure.
Reconciliation and availability of that suffix remain acceptance work, not an
implied promise of the prototype.

Tests gate a real owner fsync while a follower stages, fsyncs and applies its
record. They check offset zero and an established prefix, successful owner sync,
returned sync failure and error-after-success. Owner confirmation/application
remain blocked until success, and later appends cannot quietly reuse a failed
writer. The injected gate is test-only and absent from the benchmark release.

## Remaining gates

Combined queues support sealed-history inspection, transfer, staged installation,
agreed checkpoints and excluded-replica learner backfill. Recovery preserves live
payloads older than a snapshot's replay boundary. Owner and follower paths reject
forward payload references before append. Interrupted installation, learner
restart, reseeding and broker recovery have focused acceptance coverage.

Queues and streams can share one storage instance. Streams retain separate
payload and cursor logs, their metadata-sized event configuration and independent
replication frontiers. Mixed-root tests cover payload reads, queue compaction,
stream retention, cursor replication, cold reopen and stream recovery seals.
Broker tests cover stream catch-up, promotion and confirmations with both queue
layouts, including stream assignments that have not materialized local storage.

Remaining adoption gates include matched release-mode performance and failover
timing, larger retained histories and live backlogs, clustered repair after early
replication leaves an unconfirmed suffix, and conversion of the remaining old-layout test fixtures. Keep the old layout in a pinned benchmark control. Adoption changes the
queue storage format without migration support, while preserving stream storage.
Early replication remains opt-in and outside the dashboard.

The event codec still decodes and re-encodes payloads in replication and copies
the nested body during delivery. Those are separate optimization candidates.
Follower actor-apply metadata releases its copied bodies after storage encoding.
The experimental delivery scan-size override defaults to 1 MiB. Preallocation,
segment sizes, batching limits and durability settings keep their prior defaults.

## Recovery format groundwork

Retained-history evidence now carries an explicit QueueLogLayout. Separate is
the omitted serialization default, preserving existing separate-history identity
bytes. Combined evidence names its layout, requires a bound queue history and a
canonical empty compatibility lane, while streams retain the separate-log discriminator. Pair inspection rejects mixed layouts before fetching pages.
Recovery reads and installation use the selected physical payload log. Combined
checkpoints require exact authorized learner installation when backfilling old
payloads, and promotion remains fenced until those dependencies are verified.
