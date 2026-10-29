# Agreed queue checkpoint storage

Stroma supplies durable local material for externally coordinated queue checkpoints.
Fibril's opt-in coordinator gathers every admitted replica's receipt and publishes
one consensus certificate. These storage APIs cannot establish that authority.

## Durable ordering

1. `begin_queue_checkpoint_pin` captures a fully applied exclusive event boundary
   and a conservative retained message range. A separately encoded, lease-normalized
   base is synced before its pin enters the checksum-protected retention index.
   Ordinary snapshots may advance. Compaction clamps both logs to durable pins.
2. The coordinator chooses an owner cut at or beyond every base. A short local
   owner-operation pause chooses the cut; it never waits for another replica.
   `build_queue_checkpoint_capsule` replays the pinned event suffix and physically
   verifies the retained payload range to that fixed cut. It releases the application
   lock during scanning while retaining the lifecycle/retention guard.
3. Capsule content binds the canonical snapshot, state, full retained payloads and
   live payload identities. A local capsule also binds the exact storage history,
   epochs and replay base. Its digest differs between replicas with different bases;
   the common content must match. File and directory sync precede its receipt.
4. `accept_queue_checkpoint` requires externally verified current consensus authority
   and the exact local capsule digest. It persists a covering ordinary restart
   snapshot, then the accepted retention record, then the event and message logical
   floors. A crash can leave additional retained records. Replacement requires the
   previous certificate and monotonic boundaries; callers wait for every local
   installation before starting another candidate.
5. Sealing can use the accepted capsule in `recovery.snapshot`; the sealed receipt
   selects that file and binds its exact envelope digest. Retained logs still pass
   full CRC, continuity and canonical-digest verification. Missing compatible
   capsules use ordinary verified snapshots; malformed evidence fails closed.

## Retention and bounds

There are at most two indexed pins: an accepted checkpoint and a candidate. Base
and capsule metadata have bounded encodings and CRCs; abandoned canonical files
are reclaimed after index publication. A pending pin protects old live payloads and
payloads awaiting later enqueue events. Every offset field names its own log space;
zero is a valid exclusive event boundary.

Default build limits are one million records, 256 MiB physical scan bytes and five
seconds of elapsed work, checked between records and after encoding. A filesystem
operation may itself take longer. Byte accounting includes records scanned before
the requested head when an index hint cannot land exactly there. Missing/damaged
index headers fall back to a strict physical scan within the same budget.

`Keratin::advance_retained_head` requires an externally durable covering checkpoint
and the exact epoch. It drains preceding writer work and persists the logical floor
before physical reclamation. Whole sealed segments can be deleted; discarded bytes
inside the active segment remain allocated until a later roll and compaction.
Indexed retained cursors verify every retained record while skipping unused prefix
bytes. Bound-history cold reopening still verifies physical segments before repair.

Pins survive restart. Cancelling a caller does not cancel an already owned storage
operation or release its guards early. Reconciliation of abandoned pins requires
fresh external authority; an accepted checkpoint also needs a covering ordinary
snapshot before its special retention obligation can be dropped. The last accepted
base and its suffix can grow while replacement is delayed. Disk/age policies and
reclamation of older installed recovery generations are separate work.

## Validation

Storage tests cover unequal replay bases, later application during replay, old live
payloads, missing dependencies, exact zero, budgets, corrupt metadata, snapshot and
compaction races, replacement and cold restart. Fault retries and real SIGKILL cover
base, pin, capsule, restart snapshot, accepted index and both logical-floor publication
boundaries. Native tests cover indexed reads, missing indexes, physical CRC failures,
logical heads inside segments, restart and retention without resurrecting a prefix.
Fibril integration tests cover unanimous agreement, partial installation, repeated
metadata/broker restarts and confirmed post-checkpoint suffix recovery.
