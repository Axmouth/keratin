# Stroma

Stroma is the broker storage and state layer built on top of Keratin. It is
the internal core of Fibril. It turns Keratin logs into queue and stream
partitions with durable state, and it owns how that state is recovered.

## Crates

* `stroma/core` (`stroma-core`): the storage engine.
* `stroma/common` (`stroma-common`): small shared value types such as
  `Partition` and `Offset`. It depends only on `serde`, so network clients
  can use these types without pulling in the engine.

## What stroma-core does

* **Partition storage.** A work queue keeps payload-bearing enqueues and state
  events in one ordered Keratin log. A stream keeps a payload log and a cursor
  event log. Each partition lives in its own directory with an active
  generation, as described in `core/src/stroma/layout.rs`.
* **Queue state.** Events are applied in log order to a state machine that
  tracks ready, in-flight, acknowledged and dead-lettered messages.
* **Snapshots and checkpoints.** Periodic snapshots bound replay at restart.
  Agreed checkpoints give replicas a common cut that an outside coordinator
  can certify, see [agreed queue checkpoints](../docs/AGREED_QUEUE_CHECKPOINTS.md).
* **Retention.** Compaction truncates a log only below the payload floor, the
  applied snapshot, protected followers and retained checkpoint pins.
* **Replication support.** Owner reads, follower apply and the owner, follower
  and frozen roles that Fibril assigns.
* **Recovery.** Sealing a replica's history, inspecting and replaying it,
  staging a verified copy and installing it as a new generation, see
  [recovery segment reuse](../docs/RECOVERY_SEGMENT_REUSE.md).

## Role in the system

* **Keratin** handles persistence of individual logs.
* **Stroma** defines partition storage, state and recovery on top of them.
* **Fibril** builds broker semantics, networking and coordination on top.

Consensus and authority decisions stay in Fibril. Stroma checks the evidence it
is given but cannot establish authority itself.

## Naming

Named after **stroma**, the supportive structure in biological systems that provides organization and context for functional components.

Here, it represents the structure that organizes and governs system state.

## Status

Closely tied to Fibril's development. The on-disk layout is versioned, and
roots from older layouts are refused rather than migrated.
