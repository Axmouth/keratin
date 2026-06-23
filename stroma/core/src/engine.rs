//! Substrate / engine seam.
//!
//! The storage substrate (`Stroma` plus `QueueHandleInner`) owns everything that
//! is the same regardless of channel semantics: the per-partition message log and
//! event log, durable event append, replication, recovery replay, snapshot file
//! IO, and the per-partition control actor plumbing. What differs per channel
//! type is the in-memory state machine those events drive, and how that state is
//! snapshotted, restored, and reset. That part is an engine.
//!
//! The work queue (`QueueInternalState`: ready/inflight/ack/nack/DLQ/TTL) is the
//! first engine. The stream engine (durable cursors plus retention, for Plexus
//! fan-out channels) is the second. Each engine runs in its own control actor
//! with its own command vocabulary, so the per-command apply is deliberately not
//! part of this trait. The trait is only the contract the substrate needs to host
//! an engine generically: which kind it is, and how to persist, restore, and
//! reset its in-memory state. The substrate dispatch that routes a partition to
//! its engine by kind arrives with the stream engine.

use crate::state::SnapshotMeta;

/// Which engine owns a partition. Durable channel types map onto this. The work
/// queue is the default so existing partitions and snapshots keep their meaning
/// with no migration.
//
// The stream engine (step 2) is the first reader of the kind discriminant and the
// non-Queue variant, so the helpers are unused until then.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionKind {
    /// RabbitMQ-style work queue. A message is delivered, leased, and on ack it
    /// is gone (consumed=gone).
    #[default]
    Queue,
    /// Plexus fan-out stream. Every consumer sees every record and position is a
    /// durable named cursor rather than consume-and-delete.
    Stream,
}

#[allow(dead_code)]
impl PartitionKind {
    /// Stable on-disk and on-wire discriminant.
    pub fn as_u8(self) -> u8 {
        match self {
            PartitionKind::Queue => 0,
            PartitionKind::Stream => 1,
        }
    }

    /// Inverse of [`PartitionKind::as_u8`]. Unknown values return `None` so a
    /// reader can reject a record from a newer writer rather than guess.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(PartitionKind::Queue),
            1 => Some(PartitionKind::Stream),
            _ => None,
        }
    }
}

/// The substrate-facing contract every per-partition engine satisfies.
///
/// The substrate drives durability, replication, recovery, and snapshot file IO
/// the same way for any engine. Where it needs the engine is at the points that
/// depend on what the in-memory state actually is: writing a snapshot of that
/// state, loading one back, and resetting it. Identity (`kind`) lets the
/// substrate pick the right engine for a partition. Per-command application is
/// not here because each engine owns its own command vocabulary and actor.
//
// Implemented by the work queue today; the stream engine is the second
// implementor and brings the substrate dispatch that routes partitions by kind.
#[allow(dead_code)]
pub trait PartitionEngine: Send + 'static {
    /// Which channel type this engine implements.
    fn kind(&self) -> PartitionKind;

    /// Serialize the current in-memory state, tagging it with the event-log
    /// offset it reflects so recovery knows where to resume replay.
    fn encode_snapshot(&self, last_event_offset: u64) -> Vec<u8>;

    /// Restore state from a snapshot blob produced by `encode_snapshot`,
    /// returning the snapshot metadata (notably the offset it reflects).
    fn load_snapshot(&mut self, bytes: &[u8]) -> std::io::Result<SnapshotMeta>;

    /// Drop all in-memory state back to empty (used by ResetQueue and before a
    /// snapshot load).
    fn reset(&mut self);
}

impl PartitionEngine for crate::state::QueueInternalState {
    fn kind(&self) -> PartitionKind {
        PartitionKind::Queue
    }

    fn encode_snapshot(&self, last_event_offset: u64) -> Vec<u8> {
        crate::state::QueueInternalState::encode_snapshot(self, last_event_offset)
    }

    fn load_snapshot(&mut self, bytes: &[u8]) -> std::io::Result<SnapshotMeta> {
        crate::state::QueueInternalState::load_snapshot(self, bytes)
    }

    fn reset(&mut self) {
        crate::state::QueueInternalState::reset(self)
    }
}
