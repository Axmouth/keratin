//! Replication data types and owner-read helpers for the Stroma engine.
//!
//! Clustering-module separation: the owner/follower replication batch + read
//! types, the apply/checkpoint outcome structs, and the owner-read gap/checkpoint
//! helpers, lifted out of stroma.rs. Re-exported from `stroma` so existing
//! `stroma_core::` and `crate::stroma::` paths keep resolving.

use keratin_log::{KDurability, Keratin, Message, ReplicatedAppendOutcome};

use crate::event::StromaEvent;
use crate::state::SnapshotMeta;
use crate::{Offset, Result, StromaError};

#[derive(Debug)]
pub struct ReplicatedMessageBatch {
    pub epoch: u64,
    pub first_offset: Offset,
    pub records: Vec<Message>,
    pub durability: Option<KDurability>,
}

#[derive(Debug)]
pub struct ReplicatedEventBatch {
    pub epoch: u64,
    pub first_offset: Offset,
    pub events: Vec<StromaEvent>,
    pub durability: Option<KDurability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReplicationBatch<T> {
    pub epoch: u64,
    pub requested_offset: Offset,
    pub next_offset: Offset,
    pub records: Vec<(Offset, T)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerReplicationRead<T> {
    Batch(OwnerReplicationBatch<T>),
    CheckpointRequired {
        epoch: u64,
        requested_offset: Offset,
        head_offset: Offset,
        next_offset: Offset,
    },
}

pub(crate) fn owner_replication_checkpoint_required<T>(
    log: &Keratin,
    requested_offset: Offset,
) -> OwnerReplicationRead<T> {
    owner_replication_checkpoint_required_with_head(log, requested_offset, log.head_offset())
}

pub(crate) fn owner_replication_checkpoint_required_with_head<T>(
    log: &Keratin,
    requested_offset: Offset,
    head_offset: Offset,
) -> OwnerReplicationRead<T> {
    OwnerReplicationRead::CheckpointRequired {
        epoch: log.current_epoch(),
        requested_offset,
        head_offset: log.head_offset().max(head_offset),
        next_offset: log.next_offset(),
    }
}

pub(crate) fn owner_replication_gap<T>(
    stream: &'static str,
    log: &Keratin,
    requested_offset: Offset,
    expected_offset: Offset,
    got_offset: Offset,
) -> Result<OwnerReplicationRead<T>> {
    let head_offset = log.head_offset();
    if expected_offset < head_offset {
        return Ok(owner_replication_checkpoint_required(log, requested_offset));
    }
    if expected_offset == requested_offset && got_offset > expected_offset {
        return Ok(owner_replication_checkpoint_required_with_head(
            log,
            requested_offset,
            got_offset,
        ));
    }

    Err(StromaError::Corruption(format!(
        "{stream} log gap while reading owner records: expected offset {expected_offset}, got {got_offset}",
    )))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedQueueApplyOutcome {
    pub message_log: Option<ReplicatedAppendOutcome>,
    pub event_log: Option<ReplicatedAppendOutcome>,
}

/// Compacted queue state for a follower that fell behind retained event logs.
///
/// This is not a message transfer. Messages at or after `message_next_offset`
/// still need to be replicated through the message-log replication path before
/// the follower can safely promote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerStateCheckpointInstall {
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
    pub applied_event_offset: Offset,
    pub state_snapshot: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerStateCheckpointInstallOutcome {
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
    pub applied_event_offset: Offset,
    pub snapshot_meta: SnapshotMeta,
}

/// Compacted queue state exported by an owner at one coherent point.
///
/// `message_checkpoint_offset` is the first message offset the installed state
/// may still reference. `message_next_offset` is the owner message-log tail the
/// follower must catch up to before promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerStateCheckpoint {
    pub message_epoch: u64,
    pub event_epoch: u64,
    pub message_checkpoint_offset: Offset,
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
    pub applied_event_offset: Offset,
    pub state_snapshot: Vec<u8>,
}
