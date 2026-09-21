//! Replication data types and owner-read helpers for the Stroma engine.
//!
//! Clustering-module separation: the owner/follower replication batch + read
//! types, the apply/checkpoint outcome structs, and the owner-read gap/checkpoint
//! helpers, lifted out of stroma.rs. Re-exported from `stroma` so existing
//! `stroma_core::` and `crate::stroma::` paths keep resolving.

use std::sync::atomic::Ordering;

use keratin_log::{KDurability, Keratin, KeratinReplicaExt, Message, ReplicatedAppendOutcome};

use crate::engine::PartitionKind;
use crate::event::StromaEvent;
use crate::state::{QueueRole, SnapshotMeta};
use crate::stream_state::StreamCommand;
use crate::stroma::{
    QueueDemotionOutcome, QueuePromotionOutcome, Stroma, decode_err, event_msg, io_err,
    replicated_append_outcome_allows_state_apply,
};
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FollowerStateCheckpointInstall {
    /// Exact source epochs. Assignment/replication fencing must establish these
    /// locally before checkpoint installation; installation cannot advance them.
    pub message_epoch: u64,
    pub event_epoch: u64,
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

impl Stroma {
    pub async fn demote_queue_owner_to_follower(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueDemotionOutcome> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        qh.freeze_owner_and_wait_operations().await?;
        qh.msg_log().freeze();
        qh.event_log().freeze();

        let message_next_offset = qh.msg_log().next_offset();
        let event_next_offset = qh.event_log().next_offset();
        let applied_event_offset = if event_next_offset == 0 {
            None
        } else {
            Some(qh.applied_upto().load(Ordering::Acquire))
        };

        qh.become_follower();
        qh.msg_log().become_follower();
        qh.event_log().become_follower();

        Ok(QueueDemotionOutcome {
            message_next_offset,
            event_next_offset,
            applied_event_offset,
        })
    }

    pub async fn become_queue_follower(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        qh.become_follower();
        qh.msg_log().become_follower();
        qh.event_log().become_follower();
        Ok(())
    }

    pub async fn become_queue_owner(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let apply_state = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        if *apply_state {
            return Err(crate::StromaError::Io(
                "incomplete follower apply; recover or install a checkpoint before promotion"
                    .into(),
            ));
        }

        if qh.role() != QueueRole::Owner
            && let Some(wq) = qh.as_work_queue()
        {
            let required = wq.required_message_next().await.map_err(io_err)?;
            if qh.msg_log().next_offset() < required {
                return Err(crate::StromaError::InvalidArgument(format!(
                    "queue payload backfill incomplete: message next {} requires {required}",
                    qh.msg_log().next_offset()
                )));
            }
        }
        if qh.has_ordered_apply() && qh.role() != QueueRole::Owner { qh.align_follower_checkpoint_boundary()?; }
        qh.become_owner();
        qh.msg_log().become_owner();
        qh.event_log().become_owner();
        Ok(())
    }

    /// Client admission may fence an existing owner, but cannot promote a
    /// follower while its assignment transition/backfill is still in progress.
    pub async fn ensure_queue_owner_epoch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: Option<u64>,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, group).await?;
        let h = qh.resolve()?;
        let _operation = h.begin_owner_operation().await?;
        if let Some(epoch) = epoch {
            h.msg_log().advance_epoch(epoch).await.map_err(io_err)?;
            h.event_log().advance_epoch(epoch).await.map_err(io_err)?;
        }
        Ok(())
    }

    /// Advance both queue logs to the assignment fencing epoch.
    ///
    /// The epoch is persisted (Keratin manifest) BEFORE any role-specific
    /// work uses it; replicated appends carrying an older epoch are rejected
    /// from then on. Monotonic: re-applying the current epoch is a no-op,
    /// regressing is an error.
    pub async fn advance_queue_epoch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<u64> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        qh.msg_log().advance_epoch(epoch).await.map_err(io_err)?;
        qh.event_log().advance_epoch(epoch).await.map_err(io_err)?;
        Ok(epoch)
    }

    /// `become_queue_owner` fenced at the assignment epoch (persisted first).
    pub async fn become_queue_owner_with_epoch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<()> {
        self.advance_queue_epoch(topic, part, group, epoch).await?;
        self.become_queue_owner(topic, part, group).await
    }

    /// `become_queue_follower` fenced at the assignment epoch: the follower's
    /// logs reject replicated batches from any older-epoch (stale) owner.
    pub async fn become_queue_follower_with_epoch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<()> {
        self.advance_queue_epoch(topic, part, group, epoch).await?;
        self.become_queue_follower(topic, part, group).await
    }

    /// Stream-named role/epoch wrappers. A stream partition reuses the queue
    /// handle (record log + cursor-commit event log, no group), so these delegate
    /// to the queue role machinery; they exist as a distinct seam for the stream
    /// store and to keep stream call sites readable.
    pub async fn advance_stream_epoch(&self, topic: &str, part: u32, epoch: u64) -> Result<u64> {
        self.advance_queue_epoch(topic, part, None, epoch).await
    }

    /// Current durable queue log frontiers, for a conservative dependency on a
    /// recovered backlog before new publications supply exact batch boundaries.
    pub async fn queue_durable_frontiers(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<crate::QueuePublishCommit> {
        let qh = self.queue_handle(topic, part, group).await?;
        let h = qh.resolve()?;
        let _apply = h.follower_apply_state().await;
        h.ensure_not_recovery_sealed()?;
        let _pause = h.pause_owner_operations_and_wait().await?;
        Ok(crate::QueuePublishCommit {
            message_next: h.msg_log().next_offset(),
            event_next: h.event_log().next_offset(),
            message_epoch: h.msg_log().current_epoch(),
            event_epoch: h.event_log().current_epoch(),
        })
    }

    /// Resume cursors for an idle follower, including a checkpoint whose live
    /// payloads still require backfill. These cursors alone do not prove readiness.
    pub async fn queue_replication_next_offsets(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(Offset, Offset)> {
        let handle = self.queue_handle(topic, part, group).await?;
        let handle = handle.resolve()?;
        let apply = handle.follower_apply_state().await;
        handle.ensure_not_recovery_sealed()?;
        if *apply || handle.role() != QueueRole::Follower {
            return Err(StromaError::InvalidArgument(
                "follower apply is incomplete or role changed".into(),
            ));
        }
        Ok((
            handle.msg_log().next_offset(),
            handle.event_log().next_offset(),
        ))
    }

    /// A stream follower's next offsets for both logs: `(record_next,
    /// cursor_event_next)`. The follower worker pulls from these so it resumes at
    /// the right place after a restart without re-fetching the whole log.
    pub async fn stream_replication_next_offsets(
        &self,
        topic: &str,
        part: u32,
    ) -> Result<(Offset, Offset)> {
        let qh = self.queue_handle(topic, part, None).await?;
        let qh = qh.resolve()?;
        Ok((qh.msg_log().next_offset(), qh.event_log().next_offset()))
    }

    pub async fn become_stream_follower_with_epoch(
        &self,
        topic: &str,
        part: u32,
        epoch: u64,
    ) -> Result<()> {
        // A stream follower must materialize the partition as a stream (kind
        // marker + stream control actor) so replicated batches can advance the
        // stream tail, mirroring the owner's create_stream. Idempotent: skip if
        // the partition is already a stream. Retention is the owner's concern and
        // is mirrored via replication, so the follower opens with no local
        // retention policy here.
        if self.partition_kind(topic, part, None) != PartitionKind::Stream {
            self.create_stream(topic, part, None).await?;
        }
        self.become_queue_follower_with_epoch(topic, part, None, epoch)
            .await
    }

    pub async fn become_stream_owner_with_epoch(
        &self,
        topic: &str,
        part: u32,
        epoch: u64,
    ) -> Result<()> {
        self.become_queue_owner_with_epoch(topic, part, None, epoch)
            .await
    }

    /// Failover promotion for a stream follower (dead owner, no expected tails):
    /// promote at the local log tails under the assignment epoch fence. A stream
    /// reuses the queue handle (group None), and the events-applied gate carries
    /// over unchanged - here the "events" are cursor-commit events, so a promoted
    /// stream owner is guaranteed to have applied every replicated cursor before
    /// it serves subscribers.
    pub async fn promote_stream_follower_to_local_tail(
        &self,
        topic: &str,
        part: u32,
        epoch: u64,
    ) -> Result<QueuePromotionOutcome> {
        self.promote_queue_follower_to_local_tail(topic, part, None, epoch)
            .await
    }

    /// Verify durable, fully applied follower state without granting an owner
    /// role. Used before admitting a non-voting learner to the write set.
    pub async fn verify_queue_learner_caught_up(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
        message_target: u64,
        event_target: u64,
    ) -> Result<crate::QueuePublishCommit> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        if *apply || qh.role() != QueueRole::Follower {
            return Err(StromaError::InvalidArgument(
                "learner is not a completely applied follower".into(),
            ));
        }
        let message_next = qh.msg_log().next_offset();
        let event_next = qh.event_log().next_offset();
        if qh.msg_log().current_epoch() != epoch
            || qh.event_log().current_epoch() != epoch
            || message_next < message_target
            || event_next < event_target
            || qh.ordered_applied_next()? != Some(event_next)
        {
            return Err(StromaError::InvalidArgument(
                "learner has not reached its exact history and application cut".into(),
            ));
        }
        if let Some(wq) = qh.as_work_queue() {
            if message_next < wq.required_message_next().await.map_err(io_err)? {
                return Err(StromaError::InvalidArgument(
                    "learner payload backfill is incomplete".into(),
                ));
            }
        }
        let message_log = qh.msg_log();
        let event_log = qh.event_log();
        let (messages, events) = tokio::join!(message_log.sync(), event_log.sync());
        messages.map_err(io_err)?;
        events.map_err(io_err)?;
        Ok(crate::QueuePublishCommit {
            message_next,
            event_next,
            message_epoch: epoch,
            event_epoch: epoch,
        })
    }

    pub async fn promote_queue_follower_if_caught_up(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        expected_message_next_offset: Offset,
        expected_event_next_offset: Offset,
    ) -> Result<QueuePromotionOutcome> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let apply_state = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        if *apply_state {
            return Err(crate::StromaError::Io(
                "incomplete follower apply; recover or install a checkpoint before promotion"
                    .into(),
            ));
        }

        let role = qh.role();
        if role != QueueRole::Follower {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Follower,
                actual: role,
            });
        }

        let message_next_offset = qh.msg_log().next_offset();
        if message_next_offset < expected_message_next_offset {
            return Ok(QueuePromotionOutcome::MessageLogBehind {
                local_next_offset: message_next_offset,
                expected_next_offset: expected_message_next_offset,
            });
        }
        if message_next_offset > expected_message_next_offset {
            return Ok(QueuePromotionOutcome::MessageLogAhead {
                local_next_offset: message_next_offset,
                expected_next_offset: expected_message_next_offset,
            });
        }

        let event_next_offset = qh.event_log().next_offset();
        if event_next_offset < expected_event_next_offset {
            return Ok(QueuePromotionOutcome::EventLogBehind {
                local_next_offset: event_next_offset,
                expected_next_offset: expected_event_next_offset,
            });
        }
        if event_next_offset > expected_event_next_offset {
            return Ok(QueuePromotionOutcome::EventLogAhead {
                local_next_offset: event_next_offset,
                expected_next_offset: expected_event_next_offset,
            });
        }

        let applied_upto = qh.applied_upto().load(Ordering::Acquire);
        let applied_event_offset = if event_next_offset == 0 {
            None
        } else {
            Some(applied_upto)
        };
        let events_unapplied = match qh.ordered_applied_next()? {
            Some(next) => next != event_next_offset,
            None => event_next_offset != 0 && applied_upto < event_next_offset.saturating_sub(1),
        };
        if events_unapplied {
            return Ok(QueuePromotionOutcome::EventsNotApplied {
                applied_event_offset,
                event_next_offset,
            });
        }

        if let Some(wq) = qh.as_work_queue() {
            let required = wq.required_message_next().await.map_err(io_err)?;
            if message_next_offset < required {
                return Ok(QueuePromotionOutcome::MessageLogBehind {
                    local_next_offset: message_next_offset,
                    expected_next_offset: required,
                });
            }
        }
        if qh.has_ordered_apply() && qh.role() != QueueRole::Owner { qh.align_follower_checkpoint_boundary()?; }
        qh.become_owner();
        qh.msg_log().become_owner();
        qh.event_log().become_owner();

        Ok(QueuePromotionOutcome::Promoted {
            message_next_offset,
            event_next_offset,
            applied_event_offset,
        })
    }

    /// Promote a follower to owner at its own local log tails.
    ///
    /// Failover path: the previous owner is gone, so there are no external
    /// expected tails to verify against — the assignment epoch fences the old
    /// owner's unreplicated suffix instead. The events-applied gate stays:
    /// every locally recorded event must be applied before serving as owner.
    pub async fn promote_queue_follower_to_local_tail(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<QueuePromotionOutcome> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let apply_state = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        if *apply_state {
            return Err(crate::StromaError::Io(
                "incomplete follower apply; recover or install a checkpoint before promotion"
                    .into(),
            ));
        }

        let role = qh.role();
        if role != QueueRole::Follower {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Follower,
                actual: role,
            });
        }

        let message_next_offset = qh.msg_log().next_offset();
        let event_next_offset = qh.event_log().next_offset();
        let applied_upto = qh.applied_upto().load(Ordering::Acquire);
        let applied_event_offset = if event_next_offset == 0 {
            None
        } else {
            Some(applied_upto)
        };
        let events_unapplied = match qh.ordered_applied_next()? {
            Some(next) => next != event_next_offset,
            None => event_next_offset != 0 && applied_upto < event_next_offset.saturating_sub(1),
        };
        if events_unapplied {
            return Ok(QueuePromotionOutcome::EventsNotApplied {
                applied_event_offset,
                event_next_offset,
            });
        }

        if let Some(wq) = qh.as_work_queue() {
            let required = wq.required_message_next().await.map_err(io_err)?;
            if message_next_offset < required {
                return Ok(QueuePromotionOutcome::MessageLogBehind {
                    local_next_offset: message_next_offset,
                    expected_next_offset: required,
                });
            }
        }
        // Persist the fencing epoch BEFORE serving as owner: from here on,
        // replicated traffic from the previous (older-epoch) owner is
        // rejected by both logs.
        qh.msg_log().advance_epoch(epoch).await.map_err(io_err)?;
        qh.event_log().advance_epoch(epoch).await.map_err(io_err)?;

        if qh.has_ordered_apply() && qh.role() != QueueRole::Owner { qh.align_follower_checkpoint_boundary()?; }
        qh.become_owner();
        qh.msg_log().become_owner();
        qh.event_log().become_owner();

        Ok(QueuePromotionOutcome::Promoted {
            message_next_offset,
            event_next_offset,
            applied_event_offset,
        })
    }

    pub async fn read_owner_message_records(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
    ) -> Result<OwnerReplicationRead<Message>> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let role = qh.role();
        if role != QueueRole::Owner {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Owner,
                actual: role,
            });
        }

        let log = qh.msg_log();
        let head_offset = log.head_offset();
        if from < head_offset {
            return Ok(owner_replication_checkpoint_required(&log, from));
        }

        let (records, batch_next_offset) = if max == 0 {
            (Vec::new(), from)
        } else {
            // Keratin readers are synchronous; keep replica owner scans off
            // Tokio workers so replication polling cannot starve timers.
            let read_log = log.clone();
            let raw = tokio::task::spawn_blocking(move || {
                let reader = read_log.reader();
                reader.scan_from(from, max)
            })
            .await
            .map_err(|err| StromaError::Io(err.to_string()))?
            .map_err(io_err)?;
            let mut expected = from;
            let mut records = Vec::with_capacity(raw.len());
            for record in raw {
                if record.offset != expected {
                    return owner_replication_gap("message", &log, from, expected, record.offset);
                }
                expected = record.offset + 1;
                records.push((record.offset, record.to_message()));
            }
            (records, expected)
        };

        Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: log.current_epoch(),
            requested_offset: from,
            next_offset: batch_next_offset,
            records,
        }))
    }

    pub async fn read_owner_event_records(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let role = qh.role();
        if role != QueueRole::Owner {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Owner,
                actual: role,
            });
        }

        let log = qh.event_log();
        let head_offset = log.head_offset();
        if from < head_offset {
            return Ok(owner_replication_checkpoint_required(&log, from));
        }

        let (records, batch_next_offset) = if max == 0 {
            (Vec::new(), from)
        } else {
            // Keratin readers are synchronous; keep replica owner scans off
            // Tokio workers so replication polling cannot starve timers.
            let read_log = log.clone();
            let raw = tokio::task::spawn_blocking(move || {
                let reader = read_log.reader();
                reader.scan_from(from, max)
            })
            .await
            .map_err(|err| StromaError::Io(err.to_string()))?
            .map_err(io_err)?;
            let mut expected = from;
            let mut records = Vec::with_capacity(raw.len());
            for record in raw {
                if record.offset != expected {
                    return owner_replication_gap("event", &log, from, expected, record.offset);
                }
                expected = record.offset + 1;
                let event = StromaEvent::decode(&record.payload).map_err(decode_err)?;
                records.push((record.offset, event));
            }
            (records, expected)
        };

        Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: log.current_epoch(),
            requested_offset: from,
            next_offset: batch_next_offset,
            records,
        }))
    }

    pub async fn export_owner_state_checkpoint(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<OwnerStateCheckpoint> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        let role = qh.role();
        if role != QueueRole::Owner {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Owner,
                actual: role,
            });
        }

        let _pause = qh.pause_owner_operations_and_wait().await?;

        async {
            let message_next_offset = qh.msg_log().next_offset();
            let event_next_offset = qh.event_log().next_offset();
            let applied_event_offset = event_next_offset.saturating_sub(1);
            let state_checkpoint = if qh.has_ordered_apply() {
                let captured = qh.capture_exact_checkpoint(false).await?;
                if captured.event_next != event_next_offset {
                    return Err(StromaError::Io("owner checkpoint has unapplied events".into()));
                }
                tokio::task::spawn_blocking(move || crate::state::QueueStateCheckpointSnapshot {
                    message_checkpoint_offset: captured.message_floor,
                    state_snapshot: captured.encode(),
                }).await.map_err(io_err)?
            } else {
                qh
                .work_queue()?
                .export_state_checkpoint_snapshot(applied_event_offset)
                .await
                .map_err(|err| {
                    StromaError::Io(format!(
                        "owner checkpoint snapshot export failed for tp={topic} part={part} group={group:?}: {err}"
                    ))
                })?
            };

            Ok(OwnerStateCheckpoint {
                message_epoch: qh.msg_log().current_epoch(),
                event_epoch: qh.event_log().current_epoch(),
                message_checkpoint_offset: state_checkpoint.message_checkpoint_offset,
                message_next_offset,
                event_next_offset,
                applied_event_offset,
                state_snapshot: state_checkpoint.state_snapshot,
            })
        }
        .await
    }

    pub async fn apply_replicated_queue_batch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
    ) -> Result<ReplicatedQueueApplyOutcome> {
        self.apply_replicated_two_log_batch(topic, part, group, messages, events, false)
            .await
    }

    /// Apply a replicated two-log batch to a STREAM partition: records to the
    /// message log, cursor-commit events to the event log. Identical to the queue
    /// apply except the stream tail is advanced after the record append, so the
    /// follower's head/tail (and a promoted owner's fan-out + cursor clamping)
    /// reflect the applied records, mirroring the owner append path. Streams have
    /// no group.
    pub async fn apply_replicated_stream_batch(
        &self,
        topic: &str,
        part: u32,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
    ) -> Result<ReplicatedQueueApplyOutcome> {
        self.apply_replicated_two_log_batch(topic, part, None, messages, events, true)
            .await
    }

    /// Shared two-log follower apply for queues and streams. `advance_stream_tail`
    /// is the only difference: streams advance the keratin stream actor's tail
    /// after the record append; queues have no stream actor and pass `false`.
    async fn apply_replicated_two_log_batch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
        advance_stream_tail: bool,
    ) -> Result<ReplicatedQueueApplyOutcome> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let mut apply_state = qh.follower_apply_state().await;
        if *apply_state {
            return Err(crate::StromaError::Io(
                "incomplete follower apply; recover or install a checkpoint before retry".into(),
            ));
        }
        let role = qh.role();
        if role != QueueRole::Follower {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Follower,
                actual: role,
            });
        }
        qh.msg_log().become_follower();
        qh.event_log().become_follower();

        // Encode before either append so an encoding failure cannot leave one
        // side queued. Reject obvious epoch/gap cases through the serial path;
        // ordinary contiguous traffic overlaps the two independent writers.
        let event_records = events
            .as_ref()
            .map(|batch| {
                batch
                    .events
                    .iter()
                    .map(event_msg)
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        let parallel = matches!((&messages, &events), (Some(m), Some(e))
            if m.epoch == qh.msg_log().current_epoch() && e.epoch == qh.event_log().current_epoch()
                && m.first_offset <= qh.msg_log().next_offset() && e.first_offset <= qh.event_log().next_offset());
        let message_next = messages
            .as_ref()
            .map(|b| b.first_offset + b.records.len() as u64);
        let append_messages = async {
            match messages {
                Some(batch) => {
                    let count = batch.records.len();
                    match qh
                        .msg_log()
                        .append_replicated_batch(
                            batch.epoch,
                            batch.first_offset,
                            batch.records,
                            batch.durability,
                        )
                        .await
                    {
                        Ok(outcome) => Ok(Some(outcome)),
                        Err(error) => Err(crate::replication_diagnostics::overlap_error(
                            error,
                            qh.msg_log(),
                            topic,
                            part,
                            group,
                            batch.epoch,
                            batch.first_offset,
                            count,
                            None,
                        )
                        .await),
                    }
                }
                None => Ok(None),
            }
        };
        let append_events = async {
            match (&events, event_records) {
                (Some(batch), Some(records)) => {
                    match qh
                        .event_log()
                        .append_replicated_batch(
                            batch.epoch,
                            batch.first_offset,
                            records,
                            batch.durability,
                        )
                        .await
                    {
                        Ok(outcome) => Ok(Some(outcome)),
                        Err(error) => Err(crate::replication_diagnostics::overlap_error(
                            error,
                            qh.event_log(),
                            topic,
                            part,
                            group,
                            batch.epoch,
                            batch.first_offset,
                            batch.events.len(),
                            Some(&batch.events),
                        )
                        .await),
                    }
                }
                _ => Ok(None),
            }
        };
        *apply_state = true;
        let (message_log, event_log) = if parallel {
            // Cancellation leaves this guard dirty too. Both completions MUST be
            // drained: try_join would return early while the other writer still
            // owns an accepted append. No actor state changes until both finish.
            let (message, event) = tokio::join!(append_messages, append_events);
            if let Err(error) = &event {
                tracing::error!(topic, part, error = %error, "follower event append failed; resync required");
            }
            (message?, event?)
        } else {
            let message = append_messages.await?;
            let event = if message
                .as_ref()
                .is_none_or(replicated_append_outcome_allows_state_apply)
            {
                append_events.await?
            } else {
                drop(append_events);
                None
            };
            (message, event)
        };
        let messages_allowed = message_log
            .as_ref()
            .is_none_or(replicated_append_outcome_allows_state_apply);
        let events_allowed = event_log
            .as_ref()
            .is_none_or(replicated_append_outcome_allows_state_apply);
        if messages_allowed && events_allowed {
            if advance_stream_tail && let Some(next_offset) = message_next {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.stream_command_enqueue(StreamCommand::AdvanceTail {
                    next_offset,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            if let Some(batch) = events {
                // A verified overlap may contain already-applied NACKs, whose
                // retry increment must not run a second time. The exclusive
                // frontier also distinguishes an unapplied event zero.
                let applied_next = qh.ordered_applied_next()?;
                let skip = match applied_next {
                    Some(next) if batch.first_offset > next => {
                        return Err(StromaError::Io("follower event application gap".into()));
                    }
                    Some(next) => next.saturating_sub(batch.first_offset)
                        .min(batch.events.len() as u64) as usize,
                    None => 0,
                };
                let count = batch.events.len() - skip;
                if count > 0 {
                    let scope = qh.ordered_apply_scope();
                    let turn = crate::ordered_apply::enter(&scope, batch.first_offset + skip as u64, count as u64).await?;
                    for (idx, event) in batch.events.into_iter().enumerate().skip(skip) {
                        self.apply_event_inmem(event, &qh).await?;
                        qh.applied_upto()
                            .fetch_max(batch.first_offset + idx as u64, Ordering::Release);
                    }
                    crate::ordered_apply::finish(turn, scope)?;
                }
            }
            *apply_state = false;
        } else if !parallel {
            // A rejected serial preflight did not issue the dependent event
            // append. Errors and cancellation return above with the flag set.
            *apply_state = false;
        }
        Ok(ReplicatedQueueApplyOutcome {
            message_log,
            event_log,
        })
    }
}
