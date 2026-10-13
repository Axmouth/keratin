//! Replication data types and owner-read helpers for the Stroma engine.
//!
//! Clustering-module separation: the owner/follower replication batch + read
//! types, the apply/checkpoint outcome structs, and the owner-read gap/checkpoint
//! helpers, lifted out of stroma.rs. Re-exported from `stroma` so existing
//! `stroma_core::` and `crate::stroma::` paths keep resolving.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use keratin_log::{
    KDurability, Keratin, KeratinReplicaExt, Message, OwnedRecord, ReplicatedAppendOutcome,
};

use crate::engine::PartitionKind;
use crate::event::StromaEvent;
use crate::state::{QueueHandleInner, QueueRole, SnapshotMeta};
use crate::stream_state::StreamCommand;
use crate::stroma::{
    QueueDemotionOutcome, QueuePromotionOutcome, Stroma, StreamPromotionOutcome, decode_err,
    event_msg, io_err, replicated_append_outcome_allows_state_apply,
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

fn ensure_owner(qh: &QueueHandleInner) -> Result<()> {
    let role = qh.role();
    if role != QueueRole::Owner {
        return Err(StromaError::WrongQueueRole {
            expected: QueueRole::Owner,
            actual: role,
        });
    }
    Ok(())
}

/// Read a stream's record log. Queues have no such log.
async fn read_owner_message_log(
    qh: &QueueHandleInner,
    from: Offset,
    max: usize,
    max_bytes: usize,
    early: bool,
) -> Result<OwnerReplicationRead<Message>> {
    ensure_owner(qh)?;
    let log = qh.msg_log()?;
    read_owner_log(qh, log, "message", from, max, max_bytes, early, false, |record| {
        Ok(record.to_message())
    })
    .await
}

/// Read the event log: the single queue log, or a stream's cursor events.
async fn read_owner_event_log(
    qh: &QueueHandleInner,
    from: Offset,
    max: usize,
    max_bytes: usize,
    early: bool,
) -> Result<OwnerReplicationRead<StromaEvent>> {
    ensure_owner(qh)?;
    // Queue records carry payloads, so they get the same byte cap a payload
    // log read has. Stream cursor events are small and rely on the scan budget.
    let payload_cap = qh.has_combined_log();
    read_owner_log(qh, qh.event_log(), "event", from, max, max_bytes, early, payload_cap, |record| {
        StromaEvent::decode(&record.payload).map_err(decode_err)
    })
    .await
}

async fn read_owner_log<T>(
    qh: &QueueHandleInner,
    log: Arc<Keratin>,
    lane: &'static str,
    from: Offset,
    max: usize,
    max_bytes: usize,
    early: bool,
    payload_cap: bool,
    decode: impl Fn(OwnedRecord) -> Result<T>,
) -> Result<OwnerReplicationRead<T>> {
    let epoch = log.current_epoch();
    let head_offset = log.head_offset();
    if from < head_offset {
        return Ok(owner_replication_checkpoint_required(&log, from));
    }

    let (records, batch_next_offset) = if max == 0 {
        (Vec::new(), from)
    } else {
        // Keratin readers are synchronous. Keep replica owner scans off
        // Tokio workers so replication polling cannot starve timers.
        let read_log = log.clone();
        let raw = tokio::task::spawn_blocking(move || {
            let reader = read_log.reader();
            if early {
                return reader.scan_for_replication_with_byte_budget(from, max, max_bytes);
            }
            reader.scan_from_with_byte_budget(from, max, max_bytes)
        })
        .await
        .map_err(|err| StromaError::Io(err.to_string()))?
        .map_err(io_err)?;
        let mut expected = from;
        let mut records = Vec::with_capacity(raw.len());
        let mut used_bytes = 0usize;
        for record in raw {
            if payload_cap {
                // The reader includes one lookahead record. Always allow one
                // oversized record so the cursor can advance.
                let bytes = record.headers.len().saturating_add(record.payload.len()).saturating_add(32);
                if !records.is_empty() && used_bytes.saturating_add(bytes) > max_bytes { break; }
                used_bytes = used_bytes.saturating_add(bytes);
            }
            if record.offset != expected {
                return owner_replication_gap(lane, &log, from, expected, record.offset);
            }
            expected = record.offset + 1;
            let offset = record.offset;
            records.push((offset, decode(record)?));
        }
        (records, expected)
    };

    if qh.role() != QueueRole::Owner || log.current_epoch() != epoch {
        return Err(StromaError::Io(
            "owner changed during replication read".into(),
        ));
    }
    Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
        epoch,
        requested_offset: from,
        next_offset: batch_next_offset,
        records,
    }))
}

/// Per-log append outcomes for a two-log stream batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedStreamApplyOutcome {
    pub message_log: Option<ReplicatedAppendOutcome>,
    pub event_log: Option<ReplicatedAppendOutcome>,
}

/// Owner queue state at one coherent point, exported to a learner and
/// installed by it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueueStateCheckpoint {
    /// Source log epoch. Assignment and replication fencing must establish it
    /// locally before install. Installation cannot advance it.
    pub epoch: u64,
    /// First record to backfill: the lowest offset the state may reference.
    pub start_offset: Offset,
    /// Exclusive applied boundary, equal to the owner log tail at capture.
    /// The snapshot covers every record below it.
    pub next_offset: Offset,
    pub state_snapshot: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStateCheckpointInstallOutcome {
    /// Local log tail right after the reset. Payload backfill resumes here,
    /// at the checkpoint start offset.
    pub next_offset: Offset,
    pub snapshot_meta: SnapshotMeta,
}

/// Local tails and application state of a follower at a promotion attempt.
/// Shared by queue and stream promotion so the events-applied gate stays one
/// piece of code.
enum LocalPromotion {
    Promoted(LocalTails),
    LogBehind { local_next: Offset, expected_next: Offset },
    LogAhead { local_next: Offset, expected_next: Offset },
    NotApplied(LocalTails),
    PayloadsMissing { log_next: Offset, required_next: Offset },
}

struct LocalTails {
    /// Stream record log tail. Queues have no record log.
    record_next: Option<Offset>,
    /// Exclusive end of the queue log, or of a stream's cursor events.
    log_next: Offset,
    /// Exclusive applied boundary, 0 when nothing is applied.
    applied_next: Offset,
    /// Last applied event offset, None when the log is empty.
    applied_offset: Option<Offset>,
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
        qh.work_queue()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        qh.freeze_owner_and_wait_operations().await?;
        for log in qh.physical_logs() { log.freeze(); }

        let tails = local_tails(&qh)?;

        qh.become_follower();
        for log in qh.physical_logs() { log.become_follower(); }

        Ok(QueueDemotionOutcome {
            next: tails.log_next,
            applied_next: tails.applied_next,
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
        for log in qh.physical_logs() { log.become_follower(); }
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
            let log_next = self.payload_log_for(&qh).next_offset();
            if log_next < required {
                return Err(crate::StromaError::InvalidArgument(format!(
                    "queue payload backfill incomplete: log next {log_next} requires {required}"
                )));
            }
        }
        if qh.has_ordered_apply() && qh.role() != QueueRole::Owner { qh.align_follower_checkpoint_boundary()?; }
        qh.become_owner();
        for log in qh.physical_logs() { log.become_owner(); }
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
            for log in h.physical_logs() { log.advance_epoch(epoch).await.map_err(io_err)?; }
        }
        Ok(())
    }

    /// Advance the partition logs to the assignment fencing epoch.
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
        for log in qh.physical_logs() { log.advance_epoch(epoch).await.map_err(io_err)?; }
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

    /// Current end of the single queue log, as a dependency that a recovered
    /// backlog must reach before new publications supply exact batch boundaries.
    /// Refuses stream partitions. `next` is the staged next offset, which can be
    /// ahead of the durable frontier. That is safe for a dependency: a confirm
    /// gated on it only waits for more records, never fewer.
    pub async fn queue_durable_commit(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<crate::QueuePublishCommit> {
        let qh = self.queue_handle(topic, part, group).await?;
        let h = qh.resolve()?;
        h.work_queue()?;
        let _apply = h.follower_apply_state().await;
        h.ensure_not_recovery_sealed()?;
        let _pause = h.pause_owner_operations_and_wait().await?;
        let log = h.event_log();
        Ok(crate::QueuePublishCommit {
            next: log.next_offset(),
            epoch: log.current_epoch(),
            buffered: self.handle_is_buffered(&h),
        })
    }

    /// Resume cursor in the single queue log for an idle follower, including
    /// one whose installed checkpoint still needs payload backfill. The cursor
    /// alone does not prove readiness. Refuses stream partitions.
    ///
    /// The cursor is the durable frontier, so a caller may report it as
    /// durable progress. Records a follower wrote without syncing, as on a
    /// buffered queue, are synced first. The dirty-apply fence keeps an append
    /// in flight from landing in between.
    pub async fn queue_replication_next_offset(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let handle = self.queue_handle(topic, part, group).await?;
        let handle = handle.resolve()?;
        handle.work_queue()?;
        let apply = handle.follower_apply_state().await;
        handle.ensure_not_recovery_sealed()?;
        if *apply || handle.role() != QueueRole::Follower {
            return Err(StromaError::InvalidArgument(
                "follower apply is incomplete or role changed".into(),
            ));
        }
        let log = handle.event_log();
        let next = log.next_offset();
        crate::stroma::checkpoint_capture::sync_through(&log, next).await?;
        Ok(next)
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
        qh.stream()?;
        Ok((qh.msg_log()?.next_offset(), qh.event_log().next_offset()))
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
    ) -> Result<StreamPromotionOutcome> {
        let qh = self.queue_handle(topic, part, None).await?;
        let qh = qh.resolve()?;
        qh.stream()?;
        Ok(match self.promote_follower(&qh, Some(epoch), None).await? {
            LocalPromotion::Promoted(tails) => StreamPromotionOutcome::Promoted {
                message_next_offset: tails.record_next.unwrap_or(0),
                event_next_offset: tails.log_next,
                applied_event_offset: tails.applied_offset,
            },
            LocalPromotion::NotApplied(tails) => StreamPromotionOutcome::EventsNotApplied {
                applied_event_offset: tails.applied_offset,
                event_next_offset: tails.log_next,
            },
            LocalPromotion::LogBehind { .. }
            | LocalPromotion::LogAhead { .. }
            | LocalPromotion::PayloadsMissing { .. } => {
                return Err(StromaError::Internal(
                    "stream promotion produced a queue-only outcome".into(),
                ));
            }
        })
    }

    /// Verify durable, fully applied follower state without granting an owner
    /// role. Used before admitting a non-voting learner to the write set.
    /// `target` is the least queue log end the learner must hold.
    pub async fn verify_queue_learner_caught_up(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
        target: Offset,
    ) -> Result<crate::QueuePublishCommit> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        let wq = qh.work_queue()?;
        let apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        if *apply || qh.role() != QueueRole::Follower {
            return Err(StromaError::InvalidArgument(
                "learner is not a completely applied follower".into(),
            ));
        }
        let log = qh.event_log();
        let next = log.next_offset();
        if log.current_epoch() != epoch
            || next < target
            || qh.ordered_applied_next()? != Some(next)
        {
            return Err(StromaError::InvalidArgument(
                "learner has not reached its exact history and application cut".into(),
            ));
        }
        // The payload gate is separate from the cut: the state may reference
        // payloads that backfill has not copied yet.
        if next < wq.required_message_next().await.map_err(io_err)? {
            return Err(StromaError::InvalidArgument(
                "learner payload backfill is incomplete".into(),
            ));
        }
        log.sync().await.map_err(io_err)?;
        Ok(crate::QueuePublishCommit { next, epoch, buffered: false })
    }

    /// Promote a follower whose single queue log ends exactly at
    /// `expected_next`, with every record applied and every referenced payload
    /// present.
    pub async fn promote_queue_follower_if_caught_up(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        expected_next: Offset,
    ) -> Result<QueuePromotionOutcome> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        qh.work_queue()?;
        Ok(queue_promotion(self.promote_follower(&qh, None, Some(expected_next)).await?))
    }

    /// Promote a follower to owner at its own local log tail.
    ///
    /// Failover path: the previous owner is gone, so there is no external
    /// expected tail to verify against. The assignment epoch fences the old
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
        qh.work_queue()?;
        Ok(queue_promotion(self.promote_follower(&qh, Some(epoch), None).await?))
    }

    /// Shared promotion core. With `epoch` the follower is fenced at that
    /// epoch before it becomes owner. With `expected_next` its log must end
    /// exactly there.
    async fn promote_follower(
        &self,
        qh: &QueueHandleInner,
        epoch: Option<u64>,
        expected_next: Option<Offset>,
    ) -> Result<LocalPromotion> {
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

        let tails = local_tails(qh)?;
        if let Some(expected_next) = expected_next {
            if tails.log_next < expected_next {
                return Ok(LocalPromotion::LogBehind { local_next: tails.log_next, expected_next });
            }
            if tails.log_next > expected_next {
                return Ok(LocalPromotion::LogAhead { local_next: tails.log_next, expected_next });
            }
        }
        if tails.applied_next != tails.log_next {
            return Ok(LocalPromotion::NotApplied(tails));
        }

        if let Some(wq) = qh.as_work_queue() {
            let required = wq.required_message_next().await.map_err(io_err)?;
            let log_next = self.payload_log_for(qh).next_offset();
            if log_next < required {
                return Ok(LocalPromotion::PayloadsMissing { log_next, required_next: required });
            }
        }
        if let Some(epoch) = epoch {
            // Persist the fencing epoch BEFORE serving as owner: from here on,
            // replicated traffic from the previous (older-epoch) owner is
            // rejected by every log.
            for log in qh.physical_logs() { log.advance_epoch(epoch).await.map_err(io_err)?; }
        }

        if qh.has_ordered_apply() && qh.role() != QueueRole::Owner { qh.align_follower_checkpoint_boundary()?; }
        qh.become_owner();
        for log in qh.physical_logs() { log.become_owner(); }
        Ok(LocalPromotion::Promoted(tails))
    }

    pub async fn subscribe_owner_written(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(
        tokio::sync::watch::Receiver<()>,
        tokio::sync::watch::Receiver<()>,
    )> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        Ok((
            qh.payload_log().subscribe_written(),
            qh.event_log().subscribe_written(),
        ))
    }

    /// Write notifications for the single queue log.
    pub async fn subscribe_owner_queue_written(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<tokio::sync::watch::Receiver<()>> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        qh.work_queue()?;
        Ok(qh.event_log().subscribe_written())
    }

    /// Durable prefix of the single queue log from `from`, bounded by record
    /// count and approximate bytes. Payload enqueues and state events share
    /// these offsets, so one cursor covers the whole queue history. One
    /// oversized record is always returned so the cursor can advance.
    pub async fn read_owner_queue_records(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        self.read_owner_queue_records_inner(topic, part, group, from, max, max_bytes, false)
            .await
    }

    /// Tentative bytes for a fenced, enrolled queue history only. The caller
    /// keeps responsibility for authority and confirmation gates.
    pub async fn read_owner_queue_records_early(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        self.read_owner_queue_records_inner(topic, part, group, from, max, max_bytes, true)
            .await
    }

    async fn read_owner_queue_records_inner(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
        max_bytes: usize,
        early: bool,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        qh.work_queue()?;
        read_owner_event_log(&qh, from, max, max_bytes, early).await
    }

    /// Durable prefix of a stream's record log. Refuses queue partitions.
    pub async fn read_owner_stream_records(
        &self,
        topic: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<Message>> {
        self.read_owner_stream_records_inner(topic, part, from, max, max_bytes, false)
            .await
    }

    pub async fn read_owner_stream_records_early(
        &self,
        topic: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<Message>> {
        self.read_owner_stream_records_inner(topic, part, from, max, max_bytes, true)
            .await
    }

    async fn read_owner_stream_records_inner(
        &self,
        topic: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
        early: bool,
    ) -> Result<OwnerReplicationRead<Message>> {
        let qh = self.queue_handle(topic, part, None).await?;
        let qh = qh.resolve()?;
        qh.stream()?;
        read_owner_message_log(&qh, from, max, max_bytes, early).await
    }

    /// Durable prefix of a stream's cursor-commit event log. Refuses queue
    /// partitions.
    pub async fn read_owner_stream_cursor_events(
        &self,
        topic: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        self.read_owner_stream_cursor_events_inner(topic, part, from, max, max_bytes, false)
            .await
    }

    pub async fn read_owner_stream_cursor_events_early(
        &self,
        topic: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        self.read_owner_stream_cursor_events_inner(topic, part, from, max, max_bytes, true)
            .await
    }

    async fn read_owner_stream_cursor_events_inner(
        &self,
        topic: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
        early: bool,
    ) -> Result<OwnerReplicationRead<StromaEvent>> {
        let qh = self.queue_handle(topic, part, None).await?;
        let qh = qh.resolve()?;
        qh.stream()?;
        read_owner_event_log(&qh, from, max, max_bytes, early).await
    }

    /// Capture the owner's queue state at its log tail for a learner. Refuses
    /// stream partitions.
    pub async fn export_queue_state_checkpoint(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueStateCheckpoint> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        qh.work_queue()?;
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

        let log = qh.event_log();
        let next_offset = log.next_offset();
        // A learner installs this as recovery authority. On a buffered queue
        // the owner may not have synced all of it yet.
        crate::stroma::checkpoint_capture::sync_through(&log, next_offset).await?;
        let state_checkpoint = if qh.has_ordered_apply() {
            let captured = qh.capture_exact_checkpoint(false).await?;
            if captured.event_next != next_offset {
                return Err(StromaError::Io("owner checkpoint has unapplied events".into()));
            }
            tokio::task::spawn_blocking(move || crate::state::QueueStateCheckpointSnapshot {
                message_checkpoint_offset: captured.message_floor,
                state_snapshot: captured.encode(),
            }).await.map_err(io_err)?
        } else {
            qh
            .work_queue()?
            .export_state_checkpoint_snapshot(next_offset.saturating_sub(1))
            .await
            .map_err(|err| {
                StromaError::Io(format!(
                    "owner checkpoint snapshot export failed for tp={topic} part={part} group={group:?}: {err}"
                ))
            })?
        };

        Ok(QueueStateCheckpoint {
            epoch: log.current_epoch(),
            start_offset: state_checkpoint.message_checkpoint_offset.min(next_offset),
            next_offset,
            state_snapshot: state_checkpoint.state_snapshot,
        })
    }

    /// Apply one batch from the owner's single queue log. Payload enqueues
    /// and state events share offsets, so one batch carries both. The batch
    /// must not be empty. Refuses stream partitions.
    pub async fn apply_replicated_queue_batch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        batch: ReplicatedEventBatch,
    ) -> Result<ReplicatedAppendOutcome> {
        if batch.events.is_empty() {
            return Err(StromaError::InvalidArgument("replicated queue batch is empty".into()));
        }
        let outcome = self
            .apply_replicated_two_log_batch(
                topic, part, group, None, Some(batch), FollowerApplyTarget::Queue,
            )
            .await?;
        single_queue_log_outcome(outcome)
    }

    /// Apply a replicated two-log batch to a STREAM partition: records to the
    /// message log, cursor-commit events to the event log. The stream tail advances
    /// after the record append, so the
    /// follower's head/tail (and a promoted owner's fan-out + cursor clamping)
    /// reflect the applied records, mirroring the owner append path. Streams have
    /// no group. Refuses queue partitions.
    pub async fn apply_replicated_stream_batch(
        &self,
        topic: &str,
        part: u32,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
    ) -> Result<ReplicatedStreamApplyOutcome> {
        self.apply_replicated_two_log_batch(
            topic, part, None, messages, events, FollowerApplyTarget::Stream,
        )
        .await
    }

    async fn append_follower_frame(
        &self,
        qh: &crate::state::QueueHandleInner,
        apply_state: &mut bool,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
    ) -> Result<(ReplicatedStreamApplyOutcome, Option<ReplicatedEventBatch>, Option<u64>, bool)> {
        // Queue payloads travel as records of the single queue log, so a
        // queue frame never carries a record batch.
        if qh.has_combined_log() && messages.is_some() {
            return Err(StromaError::InvalidArgument("queue partition has no record log".into()));
        }
        if !qh.has_combined_log() && events.as_ref().is_some_and(|batch| batch.events.iter().any(|event|matches!(event,StromaEvent::PayloadEnqueue{..}))) {
            return Err(StromaError::InvalidArgument("queue payload event received by a stream".into()));
        }
        // Validate the entire incoming frame before enqueueing any writes or
        // setting the dirty-apply fence. This also covers open durability groups.
        if qh.has_combined_log() {
            if let Some(batch) = &events {
                batch.first_offset.checked_add(batch.events.len() as u64)
                    .ok_or_else(|| StromaError::InvalidArgument("combined event offset exhausted".into()))?;
                for (index, event) in batch.events.iter().enumerate() {
                    if event.max_combined_referenced_offset().is_some_and(|off| off >= batch.first_offset + index as u64) {
                        return Err(StromaError::InvalidArgument("combined follower event has a forward payload reference".into()));
                    }
                }
            }
        }
        let topic = qh.topic();
        let part = qh.partition();
        let group = qh.group();
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
        let events = {
            let mut events = events;
            if qh.has_combined_log() {
                if let Some(batch) = &mut events {
                    for event in &mut batch.events {
                        if let StromaEvent::PayloadEnqueue { headers, payload, .. } = event {
                            *headers = Vec::new();
                            *payload = Vec::new();
                        }
                    }
                }
            }
            events
        };
        let record_log = qh.optional_message_log();
        let parallel = matches!((&messages, &events, &record_log), (Some(m), Some(e), Some(records))
            if m.epoch == records.current_epoch() && e.epoch == qh.event_log().current_epoch()
                && m.first_offset <= records.next_offset() && e.first_offset <= qh.event_log().next_offset());
        let message_next = messages
            .as_ref()
            .map(|b| b.first_offset + b.records.len() as u64);
        let append_messages = async {
            match messages {
                Some(batch) => {
                    let count = batch.records.len();
                    match qh
                        .msg_log()?
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
                            qh.msg_log()?,
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
        Ok((ReplicatedStreamApplyOutcome { message_log, event_log }, events, message_next, parallel))
    }

    async fn apply_follower_events(
        &self,
        qh: &crate::state::QueueHandleInner,
        batch: ReplicatedEventBatch,
    ) -> Result<()> {
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
                // Snapshot backfill may skip old state events, but its physical
                // payload dependencies must be checked before that cut is ready.
                if qh.has_combined_log() && skip > 0
                    && applied_next.is_some_and(|next| batch.first_offset.saturating_add(batch.events.len() as u64) >= next) {
                    self.verify_combined_payload_coverage(qh, false).await?;
                }
                let count = batch.events.len() - skip;
                if count > 0 {
                    let scope = qh.ordered_apply_scope();
                    let turn = crate::ordered_apply::enter(&scope, batch.first_offset + skip as u64, count as u64).await?;
                    if qh.has_combined_log() {
                        self.apply_combined_records(batch.events.into_iter().skip(skip).collect(),batch.first_offset+skip as u64,qh).await?;
                        qh.applied_upto().fetch_max(batch.first_offset+skip as u64+count as u64-1,Ordering::Release);
                        crate::ordered_apply::finish(turn,scope)?;
                        return Ok(());
                    }
                    for (idx, event) in batch.events.into_iter().enumerate().skip(skip) {
                        self.apply_event_inmem(event, qh).await?;
                        qh.applied_upto()
                            .fetch_max(batch.first_offset + idx as u64, Ordering::Release);
                    }
                    crate::ordered_apply::finish(turn, scope)?;
                }
        Ok(())
    }

    /// Open durability group over batches from the single queue log. Every
    /// frame must carry records. Refuses stream partitions. Input is polled
    /// only after the previous frame has staged. The caller owns its deadline
    /// and byte budget. The recovery guard spans staging, the sync and actor
    /// apply. Dropping this future after mutation leaves the dirty fence set.
    pub async fn apply_open_replicated_queue_batch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        frames: futures::stream::BoxStream<'_, Result<ReplicatedEventBatch>>,
    ) -> Result<Vec<ReplicatedAppendOutcome>> {
        use futures::StreamExt;
        let frames = frames
            .map(|frame| {
                let batch = frame?;
                if batch.events.is_empty() {
                    return Err(StromaError::InvalidArgument("replicated queue batch is empty".into()));
                }
                Ok((None, Some(batch)))
            })
            .boxed();
        self.apply_open_follower_frames(topic, part, group, frames, FollowerApplyTarget::Queue)
            .await?
            .into_iter()
            .map(single_queue_log_outcome)
            .collect()
    }

    async fn apply_open_follower_frames(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        mut frames: futures::stream::BoxStream<'_, Result<(Option<ReplicatedMessageBatch>, Option<ReplicatedEventBatch>)>>,
        target: FollowerApplyTarget,
    ) -> Result<Vec<ReplicatedStreamApplyOutcome>> {
        use futures::StreamExt;
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        target.check_kind(&qh)?;
        let mut apply_state = qh.follower_apply_state().await;
        if *apply_state {
            return Err(StromaError::Io("incomplete follower apply; recover or install a checkpoint before retry".into()));
        }
        if qh.role() != QueueRole::Follower {
            return Err(StromaError::WrongQueueRole { expected: QueueRole::Follower, actual: qh.role() });
        }
        for log in qh.physical_logs() { log.become_follower(); }
        let mut pending_events = Vec::new();
        let mut sync_messages = false;
        let mut sync_events = false;
        let mut outcomes = Vec::new();
        while let Some(frame) = frames.next().await {
            let (mut messages, mut events) = frame?;
            sync_messages |= messages.as_ref().is_some_and(|b| !b.records.is_empty());
            sync_events |= events.is_some();
            if let Some(batch) = &mut messages { batch.durability = Some(KDurability::AfterWrite); }
            if let Some(batch) = &mut events { batch.durability = Some(KDurability::AfterWrite); }
            let (outcome, events, _, _) = self.append_follower_frame(&qh, &mut apply_state, messages, events).await?;
            if !outcome.message_log.as_ref().is_none_or(replicated_append_outcome_allows_state_apply)
                || !outcome.event_log.as_ref().is_none_or(replicated_append_outcome_allows_state_apply) {
                return Err(StromaError::Io("open follower batch rejected a frame; resync required".into()));
            }
            if let Some(events) = events { pending_events.push(events); }
            outcomes.push(outcome);
        }
        if !outcomes.is_empty() {
            // Drain both operations even when one fails. Neither staging nor a
            // completed single-log sync is a two-log durable acknowledgment.
            let messages = qh.optional_message_log();
            let events = qh.event_log();
            let (m, e) = tokio::join!(
                async { if sync_messages { match messages { Some(log) => log.sync().await, None => Err(std::io::Error::other("missing message log")) } } else { Ok(()) } },
                async { if sync_events { events.sync().await } else { Ok(()) } },
            );
            m.map_err(io_err)?;
            e.map_err(io_err)?;
            for batch in pending_events { self.apply_follower_events(&qh, batch).await?; }
            *apply_state = false;
        }
        Ok(outcomes)
    }

    /// Shared follower apply for single-log queues and two-log streams.
    /// Streams advance their record tail after accepted writes. Queues apply
    /// payload-bearing events in order and refuse any record batch.
    async fn apply_replicated_two_log_batch(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
        target: FollowerApplyTarget,
    ) -> Result<ReplicatedStreamApplyOutcome> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        target.check_kind(&qh)?;
        let advance_stream_tail = target == FollowerApplyTarget::Stream;
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
        for log in qh.physical_logs() { log.become_follower(); }

        let (outcome, events, message_next, parallel) = self
            .append_follower_frame(&qh, &mut apply_state, messages, events).await?;
        let ReplicatedStreamApplyOutcome { message_log, event_log } = outcome;
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
                self.apply_follower_events(&qh, batch).await?;
            }
            *apply_state = false;
        } else if !parallel {
            // A rejected serial preflight did not issue the dependent event
            // append. Errors and cancellation return above with the flag set.
            *apply_state = false;
        }
        Ok(ReplicatedStreamApplyOutcome {
            message_log,
            event_log,
        })
    }
}

/// Which follower apply entry point is running. Each refuses the other
/// partition kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowerApplyTarget {
    Queue,
    Stream,
}

impl FollowerApplyTarget {
    fn check_kind(self, qh: &QueueHandleInner) -> Result<()> {
        match self {
            Self::Queue => {
                qh.work_queue()?;
            }
            Self::Stream => {
                qh.stream()?;
            }
        }
        Ok(())
    }
}

/// A queue partition has no separate message log, so a queue batch yields
/// exactly one event-log outcome.
fn single_queue_log_outcome(outcome: ReplicatedStreamApplyOutcome) -> Result<ReplicatedAppendOutcome> {
    match outcome {
        ReplicatedStreamApplyOutcome { message_log: None, event_log: Some(outcome) } => Ok(outcome),
        other => Err(StromaError::Internal(format!(
            "queue log apply produced unexpected outcomes: {other:?}"
        ))),
    }
}

fn local_tails(qh: &QueueHandleInner) -> Result<LocalTails> {
    let log_next = qh.event_log().next_offset();
    let applied_upto = qh.applied_upto().load(Ordering::Acquire);
    let applied_next = match qh.ordered_applied_next()? {
        Some(next) => next,
        None if log_next == 0 => 0,
        // Without an exact frontier the handle only tracks the last applied
        // offset, so an empty application reads as offset zero applied.
        None => applied_upto.saturating_add(1).min(log_next),
    };
    Ok(LocalTails {
        record_next: qh.optional_message_log().map(|log| log.next_offset()),
        log_next,
        applied_next,
        applied_offset: (log_next != 0).then_some(applied_upto),
    })
}

fn queue_promotion(outcome: LocalPromotion) -> QueuePromotionOutcome {
    match outcome {
        LocalPromotion::Promoted(tails) => QueuePromotionOutcome::Promoted { next: tails.log_next },
        LocalPromotion::LogBehind { local_next, expected_next } => {
            QueuePromotionOutcome::LogBehind { local_next, expected_next }
        }
        LocalPromotion::LogAhead { local_next, expected_next } => {
            QueuePromotionOutcome::LogAhead { local_next, expected_next }
        }
        LocalPromotion::NotApplied(tails) => QueuePromotionOutcome::NotApplied {
            applied_next: tails.applied_next,
            log_next: tails.log_next,
        },
        LocalPromotion::PayloadsMissing { log_next, required_next } => {
            QueuePromotionOutcome::PayloadsMissing { log_next, required_next }
        }
    }
}
