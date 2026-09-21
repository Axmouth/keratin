//! Deterministic, clock-free reconstruction of fully retained queue histories.
//! Only the sealed inspector can finish this evidence: it first verifies every
//! transferred record against the resource-bound receipt. No actor is queried.

use crate::recovery_inspection::{decode_evidence_event, hash_record};
use crate::{QueueInternalState, RecoveryRecord, RetainedHistoryIdentity, StromaEvent};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy)]
pub struct RecoveryReplayLimits {
    /// Bound semantic operations, including every entry in batched events.
    /// State, temporary sorting space and unresolved dependencies are O(this).
    pub operations_per_replica: u64,
}
impl Default for RecoveryReplayLimits {
    fn default() -> Self {
        Self {
            operations_per_replica: 1_000_000,
        }
    }
}

/// Evidence at an exclusive event frontier, reconstructed from event zero.
/// Equal `state_digest` values alone neither establish ancestry nor preserve
/// payload identity. Compare input history and establish lineage separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryQueueReplayEvidence {
    pub version: u32,
    pub event_next: u64,
    pub required_message_next: u64,
    pub state_digest: [u8; 32],
    pub event_prefix_digest: [u8; 32],
    pub message_digest: [u8; 32],
    pub message_next: u64,
    pub history_id: [u8; 32],
    pub operations: u64,
}

pub(crate) struct QueueReplay {
    state: QueueInternalState,
    history: RetainedHistoryIdentity,
    target: u64,
    next: u64,
    remaining: u64,
    operations: u64,
    missing_enqueues: BTreeSet<u64>,
    prefix_hash: blake3::Hasher,
}
impl QueueReplay {
    pub(crate) fn new(
        topic: &str,
        partition: u32,
        history: RetainedHistoryIdentity,
        target: u64,
        limits: RecoveryReplayLimits,
    ) -> Result<Self, String> {
        if history.event_head != 0 || history.message_head != 0 {
            return Err("recovery replay needs a proven checkpoint for compacted history".into());
        }
        if target > history.event_next || limits.operations_per_replica == 0 {
            return Err("invalid recovery replay target or operation budget".into());
        }
        let mut prefix_hash = blake3::Hasher::new();
        prefix_hash.update(b"fibril-retained-log-v1\0");
        prefix_hash.update(&0u64.to_be_bytes());
        prefix_hash.update(&target.to_be_bytes());
        Ok(Self {
            state: QueueInternalState::new(topic.to_string(), partition),
            history,
            target,
            next: 0,
            remaining: limits.operations_per_replica,
            operations: 0,
            missing_enqueues: BTreeSet::new(),
            prefix_hash,
        })
    }

    pub(crate) fn apply(&mut self, record: &RecoveryRecord) -> Result<(), String> {
        // The enclosing inspector verifies the rest of the sealed log too.
        if record.offset >= self.target {
            return Ok(());
        }
        if record.offset != self.next {
            return Err("noncontiguous recovery state replay".into());
        }
        let ev = decode_evidence_event(record)
            .map_err(|gap| format!("recovery replay event {}: {gap:?}", record.offset))?;
        let refs = ev.referenced_msg_offsets();
        let work = 1 + refs.len() as u64;
        if work > self.remaining {
            return Err("recovery replay operation budget exhausted".into());
        }
        self.remaining -= work;
        self.operations += work;
        let enqueue = matches!(
            ev,
            StromaEvent::Enqueue { .. }
                | StromaEvent::EnqueueMany { .. }
                | StromaEvent::EnqueueDelayed { .. }
                | StromaEvent::EnqueueDelayedMany { .. }
        );
        let cancel = matches!(ev, StromaEvent::CancelEnqueueMany { .. });
        for off in refs {
            if off == u64::MAX {
                return Err("recovery replay message offset overflows".into());
            }
            if cancel {
                self.missing_enqueues.remove(&off);
            } else if off >= self.history.message_next {
                if enqueue {
                    // Parallel log append may leave a cancelled, never-confirmed
                    // enqueue. It must be cancelled by this exact target.
                    self.missing_enqueues.insert(off);
                } else {
                    return Err(format!(
                        "recovery replay event {} depends on missing message {off}",
                        record.offset
                    ));
                }
            }
        }
        // Same state operations used by the actor, without delivery, clocks,
        // external DLQ copies, priority scheduling or snapshot mutations.
        match ev {
            StromaEvent::Enqueue {
                off,
                retries,
                expire_at,
            } => self.state.enqueue(off, retries, expire_at),
            StromaEvent::EnqueueMany { reqs } => self.state.enqueue_many(&reqs),
            StromaEvent::EnqueueDelayed { off, not_before } => {
                self.state.enqueue_delayed(off, not_before)
            }
            StromaEvent::EnqueueDelayedMany { reqs } => self.state.enqueue_delayed_many(&reqs),
            StromaEvent::CancelEnqueueMany { offs } => self.state.cancel_enqueue_many(&offs),
            StromaEvent::MarkInflight { off, deadline } => {
                self.state.mark_inflight(off, deadline);
            }
            StromaEvent::MarkInflightMany { reqs } => self.state.mark_inflight_many(&reqs),
            StromaEvent::Ack { off } => self.state.ack(off),
            StromaEvent::AckMany { reqs } => self.state.ack_many(&reqs),
            StromaEvent::ReleaseInflightMany { reqs } => {
                self.state.release_inflight_many(&reqs);
            }
            StromaEvent::Nack { off, requeue } => {
                self.state.nack_at(off, requeue, None);
            }
            StromaEvent::NackMany { reqs } => {
                self.state.nack_many(&reqs);
            }
            StromaEvent::DeadLetter { reqs } => {
                self.state
                    .mark_pending_dlq_many(&reqs.iter().map(|r| r.off).collect::<Vec<_>>());
            }
            StromaEvent::DeadLetterCommit { offs } => {
                for off in offs {
                    self.state.commit_dlq(off);
                }
            }
            StromaEvent::Declare(meta) => self.state.apply_declare(&meta),
            // ResetQueue removes the actor/registry entry; it does not simply
            // reset this state. Embedded snapshots are unsupported by replay.
            StromaEvent::ResetQueue { .. }
            | StromaEvent::Snapshot { .. }
            | StromaEvent::CursorCommit { .. }
            | StromaEvent::CursorCommitBatch { .. }
            | StromaEvent::StreamTruncate { .. } => {
                return Err(format!(
                    "unsupported recovery queue replay event {}",
                    record.offset
                ));
            }
        }
        hash_record(&mut self.prefix_hash, record);
        self.next += 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RecoveryQueueReplayEvidence, String> {
        if self.next != self.target {
            return Err("incomplete recovery state replay".into());
        }
        if let Some(off) = self.missing_enqueues.first() {
            return Err(format!(
                "recovery replay target {} has uncancelled missing message {off}",
                self.target
            ));
        }
        let required_message_next = self.state.required_message_next();
        if required_message_next > self.history.message_next {
            return Err("recovery state exceeds verified payload coverage".into());
        }
        Ok(RecoveryQueueReplayEvidence {
            version: 1,
            event_next: self.target,
            required_message_next,
            state_digest: self.state.recovery_state_digest(),
            event_prefix_digest: *self.prefix_hash.finalize().as_bytes(),
            message_digest: self.history.message_digest,
            message_next: self.history.message_next,
            history_id: self.history.id,
            operations: self.operations,
        })
    }
}
