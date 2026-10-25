//! Deterministic, clock-free reconstruction of fully retained queue histories.
//! The sealed inspector finishes this evidence after verifying every
//! transferred record against the resource-bound receipt. A completed local phase
//! can restore the result without repeating replay. No actor is queried.

use crate::recovery_budget::{RecoveryBudget as Budget, RecoveryBudgetExceeded as Limit};
use crate::recovery_inspection::{decode_evidence_semantics, hash_record};
use crate::{
    QueueInternalState, RecoveryRecord, RetainedHistoryIdentity, RetainedLog, StromaEvent,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

/// Evidence at an exclusive applied boundary of one queue log, reconstructed
/// from offset zero or a verified checkpoint. Equal `state_digest` values alone
/// neither establish ancestry nor preserve payload identity. Compare input
/// history and establish lineage separately.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryQueueReplayEvidence {
    /// Exact retained queue log carrying payloads and state events. A state
    /// artifact does not authorize truncating this coverage.
    pub log: RetainedLog,
    pub version: u32,
    /// Exclusive applied boundary of the replay target.
    pub next: u64,
    /// Exclusive bound of payloads the replayed state still references.
    pub required_next: u64,
    pub state_digest: [u8; 32],
    pub event_prefix_digest: [u8; 32],
    pub history_id: [u8; 32],
    pub operations: u64,
    /// Starting boundary of a verified version-two snapshot, if used. These
    /// identify evidence, not an authoritative or quorum-installed baseline.
    pub checkpoint_next: Option<u64>,
    pub checkpoint_digest: Option<[u8; 32]>,
    pub lease_normalized_state_digest: [u8; 32],
    /// Exact offsets and content of live payloads at the target. Present only
    /// when message records were verified after state reconstruction.
    pub live_payload_digest: Option<[u8; 32]>,
}

/// A verified, deterministic queue-state artifact. It supplies bytes for a
/// future installation, not source-selection, lineage or activation authority.
/// Construction requires completed sealed log and live-payload verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryQueueStateArtifact {
    pub(crate) evidence: RecoveryQueueReplayEvidence,
    pub(crate) state_snapshot: Vec<u8>,
    pub(crate) snapshot_digest: [u8; 32],
}
/// Payload-free result of a verified artifact. Source selection can retain
/// summaries for every witness while snapshot bytes stay in the local phase store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryQueueStateSummary {
    evidence: RecoveryQueueReplayEvidence,
    snapshot_digest: [u8; 32],
    snapshot_bytes: u64,
}
impl RecoveryQueueStateSummary {
    pub fn evidence(&self) -> &RecoveryQueueReplayEvidence {
        &self.evidence
    }
    pub fn snapshot_digest(&self) -> [u8; 32] {
        self.snapshot_digest
    }
    pub fn snapshot_bytes(&self) -> u64 {
        self.snapshot_bytes
    }
}

impl RecoveryQueueStateArtifact {
    pub fn summary(&self) -> RecoveryQueueStateSummary {
        RecoveryQueueStateSummary {
            evidence: self.evidence.clone(),
            snapshot_digest: self.snapshot_digest,
            snapshot_bytes: self.state_snapshot.len() as u64,
        }
    }

    pub fn evidence(&self) -> &RecoveryQueueReplayEvidence {
        &self.evidence
    }
    pub fn state_snapshot(&self) -> &[u8] {
        &self.state_snapshot
    }
    pub fn snapshot_digest(&self) -> [u8; 32] {
        self.snapshot_digest
    }
}

pub(crate) struct QueueReplay {
    state: QueueInternalState,
    history: RetainedHistoryIdentity,
    log: RetainedLog,
    target: u64,
    next: u64,
    remaining: u64,
    operations: u64,
    missing_enqueues: BTreeSet<u64>,
    prefix_hash: blake3::Hasher,
    checkpoint_event_next: Option<u64>,
    live_ranges: Option<rangemap::RangeSet<u64>>,
    live_hash: blake3::Hasher,
    live_seen: u64,
    verify_live: bool,
    state_entry_limit: Option<u64>,
}
impl QueueReplay {
    pub(crate) fn new(
        topic: &str,
        partition: u32,
        history: RetainedHistoryIdentity,
        target: u64,
        limits: RecoveryReplayLimits,
    ) -> Result<Self, String> {
        let log = *history
            .queue_log()
            .ok_or("queue replay requires a queue history")?;
        if log.head != 0 {
            return Err("recovery replay needs a proven checkpoint for compacted history".into());
        }
        if !history.valid_version() || target > log.next || limits.operations_per_replica == 0 {
            return Err("invalid recovery replay target or operation budget".into());
        }
        let mut prefix_hash = blake3::Hasher::new();
        prefix_hash.update(b"fibril-retained-log-v1\0");
        prefix_hash.update(&0u64.to_be_bytes());
        prefix_hash.update(&target.to_be_bytes());
        Ok(Self {
            state: QueueInternalState::new(topic.to_string(), partition),
            history,
            log,
            target,
            next: 0,
            remaining: limits.operations_per_replica,
            operations: 0,
            missing_enqueues: BTreeSet::new(),
            prefix_hash,
            checkpoint_event_next: None,
            live_ranges: None,
            live_hash: live_hasher(),
            live_seen: 0,
            verify_live: false,
            state_entry_limit: None,
        })
    }

    pub(crate) fn from_checkpoint(
        topic: &str,
        partition: u32,
        history: RetainedHistoryIdentity,
        target: u64,
        limits: RecoveryReplayLimits,
        envelope: &[u8],
    ) -> Result<Self, String> {
        // The enclosing inspector verifies the receipt's resource binding.
        let log = *history
            .queue_log()
            .ok_or("queue replay requires a queue history")?;
        if !history.valid_version()
            || history.snapshot_digest != Some(*blake3::hash(envelope).as_bytes())
        {
            return Err("checkpoint differs from sealed snapshot digest".into());
        }
        if envelope.len() < 28
            || &envelope[..8] != b"SSNAP\0\0\0"
            || envelope[8..12] != [0, 2, 0, 0]
        {
            return Err("checkpoint replay requires an exact version-two envelope".into());
        }
        let next = u64::from_be_bytes(envelope[12..20].try_into().unwrap());
        let len = u32::from_be_bytes(envelope[20..24].try_into().unwrap()) as usize;
        if len.checked_add(28) != Some(envelope.len())
            || crc32c::crc32c(&envelope[8..envelope.len() - 4])
                != u32::from_be_bytes(envelope[envelope.len() - 4..].try_into().unwrap())
            || next < log.head
            || next > target
            || target > log.next
        {
            return Err("checkpoint envelope or replay coverage is invalid".into());
        }
        // Byte charging bounds both decode work and the number of encoded state
        // entries; ranges stay compressed and are never expanded into offsets.
        let cost = len as u64;
        if cost > limits.operations_per_replica {
            return Err(Limit::message(
                Budget::ReplayOperations,
                limits.operations_per_replica,
                0,
                cost,
            ));
        }
        let mut state = QueueInternalState::new(topic.into(), partition);
        let meta = state
            .load_snapshot(&envelope[24..24 + len])
            .map_err(|e| e.to_string())?;
        if meta.last_snapshot_event_offset != next.saturating_sub(1)
            || state.required_message_next() > next
        {
            return Err("checkpoint state/envelope boundary mismatch".into());
        }
        let mut prefix_hash = blake3::Hasher::new();
        prefix_hash.update(b"fibril-checkpoint-event-suffix-v1\0");
        prefix_hash.update(history.snapshot_digest.as_ref().unwrap());
        prefix_hash.update(&next.to_be_bytes());
        prefix_hash.update(&target.to_be_bytes());
        Ok(Self {
            state,
            history,
            log,
            target,
            next,
            remaining: limits.operations_per_replica - cost,
            operations: cost,
            missing_enqueues: BTreeSet::new(),
            prefix_hash,
            checkpoint_event_next: Some(next),
            live_ranges: None,
            live_hash: live_hasher(),
            live_seen: 0,
            verify_live: true,
            state_entry_limit: None,
        })
    }

    /// Historical work may span arbitrarily many bounded pages. Admission is
    /// instead based on the state retained by replay, plus one bounded event.
    pub(crate) fn enable_incremental_work(&mut self, max_entries: u64) -> Result<(), String> {
        self.state_entry_limit = Some(max_entries);
        self.remaining = u64::MAX - self.operations;
        self.check_retained_state()
    }

    fn check_retained_state(&self) -> Result<(), String> {
        if let Some(limit) = self.state_entry_limit {
            let entries = self
                .state
                .recovery_retained_entries()
                .checked_add(self.missing_enqueues.len() as u64)
                .ok_or("replay state entry overflow")?;
            if entries > limit {
                return Err(Limit::message(
                    Budget::ReplayStateEntries,
                    limit,
                    0,
                    entries,
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn verify_live_payloads(&mut self) {
        self.verify_live = true;
    }

    pub(crate) fn message(&mut self, record: &RecoveryRecord) -> Result<(), String> {
        if !self.verify_live {
            return Ok(());
        }
        if self.next != self.target {
            return Err("payload check precedes completed state replay".into());
        }
        let live = self
            .live_ranges
            .get_or_insert_with(|| self.state.recovery_live_ranges());
        if live.contains(&record.offset) {
            if !matches!(
                decode_evidence_semantics(record),
                Ok(StromaEvent::PayloadEnqueue { .. })
            ) {
                return Err("live payload points to a non-payload combined record".into());
            }
            hash_record(&mut self.live_hash, record);
            self.live_seen += 1;
        }
        Ok(())
    }

    pub(crate) fn apply(&mut self, record: &RecoveryRecord) -> Result<(), String> {
        // The enclosing inspector verifies the rest of the sealed log too.
        if record.offset >= self.target
            || self
                .checkpoint_event_next
                .is_some_and(|n| record.offset < n)
        {
            return Ok(());
        }
        if record.offset != self.next {
            return Err("noncontiguous recovery state replay".into());
        }
        let ev = decode_evidence_semantics(record)
            .map_err(|gap| format!("recovery replay event {}: {gap:?}", record.offset))?;
        let payload = matches!(ev, StromaEvent::PayloadEnqueue { .. });
        if !payload
            && ev
                .max_combined_referenced_offset()
                .is_some_and(|off| off >= record.offset)
        {
            return Err("combined recovery event has a forward payload reference".into());
        }
        let refs = ev.referenced_msg_offsets();
        let work = 1
            + refs.len() as u64
            + match &ev {
                StromaEvent::ActivateDelayed { max, .. } => *max as u64,
                _ => 0,
            };
        if work > self.remaining {
            return Err(Limit::message(
                Budget::ReplayOperations,
                self.operations + self.remaining,
                self.operations,
                work,
            ));
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
            } else if off >= self.log.next {
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
        let ev = ev.into_record_state(record.offset);
        match ev {
            StromaEvent::PayloadEnqueue { .. } => {
                unreachable!("payload event converted to state operation")
            }
            StromaEvent::ActivateDelayed { now, max } => {
                self.state.activate_delayed(now, max as usize);
            }
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
        // A state event occupies a log position that carries no payload.
        if !payload {
            self.state.ack(record.offset);
        }
        self.check_retained_state()?;
        hash_record(&mut self.prefix_hash, record);
        self.next += 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RecoveryQueueReplayEvidence, String> {
        self.finish_parts().map(|(evidence, _)| evidence)
    }

    pub(crate) fn finish_artifact(
        self,
        max_bytes: usize,
    ) -> Result<RecoveryQueueStateArtifact, String> {
        if !self.verify_live
            || max_bytes == 0
            || max_bytes > crate::recovery_snapshot::MAX_RECOVERY_SNAPSHOT_BYTES
        {
            return Err(
                "artifact requires live payload verification and a bounded output limit".into(),
            );
        }
        let (topic, partition) = self.state.recovery_resource();
        let (evidence, state) = self.finish_parts()?;
        let state_snapshot = state.into_recovery_snapshot(evidence.next);
        if state_snapshot.len() > max_bytes {
            return Err(Limit::message(
                Budget::ArtifactBytes,
                max_bytes as u64,
                0,
                state_snapshot.len() as u64,
            ));
        }
        // Confirm the existing snapshot codec preserves the exact projected
        // state, including optional values that might otherwise be normalized.
        let mut decoded = QueueInternalState::new(topic, partition);
        let meta = decoded
            .load_snapshot(&state_snapshot)
            .map_err(|e| e.to_string())?;
        if meta.last_snapshot_event_offset != evidence.next.saturating_sub(1)
            || decoded.recovery_state_digest() != evidence.lease_normalized_state_digest
        {
            return Err("recovery snapshot does not preserve the verified projected state".into());
        }
        Ok(RecoveryQueueStateArtifact {
            evidence,
            snapshot_digest: *blake3::hash(&state_snapshot).as_bytes(),
            state_snapshot,
        })
    }

    fn finish_parts(self) -> Result<(RecoveryQueueReplayEvidence, QueueInternalState), String> {
        if self.next != self.target {
            return Err("incomplete recovery state replay".into());
        }
        if let Some(off) = self.missing_enqueues.first() {
            return Err(format!(
                "recovery replay target {} has uncancelled missing message {off}",
                self.target
            ));
        }
        let required_next = self.state.required_message_next();
        let payload_head = self.log.head;
        let payload_next = self.log.next.min(self.target);
        if required_next > payload_next {
            return Err("recovery state exceeds verified payload coverage".into());
        }
        let live_payload_digest = if self.verify_live {
            let live = self
                .live_ranges
                .unwrap_or_else(|| self.state.recovery_live_ranges());
            let mut count = 0u64;
            for range in live.iter() {
                if range.start < payload_head || range.end > payload_next {
                    return Err("live checkpoint payload is outside retained coverage".into());
                }
                count = count
                    .checked_add(range.end - range.start)
                    .ok_or("live payload count overflow")?;
            }
            if self.live_seen != count {
                return Err("live checkpoint payloads were not all verified".into());
            }
            Some(*self.live_hash.finalize().as_bytes())
        } else {
            None
        };
        let evidence = RecoveryQueueReplayEvidence {
            log: self.log,
            version: 2,
            next: self.target,
            required_next,
            state_digest: self.state.recovery_state_digest(),
            event_prefix_digest: *self.prefix_hash.finalize().as_bytes(),
            history_id: self.history.id,
            operations: self.operations,
            checkpoint_next: self.checkpoint_event_next,
            checkpoint_digest: self.checkpoint_event_next.and(self.history.snapshot_digest),
            lease_normalized_state_digest: self.state.recovery_lease_normalized_digest(),
            live_payload_digest,
        };
        Ok((evidence, self.state))
    }
}

fn live_hasher() -> blake3::Hasher {
    let mut h = blake3::Hasher::new();
    h.update(b"fibril-recovery-live-payloads-v1\0");
    h
}
