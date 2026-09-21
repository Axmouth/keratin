//! Explicit diagnostics over sealed retained histories. No source selection or
//! activation authority is produced, including when every shared record matches.

use crate::{
    RecoveryReadPage, RecoveryReadRequest, RecoveryReadSource, RecoveryRecord,
    RetainedHistoryIdentity, SealedReplicaFrontiers, StromaEvent,
};
use std::collections::BTreeMap;
use crate::recovery_replay::{QueueReplay, RecoveryQueueReplayEvidence, RecoveryQueueStateArtifact, RecoveryReplayLimits};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySide {
    Left,
    Right,
}
impl RecoverySide {
    fn index(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }
    fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryInspectionLimits {
    pub page_records: u32,
    pub page_bytes: u32,
    pub total_pages: u64,
    pub total_records: u64,
    /// Canonical record wire bytes (18 bytes of metadata plus headers/payload).
    pub total_bytes: u64,
}
impl Default for RecoveryInspectionLimits {
    fn default() -> Self {
        Self {
            page_records: 256,
            page_bytes: 1024 * 1024,
            total_pages: 1024,
            total_records: 1_000_000,
            total_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRecordDifference {
    pub offset: u64,
    pub left_id: [u8; 32],
    pub right_id: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOverlap {
    /// Empty/disjoint ranges supply no shared-record evidence.
    NoSharedRecords,
    Matching {
        from: u64,
        next: u64,
    },
    Divergent {
        from: u64,
        next: u64,
        first: RecoveryRecordDifference,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryLogInspection {
    pub left_range: (u64, u64),
    pub right_range: (u64, u64),
    pub overlap: RecoveryOverlap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReferenceGap {
    pub event_offset: u64,
    pub message_offset: u64,
    /// Includes every reference in this event, including an entire enqueue batch.
    /// None means an offset was u64::MAX and has no representable exclusive end.
    pub event_required_message_next: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySemanticGap {
    UnknownOrMalformedEncoding,
    EventEntryLimit,
    StateReplayRequired,
    CheckpointRequired,
    StreamStateRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryReferenceInspection {
    pub checked_events: u64,
    pub reference_count: u64,
    pub required_message_next: u64,
    pub offset_overflow: bool,
    pub first_outside_retained: Option<RecoveryReferenceGap>,
    pub first_semantic_gap: Option<(u64, RecoverySemanticGap)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryProofRequirement {
    CommonOriginAndInstalledLineage,
    StateReplayAndDependencies,
    CompactedPrefix,
    CheckpointCoverage,
}

/// All fields describe retained evidence only. There is deliberately no
/// compatible/eligible/selected/promotable flag. Even all-empty or equal records
/// require common-origin, state, checkpoint and fresh activation proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPairInspection {
    pub transition: [u8; 32],
    pub history_ids: [[u8; 32]; 2],
    pub messages: RecoveryLogInspection,
    pub events: RecoveryLogInspection,
    pub references: [RecoveryReferenceInspection; 2],
    /// Reported digests only: snapshot state/lineage has NOT been interpreted.
    pub snapshot_digests: [Option<[u8; 32]>; 2],
    /// Optional reconstruction from event zero at one common exclusive target.
    /// Existing snapshot bytes are not used as a trusted replay baseline.
    pub queue_replay: Option<[RecoveryQueueReplayEvidence; 2]>,
    pub remaining_proofs: Vec<RecoveryProofRequirement>,
    pub pages: u64,
    pub records: u64,
    pub bytes: u64,
}

struct Cursor {
    next: u64,
    end: u64,
    digest: [u8; 32],
    hash: blake3::Hasher,
    done: bool,
}
impl Cursor {
    fn new(head: u64, end: u64, digest: [u8; 32]) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-retained-log-v1\0");
        hash.update(&head.to_be_bytes());
        hash.update(&end.to_be_bytes());
        Self {
            next: head,
            end,
            digest,
            hash,
            done: false,
        }
    }
}
struct LogPair {
    cursors: [Cursor; 2],
    ranges: [(u64, u64); 2],
    overlap: (u64, u64),
    pending: BTreeMap<u64, [u8; 32]>,
    pending_side: RecoverySide,
    first_difference: Option<RecoveryRecordDifference>,
}
impl LogPair {
    fn new(left: (u64, u64, [u8; 32]), right: (u64, u64, [u8; 32])) -> Self {
        Self {
            cursors: [
                Cursor::new(left.0, left.1, left.2),
                Cursor::new(right.0, right.1, right.2),
            ],
            ranges: [(left.0, left.1), (right.0, right.1)],
            overlap: (left.0.max(right.0), left.1.min(right.1)),
            pending: BTreeMap::new(),
            pending_side: RecoverySide::Left,
            first_difference: None,
        }
    }
    fn next_side(&self) -> Option<RecoverySide> {
        if !self.pending.is_empty() {
            return Some(self.pending_side.other());
        }
        match (self.cursors[0].done, self.cursors[1].done) {
            (true, true) => None,
            (false, true) => Some(RecoverySide::Left),
            (true, false) => Some(RecoverySide::Right),
            (false, false) => Some(if self.cursors[0].next <= self.cursors[1].next {
                RecoverySide::Left
            } else {
                RecoverySide::Right
            }),
        }
    }
    fn result(self) -> RecoveryLogInspection {
        let overlap = if self.overlap.0 >= self.overlap.1 {
            RecoveryOverlap::NoSharedRecords
        } else if let Some(first) = self.first_difference {
            RecoveryOverlap::Divergent {
                from: self.overlap.0,
                next: self.overlap.1,
                first,
            }
        } else {
            RecoveryOverlap::Matching {
                from: self.overlap.0,
                next: self.overlap.1,
            }
        };
        RecoveryLogInspection {
            left_range: self.ranges[0],
            right_range: self.ranges[1],
            overlap,
        }
    }
}

/// Streaming, budgeted pair inspection. Fetch only `next_read()` and pass its
/// reply to `accept_page()`. At most one page's unmatched record IDs is retained;
/// message bodies are never stored in the inspector or diagnostic report.
pub struct RecoveryPairInspector {
    seals: [SealedReplicaFrontiers; 2],
    limits: RecoveryInspectionLimits,
    logs: [LogPair; 2],
    references: [RecoveryReferenceInspection; 2],
    stream: bool,
    pages: u64,
    records: u64,
    bytes: u64,
    failed: bool,
    resource: (String, u32),
    replay: Option<[QueueReplay; 2]>,
    checkpoint_replay: Option<CheckpointReplayPlan>,
}

struct CheckpointReplayPlan {
    target: u64,
    limits: RecoveryReplayLimits,
    max_bytes: u32,
    buffers: [Vec<u8>; 2],
    ends: [Option<u64>; 2],
    done: [bool; 2],
}

pub(crate) fn hash_record(hash: &mut blake3::Hasher, record: &RecoveryRecord) {
    hash.update(&record.offset.to_be_bytes());
    hash.update(&record.flags.to_be_bytes());
    hash.update(&(record.headers.len() as u64).to_be_bytes());
    hash.update(&record.headers);
    hash.update(&(record.payload.len() as u64).to_be_bytes());
    hash.update(&record.payload);
}
fn record_id(record: &RecoveryRecord) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-recovery-record-v1\0");
    hash_record(&mut hash, record);
    *hash.finalize().as_bytes()
}
fn bounds(h: &RetainedHistoryIdentity, events: bool) -> (u64, u64, [u8; 32]) {
    if events {
        (h.event_head, h.event_next, h.event_digest)
    } else {
        (h.message_head, h.message_next, h.message_digest)
    }
}

impl RecoveryPairInspector {
    pub fn new(
        topic: &str,
        partition: u32,
        group: Option<&str>,
        stream: bool,
        left: SealedReplicaFrontiers,
        right: SealedReplicaFrontiers,
        limits: RecoveryInspectionLimits,
    ) -> Result<Self, String> {
        if limits.page_records == 0
            || limits.page_records > 4096
            || limits.page_bytes == 0
            || limits.page_bytes > 16 * 1024 * 1024
            || limits.total_pages == 0
            || limits.total_records == 0
            || limits.total_bytes == 0
        {
            return Err("invalid recovery inspection limits".into());
        }
        if left.request != right.request {
            return Err("recovery inspection requires one exact seal transition".into());
        }
        for seal in [&left, &right] {
            let history = &seal.history;
            if !history.valid_version()
                || history.message_head > history.message_next
                || history.event_head > history.event_next
                || (
                    seal.message_head,
                    seal.message_next,
                    seal.event_head,
                    seal.event_next,
                ) != (
                    history.message_head,
                    history.message_next,
                    history.event_head,
                    history.event_next,
                )
            {
                return Err("malformed recovery history bounds or version".into());
            }
            // Bind a supplied receipt to this resource, not just the caller's
            // alleged offsets. Same-named incarnations still need lineage proof.
            let mut identity = history.clone();
            identity.id = [0; 32];
            let encoded = rmp_serde::to_vec_named(&(topic, partition, group, stream, &identity))
                .map_err(|_| "cannot encode recovery identity")?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"fibril-retained-history-v1\0");
            hash.update(&encoded);
            if hash.finalize().as_bytes() != &history.id {
                return Err("recovery history does not identify requested resource".into());
            }
        }
        Ok(Self {
            logs: [
                LogPair::new(bounds(&left.history, false), bounds(&right.history, false)),
                LogPair::new(bounds(&left.history, true), bounds(&right.history, true)),
            ],
            seals: [left, right],
            limits,
            references: Default::default(),
            stream,
            pages: 0,
            records: 0,
            bytes: 0,
            failed: false,
            resource: (topic.to_string(), partition),
            replay: None,
            checkpoint_replay: None,
        })
    }
    /// Request deterministic queue reconstruction in addition to retained-log
    /// comparison. This must precede all reads. The target is common to both
    /// sources and may be zero; all sealed records are still verified, including
    /// records after the target. No authority or activation proof is implied.
    pub fn with_queue_replay(mut self, event_next: u64, limits: RecoveryReplayLimits) -> Result<Self, String> {
        if self.stream || self.pages != 0 || self.failed || self.replay.is_some() || self.checkpoint_replay.is_some() {
            return Err("queue replay must be configured once before queue inspection".into());
        }
        self.replay = Some([
            QueueReplay::new(&self.resource.0, self.resource.1, self.seals[0].history.clone(), event_next, limits)?,
            QueueReplay::new(&self.resource.0, self.resource.1, self.seals[1].history.clone(), event_next, limits)?,
        ]);
        Ok(self)
    }

    /// Reconstruct each side from its own exact checkpoint (or event zero when
    /// no checkpoint exists). Snapshot pages and retained records are verified
    /// against the resource-bound seals. This produces diagnostic evidence;
    /// authority, timer transitions and installed lineage still require proof.
    pub fn with_queue_checkpoint_replay(mut self, event_next: u64, limits: RecoveryReplayLimits, max_checkpoint_bytes: u32) -> Result<Self, String> {
        if self.stream || self.pages != 0 || self.failed || self.replay.is_some() || self.checkpoint_replay.is_some()
            || max_checkpoint_bytes == 0 || max_checkpoint_bytes > 16 * 1024 * 1024
        { return Err("invalid checkpoint replay configuration".into()); }
        self.checkpoint_replay = Some(CheckpointReplayPlan {
            target: event_next, limits, max_bytes: max_checkpoint_bytes,
            buffers: Default::default(), ends: [None; 2],
            done: self.seals.each_ref().map(|s| s.history.snapshot_digest.is_none()),
        });
        self.initialize_checkpoint_replay()?;
        Ok(self)
    }

    fn initialize_checkpoint_replay(&mut self) -> Result<(), String> {
        let Some(plan) = &mut self.checkpoint_replay else { return Ok(()); };
        if !plan.done.iter().all(|done| *done) { return Ok(()); }
        let build = |i: usize| -> Result<QueueReplay, String> {
            let mut replay = if self.seals[i].history.snapshot_digest.is_some() {
                QueueReplay::from_checkpoint(&self.resource.0, self.resource.1, self.seals[i].history.clone(), plan.target, plan.limits, &plan.buffers[i])?
            } else {
                QueueReplay::new(&self.resource.0, self.resource.1, self.seals[i].history.clone(), plan.target, plan.limits)?
            };
            replay.verify_live_payloads();
            Ok(replay)
        };
        self.replay = Some([build(0)?, build(1)?]);
        plan.buffers = Default::default(); // release raw snapshots after decoding
        Ok(())
    }

    pub fn next_read(&self) -> Result<Option<(RecoverySide, RecoveryReadRequest)>, String> {
        if self.failed {
            return Err("recovery inspection failed; discard partial evidence".into());
        }
        if let Some(plan) = &self.checkpoint_replay {
            for (i, side) in [RecoverySide::Left, RecoverySide::Right].into_iter().enumerate() {
                if !plan.done[i] {
                    if self.pages >= self.limits.total_pages { return Err("recovery inspection page budget exhausted".into()); }
                    return Ok(Some((side, RecoveryReadRequest {
                        seal: self.seals[i].request.clone(), history_id: self.seals[i].history.id,
                        source: RecoveryReadSource::Snapshot, from: plan.buffers[i].len() as u64,
                        max_records: self.limits.page_records,
                        max_bytes: self.limits.page_bytes.min(plan.max_bytes),
                    })));
                }
            }
        }
        // Reconstruct state before streaming live payload identities. Ordinary
        // retained-log inspection preserves its original message-first order.
        let order = if self.checkpoint_replay.is_some() { [1, 0] } else { [0, 1] };
        for index in order {
            let log = &self.logs[index];
            if let Some(side) = log.next_side() {
                if self.pages >= self.limits.total_pages {
                    return Err("recovery inspection page budget exhausted".into());
                }
                let seal = &self.seals[side.index()];
                return Ok(Some((
                    side,
                    RecoveryReadRequest {
                        seal: seal.request.clone(),
                        history_id: seal.history.id,
                        source: if index == 0 {
                            RecoveryReadSource::Messages
                        } else {
                            RecoveryReadSource::Events
                        },
                        from: log.cursors[side.index()].next,
                        max_records: self.limits.page_records,
                        max_bytes: self.limits.page_bytes,
                    },
                )));
            }
        }
        Ok(None)
    }
    pub fn accept_page(
        &mut self,
        side: RecoverySide,
        page: RecoveryReadPage,
    ) -> Result<(), String> {
        let result = self.accept_inner(side, page);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn accept_inner(&mut self, side: RecoverySide, page: RecoveryReadPage) -> Result<(), String> {
        let Some((expected_side, request)) = self.next_read()? else {
            return Err("unexpected page after inspection completed".into());
        };
        let i = side.index();
        if request.source == RecoveryReadSource::Snapshot {
            let plan = self.checkpoint_replay.as_mut().ok_or("unexpected snapshot page")?;
            let bytes = page.snapshot_bytes.len() as u64;
            if side != expected_side || page.history_id != request.history_id
                || page.source != request.source || page.from != request.from
                || !page.records.is_empty() || page.end > plan.max_bytes as u64
                || page.end < 28 || page.next <= page.from || page.next > page.end
                || page.next - page.from != bytes || bytes > request.max_bytes as u64
                || bytes > self.limits.total_bytes - self.bytes
                || plan.ends[i].is_some_and(|end| end != page.end)
            { return Err("checkpoint page identity, range or budget mismatch".into()); }
            plan.ends[i] = Some(page.end);
            plan.buffers[i].extend(page.snapshot_bytes);
            plan.done[i] = page.next == page.end;
            self.pages += 1;
            self.bytes += bytes;
            self.initialize_checkpoint_replay()?;
            return Ok(());
        }
        let log_index = if request.source == RecoveryReadSource::Messages {
            0
        } else {
            1
        };
        let log = &mut self.logs[log_index];
        let cursor = &mut log.cursors[i];
        if side != expected_side
            || page.history_id != request.history_id
            || page.source != request.source
            || page.from != request.from
            || page.end != cursor.end
            || page.next < page.from
            || page.next > page.end
            || page.next - page.from != page.records.len() as u64
            || page.records.len() > request.max_records as usize
            || !page.snapshot_bytes.is_empty()
            || (page.next == page.from && page.next != page.end)
        {
            return Err("recovery inspection page identity, range or progress mismatch".into());
        }
        let mut bytes = 0u64;
        for (n, record) in page.records.iter().enumerate() {
            if record.offset != page.from + n as u64 {
                return Err("noncontiguous recovery inspection page".into());
            }
            bytes = bytes
                .checked_add(18)
                .and_then(|n| n.checked_add(record.headers.len() as u64))
                .and_then(|n| n.checked_add(record.payload.len() as u64))
                .ok_or("recovery inspection byte overflow")?;
        }
        if bytes > request.max_bytes as u64
            || bytes > self.limits.total_bytes - self.bytes
            || page.records.len() as u64 > self.limits.total_records - self.records
        {
            return Err("recovery inspection record/byte budget exhausted".into());
        }
        for record in &page.records {
            hash_record(&mut cursor.hash, record);
            if log_index == 1 {
                self.references[i].inspect(record, &self.seals[i].history, self.stream);
                if let Some(replay) = &mut self.replay {
                    replay[i].apply(record)?;
                }
            } else if let Some(replay) = &mut self.replay {
                replay[i].message(record)?;
            }
            if record.offset >= log.overlap.0 && record.offset < log.overlap.1 {
                let id = record_id(record);
                if let Some(other) = log.pending.remove(&record.offset) {
                    if id != other && log.first_difference.is_none() {
                        let (left_id, right_id) = if side == RecoverySide::Left {
                            (id, other)
                        } else {
                            (other, id)
                        };
                        log.first_difference = Some(RecoveryRecordDifference {
                            offset: record.offset,
                            left_id,
                            right_id,
                        });
                    }
                } else {
                    if !log.pending.is_empty() && log.pending_side != side {
                        return Err("recovery comparison lost aligned offset".into());
                    }
                    log.pending_side = side;
                    log.pending.insert(record.offset, id);
                }
            }
        }
        cursor.next = page.next;
        if cursor.next == cursor.end {
            if cursor.hash.finalize().as_bytes() != &cursor.digest {
                return Err("transferred recovery log does not match sealed digest".into());
            }
            cursor.done = true;
        }
        self.pages += 1;
        self.records += page.records.len() as u64;
        self.bytes += bytes;
        Ok(())
    }
    pub fn finish(self) -> Result<RecoveryPairInspection, String> {
        self.finish_inner(None).map(|(report, _)| report)
    }

    /// Produce deterministic state bytes only after full sealed transfer and
    /// live-payload verification. These artifacts confer no recovery authority.
    pub fn finish_with_queue_artifacts(
        self,
        max_bytes_per_replica: usize,
    ) -> Result<(RecoveryPairInspection, [RecoveryQueueStateArtifact; 2]), String> {
        if self.replay.is_none()
            || max_bytes_per_replica == 0
            || max_bytes_per_replica > 16 * 1024 * 1024
        {
            return Err("queue artifacts require replay and a bounded output limit".into());
        }
        let (report, artifacts) = self.finish_inner(Some(max_bytes_per_replica))?;
        Ok((
            report,
            artifacts.ok_or("queue artifacts were not constructed")?,
        ))
    }

    fn finish_inner(
        self,
        artifact_limit: Option<usize>,
    ) -> Result<
        (
            RecoveryPairInspection,
            Option<[RecoveryQueueStateArtifact; 2]>,
        ),
        String,
    > {
        if self.next_read()?.is_some() || self.logs.iter().any(|log| !log.pending.is_empty()) {
            return Err("incomplete recovery inspection".into());
        }
        let (queue_replay, artifacts) = match (self.replay, artifact_limit) {
            (Some([left, right]), Some(limit)) => {
                let artifacts = [left.finish_artifact(limit)?, right.finish_artifact(limit)?];
                let evidence = artifacts.each_ref().map(|a| a.evidence().clone());
                (Some(evidence), Some(artifacts))
            }
            (Some([left, right]), None) => (Some([left.finish()?, right.finish()?]), None),
            (None, _) => (None, None),
        };
        let mut remaining_proofs = vec![
            RecoveryProofRequirement::CommonOriginAndInstalledLineage,
            RecoveryProofRequirement::StateReplayAndDependencies,
        ];
        if self
            .seals
            .iter()
            .any(|s| s.history.message_head > 0 || s.history.event_head > 0)
        {
            remaining_proofs.push(RecoveryProofRequirement::CompactedPrefix);
        }
        if self
            .seals
            .iter()
            .any(|s| s.history.snapshot_digest.is_some())
        {
            remaining_proofs.push(RecoveryProofRequirement::CheckpointCoverage);
        }
        let [messages, events] = self.logs;
        Ok((
            RecoveryPairInspection {
                transition: self.seals[0].request.transition,
                history_ids: [self.seals[0].history.id, self.seals[1].history.id],
                messages: messages.result(),
                events: events.result(),
                references: self.references,
                snapshot_digests: [
                    self.seals[0].history.snapshot_digest,
                    self.seals[1].history.snapshot_digest,
                ],
                queue_replay,
                remaining_proofs,
                pages: self.pages,
                records: self.records,
                bytes: self.bytes,
            },
            artifacts,
        ))
    }


}

/// Strict proof decoding. Ordinary replay remains forward-compatible, but an
/// evidence consumer must understand every encoded field and bound allocation.
pub(crate) fn decode_evidence_event(record: &RecoveryRecord) -> Result<StromaEvent, RecoverySemanticGap> {
    let payload = &record.payload;
    if payload.len() >= 16 {
        let tag = u16::from_be_bytes(payload[10..12].try_into().unwrap());
        if matches!(tag, 1 | 3 | 4 | 11 | 21 | 22 | 31 | 40 | 41 | 82) {
            let count = u32::from_be_bytes(payload[12..16].try_into().unwrap()) as usize;
            if count > 65_536 || count > payload.len() / 8 {
                return Err(RecoverySemanticGap::EventEntryLimit);
            }
        }
    }
    let event = StromaEvent::decode(payload)
        .map_err(|_| RecoverySemanticGap::UnknownOrMalformedEncoding)?;
    if record.flags != 0 || !record.headers.is_empty()
        || event.encode().ok().as_deref() != Some(payload.as_slice()) {
        return Err(RecoverySemanticGap::UnknownOrMalformedEncoding);
    }
    Ok(event)
}

impl RecoveryReferenceInspection {
    fn gap(&mut self, offset: u64, gap: RecoverySemanticGap) {
        self.first_semantic_gap.get_or_insert((offset, gap));
    }
    fn inspect(
        &mut self,
        record: &RecoveryRecord,
        history: &RetainedHistoryIdentity,
        stream: bool,
    ) {
        self.checked_events += 1;
        let event = match decode_evidence_event(record) {
            Ok(event) => event,
            Err(gap) => {
                self.gap(record.offset, gap);
                return;
            }
        };
        if stream {
            self.gap(record.offset, RecoverySemanticGap::StreamStateRequired);
            return;
        }
        let mut first_missing = None;
        let mut required = Some(0u64);
        let mut reference = |off: u64| {
            self.reference_count += 1;
            match off.checked_add(1) {
                Some(next) => {
                    self.required_message_next = self.required_message_next.max(next);
                    required = required.map(|r| r.max(next));
                }
                None => {
                    self.offset_overflow = true;
                    required = None;
                }
            }
            if off < history.message_head || off >= history.message_next {
                first_missing.get_or_insert(off);
            }
        };
        match event {
            StromaEvent::Enqueue { off, .. } | StromaEvent::EnqueueDelayed { off, .. } => {
                reference(off)
            }
            StromaEvent::EnqueueMany { reqs } => {
                for req in reqs {
                    reference(req.off);
                }
            }
            StromaEvent::EnqueueDelayedMany { reqs } => {
                for req in reqs {
                    reference(req.off);
                }
            }
            // These references constrain history/offset reuse, even when their
            // bodies have legitimately been compacted. State replay must decide.
            StromaEvent::Ack { off }
            | StromaEvent::Nack { off, .. }
            | StromaEvent::MarkInflight { off, .. } => reference(off),
            StromaEvent::AckMany { reqs } | StromaEvent::ReleaseInflightMany { reqs } => {
                for req in reqs {
                    reference(req.off);
                }
            }
            StromaEvent::MarkInflightMany { reqs } => {
                for req in reqs {
                    reference(req.off);
                }
            }
            StromaEvent::NackMany { reqs } => {
                for req in reqs {
                    reference(req.off);
                }
            }
            StromaEvent::DeadLetter { reqs } => {
                for req in reqs {
                    reference(req.off);
                }
            }
            StromaEvent::DeadLetterCommit { offs } => {
                for off in offs {
                    reference(off);
                }
            }
            StromaEvent::CancelEnqueueMany { .. } | StromaEvent::ResetQueue { .. }
            | StromaEvent::ActivateDelayed { .. } => {
                self.gap(record.offset, RecoverySemanticGap::StateReplayRequired)
            }
            StromaEvent::Snapshot { .. } => {
                self.gap(record.offset, RecoverySemanticGap::CheckpointRequired)
            }
            StromaEvent::CursorCommit { .. }
            | StromaEvent::CursorCommitBatch { .. }
            | StromaEvent::StreamTruncate { .. } => {
                self.gap(record.offset, RecoverySemanticGap::StreamStateRequired)
            }
            StromaEvent::Declare(_) => {}
        }
        if let Some(message_offset) = first_missing {
            self.first_outside_retained
                .get_or_insert(RecoveryReferenceGap {
                    event_offset: record.offset,
                    message_offset,
                    event_required_message_next: required,
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EnqueueEventMeta, RecoverySealRequest};


    // Kept with these helpers so sealed page identity and digest validation use
    // exactly the same path as retained-history comparisons.
    include!("recovery_checkpoint_tests.rs");

    #[test]
    fn replay_proves_exact_zero_and_nonzero_boundaries_without_trusting_labels() {
        let msg = messages(0, 2);
        let ev = events(vec![
            StromaEvent::Enqueue { off: 0, retries: 0, expire_at: None },
            StromaEvent::MarkInflight { off: 0, deadline: 500 },
            StromaEvent::Ack { off: 0 },
            StromaEvent::Enqueue { off: 1, retries: 2, expire_at: Some(800) },
        ]);
        for target in 0..=4 {
            let seal = seal(0, &msg, 0, &ev);
            let pair = inspector(seal.clone(), seal, RecoveryInspectionLimits {
                page_records: 1, ..Default::default()
            }).with_queue_replay(target, Default::default()).unwrap();
            let report = run(pair, [[&msg, &ev], [&msg, &ev]]).unwrap();
            let [a, b] = report.queue_replay.unwrap();
            assert_eq!(a, b);
            assert_eq!(a.event_next, target);
            let mut expected = crate::QueueInternalState::new("q".into(), 0);
            if target >= 1 { expected.enqueue(0, 0, None); }
            if target >= 2 { expected.mark_inflight(0, 500); }
            if target >= 3 { expected.ack(0); }
            if target >= 4 { expected.enqueue(1, 2, Some(800)); }
            assert_eq!(a.state_digest, expected.recovery_state_digest());
            assert!(report.remaining_proofs.contains(&RecoveryProofRequirement::CommonOriginAndInstalledLineage));
            // A common target is evidence only for that cut, not either sealed tail.
            assert!(report.remaining_proofs.contains(&RecoveryProofRequirement::StateReplayAndDependencies));
        }
    }

    #[test]
    fn replay_equal_state_does_not_hide_different_payload_history() {
        let a = messages(0, 1);
        let mut b = a.clone();
        b[0].payload = b"different confirmed work".to_vec();
        let ev = events(vec![StromaEvent::Enqueue { off: 0, retries: 0, expire_at: None }, StromaEvent::Ack { off: 0 }]);
        let pair = inspector(seal(0, &a, 0, &ev), seal(0, &b, 0, &ev), Default::default())
            .with_queue_replay(2, Default::default()).unwrap();
        let report = run(pair, [[&a, &ev], [&b, &ev]]).unwrap();
        let [a, b] = report.queue_replay.unwrap();
        assert_eq!(a.state_digest, b.state_digest);
        assert_eq!(a.event_prefix_digest, b.event_prefix_digest);
        assert_ne!(a.message_digest, b.message_digest);
        assert_ne!(a.history_id, b.history_id);
        assert!(matches!(report.messages.overlap, RecoveryOverlap::Divergent { .. }));
    }

    #[test]
    fn replay_requires_whole_enqueue_batch_coverage_or_completed_cancellation() {
        let msg = messages(0, 1);
        let ev = events(vec![
            StromaEvent::EnqueueMany { reqs: vec![
                EnqueueEventMeta { off: 0, retries: 0, expire_at: None },
                EnqueueEventMeta { off: 7, retries: 0, expire_at: None },
            ] },
            StromaEvent::CancelEnqueueMany { offs: vec![7] },
        ]);
        let check = |target| {
            let pair = inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default())
                .with_queue_replay(target, Default::default()).unwrap();
            run(pair, [[&msg, &ev], [&msg, &ev]])
        };
        assert!(check(1).unwrap_err().contains("uncancelled missing message 7"));
        let result = check(2).unwrap().queue_replay.unwrap();
        assert_eq!(result[0].required_message_next, 1);
        // A later ACK of a missing payload must not turn it into valid evidence.
        let ev = events(vec![StromaEvent::Ack { off: 7 }, StromaEvent::CancelEnqueueMany { offs: vec![7] }]);
        let pair = inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default())
            .with_queue_replay(2, Default::default()).unwrap();
        assert!(run(pair, [[&msg, &ev], [&msg, &ev]]).unwrap_err().contains("missing message 7"));
    }

    #[test]
    fn replay_delayed_retry_ttl_and_dlq_match_state_operations() {
        use crate::{DeadLetterMeta, DeadLetterReason, DeclareMeta, DLQDiscardPolicyWire, NackEventMeta};
        let msg = messages(0, 4);
        let meta = DeclareMeta { dlq_policy: Some(DLQDiscardPolicyWire::Discard), dlq_max_retries: Some(5), default_message_ttl_ms: Some(80) };
        let ev = events(vec![
            StromaEvent::Declare(meta.clone()),
            StromaEvent::Enqueue { off: 0, retries: 2, expire_at: Some(500) },
            StromaEvent::MarkInflight { off: 0, deadline: 200 },
            StromaEvent::NackMany { reqs: vec![NackEventMeta { off: 0, requeue: true, not_before: Some(300) }] },
            StromaEvent::EnqueueDelayed { off: 1, not_before: 400 },
            StromaEvent::EnqueueDelayed { off: 2, not_before: 600 },
            StromaEvent::CancelEnqueueMany { offs: vec![2] },
            StromaEvent::DeadLetter { reqs: vec![DeadLetterMeta { off: 3, retry_count: 4, reason: DeadLetterReason::RetriesExhausted,
                target_tp: "dlq".into(), target_part: 0, target_group: None }] },
        ]);
        let pair = inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default())
            .with_queue_replay(ev.len() as u64, Default::default()).unwrap();
        let report = run(pair, [[&msg, &ev], [&msg, &ev]]).unwrap();
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        state.apply_declare(&meta);
        state.enqueue(0, 2, Some(500));
        state.mark_inflight(0, 200);
        state.nack_at(0, true, Some(300));
        state.enqueue_delayed(1, 400);
        state.enqueue_delayed(2, 600);
        state.cancel_enqueue_many(&[2]);
        state.mark_pending_dlq_many(&[3]);
        assert_eq!(report.queue_replay.unwrap()[0].state_digest, state.recovery_state_digest());
    }

    #[test]
    fn replay_fails_closed_on_unknown_semantics_overflow_limits_and_compacted_origin() {
        let msg = messages(0, 1);
        for event in [
            StromaEvent::Ack { off: u64::MAX },
            StromaEvent::ResetQueue { tp: "q".into(), part: 0, group: None },
            StromaEvent::StreamTruncate { before: 0 },
        ] {
            let ev = events(vec![event]);
            let pair = inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default())
                .with_queue_replay(1, Default::default()).unwrap();
            assert!(run(pair, [[&msg, &ev], [&msg, &ev]]).is_err());
        }
        let ev = events(vec![StromaEvent::Ack { off: 0 }]);
        let pair = inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default())
            .with_queue_replay(1, RecoveryReplayLimits { operations_per_replica: 1 }).unwrap();
        assert!(run(pair, [[&msg, &ev], [&msg, &ev]]).unwrap_err().contains("operation budget"));
        let mut future = ev.clone();
        future[0].payload.push(42);
        let pair = inspector(seal(0, &msg, 0, &future), seal(0, &msg, 0, &future), Default::default())
            .with_queue_replay(1, Default::default()).unwrap();
        assert!(run(pair, [[&msg, &future], [&msg, &future]]).unwrap_err().contains("UnknownOrMalformed"));
        let compacted = seal(1, &[], 1, &[]);
        assert!(inspector(compacted.clone(), compacted, Default::default()).with_queue_replay(1, Default::default()).is_err());
        assert!(inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default()).with_queue_replay(2, Default::default()).is_err());
        // Reconstructing an early target does not permit corruption after it.
        let pair = inspector(seal(0, &msg, 0, &ev), seal(0, &msg, 0, &ev), Default::default())
            .with_queue_replay(0, Default::default()).unwrap();
        assert!(run(pair, [[&msg, &future], [&msg, &ev]]).unwrap_err().contains("sealed digest"));
    }
    fn messages(head: u64, next: u64) -> Vec<RecoveryRecord> {
        (head..next)
            .map(|offset| RecoveryRecord {
                offset,
                flags: offset as u16,
                headers: vec![1],
                payload: vec![offset as u8; 1 + (offset % 4) as usize],
            })
            .collect()
    }
    fn events(events: Vec<StromaEvent>) -> Vec<RecoveryRecord> {
        events
            .into_iter()
            .enumerate()
            .map(|(i, e)| RecoveryRecord {
                offset: i as u64,
                flags: 0,
                headers: vec![],
                payload: e.encode().unwrap(),
            })
            .collect()
    }
    fn seal(
        msg_head: u64,
        msg: &[RecoveryRecord],
        ev_head: u64,
        events: &[RecoveryRecord],
    ) -> SealedReplicaFrontiers {
        let digest = |head: u64, records: &[RecoveryRecord]| {
            let mut cursor = Cursor::new(head, head + records.len() as u64, [0; 32]);
            for record in records {
                hash_record(&mut cursor.hash, record);
            }
            *cursor.hash.finalize().as_bytes()
        };
        let mut history = RetainedHistoryIdentity {
            storage_history: None,
            version: 1,
            id: [0; 32],
            message_digest: digest(msg_head, msg),
            event_digest: digest(ev_head, events),
            snapshot_digest: None,
            message_head: msg_head,
            message_next: msg_head + msg.len() as u64,
            event_head: ev_head,
            event_next: ev_head + events.len() as u64,
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-retained-history-v1\0");
        hash.update(&rmp_serde::to_vec_named(&("q", 0u32, None::<&str>, false, &history)).unwrap());
        history.id = *hash.finalize().as_bytes();
        SealedReplicaFrontiers {
            request: RecoverySealRequest {
                transition: [7; 32],
                fence_epoch: 8,
            },
            message_head: history.message_head,
            message_next: history.message_next,
            event_head: history.event_head,
            event_next: history.event_next,
            history,
        }
    }
    fn page(
        request: &RecoveryReadRequest,
        records: &[RecoveryRecord],
        end: u64,
    ) -> RecoveryReadPage {
        let mut chosen = vec![];
        let mut bytes = 0;
        for record in records
            .iter()
            .filter(|r| r.offset >= request.from)
            .take(request.max_records as usize)
        {
            let size = 18 + record.headers.len() + record.payload.len();
            if bytes + size > request.max_bytes as usize {
                break;
            }
            chosen.push(record.clone());
            bytes += size;
        }
        RecoveryReadPage {
            history_id: request.history_id,
            source: request.source,
            from: request.from,
            next: request.from + chosen.len() as u64,
            end,
            records: chosen,
            snapshot_bytes: vec![],
        }
    }
    fn run(
        mut inspector: RecoveryPairInspector,
        data: [[&[RecoveryRecord]; 2]; 2],
    ) -> Result<RecoveryPairInspection, String> {
        while let Some((side, request)) = inspector.next_read()? {
            let log = if request.source == RecoveryReadSource::Messages {
                0
            } else {
                1
            };
            let end = inspector.logs[log].cursors[side.index()].end;
            let response = page(&request, data[side.index()][log], end);
            inspector.accept_page(side, response)?;
            assert!(inspector.logs[log].pending.len() <= inspector.limits.page_records as usize);
        }
        inspector.finish()
    }
    fn inspector(
        left: SealedReplicaFrontiers,
        right: SealedReplicaFrontiers,
        limits: RecoveryInspectionLimits,
    ) -> RecoveryPairInspector {
        RecoveryPairInspector::new("q", 0, None, false, left, right, limits).unwrap()
    }

    #[test]
    fn shared_ranges_are_compared_across_unequal_pages_and_empty_or_compacted_histories() {
        for left_head in 0..4 {
            for right_head in 0..4 {
                for left_next in left_head..7 {
                    for right_next in right_head..7 {
                        for page_records in 1..4 {
                            let left = messages(left_head, left_next);
                            let right = messages(right_head, right_next);
                            let limits = RecoveryInspectionLimits {
                                page_records,
                                page_bytes: 45,
                                ..Default::default()
                            };
                            let check = inspector(
                                seal(left_head, &left, 0, &[]),
                                seal(right_head, &right, 0, &[]),
                                limits,
                            );
                            let report = run(check, [[&left, &[]], [&right, &[]]]).unwrap();
                            let (from, next) =
                                (left_head.max(right_head), left_next.min(right_next));
                            assert_eq!(
                                report.messages.overlap,
                                if from < next {
                                    RecoveryOverlap::Matching { from, next }
                                } else {
                                    RecoveryOverlap::NoSharedRecords
                                }
                            );
                            assert_eq!(report.events.overlap, RecoveryOverlap::NoSharedRecords);
                            assert_eq!(report.records, left.len() as u64 + right.len() as u64);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn first_divergence_reports_only_offset_and_canonical_ids_including_headers_and_flags() {
        let left = messages(0, 9);
        for mutation in 0..3 {
            let mut right = left.clone();
            match mutation {
                0 => right[3].payload[0] ^= 1,
                1 => right[3].headers[0] ^= 1,
                _ => right[3].flags ^= 1,
            }
            right[7].payload[0] ^= 2;
            let check = inspector(
                seal(0, &left, 0, &[]),
                seal(0, &right, 0, &[]),
                RecoveryInspectionLimits {
                    page_records: 2,
                    ..Default::default()
                },
            );
            let report = run(check, [[&left, &[]], [&right, &[]]]).unwrap();
            assert_eq!(
                report.messages.overlap,
                RecoveryOverlap::Divergent {
                    from: 0,
                    next: 9,
                    first: RecoveryRecordDifference {
                        offset: 3,
                        left_id: record_id(&left[3]),
                        right_id: record_id(&right[3])
                    }
                }
            );
        }
    }

    #[test]
    fn whole_enqueue_record_zero_requires_all_payloads_and_cancellation_requires_replay() {
        let msg = messages(0, 3);
        let ev = events(vec![
            StromaEvent::EnqueueMany {
                reqs: vec![
                    EnqueueEventMeta {
                        off: 0,
                        retries: 0,
                        expire_at: None,
                    },
                    EnqueueEventMeta {
                        off: 3,
                        retries: 0,
                        expire_at: None,
                    },
                    EnqueueEventMeta {
                        off: 8,
                        retries: 0,
                        expire_at: None,
                    },
                ],
            },
            StromaEvent::CancelEnqueueMany { offs: vec![3, 8] },
        ]);
        let check = inspector(
            seal(0, &msg, 0, &ev),
            seal(0, &msg, 0, &ev),
            RecoveryInspectionLimits {
                page_records: 1,
                ..Default::default()
            },
        );
        let report = run(check, [[&msg, &ev], [&msg, &ev]]).unwrap();
        for refs in &report.references {
            assert_eq!(
                refs.first_outside_retained,
                Some(RecoveryReferenceGap {
                    event_offset: 0,
                    message_offset: 3,
                    event_required_message_next: Some(9)
                })
            );
            assert_eq!(
                refs.first_semantic_gap,
                Some((1, RecoverySemanticGap::StateReplayRequired))
            );
            assert_eq!(refs.reference_count, 3);
        }
    }

    #[test]
    fn compacted_acked_offsets_and_unrepresentable_tail_remain_explicit_gaps() {
        let msg = messages(5, 7);
        let ev = events(vec![
            StromaEvent::Ack { off: 0 },
            StromaEvent::Enqueue {
                off: u64::MAX,
                retries: 0,
                expire_at: None,
            },
        ]);
        let report = run(
            inspector(
                seal(5, &msg, 0, &ev),
                seal(5, &msg, 0, &ev),
                Default::default(),
            ),
            [[&msg, &ev], [&msg, &ev]],
        )
        .unwrap();
        assert_eq!(
            report.references[0]
                .first_outside_retained
                .as_ref()
                .unwrap()
                .message_offset,
            0
        );
        assert!(report.references[0].offset_overflow);
        let ev = events(vec![StromaEvent::Enqueue {
            off: u64::MAX,
            retries: 0,
            expire_at: None,
        }]);
        let report = run(
            inspector(
                seal(0, &[], 0, &ev),
                seal(0, &[], 0, &ev),
                Default::default(),
            ),
            [[&[], &ev], [&[], &ev]],
        )
        .unwrap();
        assert_eq!(
            report.references[0]
                .first_outside_retained
                .as_ref()
                .unwrap()
                .event_required_message_next,
            None
        );
    }

    #[test]
    fn malformed_or_future_events_cannot_silently_gain_dependency_approval() {
        let canonical = events(vec![StromaEvent::EnqueueMany {
            reqs: vec![EnqueueEventMeta {
                off: 0,
                retries: 0,
                expire_at: None,
            }],
        }]);
        for mutation in 0..4 {
            let mut ev = canonical.clone();
            let expected = match mutation {
                0 => {
                    ev[0].payload.extend([42]);
                    RecoverySemanticGap::UnknownOrMalformedEncoding
                }
                1 => {
                    ev[0].payload[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
                    RecoverySemanticGap::EventEntryLimit
                }
                2 => {
                    ev[0].payload.truncate(5);
                    RecoverySemanticGap::UnknownOrMalformedEncoding
                }
                _ => {
                    ev[0].flags = 1;
                    RecoverySemanticGap::UnknownOrMalformedEncoding
                }
            };
            let report = run(
                inspector(
                    seal(0, &[], 0, &ev),
                    seal(0, &[], 0, &ev),
                    Default::default(),
                ),
                [[&[], &ev], [&[], &ev]],
            )
            .unwrap();
            assert_eq!(report.references[0].first_semantic_gap, Some((0, expected)));
        }
    }

    #[test]
    fn receipt_binding_and_page_failures_cannot_return_partial_success() {
        let msg = messages(0, 3);
        let left = seal(0, &msg, 0, &[]);
        assert!(
            RecoveryPairInspector::new(
                "other",
                0,
                None,
                false,
                left.clone(),
                left.clone(),
                Default::default()
            )
            .is_err()
        );
        let mut wrong = left.clone();
        wrong.request.transition[0] ^= 1;
        assert!(
            RecoveryPairInspector::new(
                "q",
                0,
                None,
                false,
                left.clone(),
                wrong,
                Default::default()
            )
            .is_err()
        );
        for case in 0..10 {
            let mut check = inspector(left.clone(), left.clone(), Default::default());
            let (mut side, request) = check.next_read().unwrap().unwrap();
            let mut response = page(&request, &msg, 3);
            match case {
                0 => response.history_id[0] ^= 1,
                1 => response.from = 1,
                2 => response.end = 4,
                3 => response.next = 4,
                4 => response.records[1].offset = 9,
                5 => response.snapshot_bytes.push(1),
                6 => response.records[2].payload[0] ^= 1,
                7 => response.source = RecoveryReadSource::Events,
                8 => {
                    response.records.clear();
                    response.next = 0;
                }
                _ => side = side.other(),
            }
            assert!(check.accept_page(side, response).is_err(), "case {case}");
            assert!(check.next_read().is_err());
            assert!(check.finish().is_err());
        }
        for case in 0..3 {
            let mut limits = RecoveryInspectionLimits::default();
            match case {
                0 => limits.total_pages = 1,
                1 => limits.total_records = 1,
                _ => limits.total_bytes = 1,
            }
            assert!(
                run(
                    inspector(left.clone(), left.clone(), limits),
                    [[&msg, &[]], [&msg, &[]]]
                )
                .is_err()
            );
        }
    }

    #[test]
    fn event_divergence_and_unproven_checkpoint_state_remain_visible() {
        let msg = messages(0, 2);
        let left = events(vec![
            StromaEvent::Enqueue {
                off: 0,
                retries: 0,
                expire_at: None,
            },
            StromaEvent::Ack { off: 0 },
        ]);
        let right = events(vec![
            StromaEvent::Enqueue {
                off: 0,
                retries: 0,
                expire_at: None,
            },
            StromaEvent::Nack {
                off: 0,
                requeue: true,
            },
        ]);
        let report = run(
            inspector(
                seal(0, &msg, 0, &left),
                seal(0, &msg, 0, &right),
                Default::default(),
            ),
            [[&msg, &left], [&msg, &right]],
        )
        .unwrap();
        assert!(matches!(
            report.events.overlap,
            RecoveryOverlap::Divergent {
                first: RecoveryRecordDifference { offset: 1, .. },
                ..
            }
        ));
        assert_eq!(
            report.messages.overlap,
            RecoveryOverlap::Matching { from: 0, next: 2 }
        );
        assert!(
            report
                .remaining_proofs
                .contains(&RecoveryProofRequirement::CommonOriginAndInstalledLineage)
        );
        assert!(
            report
                .remaining_proofs
                .contains(&RecoveryProofRequirement::StateReplayAndDependencies)
        );
        let mut sealed = seal(0, &msg, 0, &[]);
        sealed.history.snapshot_digest = Some([8; 32]);
        sealed.history.id = [0; 32];
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-retained-history-v1\0");
        hash.update(
            &rmp_serde::to_vec_named(&("q", 0u32, None::<&str>, false, &sealed.history)).unwrap(),
        );
        sealed.history.id = *hash.finalize().as_bytes();
        let report = run(
            inspector(sealed.clone(), sealed, Default::default()),
            [[&msg, &[]], [&msg, &[]]],
        )
        .unwrap();
        assert!(
            report
                .remaining_proofs
                .contains(&RecoveryProofRequirement::CheckpointCoverage)
        );
    }

    #[test]
    fn longer_message_tail_and_longer_event_tail_are_not_automatically_combined() {
        let left_msg = messages(0, 4);
        let right_msg = messages(0, 2);
        let left_ev = events(vec![StromaEvent::Enqueue {
            off: 0,
            retries: 0,
            expire_at: None,
        }]);
        let right_ev = events(vec![
            StromaEvent::Enqueue {
                off: 0,
                retries: 0,
                expire_at: None,
            },
            StromaEvent::EnqueueMany {
                reqs: vec![
                    EnqueueEventMeta {
                        off: 1,
                        retries: 0,
                        expire_at: None,
                    },
                    EnqueueEventMeta {
                        off: 3,
                        retries: 0,
                        expire_at: None,
                    },
                ],
            },
        ]);
        let report = run(
            inspector(
                seal(0, &left_msg, 0, &left_ev),
                seal(0, &right_msg, 0, &right_ev),
                Default::default(),
            ),
            [[&left_msg, &left_ev], [&right_msg, &right_ev]],
        )
        .unwrap();
        assert_eq!(
            report.messages.overlap,
            RecoveryOverlap::Matching { from: 0, next: 2 }
        );
        assert_eq!(
            report.events.overlap,
            RecoveryOverlap::Matching { from: 0, next: 1 }
        );
        assert_eq!(
            report.references[1].first_outside_retained,
            Some(RecoveryReferenceGap {
                event_offset: 1,
                message_offset: 3,
                event_required_message_next: Some(4)
            })
        );
    }

    #[tokio::test]
    async fn real_sealed_storage_pages_compare_different_tails_without_serving_or_mutation() {
        use crate::{
            KDurability, KeratinConfig, Message, PartitionKind, ReplicatedMessageBatch,
            SnapshotConfig, Stroma, StromaKeratinConfig,
        };
        let left_dir = crate::test_dir!("inspect_left");
        let right_dir = crate::test_dir!("inspect_right");
        let mut stores = vec![];
        let mut seals = vec![];
        for (root, len) in [(&left_dir.root, 2), (&right_dir.root, 4)] {
            let storage = Stroma::open(
                root,
                StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                SnapshotConfig::default(),
            )
            .await
            .unwrap();
            storage
                .become_queue_follower_with_epoch("q", 0, None, 7)
                .await
                .unwrap();
            storage
                .apply_replicated_queue_batch(
                    "q",
                    0,
                    None,
                    Some(ReplicatedMessageBatch {
                        epoch: 7,
                        first_offset: 0,
                        durability: Some(KDurability::AfterFsync),
                        records: messages(0, len)
                            .into_iter()
                            .map(|r| Message {
                                flags: r.flags,
                                headers: r.headers,
                                payload: r.payload,
                            })
                            .collect(),
                    }),
                    None,
                )
                .await
                .unwrap();
            seals.push(
                storage
                    .seal_replica_for_recovery(
                        "q",
                        0,
                        None,
                        RecoverySealRequest {
                            transition: [7; 32],
                            fence_epoch: 8,
                        },
                    )
                    .await
                    .unwrap(),
            );
            stores.push(storage);
        }
        let mut check = inspector(
            seals[0].clone(),
            seals[1].clone(),
            RecoveryInspectionLimits {
                page_records: 1,
                ..Default::default()
            },
        );
        while let Some((side, request)) = check.next_read().unwrap() {
            let page = stores[side.index()]
                .read_sealed_replica("q", 0, None, PartitionKind::Queue, request)
                .await
                .unwrap();
            check.accept_page(side, page).unwrap();
        }
        let report = check.finish().unwrap();
        assert_eq!(
            report.messages.overlap,
            RecoveryOverlap::Matching { from: 0, next: 2 }
        );
        assert_ne!(report.history_ids[0], report.history_ids[1]);
        for storage in stores {
            assert!(storage.queue_handle("q", 0, None).await.is_err());
            storage.shutdown().await.unwrap();
        }
    }
}
