//! Explicit diagnostics over sealed retained histories. No source selection or
//! activation authority is produced, including when every shared record matches.

use crate::recovery_budget::{RecoveryBudget as Budget, RecoveryBudgetExceeded as Limit};
use crate::recovery_replay::{
    QueueReplay, RecoveryQueueReplayEvidence, RecoveryQueueStateArtifact, RecoveryReplayLimits,
};
use crate::{
    RecoveryReadPage, RecoveryReadRequest, RecoveryReadSource, RecoveryRecord,
    RetainedHistoryIdentity, RetainedLog, SealedReplicaFrontiers, StromaEvent,
};
use std::collections::BTreeMap;

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

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryRecordDifference {
    pub offset: u64,
    pub left_id: [u8; 32],
    pub right_id: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryLogInspection {
    pub left_range: (u64, u64),
    pub right_range: (u64, u64),
    pub overlap: RecoveryOverlap,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryReferenceGap {
    pub event_offset: u64,
    /// First referenced payload offset outside the retained log.
    pub payload_offset: u64,
    /// Includes every reference in this event, including an entire enqueue batch.
    /// None means an offset was u64::MAX and has no representable exclusive end.
    pub event_required_next: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RecoverySemanticGap {
    UnknownOrMalformedEncoding,
    EventEntryLimit,
    StateReplayRequired,
    CheckpointRequired,
    StreamStateRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RecoveryReferenceInspection {
    pub checked_events: u64,
    pub reference_count: u64,
    /// Exclusive bound of every payload offset referenced so far.
    pub required_next: u64,
    pub offset_overflow: bool,
    pub first_outside_retained: Option<RecoveryReferenceGap>,
    pub first_semantic_gap: Option<(u64, RecoverySemanticGap)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RecoveryProofRequirement {
    CommonOriginAndInstalledLineage,
    StateReplayAndDependencies,
    CompactedPrefix,
    CheckpointCoverage,
}

/// All fields describe retained evidence only. There is deliberately no
/// compatible/eligible/selected/promotable flag. Even all-empty or equal records
/// require common-origin, state, checkpoint and fresh activation proofs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryPairInspection {
    pub transition: [u8; 32],
    pub history_ids: [[u8; 32]; 2],
    /// Comparison of the two single queue logs.
    pub log: RecoveryLogInspection,
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
    fn new(left: &RetainedLog, right: &RetainedLog) -> Self {
        Self {
            cursors: [
                Cursor::new(left.head, left.next, left.digest),
                Cursor::new(right.head, right.next, right.digest),
            ],
            ranges: [(left.head, left.next), (right.head, right.next)],
            overlap: (left.head.max(right.head), left.next.min(right.next)),
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

/// Streaming, budgeted pair inspection of two queue histories. Fetch only
/// `next_read()` and pass its reply to `accept_page()`. At most one page's
/// unmatched record IDs is retained; message bodies are never stored in the
/// inspector or diagnostic report. Stream histories are refused outright, so
/// one of their two logs can never be compared alone.
pub struct RecoveryPairInspector {
    seals: [SealedReplicaFrontiers; 2],
    limits: RecoveryInspectionLimits,
    log: LogPair,
    references: [RecoveryReferenceInspection; 2],
    pages: u64,
    records: u64,
    bytes: u64,
    failed: bool,
    single_source: bool,
    incremental_state_entries: Option<u64>,
    resource: (String, u32),
    replay: Option<[QueueReplay; 2]>,
    checkpoint_replay: Option<CheckpointReplayPlan>,
    // A bounded second log pass verifies live payloads after state
    // reconstruction, including payloads older than the snapshot.
    live_payloads: Option<LogPair>,
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
fn queue_log(seal: &SealedReplicaFrontiers) -> &RetainedLog {
    seal.history
        .queue_log()
        .expect("inspector admits queue histories only")
}

impl RecoveryPairInspector {
    pub fn new(
        topic: &str,
        partition: u32,
        group: Option<&str>,
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
        if left.history.logs.is_stream() || right.history.logs.is_stream() {
            return Err("recovery inspection supports queue histories only".into());
        }
        if left.request != right.request {
            return Err("recovery inspection requires one exact seal transition".into());
        }
        for seal in [&left, &right] {
            let history = &seal.history;
            if !history.valid_version() || history.queue_log().is_none() {
                return Err("malformed recovery history bounds or version".into());
            }
            // Bind a supplied receipt to this resource, not just the caller's
            // alleged offsets. Same-named incarnations still need lineage proof.
            let id = history
                .compute_id(topic, partition, group, false)
                .map_err(|_| "cannot encode recovery identity")?;
            if id != history.id {
                return Err("recovery history does not identify requested resource".into());
            }
        }
        Ok(Self {
            log: LogPair::new(queue_log(&left), queue_log(&right)),
            seals: [left, right],
            limits,
            references: Default::default(),
            pages: 0,
            records: 0,
            bytes: 0,
            failed: false,
            single_source: false,
            incremental_state_entries: None,
            resource: (topic.to_string(), partition),
            replay: None,
            checkpoint_replay: None,
            live_payloads: None,
        })
    }
    /// Request deterministic queue reconstruction in addition to retained-log
    /// comparison. This must precede all reads. The target is common to both
    /// sources and may be zero; all sealed records are still verified, including
    /// records after the target. No authority or activation proof is implied.
    pub fn with_queue_replay(
        mut self,
        next: u64,
        limits: RecoveryReplayLimits,
    ) -> Result<Self, String> {
        if self.pages != 0
            || self.failed
            || self.single_source
            || self.incremental_state_entries.is_some()
            || self.replay.is_some()
            || self.checkpoint_replay.is_some()
        {
            return Err("queue replay must be configured once before queue inspection".into());
        }
        self.replay = Some([
            QueueReplay::new(
                &self.resource.0,
                self.resource.1,
                self.seals[0].history.clone(),
                next,
                limits,
            )?,
            QueueReplay::new(
                &self.resource.0,
                self.resource.1,
                self.seals[1].history.clone(),
                next,
                limits,
            )?,
        ]);
        Ok(self)
    }

    /// Reconstruct each side from its own exact checkpoint (or event zero when
    /// no checkpoint exists). Snapshot pages and retained records are verified
    /// against the resource-bound seals. This produces diagnostic evidence;
    /// authority, timer transitions and installed lineage still require proof.
    pub fn with_queue_checkpoint_replay(
        mut self,
        next: u64,
        limits: RecoveryReplayLimits,
        max_checkpoint_bytes: u32,
    ) -> Result<Self, String> {
        if self.pages != 0
            || self.failed
            || self.single_source
            || self.incremental_state_entries.is_some()
            || self.replay.is_some()
            || self.checkpoint_replay.is_some()
            || max_checkpoint_bytes == 0
            || max_checkpoint_bytes as usize > crate::recovery_snapshot::MAX_RECOVERY_SNAPSHOT_BYTES
        {
            return Err("invalid checkpoint replay configuration".into());
        }
        self.live_payloads = Some(LogPair::new(
            queue_log(&self.seals[0]),
            queue_log(&self.seals[1]),
        ));
        self.checkpoint_replay = Some(CheckpointReplayPlan {
            target: next,
            limits,
            max_bytes: max_checkpoint_bytes,
            buffers: Default::default(),
            ends: [None; 2],
            done: self
                .seals
                .each_ref()
                .map(|s| s.history.snapshot_digest.is_none()),
        });
        self.initialize_checkpoint_replay()?;
        Ok(self)
    }

    /// Opt into page-by-page work with a retained-state bound, independently of
    /// total historical bytes/records/operations.
    /// The caller must provide durable continuation and per-page I/O deadlines.
    /// Configure replay before opting in. This does not relax page size, entry
    /// encoding, digest or snapshot bounds.
    pub fn with_incremental_work(mut self, max_state_entries: u64) -> Result<Self, String> {
        if max_state_entries == 0 || self.pages != 0 || self.failed {
            return Err("incremental recovery requires an unread queue and state admission".into());
        }
        self.incremental_state_entries = Some(max_state_entries);
        self.limits.total_pages = u64::MAX;
        self.limits.total_records = u64::MAX;
        self.limits.total_bytes = u64::MAX;
        if let Some(plan) = &mut self.checkpoint_replay {
            plan.limits.operations_per_replica = u64::MAX;
        }
        if let Some(replay) = &mut self.replay {
            for state in replay {
                state.enable_incremental_work(max_state_entries)?;
            }
        }
        Ok(self)
    }

    /// Verify one exact sealed source without reading or charging its duplicate
    /// twice. The returned pair contains the same source evidence in both slots
    /// and must never be interpreted as two independent witnesses.
    /// Configure replay before selecting this mode, and before accepting pages.
    pub fn with_single_source(mut self) -> Result<Self, String> {
        if self.pages != 0 || self.failed || self.seals[0] != self.seals[1] {
            return Err("single-source inspection requires one exact unread seal".into());
        }
        self.single_source = true;
        for log in std::iter::once(&mut self.log).chain(self.live_payloads.iter_mut()) {
            log.cursors[1].done = true;
        }
        Ok(self)
    }

    fn initialize_checkpoint_replay(&mut self) -> Result<(), String> {
        let Some(plan) = &mut self.checkpoint_replay else {
            return Ok(());
        };
        if !plan.done[0] || (!self.single_source && !plan.done[1]) {
            return Ok(());
        }
        let build = |i: usize| -> Result<QueueReplay, String> {
            let i = if self.single_source { 0 } else { i };
            let mut replay = if self.seals[i].history.snapshot_digest.is_some() {
                QueueReplay::from_checkpoint(
                    &self.resource.0,
                    self.resource.1,
                    self.seals[i].history.clone(),
                    plan.target,
                    plan.limits,
                    &plan.buffers[i],
                )?
            } else {
                QueueReplay::new(
                    &self.resource.0,
                    self.resource.1,
                    self.seals[i].history.clone(),
                    plan.target,
                    plan.limits,
                )?
            };
            replay.verify_live_payloads();
            Ok(replay)
        };
        self.replay = Some([build(0)?, build(1)?]);
        plan.buffers = Default::default(); // release raw snapshots after decoding
        if let (Some(limit), Some(replay)) = (self.incremental_state_entries, &mut self.replay) {
            for state in replay {
                state.enable_incremental_work(limit)?;
            }
        }
        Ok(())
    }

    pub fn next_read(&self) -> Result<Option<(RecoverySide, RecoveryReadRequest)>, String> {
        if self.failed {
            return Err("recovery inspection failed; discard partial evidence".into());
        }
        if let Some(plan) = &self.checkpoint_replay {
            for (i, side) in [RecoverySide::Left, RecoverySide::Right]
                .into_iter()
                .enumerate()
            {
                if !plan.done[i] && !(self.single_source && i == 1) {
                    if self.pages >= self.limits.total_pages {
                        return Err(Limit::message(
                            Budget::InspectionPages,
                            self.limits.total_pages,
                            self.pages,
                            1,
                        ));
                    }
                    return Ok(Some((
                        side,
                        RecoveryReadRequest {
                            seal: self.seals[i].request.clone(),
                            history_id: self.seals[i].history.id,
                            source: RecoveryReadSource::Snapshot,
                            from: plan.buffers[i].len() as u64,
                            max_records: self.limits.page_records,
                            max_bytes: self.limits.page_bytes.min(plan.max_bytes),
                        },
                    )));
                }
            }
        }
        // Reconstruct state before streaming live payload identities.
        let reads = std::iter::once(&self.log).chain(self.live_payloads.iter());
        for log in reads {
            if let Some(side) = log.next_side() {
                if self.pages >= self.limits.total_pages {
                    return Err(Limit::message(
                        Budget::InspectionPages,
                        self.limits.total_pages,
                        self.pages,
                        1,
                    ));
                }
                let seal = &self.seals[side.index()];
                return Ok(Some((
                    side,
                    RecoveryReadRequest {
                        seal: seal.request.clone(),
                        history_id: seal.history.id,
                        source: RecoveryReadSource::Log,
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
            let plan = self
                .checkpoint_replay
                .as_mut()
                .ok_or("unexpected snapshot page")?;
            let bytes = page.snapshot_bytes.len() as u64;
            if side != expected_side
                || page.history_id != request.history_id
                || page.source != request.source
                || page.from != request.from
                || !page.records.is_empty()
                || page.end < 28
                || page.next <= page.from
                || page.next > page.end
                || page.next - page.from != bytes
                || bytes > request.max_bytes as u64
                || plan.ends[i].is_some_and(|end| end != page.end)
            {
                return Err("checkpoint page identity, range or budget mismatch".into());
            }
            if page.end > plan.max_bytes as u64 {
                return Err(Limit::message(
                    Budget::SnapshotBytes,
                    plan.max_bytes as u64,
                    0,
                    page.end,
                ));
            }
            if bytes > self.limits.total_bytes - self.bytes {
                return Err(Limit::message(
                    Budget::InspectionBytes,
                    self.limits.total_bytes,
                    self.bytes,
                    bytes,
                ));
            }
            plan.ends[i] = Some(page.end);
            plan.buffers[i].extend(page.snapshot_bytes);
            plan.done[i] = page.next == page.end;
            self.pages += 1;
            self.bytes += bytes;
            self.initialize_checkpoint_replay()?;
            return Ok(());
        }
        let payload_pass = self.log.next_side().is_none();
        let log = if payload_pass {
            self.live_payloads
                .as_mut()
                .ok_or("unexpected payload-verification pass")?
        } else {
            &mut self.log
        };
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
        if bytes > request.max_bytes as u64 {
            return Err("recovery inspection page exceeds requested byte bound".into());
        }
        if bytes > self.limits.total_bytes - self.bytes {
            return Err(Limit::message(
                Budget::InspectionBytes,
                self.limits.total_bytes,
                self.bytes,
                bytes,
            ));
        }
        if page.records.len() as u64 > self.limits.total_records - self.records {
            return Err(Limit::message(
                Budget::InspectionRecords,
                self.limits.total_records,
                self.records,
                page.records.len() as u64,
            ));
        }
        for record in &page.records {
            hash_record(&mut cursor.hash, record);
            if payload_pass {
                self.replay
                    .as_mut()
                    .ok_or("payload verification needs replay")?[i]
                    .message(record)?;
            } else {
                self.references[i].inspect(record, &self.seals[i].history);
                if let Some(replay) = &mut self.replay {
                    replay[i].apply(record)?;
                }
            }
            if !self.single_source
                && record.offset >= log.overlap.0
                && record.offset < log.overlap.1
            {
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
            || max_bytes_per_replica > crate::recovery_snapshot::MAX_RECOVERY_SNAPSHOT_BYTES
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
        if self.next_read()?.is_some() || !self.log.pending.is_empty() {
            return Err("incomplete recovery inspection".into());
        }
        let (queue_replay, artifacts) = match (self.replay, artifact_limit) {
            (Some([left, right]), Some(limit)) => {
                let left = left.finish_artifact(limit)?;
                let right = if self.single_source {
                    left.clone()
                } else {
                    right.finish_artifact(limit)?
                };
                let artifacts = [left, right];
                let evidence = artifacts.each_ref().map(|a| a.evidence().clone());
                (Some(evidence), Some(artifacts))
            }
            (Some([left, right]), None) => {
                let left = left.finish()?;
                let right = if self.single_source {
                    left.clone()
                } else {
                    right.finish()?
                };
                (Some([left, right]), None)
            }
            (None, _) => (None, None),
        };
        let mut remaining_proofs = vec![
            RecoveryProofRequirement::CommonOriginAndInstalledLineage,
            RecoveryProofRequirement::StateReplayAndDependencies,
        ];
        if self.seals.iter().any(|s| s.history.compacted()) {
            remaining_proofs.push(RecoveryProofRequirement::CompactedPrefix);
        }
        if self
            .seals
            .iter()
            .any(|s| s.history.snapshot_digest.is_some())
        {
            remaining_proofs.push(RecoveryProofRequirement::CheckpointCoverage);
        }
        Ok((
            RecoveryPairInspection {
                transition: self.seals[0].request.transition,
                history_ids: [self.seals[0].history.id, self.seals[1].history.id],
                log: self.log.result(),
                references: if self.single_source {
                    [self.references[0].clone(), self.references[0].clone()]
                } else {
                    self.references
                },
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
/// Returns semantic fields only. PayloadEnqueue bodies are deliberately empty:
/// payload identity is hashed from the original RecoveryRecord, never this event.
pub(crate) fn decode_evidence_semantics(
    record: &RecoveryRecord,
) -> Result<StromaEvent, RecoverySemanticGap> {
    if record.flags != 0 || !record.headers.is_empty() {
        return Err(RecoverySemanticGap::UnknownOrMalformedEncoding);
    }
    if let Some(view) = crate::event::strict_payload_enqueue(&record.payload)
        .map_err(|_| RecoverySemanticGap::UnknownOrMalformedEncoding)?
    {
        return Ok(StromaEvent::PayloadEnqueue {
            not_before: view.not_before,
            expire_at: view.expire_at,
            headers: Vec::new(),
            payload: Vec::new(),
        });
    }
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
    if record.flags != 0
        || !record.headers.is_empty()
        || event.encode().ok().as_deref() != Some(payload.as_slice())
    {
        return Err(RecoverySemanticGap::UnknownOrMalformedEncoding);
    }
    Ok(event)
}

impl RecoveryReferenceInspection {
    fn gap(&mut self, offset: u64, gap: RecoverySemanticGap) {
        self.first_semantic_gap.get_or_insert((offset, gap));
    }
    fn inspect(&mut self, record: &RecoveryRecord, history: &RetainedHistoryIdentity) {
        self.checked_events += 1;
        let event = match decode_evidence_semantics(record) {
            Ok(event) => event,
            Err(gap) => {
                self.gap(record.offset, gap);
                return;
            }
        };
        let Some(log) = history.queue_log() else {
            self.gap(record.offset, RecoverySemanticGap::StreamStateRequired);
            return;
        };
        let mut first_missing = None;
        let mut required = Some(0u64);
        let mut reference = |off: u64| {
            self.reference_count += 1;
            match off.checked_add(1) {
                Some(next) => {
                    self.required_next = self.required_next.max(next);
                    required = required.map(|r| r.max(next));
                }
                None => {
                    self.offset_overflow = true;
                    required = None;
                }
            }
            if off < log.head || off >= log.next {
                first_missing.get_or_insert(off);
            }
        };
        match event {
            StromaEvent::PayloadEnqueue { .. } => reference(record.offset),
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
            StromaEvent::CancelEnqueueMany { .. }
            | StromaEvent::ResetQueue { .. }
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
        if let Some(payload_offset) = first_missing {
            self.first_outside_retained
                .get_or_insert(RecoveryReferenceGap {
                    event_offset: record.offset,
                    payload_offset,
                    event_required_next: required,
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
    include!("combined_recovery_replay_tests.rs");
    include!("recovery_journal_tests.rs");
    include!("recovery_phase_tests.rs");

    #[test]
    fn replay_proves_exact_zero_and_nonzero_boundaries_without_trusting_labels() {
        let log = events(vec![
            payload(None, None, 1),
            StromaEvent::MarkInflight {
                off: 0,
                deadline: 500,
            },
            StromaEvent::Ack { off: 0 },
            payload(None, Some(800), 2),
        ]);
        for target in 0..=4 {
            let seal = seal(0, &log);
            let pair = inspector(
                seal.clone(),
                seal,
                RecoveryInspectionLimits {
                    page_records: 1,
                    ..Default::default()
                },
            )
            .with_queue_replay(target, Default::default())
            .unwrap();
            let report = run(pair, [&log, &log]).unwrap();
            let [a, b] = report.queue_replay.unwrap();
            assert_eq!(a, b);
            assert_eq!(a.next, target);
            let mut expected = crate::QueueInternalState::new("q".into(), 0);
            if target >= 1 {
                expected.enqueue(0, 0, None);
            }
            if target >= 2 {
                expected.mark_inflight(0, 500);
                expected.ack(1);
            }
            if target >= 3 {
                expected.ack(0);
                expected.ack(2);
            }
            if target >= 4 {
                expected.enqueue(3, 0, Some(800));
            }
            assert_eq!(a.state_digest, expected.recovery_state_digest());
            assert!(
                report
                    .remaining_proofs
                    .contains(&RecoveryProofRequirement::CommonOriginAndInstalledLineage)
            );
            // A common target is evidence only for that cut, not either sealed tail.
            assert!(
                report
                    .remaining_proofs
                    .contains(&RecoveryProofRequirement::StateReplayAndDependencies)
            );
        }
    }

    #[test]
    fn replay_equal_state_does_not_hide_different_payload_history() {
        let a = events(vec![payload(None, None, 1), StromaEvent::Ack { off: 0 }]);
        let b = events(vec![payload(None, None, 9), StromaEvent::Ack { off: 0 }]);
        let pair = inspector(seal(0, &a), seal(0, &b), Default::default())
            .with_queue_replay(2, Default::default())
            .unwrap();
        let report = run(pair, [&a, &b]).unwrap();
        let [a, b] = report.queue_replay.unwrap();
        assert_eq!(a.state_digest, b.state_digest);
        assert_ne!(a.log.digest, b.log.digest);
        assert_ne!(a.history_id, b.history_id);
        assert!(matches!(
            report.log.overlap,
            RecoveryOverlap::Divergent {
                first: RecoveryRecordDifference { offset: 0, .. },
                ..
            }
        ));
    }

    /// A payload that was never written can never become replay evidence,
    /// whether an enqueue batch, an ACK or a cancellation names it.
    #[test]
    fn replay_refuses_references_to_payloads_not_yet_written() {
        for later in [
            StromaEvent::EnqueueMany {
                reqs: vec![
                    EnqueueEventMeta {
                        off: 0,
                        retries: 0,
                        expire_at: None,
                    },
                    EnqueueEventMeta {
                        off: 7,
                        retries: 0,
                        expire_at: None,
                    },
                ],
            },
            StromaEvent::Ack { off: 7 },
            StromaEvent::CancelEnqueueMany { offs: vec![7] },
        ] {
            let log = events(vec![payload(None, None, 1), later]);
            let pair = inspector(seal(0, &log), seal(0, &log), Default::default())
                .with_queue_replay(2, Default::default())
                .unwrap();
            assert!(
                run(pair, [&log, &log])
                    .unwrap_err()
                    .contains("forward payload reference")
            );
        }
        let log = events(vec![payload(None, None, 1), StromaEvent::Ack { off: 0 }]);
        let pair = inspector(seal(0, &log), seal(0, &log), Default::default())
            .with_queue_replay(2, Default::default())
            .unwrap();
        let result = run(pair, [&log, &log]).unwrap().queue_replay.unwrap();
        assert!(result[0].required_next <= 2);
    }

    #[test]
    fn replay_delayed_retry_ttl_and_dlq_match_state_operations() {
        use crate::{
            DLQDiscardPolicyWire, DeadLetterMeta, DeadLetterReason, DeclareMeta, NackEventMeta,
        };
        let meta = DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::Discard),
            dlq_max_retries: Some(5),
            default_message_ttl_ms: Some(80),
        };
        let log = events(vec![
            StromaEvent::Declare(meta.clone()),
            payload(None, Some(500), 1),
            StromaEvent::Enqueue {
                off: 1,
                retries: 2,
                expire_at: Some(500),
            },
            StromaEvent::MarkInflight {
                off: 1,
                deadline: 200,
            },
            StromaEvent::NackMany {
                reqs: vec![NackEventMeta {
                    off: 1,
                    requeue: true,
                    not_before: Some(300),
                }],
            },
            payload(Some(400), None, 2),
            payload(Some(600), None, 3),
            StromaEvent::CancelEnqueueMany { offs: vec![6] },
            payload(None, None, 4),
            StromaEvent::DeadLetter {
                reqs: vec![DeadLetterMeta {
                    off: 8,
                    retry_count: 4,
                    reason: DeadLetterReason::RetriesExhausted,
                    target_tp: "dlq".into(),
                    target_part: 0,
                    target_group: None,
                }],
            },
        ]);
        let pair = inspector(seal(0, &log), seal(0, &log), Default::default())
            .with_queue_replay(log.len() as u64, Default::default())
            .unwrap();
        let report = run(pair, [&log, &log]).unwrap();
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        state.apply_declare(&meta);
        state.ack(0);
        state.enqueue(1, 0, Some(500));
        state.enqueue(1, 2, Some(500));
        state.ack(2);
        state.mark_inflight(1, 200);
        state.ack(3);
        state.nack_at(1, true, Some(300));
        state.ack(4);
        state.enqueue_delayed(5, 400);
        state.enqueue_delayed(6, 600);
        state.cancel_enqueue_many(&[6]);
        state.ack(7);
        state.enqueue(8, 0, None);
        state.mark_pending_dlq_many(&[8]);
        state.ack(9);
        assert_eq!(
            report.queue_replay.unwrap()[0].state_digest,
            state.recovery_state_digest()
        );
    }

    #[test]
    fn replay_fails_closed_on_unknown_semantics_overflow_limits_and_compacted_origin() {
        for event in [
            StromaEvent::Ack { off: u64::MAX },
            StromaEvent::ResetQueue {
                tp: "q".into(),
                part: 0,
                group: None,
            },
            StromaEvent::StreamTruncate { before: 0 },
        ] {
            let log = events(vec![event]);
            let pair = inspector(seal(0, &log), seal(0, &log), Default::default())
                .with_queue_replay(1, Default::default())
                .unwrap();
            assert!(run(pair, [&log, &log]).is_err());
        }
        let log = events(vec![payload(None, None, 1), StromaEvent::Ack { off: 0 }]);
        let pair = inspector(seal(0, &log), seal(0, &log), Default::default())
            .with_queue_replay(
                2,
                RecoveryReplayLimits {
                    operations_per_replica: 1,
                },
            )
            .unwrap();
        assert!(
            run(pair, [&log, &log])
                .unwrap_err()
                .contains("operation budget")
        );
        let mut future = log.clone();
        future[1].payload.push(42);
        let pair = inspector(seal(0, &future), seal(0, &future), Default::default())
            .with_queue_replay(2, Default::default())
            .unwrap();
        assert!(
            run(pair, [&future, &future])
                .unwrap_err()
                .contains("UnknownOrMalformed")
        );
        let compacted = seal(1, &[]);
        assert!(
            inspector(compacted.clone(), compacted, Default::default())
                .with_queue_replay(1, Default::default())
                .is_err()
        );
        assert!(
            inspector(seal(0, &log), seal(0, &log), Default::default())
                .with_queue_replay(3, Default::default())
                .is_err()
        );
        // Reconstructing an early target does not permit corruption after it.
        let pair = inspector(seal(0, &log), seal(0, &log), Default::default())
            .with_queue_replay(0, Default::default())
            .unwrap();
        assert!(
            run(pair, [&future, &log])
                .unwrap_err()
                .contains("sealed digest")
        );
    }
    #[test]
    fn single_source_verifies_once_without_hiding_corruption_or_relaxing_budgets() {
        let log = records(0, 4);
        let sealed = seal(0, &log);
        let limits = RecoveryInspectionLimits {
            total_records: 4,
            ..Default::default()
        };
        assert!(
            run(
                inspector(sealed.clone(), sealed.clone(), limits),
                [&log, &log]
            )
            .unwrap_err()
            .contains("records budget")
        );
        let single = || {
            inspector(sealed.clone(), sealed.clone(), limits)
                .with_single_source()
                .unwrap()
        };
        let report = run(single(), [&log, &log]).unwrap();
        assert_eq!(report.records, 4);
        let mut corrupt = log.clone();
        corrupt[0].payload[0] ^= 1;
        assert!(
            run(single(), [&corrupt, &log])
                .unwrap_err()
                .contains("sealed digest")
        );
        let different = seal(0, &corrupt);
        assert!(
            inspector(sealed.clone(), different, limits)
                .with_single_source()
                .is_err()
        );
        let mut started = single();
        let (side, request) = started.next_read().unwrap().unwrap();
        started.accept_page(side, page(&request, &log, 4)).unwrap();
        assert!(started.with_single_source().is_err());
        let too_small = RecoveryInspectionLimits {
            total_records: 3,
            ..limits
        };
        let check = inspector(sealed.clone(), sealed, too_small)
            .with_single_source()
            .unwrap();
        assert!(
            run(check, [&log, &log])
                .unwrap_err()
                .contains("records budget")
        );
    }

    /// Only queue histories are inspected. A stream history is refused
    /// before any read, so one of its two logs can never be compared on its
    /// own.
    #[test]
    fn stream_histories_are_refused_before_inspection() {
        let queue = seal(0, &records(0, 2));
        let mut stream = queue.clone();
        let log = *queue.history.queue_log().unwrap();
        stream.history.logs = crate::RetainedLogs::Stream {
            records: log,
            cursor_events: log,
        };
        stream.history.storage_history.as_mut().unwrap().stream = true;
        stream.history.id = stream.history.compute_id("q", 0, None, true).unwrap();
        for (left, right) in [
            (queue.clone(), stream.clone()),
            (stream.clone(), queue.clone()),
            (stream.clone(), stream.clone()),
        ] {
            let error = RecoveryPairInspector::new("q", 0, None, left, right, Default::default())
                .err()
                .expect("stream inspection must fail");
            assert!(error.contains("queue histories only"), "{error}");
        }
    }

    fn records(head: u64, next: u64) -> Vec<RecoveryRecord> {
        (head..next)
            .map(|offset| RecoveryRecord {
                offset,
                flags: offset as u16,
                headers: vec![1],
                payload: vec![offset as u8; 1 + (offset % 4) as usize],
            })
            .collect()
    }
    fn payload(not_before: Option<u64>, expire_at: Option<u64>, body: u8) -> StromaEvent {
        StromaEvent::PayloadEnqueue {
            not_before,
            expire_at,
            headers: vec![],
            payload: vec![body; 8],
        }
    }
    fn events(events: Vec<StromaEvent>) -> Vec<RecoveryRecord> {
        events_at(0, events)
    }
    fn events_at(head: u64, events: Vec<StromaEvent>) -> Vec<RecoveryRecord> {
        events
            .into_iter()
            .enumerate()
            .map(|(i, e)| RecoveryRecord {
                offset: head + i as u64,
                flags: 0,
                headers: vec![],
                payload: e.encode().unwrap(),
            })
            .collect()
    }
    /// Sealed queue history whose single log starts at `head` and holds
    /// `records`, bound to resource `q` partition 0.
    fn seal(head: u64, records: &[RecoveryRecord]) -> SealedReplicaFrontiers {
        let mut cursor = Cursor::new(head, head + records.len() as u64, [0; 32]);
        for record in records {
            hash_record(&mut cursor.hash, record);
        }
        let mut history = RetainedHistoryIdentity {
            version: 2,
            storage_history: Some(crate::PreparedStorageHistory {
                topic: "q".into(),
                partition: 0,
                group: None,
                stream: false,
                binding: crate::StorageHistoryBinding {
                    resource_incarnation: [1; 16],
                    accepted_history: [2; 16],
                    writer_session: [3; 16],
                },
                storage_instance: [4; 16],
            }),
            id: [0; 32],
            snapshot_digest: None,
            logs: crate::RetainedLogs::Queue(RetainedLog {
                head,
                next: head + records.len() as u64,
                digest: *cursor.hash.finalize().as_bytes(),
            }),
        };
        history.id = history.compute_id("q", 0, None, false).unwrap();
        SealedReplicaFrontiers {
            request: RecoverySealRequest {
                transition: [7; 32],
                fence_epoch: 8,
            },
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
        data: [&[RecoveryRecord]; 2],
    ) -> Result<RecoveryPairInspection, String> {
        while let Some((side, request)) = inspector.next_read()? {
            assert_eq!(request.source, RecoveryReadSource::Log);
            let end = inspector.log.cursors[side.index()].end;
            let response = page(&request, data[side.index()], end);
            inspector.accept_page(side, response)?;
            assert!(inspector.log.pending.len() <= inspector.limits.page_records as usize);
        }
        inspector.finish()
    }
    fn inspector(
        left: SealedReplicaFrontiers,
        right: SealedReplicaFrontiers,
        limits: RecoveryInspectionLimits,
    ) -> RecoveryPairInspector {
        RecoveryPairInspector::new("q", 0, None, left, right, limits).unwrap()
    }

    #[test]
    fn shared_ranges_are_compared_across_unequal_pages_and_empty_or_compacted_histories() {
        for left_head in 0..4 {
            for right_head in 0..4 {
                for left_next in left_head..7 {
                    for right_next in right_head..7 {
                        for page_records in 1..4 {
                            let left = records(left_head, left_next);
                            let right = records(right_head, right_next);
                            let limits = RecoveryInspectionLimits {
                                page_records,
                                page_bytes: 45,
                                ..Default::default()
                            };
                            let check =
                                inspector(seal(left_head, &left), seal(right_head, &right), limits);
                            let report = run(check, [&left, &right]).unwrap();
                            let (from, next) =
                                (left_head.max(right_head), left_next.min(right_next));
                            assert_eq!(
                                report.log.overlap,
                                if from < next {
                                    RecoveryOverlap::Matching { from, next }
                                } else {
                                    RecoveryOverlap::NoSharedRecords
                                }
                            );
                            assert_eq!(report.log.left_range, (left_head, left_next));
                            assert_eq!(report.records, left.len() as u64 + right.len() as u64);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn first_divergence_reports_only_offset_and_canonical_ids_including_headers_and_flags() {
        let left = records(0, 9);
        for mutation in 0..3 {
            let mut right = left.clone();
            match mutation {
                0 => right[3].payload[0] ^= 1,
                1 => right[3].headers[0] ^= 1,
                _ => right[3].flags ^= 1,
            }
            right[7].payload[0] ^= 2;
            let check = inspector(
                seal(0, &left),
                seal(0, &right),
                RecoveryInspectionLimits {
                    page_records: 2,
                    ..Default::default()
                },
            );
            let report = run(check, [&left, &right]).unwrap();
            assert_eq!(
                report.log.overlap,
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
    fn whole_enqueue_record_requires_all_payloads_and_cancellation_requires_replay() {
        let log = events(vec![
            payload(None, None, 1),
            payload(None, None, 2),
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
            seal(0, &log),
            seal(0, &log),
            RecoveryInspectionLimits {
                page_records: 1,
                ..Default::default()
            },
        );
        let report = run(check, [&log, &log]).unwrap();
        for refs in &report.references {
            assert_eq!(
                refs.first_outside_retained,
                Some(RecoveryReferenceGap {
                    event_offset: 2,
                    payload_offset: 8,
                    event_required_next: Some(9)
                })
            );
            assert_eq!(
                refs.first_semantic_gap,
                Some((3, RecoverySemanticGap::StateReplayRequired))
            );
            assert_eq!(refs.reference_count, 5);
        }
    }

    #[test]
    fn compacted_acked_offsets_and_unrepresentable_tail_remain_explicit_gaps() {
        let log = events_at(
            5,
            vec![
                StromaEvent::Ack { off: 0 },
                StromaEvent::Enqueue {
                    off: u64::MAX,
                    retries: 0,
                    expire_at: None,
                },
            ],
        );
        let report = run(
            inspector(seal(5, &log), seal(5, &log), Default::default()),
            [&log, &log],
        )
        .unwrap();
        assert_eq!(
            report.references[0]
                .first_outside_retained
                .as_ref()
                .unwrap()
                .payload_offset,
            0
        );
        assert!(report.references[0].offset_overflow);
        let log = events(vec![StromaEvent::Enqueue {
            off: u64::MAX,
            retries: 0,
            expire_at: None,
        }]);
        let report = run(
            inspector(seal(0, &log), seal(0, &log), Default::default()),
            [&log, &log],
        )
        .unwrap();
        assert_eq!(
            report.references[0]
                .first_outside_retained
                .as_ref()
                .unwrap()
                .event_required_next,
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
                inspector(seal(0, &ev), seal(0, &ev), Default::default()),
                [&ev, &ev],
            )
            .unwrap();
            assert_eq!(report.references[0].first_semantic_gap, Some((0, expected)));
        }
    }

    #[test]
    fn receipt_binding_and_page_failures_cannot_return_partial_success() {
        let log = records(0, 3);
        let left = seal(0, &log);
        assert!(
            RecoveryPairInspector::new(
                "other",
                0,
                None,
                left.clone(),
                left.clone(),
                Default::default()
            )
            .is_err()
        );
        let mut wrong = left.clone();
        wrong.request.transition[0] ^= 1;
        assert!(
            RecoveryPairInspector::new("q", 0, None, left.clone(), wrong, Default::default())
                .is_err()
        );
        for case in 0..10 {
            let mut check = inspector(left.clone(), left.clone(), Default::default());
            let (mut side, request) = check.next_read().unwrap().unwrap();
            let mut response = page(&request, &log, 3);
            match case {
                0 => response.history_id[0] ^= 1,
                1 => response.from = 1,
                2 => response.end = 4,
                3 => response.next = 4,
                4 => response.records[1].offset = 9,
                5 => response.snapshot_bytes.push(1),
                6 => response.records[2].payload[0] ^= 1,
                7 => response.source = RecoveryReadSource::StreamRecords,
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
            let error =
                run(inspector(left.clone(), left.clone(), limits), [&log, &log]).unwrap_err();
            let budget = Limit::from_message(&error).unwrap();
            assert_eq!(
                budget.budget,
                [
                    Budget::InspectionPages,
                    Budget::InspectionRecords,
                    Budget::InspectionBytes
                ][case]
            );
            assert_eq!(budget.limit, 1);
            assert!(budget.requested > budget.limit - budget.completed);
            assert!(!budget.unchanged_retry_can_help);
        }
    }

    #[test]
    fn event_divergence_and_unproven_checkpoint_state_remain_visible() {
        let left = events(vec![payload(None, None, 1), StromaEvent::Ack { off: 0 }]);
        let right = events(vec![
            payload(None, None, 1),
            StromaEvent::Nack {
                off: 0,
                requeue: true,
            },
        ]);
        let report = run(
            inspector(seal(0, &left), seal(0, &right), Default::default()),
            [&left, &right],
        )
        .unwrap();
        // The shared payload record matches and the first difference is the
        // settlement event after it.
        assert!(matches!(
            report.log.overlap,
            RecoveryOverlap::Divergent {
                first: RecoveryRecordDifference { offset: 1, .. },
                ..
            }
        ));
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
        let mut sealed = seal(0, &left);
        sealed.history.snapshot_digest = Some([8; 32]);
        sealed.history.id = sealed.history.compute_id("q", 0, None, false).unwrap();
        let report = run(
            inspector(sealed.clone(), sealed, Default::default()),
            [&left, &left],
        )
        .unwrap();
        assert!(
            report
                .remaining_proofs
                .contains(&RecoveryProofRequirement::CheckpointCoverage)
        );
    }

    /// Each side's references are checked against its own retained log. A
    /// longer log on the other side never fills a gap.
    #[test]
    fn longer_tail_on_one_side_is_not_borrowed_by_the_other() {
        let left = events(vec![
            payload(None, None, 1),
            payload(None, None, 2),
            payload(None, None, 3),
            payload(None, None, 4),
        ]);
        let right = events(vec![
            payload(None, None, 1),
            payload(None, None, 2),
            StromaEvent::Enqueue {
                off: 3,
                retries: 0,
                expire_at: None,
            },
        ]);
        let report = run(
            inspector(seal(0, &left), seal(0, &right), Default::default()),
            [&left, &right],
        )
        .unwrap();
        assert!(matches!(
            report.log.overlap,
            RecoveryOverlap::Divergent {
                from: 0,
                next: 3,
                first: RecoveryRecordDifference { offset: 2, .. }
            }
        ));
        assert_eq!(report.references[0].first_outside_retained, None);
        assert_eq!(
            report.references[1].first_outside_retained,
            Some(RecoveryReferenceGap {
                event_offset: 2,
                payload_offset: 3,
                event_required_next: Some(4)
            })
        );
    }

    #[tokio::test]
    async fn real_sealed_storage_pages_compare_different_tails_without_serving_or_mutation() {
        use crate::{
            KDurability, KeratinConfig, PartitionKind, ReplicatedEventBatch, SnapshotConfig,
            Stroma, StromaKeratinConfig,
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
                .initialize_empty_storage_history(
                    "q",
                    0,
                    None,
                    PartitionKind::Queue,
                    crate::StorageHistoryBinding {
                        resource_incarnation: [1; 16],
                        accepted_history: [2; 16],
                        writer_session: [3; 16],
                    },
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
                    ReplicatedEventBatch {
                        epoch: 7,
                        first_offset: 0,
                        durability: Some(KDurability::AfterFsync),
                        events: records(0, len)
                            .into_iter()
                            .map(|r| StromaEvent::PayloadEnqueue {
                                not_before: None,
                                expire_at: None,
                                headers: r.headers,
                                payload: r.payload,
                            })
                            .collect(),
                    },
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
            report.log.overlap,
            RecoveryOverlap::Matching { from: 0, next: 2 }
        );
        assert_ne!(report.history_ids[0], report.history_ids[1]);
        for storage in stores {
            assert!(storage.queue_handle("q", 0, None).await.is_err());
            storage.shutdown().await.unwrap();
        }
    }
}
