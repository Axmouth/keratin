//! Completed local inspection phases. Only this node's durable result directory
//! may be opened here. These files are never accepted from peers as evidence.
//! Fresh lineage, witness membership and activation authority remain external.
use keratin_log::util::fsync_dir;
use crate::{
    QueueInternalState,
    recovery_inspection::{
        RecoveryInspectionLimits, RecoveryPairInspection, RecoveryPairInspector,
    },
    recovery_journal::RecoveryJournalIdentity,
    recovery_replay::{
        RecoveryQueueReplayEvidence, RecoveryQueueStateArtifact, RecoveryReplayLimits,
    },
    recovery_snapshot::{RecoverySnapshotFile, RecoverySnapshotIdentity, SNAPSHOT_CHUNK_BYTES},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

// Bump whenever evidence decoding, replay or comparison semantics change.
const SEMANTICS: u32 = 1;
const MAX_METADATA: usize = 64 * 1024;
const MAGIC: &[u8; 8] = b"RIPHASE1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RecoveryPhaseKind {
    Comparison,
    SourceArtifact {
        replay: RecoveryReplayLimits,
        checkpoint_bytes: u32,
        artifact_bytes: usize,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPhaseRecipe {
    pub sources: [RecoveryJournalIdentity; 2],
    pub limits: RecoveryInspectionLimits,
    pub state_entries: u64,
    pub kind: RecoveryPhaseKind,
}
impl RecoveryPhaseRecipe {
    /// Shared configuration validation for a new pass and a cached complete pass.
    pub fn inspector(&self) -> Result<RecoveryPairInspector, String> {
        let [a, b] = &self.sources;
        if a.topic != b.topic
            || a.partition != b.partition
            || a.group != b.group
            || a.stream != b.stream
        {
            return Err("phase sources belong to different resources".into());
        }
        let mut inspector = RecoveryPairInspector::new(
            &a.topic,
            a.partition,
            a.group.as_deref(),
            a.stream,
            a.seal.clone(),
            b.seal.clone(),
            self.limits,
        )?;
        match self.kind {
            RecoveryPhaseKind::Comparison => {
                if a.replica == b.replica || a.replica.is_empty() || b.replica.is_empty() {
                    return Err("comparison requires distinct replicas".into());
                }
            }
            RecoveryPhaseKind::SourceArtifact {
                replay,
                checkpoint_bytes,
                artifact_bytes,
            } => {
                if a != b
                    || a.replica.is_empty()
                    || artifact_bytes == 0
                    || artifact_bytes > crate::recovery_snapshot::MAX_RECOVERY_SNAPSHOT_BYTES
                {
                    return Err("invalid source artifact recipe".into());
                }
                let next = a
                    .seal
                    .history
                    .queue_log()
                    .ok_or("source artifact requires a queue history")?
                    .next;
                inspector = inspector.with_queue_checkpoint_replay(
                    next,
                    replay,
                    checkpoint_bytes,
                )?;
            }
        }
        inspector = inspector.with_incremental_work(self.state_entries)?;
        if matches!(self.kind, RecoveryPhaseKind::SourceArtifact { .. }) {
            inspector = inspector.with_single_source()?;
        }
        Ok(inspector)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    semantics: u32,
    recipe: RecoveryPhaseRecipe,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactMetadata {
    evidence: RecoveryQueueReplayEvidence,
    message_head: u64,
    snapshot: RecoverySnapshotIdentity,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Completed {
    identity: [u8; 32],
    report: RecoveryPairInspection,
    artifact: Option<ArtifactMetadata>,
}
pub struct RecoveryCompletedPhase {
    pub report: RecoveryPairInspection,
    pub artifact: Option<RecoveryQueueStateArtifact>,
}
pub struct RecoveryPhaseStore {
    _namespace: crate::recovery_cache::RecoveryCacheLease,
    root: PathBuf,
    recipe: RecoveryPhaseRecipe,
    id: [u8; 32],
    _lock: File,
}
fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
fn encode(value: &impl Serialize) -> io::Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(invalid)
}
fn read(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut bytes = Vec::new();
    file.take(MAX_METADATA as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() < 40 || bytes.len() > MAX_METADATA || &bytes[..8] != MAGIC {
        return Err(invalid("invalid completed phase frame size or header"));
    }
    let end = bytes.len() - 32;
    if blake3::hash(&bytes[..end]).as_bytes() != &bytes[end..] {
        return Err(invalid("completed phase checksum mismatch"));
    }
    Ok(Some(bytes[8..end].to_vec()))
}
fn publish(root: &Path, name: &str, content: &[u8]) -> io::Result<()> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(content);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    if bytes.len() > MAX_METADATA {
        return Err(invalid("completed phase metadata exceeds bound"));
    }
    let free = fs2::available_space(root)?;
    if free.saturating_sub(256 * 1024 * 1024) < bytes.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::StorageFull,
            "completed phase disk reserve refused metadata publication",
        ));
    }
    let path = root.join("phase.writing");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    #[cfg(test)]
    phase_crash_gate(root, name, "file_synced");
    fs::rename(path, root.join(name))?;
    #[cfg(test)]
    phase_crash_gate(root, name, "renamed");
    fsync_dir(root)?;
    #[cfg(test)]
    phase_crash_gate(root, name, "directory_synced");
    Ok(())
}
impl RecoveryPhaseStore {
    pub fn open(parent: &Path, recipe: RecoveryPhaseRecipe) -> io::Result<Self> {
        recipe.inspector().map_err(invalid)?;
        let namespace = crate::recovery_cache::RecoveryCacheLease::acquire(parent)?;
        let identity = encode(&Identity {
            semantics: SEMANTICS,
            recipe: recipe.clone(),
        })?;
        let id = *blake3::hash(&identity).as_bytes();
        let root = parent
            .join("phases")
            .join(blake3::Hash::from_bytes(id).to_hex().as_str());
        fs::create_dir_all(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("lock"))?;
        lock.try_lock().map_err(io::Error::other)?;
        crate::dir_sync::sync_dir_chain(&root)?;
        match read(&root.join("identity"))? {
            Some(saved) if saved != identity => return Err(invalid("phase identity mismatch")),
            Some(_) => {}
            None => publish(&root, "identity", &identity)?,
        }
        Ok(Self {
            root,
            recipe,
            id,
            _namespace: namespace,
            _lock: lock,
        })
    }

    fn validate_report(&self, report: &RecoveryPairInspection) -> io::Result<()> {
        let [a, b] = &self.recipe.sources;
        if matches!(self.recipe.kind, RecoveryPhaseKind::Comparison)
            && report.queue_replay.is_some()
        {
            return Err(invalid(
                "comparison phase contains unexpected replay evidence",
            ));
        }
        let range = |seal: &crate::SealedReplicaFrontiers| {
            seal.history.queue_log().map(|log| (log.head, log.next))
        };
        if report.transition != a.seal.request.transition
            || report.history_ids != [a.seal.history.id, b.seal.history.id]
            || Some(report.events.left_range) != range(&a.seal)
            || Some(report.events.right_range) != range(&b.seal)
            || report.messages.left_range != (0, 0)
            || report.messages.right_range != (0, 0)
            || report.snapshot_digests
                != [
                    a.seal.history.snapshot_digest,
                    b.seal.history.snapshot_digest,
                ]
        {
            return Err(invalid("completed phase differs from exact source recipe"));
        }
        Ok(())
    }
    fn validate_artifact(
        &self,
        report: &RecoveryPairInspection,
        artifact: &RecoveryQueueStateArtifact,
    ) -> io::Result<()> {
        let RecoveryPhaseKind::SourceArtifact { artifact_bytes, .. } = self.recipe.kind else {
            return Err(invalid("comparison phase cannot contain a state artifact"));
        };
        let source = &self.recipe.sources[0];
        let e = artifact.evidence();
        let log = source
            .seal
            .history
            .queue_log()
            .ok_or_else(|| invalid("source artifact requires a queue history"))?;
        if artifact.state_snapshot().is_empty()
            || artifact.state_snapshot().len() > artifact_bytes
            || artifact.message_head() != 0
            || e.version != 2
            || e.log_layout != crate::QueueLogLayout::Combined
            || e.history_id != source.seal.history.id
            || Some(&e.payload_log) != source.seal.history.payload_log().as_ref()
            || e.event_next != log.next
            || e.message_next != 0
            || e.live_payload_digest.is_none()
            || report.queue_replay.as_ref() != Some(&[e.clone(), e.clone()])
            || blake3::hash(artifact.state_snapshot()).as_bytes() != &artifact.snapshot_digest()
        {
            return Err(invalid("completed artifact differs from selected phase"));
        }
        let mut state = QueueInternalState::new(source.topic.clone(), source.partition);
        let meta = state
            .load_snapshot(artifact.state_snapshot())
            .map_err(invalid)?;
        if meta.last_snapshot_event_offset != e.event_next.saturating_sub(1)
            || state.recovery_state_digest() != e.lease_normalized_state_digest
            || state.recovery_retained_entries() > self.recipe.state_entries
        {
            return Err(invalid(
                "completed artifact does not reproduce recorded state",
            ));
        }
        Ok(())
    }

    /// Restore only our own completed result. An interrupted page cache never
    /// produces this record. The ordinary authority checks still follow it.
    pub fn load(&self) -> io::Result<Option<RecoveryCompletedPhase>> {
        let Some(bytes) = read(&self.root.join("complete"))? else {
            return Ok(None);
        };
        let saved: Completed = rmp_serde::from_slice(&bytes).map_err(invalid)?;
        if saved.identity != self.id {
            return Err(invalid("completed phase recipe mismatch"));
        }
        self.validate_report(&saved.report)?;
        let artifact = match saved.artifact {
            Some(metadata) => {
                if metadata.snapshot.plan != self.id {
                    return Err(invalid("completed artifact belongs to another phase"));
                }
                let mut file = RecoverySnapshotFile::open(
                    &self.root.join("artifact"),
                    metadata.snapshot.clone(),
                    Default::default(),
                )?;
                let artifact = RecoveryQueueStateArtifact {
                    evidence: metadata.evidence,
                    message_head: metadata.message_head,
                    state_snapshot: file.read_complete()?,
                    snapshot_digest: metadata.snapshot.digest,
                };
                self.validate_artifact(&saved.report, &artifact)?;
                Some(artifact)
            }
            None if matches!(self.recipe.kind, RecoveryPhaseKind::Comparison) => None,
            None => return Err(invalid("completed source phase has no artifact")),
        };
        Ok(Some(RecoveryCompletedPhase {
            report: saved.report,
            artifact,
        }))
    }

    pub fn save(
        &self,
        report: &RecoveryPairInspection,
        artifact: Option<&RecoveryQueueStateArtifact>,
    ) -> io::Result<()> {
        self.validate_report(report)?;
        let metadata = match artifact {
            Some(artifact) => {
                self.validate_artifact(report, artifact)?;
                let snapshot = RecoverySnapshotIdentity {
                    plan: self.id,
                    digest: artifact.snapshot_digest(),
                    bytes: artifact.state_snapshot().len() as u64,
                };
                let mut file = RecoverySnapshotFile::open(
                    &self.root.join("artifact"),
                    snapshot.clone(),
                    Default::default(),
                )?;
                while file.next_offset() < snapshot.bytes {
                    let from = file.next_offset() as usize;
                    let end = (from + SNAPSHOT_CHUNK_BYTES).min(snapshot.bytes as usize);
                    file.append(from as u64, &artifact.state_snapshot()[from..end])?;
                }
                file.verify_complete()?;
                Some(ArtifactMetadata {
                    evidence: artifact.evidence().clone(),
                    message_head: artifact.message_head(),
                    snapshot,
                })
            }
            None if matches!(self.recipe.kind, RecoveryPhaseKind::Comparison) => None,
            None => return Err(invalid("cannot complete source phase without artifact")),
        };
        let bytes = encode(&Completed {
            identity: self.id,
            report: report.clone(),
            artifact: metadata,
        })?;
        if let Some(existing) = read(&self.root.join("complete"))? {
            if existing != bytes {
                return Err(invalid("conflicting completed inspection phase"));
            }
        }
        publish(&self.root, "complete", &bytes)
    }
}

#[cfg(test)]
fn phase_crash_gate(root: &Path, name: &str, phase: &str) {
    if name == "complete" && std::env::var("STROMA_TEST_PHASE_CRASH_AT").as_deref() == Ok(phase) {
        fs::write(root.join("crash-marker"), phase).unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
