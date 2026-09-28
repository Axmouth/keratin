//! Non-serving recovery staging. Transferred payloads use a separate native log;
//! no active partition path, receipt, role or seal is changed by these methods.
use super::*;
use crate::recovery_budget::{RecoveryBudget as Budget, RecoveryBudgetExceeded as Limit};
use crate::QueueInternalState;
use crate::recovery_replay::RecoveryPayloadLog;
use keratin_log::{KeratinReplicaExt, ReplicatedAppendOutcome, lock_existing_log};
use std::io::{Read, Write};

use crate::recovery_snapshot::{RecoverySnapshotFile, RecoverySnapshotIdentity, SNAPSHOT_CHUNK_BYTES, MAX_RECOVERY_SNAPSHOT_BYTES};
const MAX_SNAPSHOT: usize = MAX_RECOVERY_SNAPSHOT_BYTES;
const MAX_PAGE: u64 = 16 * 1024 * 1024;
// Snapshot bytes live in a separately verified raw artifact file.
const MAX_METADATA: usize = 65_536;
const MAGIC: &[u8; 8] = b"RSTAGE\0\x01";

/// Exact baseline authorized by a consensus recovery plan. The caller owns
/// fresh consensus authorization; these fields alone are not proof of authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryStageSpec {
    pub plan: [u8; 32],
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    pub binding: StorageHistoryBinding,
    pub fence_epoch: u64,
    pub source_history: [u8; 32],
    pub payload_log: RecoveryPayloadLog,
    pub message_head: u64,
    pub message_next: u64,
    pub event_next: u64,
    pub message_digest: [u8; 32],
    pub snapshot_digest: [u8; 32],
    pub snapshot_bytes: u64,
    pub state_digest: [u8; 32],
    pub live_payload_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryStageLimits {
    pub max_records: u64,
    /// Logical transferred record bytes, including offset/flags/length fields.
    /// Native log framing, indexes and segment preallocation use additional disk.
    pub max_bytes: u64,
    /// Best-effort free-space admission before additional private storage work.
    /// Unrelated writes and preallocation can still race this check.
    pub reserve_free_bytes: u64,
}
impl Default for RecoveryStageLimits {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,
            max_bytes: 1024 * 1024 * 1024,
            reserve_free_bytes: 256 * 1024 * 1024,
        }
    }
}

impl RecoveryStageLimits {
    /// Automatic continuation bounds pages, retained state and free disk, rather
    /// than refusing progress after a fixed accumulated record/byte count.
    pub fn incremental() -> Self {
        Self { max_records: u64::MAX, max_bytes: u64::MAX, ..Default::default() }
    }
}

fn admit_stage_bytes(root: &Path, limits: RecoveryStageLimits, requested: u64) -> Result<()> {
    let free = fs2::available_space(root).map_err(io_err)?;
    if free.saturating_sub(limits.reserve_free_bytes) < requested {
        return Err(io_err(io::Error::new(io::ErrorKind::StorageFull,
            format!("recovery stage disk reserve: free={free}, reserve={}, requested={requested}", limits.reserve_free_bytes))));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Intent {
    pub(super) spec: QueueRecoveryStageSpec,
    pub(super) snapshot: Vec<u8>,
}

/// Durable staged data only. It is not an installed-quorum receipt or permission
/// to serve; replacement admission will require its own lifecycle protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRecoveryStageReceipt {
    pub plan: [u8; 32],
    pub source_history: [u8; 32],
    pub snapshot_digest: [u8; 32],
    pub message_next: u64,
    pub event_next: u64,
}

pub(super) struct StageInner {
    pub(super) messages: Arc<Keratin>,
    pub(super) root: PathBuf,
    pub(super) intent: Intent,
    pub(super) limits: RecoveryStageLimits,
    used: u64,
    pub(super) complete: bool,
    pub(super) needs_reopen: bool,
    // Keep admission/OS locks through all owned work and writer shutdown.
    _lock: fs::File,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct QueueRecoveryStage {
    pub(super) inner: Arc<AsyncMutex<StageInner>>,
}

fn invalid(message: impl Into<String>) -> StromaError {
    StromaError::InvalidArgument(message.into())
}
fn corrupt(message: impl Into<String>) -> StromaError {
    StromaError::Corruption(message.into())
}

pub(super) fn read_bounded(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
    };
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_err)?;
    if bytes.len() > limit || bytes.len() < 12 || &bytes[..8] != MAGIC {
        return Err(corrupt("invalid recovery staging metadata size or header"));
    }
    let end = bytes.len() - 4;
    if crc32c::crc32c(&bytes[..end]) != u32::from_be_bytes(bytes[end..].try_into().unwrap()) {
        return Err(corrupt("recovery staging metadata checksum mismatch"));
    }
    Ok(Some(bytes[8..end].to_vec()))
}

pub(super) fn persist_exact(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap();
    if let Some(existing) = read_bounded(path, MAX_METADATA)? {
        if existing != content {
            return Err(invalid("conflicting recovery staging intent"));
        }
        fs::File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(io_err)?;
        return recovery_seal::sync_directories(parent);
    }
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(content);
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_be_bytes());
    if bytes.len() > MAX_METADATA {
        return Err(invalid("recovery staging metadata exceeds limit"));
    }
    // Exclusive stage lock owns this scratch file. A crash may leave it behind;
    // retries overwrite only scratch, never a published intent or active log.
    let temp = parent.join("metadata.writing");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .map_err(io_err)?;
    file.write_all(&bytes).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    drop(file);
    fs::rename(&temp, path).map_err(io_err)?;
    recovery_seal::sync_directories(parent)
}

fn validate(intent: &Intent, limits: RecoveryStageLimits) -> Result<QueueInternalState> {
    let s = &intent.spec;
    s.validate_payload_log()?;
    if s.plan == [0; 32]
        || s.source_history == [0; 32]
        || s.topic.is_empty()
        || s.group
            .as_deref()
            .is_some_and(|g| g.is_empty() || g == "default")
        || [
            s.binding.resource_incarnation,
            s.binding.accepted_history,
            s.binding.writer_session,
        ]
        .contains(&[0; 16])
        || s.payload_log.head > s.payload_log.next
        || limits.max_bytes == 0
        || intent.snapshot.is_empty()
    {
        return Err(invalid(
            "invalid recovery staging identity, snapshot or budget",
        ));
    }
    if s.payload_log.next - s.payload_log.head > limits.max_records {
        return Err(invalid(Limit::message(Budget::StageRecords, limits.max_records, 0, s.payload_log.next - s.payload_log.head)));
    }
    if intent.snapshot.len() as u64 != s.snapshot_bytes {
        return Err(invalid("recovery staging snapshot length differs from plan"));
    }
    if intent.snapshot.len() > MAX_SNAPSHOT {
        return Err(invalid(Limit::message(Budget::SnapshotBytes, MAX_SNAPSHOT as u64, 0, intent.snapshot.len() as u64)));
    }
    if *blake3::hash(&intent.snapshot).as_bytes() != s.snapshot_digest {
        return Err(invalid("recovery staging snapshot digest mismatch"));
    }
    let mut state = QueueInternalState::new(s.topic.clone(), s.partition);
    let meta = state.load_snapshot(&intent.snapshot).map_err(decode_err)?;
    if state.recovery_retained_entries() > 1_000_000 {
        return Err(invalid(Limit::message(Budget::ReplayStateEntries, 1_000_000, 0, state.recovery_retained_entries())));
    }
    if meta.last_snapshot_event_offset != s.event_next.saturating_sub(1)
        || state.recovery_state_digest() != s.state_digest
        || state.recovery_lease_normalized_digest() != s.state_digest
        || state.required_message_next() > s.payload_log.next
        || state
            .recovery_live_ranges()
            .iter()
            .any(|r| r.start < s.payload_log.head || r.end > s.payload_log.next)
    {
        return Err(invalid(
            "recovery snapshot does not match the exact staged baseline",
        ));
    }
    Ok(state)
}

fn record_bytes(record: &RecoveryRecord) -> Result<u64> {
    18u64
        .checked_add(record.headers.len() as u64)
        .and_then(|n| n.checked_add(record.payload.len() as u64))
        .ok_or_else(|| invalid("recovery record size overflow"))
}

pub(super) fn scan(inner: &StageInner, require_complete: bool) -> Result<u64> {
    scan_parts(
        &inner.messages,
        &inner.intent,
        inner.limits,
        require_complete,
    )
}

pub(super) fn scan_parts(
    messages: &Keratin,
    intent: &Intent,
    limits: RecoveryStageLimits,
    require_complete: bool,
) -> Result<u64> {
    let s = &intent.spec;
    let end = messages.next_offset();
    if messages.head_offset() != s.payload_log.head
        || end > s.payload_log.next
        || messages.current_epoch() != s.fence_epoch
        || require_complete && end != s.payload_log.next
    {
        return Err(corrupt("staged log does not cover the expected boundaries"));
    }
    let state = validate(&intent, limits)?;
    let live = state.recovery_live_ranges();
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-log-v1\0");
    hash.update(&s.payload_log.head.to_be_bytes());
    hash.update(&s.payload_log.next.to_be_bytes());
    let mut live_hash = blake3::Hasher::new();
    live_hash.update(b"fibril-recovery-live-payloads-v1\0");
    let mut used = 0u64;
    // This stage owns all append access under its mutex. Freeze while a strict
    // sequential disk scan verifies records without caches or repeated seeks.
    struct RestoreRole<'a>(&'a Keratin, KeratinRole);
    impl Drop for RestoreRole<'_> {
        fn drop(&mut self) {
            if self.1 == KeratinRole::Follower {
                self.0.become_follower();
            }
        }
    }
    let _role = RestoreRole(&messages, messages.role());
    messages.freeze();
    let reader = messages.frozen_reader().map_err(io_err)?;
    reader
        .scan(|record| {
            let bytes = 18u64
                .checked_add(record.headers.len() as u64)
                .and_then(|n| n.checked_add(record.payload.len() as u64))
                .ok_or_else(|| io::Error::other("staged record size overflow"))?;
            used = used
                .checked_add(bytes)
                .ok_or_else(|| io::Error::other("staged byte count overflow"))?;
            if used > limits.max_bytes {
                return Err(io::Error::other(Limit::message(Budget::StageBytes, limits.max_bytes, used - bytes, bytes)));
            }
            let hash_record = |h: &mut blake3::Hasher| {
                h.update(&record.offset.to_be_bytes());
                h.update(&record.flags.to_be_bytes());
                h.update(&(record.headers.len() as u64).to_be_bytes());
                h.update(record.headers);
                h.update(&(record.payload.len() as u64).to_be_bytes());
                h.update(record.payload);
            };
            hash_record(&mut hash);
            if live.contains(&record.offset) {
                if s.payload_log.source == RecoveryReadSource::Events {
                    let record = RecoveryRecord { offset: record.offset, flags: record.flags,
                        headers: record.headers.to_vec(), payload: record.payload.to_vec() };
                    if !matches!(crate::recovery_inspection::decode_evidence_semantics(&record), Ok(StromaEvent::PayloadEnqueue { .. })) {
                        return Err(io::Error::other("staged live payload points to non-payload event"));
                    }
                }
                hash_record(&mut live_hash);
            }
            Ok(())
        })
        .map_err(io_err)?;
    if require_complete
        && (*hash.finalize().as_bytes() != s.payload_log.digest
            || *live_hash.finalize().as_bytes() != s.live_payload_digest)
    {
        return Err(corrupt(
            "staged payloads differ from the selected recovery history",
        ));
    }
    Ok(used)
}

impl QueueRecoveryStageSpec {
    pub fn snapshot_identity(&self) -> RecoverySnapshotIdentity {
        RecoverySnapshotIdentity { plan: self.plan, digest: self.snapshot_digest, bytes: self.snapshot_bytes }
    }

    pub(super) fn validate_payload_log(&self) -> Result<()> {
        let p = &self.payload_log;
        let valid = p.head <= p.next && match p.source {
            RecoveryReadSource::Events => self.message_head == 0 && self.message_next == 0
                && self.message_digest == recovery_history::empty_log_digest()
                && p.next == self.event_next,
            RecoveryReadSource::Messages | RecoveryReadSource::Snapshot => false,
        };
        if !valid { return Err(invalid("recovery payload source differs from exact selected baseline")); }
        Ok(())
    }
}

impl Stroma {
    fn recovery_stage_root(&self, spec: &QueueRecoveryStageSpec) -> PathBuf {
        self.root.join("recovery-staging").join(blake3::Hash::from_bytes(spec.plan).to_hex().as_str())
    }

    /// One bounded artifact step under caller-provided fresh plan authority.
    /// The owned task retains admission and the file lock after cancellation.
    pub async fn transfer_queue_recovery_snapshot(
        &self, spec: QueueRecoveryStageSpec, chunk: Option<(u64, Vec<u8>)>,
    ) -> Result<u64> {
        self.validate_recovery_payload_layout(&spec)?;
        let permit = self.recovery_stage_slots.clone().try_acquire_owned()
            .map_err(|_| invalid("another recovery stage is open, retry later"))?;
        let root = self.recovery_stage_root(&spec).join("snapshot");
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut file = RecoverySnapshotFile::open(&root, spec.snapshot_identity(), Default::default()).map_err(io_err)?;
            if let Some((from, bytes)) = chunk { file.append(from, &bytes).map_err(io_err)?; }
            Ok(file.next_offset())
        }).await.map_err(io_err)?
    }

    pub async fn open_queue_recovery_stage_from_snapshot(
        &self, spec: QueueRecoveryStageSpec, limits: RecoveryStageLimits, seal: RecoverySealRequest,
    ) -> Result<QueueRecoveryStage> {
        if seal.fence_epoch != spec.fence_epoch { return Err(invalid("reuse fence differs from stage")); }
        self.open_queue_recovery_stage_inner(spec, SnapshotInput::Transferred, limits, Some(seal)).await
    }

    pub(super) fn validate_recovery_payload_layout(&self, spec: &QueueRecoveryStageSpec) -> Result<()> {
        spec.validate_payload_log()?;
        if spec.payload_log.source != RecoveryReadSource::Events {
            return Err(invalid("recovery staging layout differs from local queue format"));
        }
        Ok(())
    }

    pub(super) fn recovery_payload_path(&self, spec: &QueueRecoveryStageSpec) -> PathBuf {
        self.tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref())
    }

    /// Open/resume non-serving staging after the caller freshly authorizes its
    /// persisted plan. One stage is open per storage instance. Caller cancellation
    /// does not interrupt admitted initialization. No active files are replaced.
    pub async fn open_queue_recovery_stage(
        &self,
        spec: QueueRecoveryStageSpec,
        snapshot: Vec<u8>,
        limits: RecoveryStageLimits,
    ) -> Result<QueueRecoveryStage> {
        self.open_queue_recovery_stage_inner(spec, SnapshotInput::Bytes(snapshot), limits, None)
            .await
    }

    /// Prefer exact, freshly sealed local payloads. Snapshot/authority checks and
    /// installation receipts are unchanged; incompatible data uses transfer.
    pub async fn open_queue_recovery_stage_reusing(
        &self,
        spec: QueueRecoveryStageSpec,
        snapshot: Vec<u8>,
        limits: RecoveryStageLimits,
        seal: RecoverySealRequest,
    ) -> Result<QueueRecoveryStage> {
        if seal.fence_epoch != spec.fence_epoch {
            return Err(invalid("reuse fence differs from stage"));
        }
        self.open_queue_recovery_stage_inner(spec, SnapshotInput::Bytes(snapshot), limits, Some(seal))
            .await
    }

    /// Resume from the immutable staged snapshot without requiring the old
    /// source to return. The caller must freshly authorize the same plan.
    pub async fn resume_queue_recovery_stage(
        &self,
        spec: QueueRecoveryStageSpec,
        limits: RecoveryStageLimits,
    ) -> Result<QueueRecoveryStage> {
        self.open_queue_recovery_stage_inner(spec, SnapshotInput::Resume, limits, None)
            .await
    }

    async fn try_reuse_stage_messages(
        &self,
        intent: &Intent,
        limits: RecoveryStageLimits,
        seal: &RecoverySealRequest,
        destination: PathBuf,
    ) -> Result<bool> {
        let s = &intent.spec;
        let _lifecycle = self
            .lock_partition_lifecycle(&s.topic, s.partition, s.group.as_deref())
            .await;
        if !self
            .recovery_seal_path(&s.topic, s.partition, s.group.as_deref())
            .try_exists()
            .map_err(io_err)?
        {
            return Ok(false);
        }
        self.require_matching_recovery_seal(&s.topic, s.partition, s.group.as_deref(), seal)?;
        self.ensure_checkpoint_not_pending(&s.topic, s.partition, s.group.as_deref())?;
        let Some(receipt) =
            self.durable_storage_history_receipt(&s.topic, s.partition, s.group.as_deref())?
        else {
            return Ok(false);
        };
        if receipt.stream || receipt.binding.resource_incarnation != s.binding.resource_incarnation
        {
            return Ok(false);
        }
        let handle = {
            let current = self.queue_handles.load();
            slot_lookup_no_alloc(&current, &s.topic, s.partition, s.group.as_deref())
                .and_then(|slot| slot.handle.get().cloned())
        };
        let _apply = match &handle {
            Some(handle) => Some(handle.follower_apply_state().await),
            None => None,
        };
        let messages = match &handle {
            Some(handle) => handle.payload_log(),
            None => Arc::new(
                self.open_keratin(
                    self.recovery_payload_path(s),
                    self.keratin_cfg_msg,
                    true,
                )
                .await
                .map_err(io_err)?,
            ),
        };
        let result = async {
            if handle.is_none() {
                messages.freeze();
            }
            if messages.role() != KeratinRole::Frozen {
                return Err(invalid("sealed reuse source still has a writer role"));
            }
            if messages.current_epoch() != s.fence_epoch
                || messages.head_offset() != s.payload_log.head
                || messages.next_offset() != s.payload_log.next
            {
                return Ok(false);
            }
            let checked = messages.clone();
            let expected = intent.clone();
            let compatible =
                tokio::task::spawn_blocking(move || scan_parts(&checked, &expected, limits, true))
                    .await
                    .map_err(io_err)?;
            if let Err(error) = compatible {
                tracing::info!(topic = s.topic, partition = s.partition, %error,
                    "local retained payloads cannot be reused; using verified transfer");
                return Ok(false);
            }
            let admission_source = messages.clone();
            let admission_root = intent.spec.clone();
            let admission_parent = self.recovery_stage_root(&admission_root);
            tokio::task::spawn_blocking(move || {
                let private_bytes = admission_source.segment_infos().map_err(io_err)?.last().map_or(0, |s| s.bytes);
                admit_stage_bytes(&admission_parent, limits, private_bytes.saturating_add(1024 * 1024))
            }).await.map_err(io_err)??;
            let stats = messages
                .fork_frozen(destination, s.fence_epoch, s.payload_log.head, s.payload_log.next)
                .await
                .map_err(io_err)?;
            tracing::info!(
                topic = s.topic,
                partition = s.partition,
                shared_bytes = stats.shared_bytes,
                copied_bytes = stats.copied_bytes,
                shared_segments = stats.shared_segments,
                "recovery stage reused compatible retained payloads"
            );
            stage_boundary("reused-messages");
            Ok(true)
        }
        .await;
        if handle.is_none() {
            messages.shutdown().await.map_err(io_err)?;
        }
        result
    }

    async fn open_queue_recovery_stage_inner(
        &self,
        spec: QueueRecoveryStageSpec,
        snapshot: SnapshotInput,
        limits: RecoveryStageLimits,
        reuse_seal: Option<RecoverySealRequest>,
    ) -> Result<QueueRecoveryStage> {
        if !cfg!(unix) {
            return Err(StromaError::Unsupported(
                "recovery staging requires durable Unix metadata".into(),
            ));
        }
        self.validate_recovery_payload_layout(&spec)?;
        let resume_only = matches!(snapshot, SnapshotInput::Resume);
        let permit = self
            .recovery_stage_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| invalid("another recovery stage is open; retry later"))?;
        let root = self.recovery_stage_root(&spec);
        let cfg = self.keratin_cfg_msg;
        let runtime = self.log_runtime.clone();
        let st = self.clone();
        tokio::spawn(async move {
            let read_root = root.clone();
            let (intent, encoded) = tokio::task::spawn_blocking(move || {
                let snapshot = match snapshot {
                    SnapshotInput::Bytes(snapshot) => snapshot,
                    SnapshotInput::Transferred | SnapshotInput::Resume => {
                        if resume_only {
                            let bytes = read_bounded(&read_root.join("intent"), MAX_METADATA)?
                                .ok_or_else(|| invalid("recovery staging intent is absent"))?;
                            let saved: QueueRecoveryStageSpec = rmp_serde::from_slice(&bytes).map_err(decode_err)?;
                            if saved != spec { return Err(invalid("saved recovery stage differs from the authorized plan")); }
                        }
                        RecoverySnapshotFile::open(&read_root.join("snapshot"), spec.snapshot_identity(), Default::default())
                            .map_err(io_err)?.read_complete().map_err(io_err)?
                    }
                };
                let intent = Intent { spec, snapshot };
                validate(&intent, limits)?;
                let encoded = rmp_serde::to_vec_named(&intent.spec).map_err(encode_err)?;
                if encoded.len() + 12 > MAX_METADATA {
                    return Err(invalid("recovery staging metadata exceeds limit"));
                }
                // The raw artifact becomes durable before its small intent.
                let mut file = RecoverySnapshotFile::open(&read_root.join("snapshot"), intent.spec.snapshot_identity(), Default::default()).map_err(io_err)?;
                while file.next_offset() < intent.snapshot.len() as u64 {
                    let from = file.next_offset() as usize;
                    let end = (from + SNAPSHOT_CHUNK_BYTES).min(intent.snapshot.len());
                    file.append(from as u64, &intent.snapshot[from..end]).map_err(io_err)?;
                }
                file.verify_complete().map_err(io_err)?;
                Ok((intent, encoded))
            })
            .await
            .map_err(io_err)??;
            let setup_root = root.clone();
            let plan = intent.spec.plan;
            let (lock, mut initialized, complete) = tokio::task::spawn_blocking(move || {
                if !resume_only {
                    fs::create_dir_all(&setup_root).map_err(io_err)?;
                    fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(setup_root.join(".keratin.lock"))
                        .map_err(io_err)?;
                }
                let lock = lock_existing_log(&setup_root).map_err(io_err)?;
                if resume_only
                    && read_bounded(&setup_root.join("intent"), MAX_METADATA)?.as_deref()
                        != Some(encoded.as_slice())
                {
                    return Err(corrupt("saved recovery stage changed before locked resume"));
                }
                persist_exact(&setup_root.join("intent"), &encoded)?;
                stage_boundary("intent");
                let marker = |name| -> Result<bool> {
                    match read_bounded(&setup_root.join(name), 44)? {
                        None => Ok(false),
                        Some(bytes) if bytes == plan => Ok(true),
                        Some(_) => Err(corrupt("recovery staging marker differs from intent")),
                    }
                };
                Ok::<_, StromaError>((lock, marker("initialized")?, marker("complete")?))
            })
            .await
            .map_err(io_err)??;
            if complete && !initialized {
                return Err(corrupt("completed stage lost initialization marker"));
            }
            if !initialized {
                // Only an unpublished attempt owned by this stage lock is disposable.
                let unfinished = root.join("messages");
                tokio::task::spawn_blocking(move || {
                    if unfinished.try_exists().map_err(io_err)? {
                        fs::remove_dir_all(unfinished).map_err(io_err)?;
                    }
                    Ok::<_, StromaError>(())
                }).await.map_err(io_err)??;
                if let Some(seal) = reuse_seal {
                    if st.try_reuse_stage_messages(&intent, limits, &seal, root.join("messages")).await? {
                        let initialized_root = root.clone();
                        tokio::task::spawn_blocking(move || {
                            persist_exact(&initialized_root.join("initialized"), &plan)?;
                            stage_boundary("initialized");
                            Ok::<_, StromaError>(())
                        }).await.map_err(io_err)??;
                        initialized = true;
                    }
                }
            }
            if !initialized {
                let admission_root = root.clone();
                tokio::task::spawn_blocking(move || admit_stage_bytes(&admission_root, limits,
                    (cfg.segment_preallocate_bytes as u64).saturating_add(1024 * 1024))).await.map_err(io_err)??;
            }
            // Unfinished staging is not accepted history and can repair an
            // interrupted append. Completed data must never silently lose a tail.
            let messages = Arc::new(
                match runtime {
                    Some(runtime) => {
                        Keratin::open_with_runtime(root.join("messages"), cfg, complete, runtime)
                            .await
                    }
                    None if complete => {
                        Keratin::open_preserving_history(root.join("messages"), cfg).await
                    }
                    None => Keratin::open(root.join("messages"), cfg).await,
                }
                .map_err(io_err)?,
            );
            messages.become_follower();
            if !initialized {
                messages
                    .advance_epoch(intent.spec.fence_epoch)
                    .await
                    .map_err(io_err)?;
                messages
                    .destructive_reset_to_checkpoint_at_epoch(
                        intent.spec.payload_log.head,
                        intent.spec.fence_epoch,
                    )
                    .await
                    .map_err(io_err)?;
                messages.sync().await.map_err(io_err)?;
                let initialized_root = root.clone();
                tokio::task::spawn_blocking(move || {
                    recovery_seal::sync_directories(&initialized_root.join("messages"))?;
                    persist_exact(&initialized_root.join("initialized"), &plan)?;
                    stage_boundary("initialized");
                    Ok::<_, StromaError>(())
                })
                .await
                .map_err(io_err)??;
            }
            let mut inner = StageInner {
                messages,
                root,
                intent,
                limits,
                used: 0,
                complete,
                needs_reopen: false,
                _lock: lock,
                _permit: permit,
            };
            inner = tokio::task::spawn_blocking(move || {
                inner.used = scan(&inner, complete)?;
                Ok::<_, StromaError>(inner)
            })
            .await
            .map_err(io_err)??;
            if complete {
                inner.messages.freeze();
            }
            Ok(QueueRecoveryStage {
                inner: Arc::new(AsyncMutex::new(inner)),
            })
        })
        .await
        .map_err(io_err)?
    }
}

enum SnapshotInput { Bytes(Vec<u8>), Transferred, Resume }

impl QueueRecoveryStage {
    /// Export the immutable snapshot of a completed stage. The receiving target
    /// must independently authorize its plan and verify the snapshot digest.
    pub async fn completed_snapshot(&self) -> Result<Vec<u8>> {
        let guard = self.inner.lock().await;
        if !guard.complete || guard.needs_reopen {
            return Err(invalid("stage is not complete"));
        }
        Ok(guard.intent.snapshot.clone())
    }

    pub async fn completed_snapshot_chunk(&self, from: u64, max_bytes: usize) -> Result<Vec<u8>> {
        if !self.inner.lock().await.complete { return Err(invalid("stage is not complete")); }
        self.snapshot_chunk(from, max_bytes).await
    }

    /// The snapshot itself was verified before opening this private stage. This
    /// does not imply the retained log has completed transfer or verification.
    pub async fn snapshot_chunk(&self, from: u64, max_bytes: usize) -> Result<Vec<u8>> {
        let guard = self.inner.lock().await;
        if guard.needs_reopen || max_bytes == 0 || max_bytes > SNAPSHOT_CHUNK_BYTES
            || from >= guard.intent.snapshot.len() as u64 {
            return Err(invalid("completed snapshot chunk is unavailable or outside bounds"));
        }
        let from = from as usize;
        Ok(guard.intent.snapshot[from..(from + max_bytes).min(guard.intent.snapshot.len())].to_vec())
    }

    /// Bounded, verified messages from a completed stage, even when the original
    /// sealed source is gone. No actor or ordinary writer can access this log.
    pub async fn read_completed_messages(
        &self,
        from: u64,
        max_records: u32,
        max_bytes: u32,
    ) -> Result<RecoveryReadPage> {
        if max_records == 0
            || max_records > 4096
            || max_bytes == 0
            || u64::from(max_bytes) > MAX_PAGE
        {
            return Err(invalid("invalid stage read budget"));
        }
        let guard = self
            .inner
            .clone()
            .try_lock_owned()
            .map_err(|_| invalid("stage is busy"))?;
        if !guard.complete
            || guard.needs_reopen
            || from < guard.intent.spec.payload_log.head
            || from > guard.intent.spec.payload_log.next
        {
            return Err(invalid("invalid completed stage read"));
        }
        tokio::task::spawn_blocking(move || {
            scan(&guard, true)?;
            let s = &guard.intent.spec;
            let mut page = RecoveryReadPage {
                history_id: s.source_history,
                source: s.payload_log.source,
                from,
                next: from,
                end: s.payload_log.next,
                records: vec![],
                snapshot_bytes: vec![],
            };
            let mut used = 0usize;
            let mut full = false;
            guard
                .messages
                .frozen_reader()
                .map_err(io_err)?
                .scan(|record| {
                    if record.offset < from || full {
                        return Ok(());
                    }
                    let size = 18 + record.headers.len() + record.payload.len();
                    if page.records.len() == max_records as usize
                        || size > max_bytes as usize - used
                    {
                        if page.records.is_empty() {
                            return Err(io::Error::other("next staged record exceeds page budget"));
                        }
                        full = true;
                    } else {
                        used += size;
                        page.next = record.offset + 1;
                        page.records.push(RecoveryRecord {
                            offset: record.offset,
                            flags: record.flags,
                            headers: record.headers.to_vec(),
                            payload: record.payload.to_vec(),
                        });
                    }
                    Ok(())
                })
                .map_err(io_err)?;
            Ok(page)
        })
        .await
        .map_err(io_err)?
    }

    pub async fn next_offset(&self) -> u64 {
        self.inner.lock().await.messages.next_offset()
    }

    /// An exact repeated page is idempotent. Gaps, partial overlaps, substituted
    /// records and pages beyond the selected tail are refused. A started append
    /// owns its admission guard until durability/accounting finish.
    pub async fn append(&self, page: RecoveryReadPage) -> Result<u64> {
        let mut guard = self
            .inner
            .clone()
            .try_lock_owned()
            .map_err(|_| invalid("recovery stage is busy"))?;
        let s = &guard.intent.spec;
        if guard.complete || guard.needs_reopen {
            return Err(invalid(
                "recovery stage is complete or needs reopen after an append error",
            ));
        }
        let mut bytes = 0u64;
        for (index, record) in page.records.iter().enumerate() {
            if page.from.checked_add(index as u64) != Some(record.offset) {
                return Err(invalid("noncontiguous recovery staging page"));
            }
            bytes = bytes
                .checked_add(record_bytes(record)?)
                .ok_or_else(|| invalid("page size overflow"))?;
        }
        let next = guard.messages.next_offset();
        let duplicate = page.next <= next;
        if page.source != s.payload_log.source
            || page.history_id != s.source_history
            || page.end != s.payload_log.next
            || page.from < s.payload_log.head
            || page.next > page.end
            || page.from.checked_add(page.records.len() as u64) != Some(page.next)
            || page.records.is_empty()
            || page.records.len() > 4096
            || !page.snapshot_bytes.is_empty()
            || bytes > MAX_PAGE
            || page.from > next
            || !duplicate && page.from != next
        {
            return Err(invalid(
                "recovery page differs from staging identity, boundary or budget",
            ));
        }
        if !duplicate && guard.used.checked_add(bytes).is_none_or(|n| n > guard.limits.max_bytes) {
            return Err(invalid(Limit::message(Budget::StageBytes, guard.limits.max_bytes, guard.used, bytes)));
        }
        tokio::spawn(async move {
            if !duplicate {
                let root = guard.root.clone();
                let limits = guard.limits;
                // Include log framing/index slack. This is admission, not an
                // exclusive reservation against other processes or writers.
                tokio::task::spawn_blocking(move || admit_stage_bytes(&root, limits,
                    bytes.saturating_mul(2).saturating_add(1024 * 1024))).await.map_err(io_err)??;
            }
            let records = page
                .records
                .into_iter()
                .map(|r| Message {
                    flags: r.flags,
                    headers: r.headers,
                    payload: r.payload,
                })
                .collect();
            guard.needs_reopen = true;
            let outcome = guard
                .messages
                .append_replicated_batch(
                    guard.intent.spec.fence_epoch,
                    page.from,
                    records,
                    Some(KDurability::AfterFsync),
                )
                .await
                .map_err(io_err)?;
            if !matches!(
                outcome,
                ReplicatedAppendOutcome::Applied(_)
                    | ReplicatedAppendOutcome::AlreadyPresent { .. }
            ) {
                return Err(corrupt("unexpected recovery staging append outcome"));
            }
            stage_boundary("payloads");
            if !duplicate {
                guard.used += bytes;
            }
            guard.needs_reopen = false;
            Ok(guard.messages.next_offset())
        })
        .await
        .map_err(io_err)?
    }

    /// Reuse a completed verification on this immutable, non-serving handle.
    /// Reopen validates the whole staged log before setting `complete`. The
    /// installer separately validates the frozen input and installed output.
    pub async fn completion_receipt(&self) -> Result<Option<QueueRecoveryStageReceipt>> {
        let guard = self.inner.clone().try_lock_owned().map_err(|_| invalid("recovery stage is busy"))?;
        if guard.needs_reopen { return Err(invalid("recovery stage must reopen after an append error")); }
        Ok(guard.complete.then(|| {
            let s = &guard.intent.spec;
            QueueRecoveryStageReceipt { plan: s.plan, source_history: s.source_history,
                snapshot_digest: s.snapshot_digest, message_next: s.message_next, event_next: s.event_next }
        }))
    }

    /// Verify the complete selected payload range and its live-state subset, then
    /// fsync a completion receipt. The staged log remains unavailable to actors.
    pub async fn finish(&self) -> Result<QueueRecoveryStageReceipt> {
        let mut guard = self
            .inner
            .clone()
            .try_lock_owned()
            .map_err(|_| invalid("recovery stage is busy"))?;
        if guard.needs_reopen {
            return Err(invalid("recovery stage must reopen after an append error"));
        }
        tokio::spawn(async move {
            guard.messages.sync().await.map_err(io_err)?;
            tokio::task::spawn_blocking(move || {
                scan(&guard, true)?;
                recovery_seal::sync_directories(&guard.root.join("messages"))?;
                persist_exact(&guard.root.join("complete"), &guard.intent.spec.plan)?;
                stage_boundary("complete");
                guard.complete = true;
                guard.messages.freeze();
                let s = &guard.intent.spec;
                Ok(QueueRecoveryStageReceipt {
                    plan: s.plan,
                    source_history: s.source_history,
                    snapshot_digest: s.snapshot_digest,
                    message_next: s.message_next,
                    event_next: s.event_next,
                })
            })
            .await
            .map_err(io_err)?
        })
        .await
        .map_err(io_err)?
    }
}

fn stage_boundary(_name: &str) {
    #[cfg(test)]
    if std::env::var("STROMA_STAGE_CRASH_BOUNDARY").as_deref() == Ok(_name) {
        fs::write(std::env::var_os("STROMA_STAGE_CRASH_READY").unwrap(), _name).unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(test)]
#[path = "recovery_stage_tests.rs"]
mod tests;
