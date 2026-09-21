use fs2::FileExt;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use tokio::sync::oneshot;

use crate::log::{AppendResult, Log, LogState, ReplicatedAppendMode, ReplicatedAppendOutcome};
use crate::reader::LogReader;
use crate::record::Message;
use crate::segment::{SegmentInfo, read_segment_created_ts_ms};
use crate::tail_cache::TailCache;
use crate::writer::{AppendCompletionTarget, AppendPayload, AppendReq, IoError, WriterHandle};
use crate::{AppendCompletion, DurableFrontier, KDurability, KeratinConfig};

#[derive(Debug)]
pub struct Keratin {
    root: std::path::PathBuf,
    tx: crossbeam_channel::Sender<WriterCmd>,
    log_state: Arc<LogState>,
    segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
    tail_cache: Arc<TailCache>,
    _lock: Option<File>,
    shutdown_started: AtomicBool,
    role: AtomicU8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeratinRole {
    Owner,
    Follower,
    Frozen,
}

impl KeratinRole {
    const OWNER: u8 = 0;
    const FOLLOWER: u8 = 1;
    const FROZEN: u8 = 2;

    fn from_u8(value: u8) -> Self {
        match value {
            Self::FOLLOWER => Self::Follower,
            Self::FROZEN => Self::Frozen,
            _ => Self::Owner,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Owner => Self::OWNER,
            Self::Follower => Self::FOLLOWER,
            Self::Frozen => Self::FROZEN,
        }
    }
}

// Internal single-impl extension trait. We deliberately use async fn in the
// trait. We do not need the explicit Send-bound flexibility that the desugared
// form would give, so the lint does not apply here.
#[allow(async_fn_in_trait)]
pub trait KeratinReplicaExt {
    async fn append_replicated_batch(
        &self,
        epoch: u64,
        first_offset: u64,
        records: Vec<Message>,
        durability: Option<KDurability>,
    ) -> Result<ReplicatedAppendOutcome, IoError>;

    async fn append_replicated_batch_with_mode(
        &self,
        epoch: u64,
        first_offset: u64,
        records: Vec<Message>,
        mode: ReplicatedAppendMode,
        durability: Option<KDurability>,
    ) -> Result<ReplicatedAppendOutcome, IoError>;

    /// Local maintenance reset (for example, truncating a corrupt local tail).
    /// Remote checkpoints must use the epoch-fenced variant below.
    async fn destructive_reset_to_checkpoint(&self, next_offset: u64) -> std::io::Result<()>;

    /// Reset only if the writer is still at the checkpoint's exact epoch.
    /// Checkpoint transfer must not advance the assignment fencing epoch.
    async fn destructive_reset_to_checkpoint_at_epoch(
        &self,
        next_offset: u64,
        expected_epoch: u64,
    ) -> std::io::Result<()>;
}

pub enum WriterCmd {
    Append(AppendReq),
    ReplicatedAppend {
        epoch: u64,
        first_offset: u64,
        records: Vec<Message>,
        mode: ReplicatedAppendMode,
        durability: Option<KDurability>,
        respond_to: oneshot::Sender<Result<ReplicatedAppendOutcome, IoError>>,
    },
    Truncate {
        before: u64,
        respond_to: oneshot::Sender<io::Result<u64>>,
    },
    ResetToCheckpoint {
        next_offset: u64,
        expected_epoch: Option<u64>,
        respond_to: oneshot::Sender<io::Result<()>>,
    },
    RepairSuffix {
        next_offset: u64,
        expected_epoch: u64,
        respond_to: oneshot::Sender<io::Result<()>>,
    },
    AdvanceEpoch {
        epoch: u64,
        respond_to: oneshot::Sender<io::Result<u64>>,
    },
    /// Make everything staged so far durable (fsync now) without appending.
    /// Lets a caller stage with `AfterWrite` and fsync separately, e.g. to fsync
    /// two logs concurrently after staging both.
    Sync {
        respond_to: oneshot::Sender<io::Result<()>>,
    },
    Shutdown {
        notify_tx: oneshot::Sender<()>,
    },
    SizeEstimate {
        respond_to: oneshot::Sender<io::Result<u64>>,
    },
}

impl Keratin {
    /// Rebuild unopened logs for an interrupted checkpoint installation.
    ///
    /// Destructive: the caller must have a durable installation journal which
    /// prevents ordinary access until the entire checkpoint is installed.
    /// Entries are (root, checkpoint next offset, expected epoch). All roots are
    /// locked and all epochs checked before any files are removed. The outer
    /// journal must remain pending if any rebuild fails.
    pub async fn rebuild_for_checkpoint_recovery(logs: Vec<(PathBuf, u64, u64)>) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            let mut locks = Vec::with_capacity(logs.len());
            for (root, _, expected_epoch) in &logs {
                let lock = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(root.join(".keratin.lock"))?;
                lock.try_lock_exclusive()?;
                locks.push(lock);
                let manifest = crate::manifest::Manifest::read_from(&mut File::open(
                    crate::manifest::Manifest::path(root),
                )?)?;
                if manifest.epoch != *expected_epoch {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "checkpoint recovery cannot change the persisted epoch",
                    ));
                }
            }
            for (root, next_offset, expected_epoch) in &logs {
                Log::rebuild_checkpoint_files(root, *next_offset, *expected_epoch)?;
            }
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    pub async fn open(root: impl AsRef<Path>, cfg: KeratinConfig) -> std::io::Result<Self> {
        Self::open_with_history_guard(root, cfg, false).await
    }

    /// Reopen accepted history without silently repairing/reusing a damaged tail.
    /// Valid complete records and zero preallocation are supported. Missing files,
    /// pending suffix repair and nonzero damaged tails require coordinated recovery.
    pub async fn open_preserving_history(
        root: impl AsRef<Path>,
        cfg: KeratinConfig,
    ) -> std::io::Result<Self> {
        Self::open_with_history_guard(root, cfg, true).await
    }

    async fn open_with_history_guard(
        root: impl AsRef<Path>,
        cfg: KeratinConfig,
        preserve_history: bool,
    ) -> std::io::Result<Self> {
        // Reject invalid capacities before creating directories or opening logs.
        let writer_channel_capacity = cfg.writer_channel_capacity()?;
        let root = root.as_ref().to_path_buf();

        std::fs::create_dir_all(&root)?;

        let lock_path = root.join(".keratin.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        tracing::debug!(
            "attempting to acquire Keratin lock at {}",
            lock_path.display()
        );
        // Try to acquire exclusive lock (non-blocking)
        lock_file.try_lock_exclusive().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Keratin already open for {}", root.display()),
            )
        })?;
        tracing::debug!("acquired Keratin lock at {}", lock_path.display());

        let now = crate::util::unix_millis();

        let log_state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));

        let (log, segment_mapping) = Log::open(
            &root,
            now,
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            cfg.tail_cache_bytes,
            cfg.segment_preallocate_bytes,
            cfg.force_recovery_scan,
            preserve_history,
            log_state.clone(),
        )?;

        log_state.tail.store(log.next_offset(), Ordering::SeqCst); // add getter or read field
        // Recovery baseline: authoritatively establish the frontier at startup.
        log_state.durable.reset(log.durable_end_exclusive());
        log_state
            .head
            .store(log.manifest.head_offset, Ordering::SeqCst);
        log_state.epoch.store(log.current_epoch(), Ordering::SeqCst);

        log_state.diagnostics.lock().record(
            crate::LogControlKind::Opened,
            log.current_epoch(),
            log.next_offset(),
        );
        let tail_cache = log.tail_cache();
        let WriterHandle { tx } =
            crate::writer::spawn_writer(log, cfg, log_state.clone(), writer_channel_capacity);

        Ok(Self {
            root,
            tx,
            log_state,
            segment_mapping,
            tail_cache,
            _lock: Some(lock_file),
            shutdown_started: AtomicBool::new(false),
            role: AtomicU8::new(KeratinRole::Owner.as_u8()),
        })
    }

    pub fn reader(&self) -> LogReader {
        LogReader::new(
            &self.root,
            self.segment_mapping.clone(),
            self.tail_cache.clone(),
        )
    }

    pub fn append_enqueue(
        &self,
        payload: Message,
        durability: Option<KDurability>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<(), IoError> {
        self.ensure_role(KeratinRole::Owner, "append")?;
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::One(payload),
                durability,
                completion: completion.into(),
                staged_offset_tx: None,
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(())
    }

    /// Append one record, reporting its assigned base offset as soon as it is
    /// staged (offset assigned, in the in-memory buffer) over `staged_offset_tx`,
    /// while `completion` still resolves at the chosen durability point. Lets a
    /// caller act on the offset without waiting for the flush. Used by the stream
    /// express lane.
    pub fn append_enqueue_staged(
        &self,
        payload: Message,
        durability: Option<KDurability>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
        staged_offset_tx: oneshot::Sender<u64>,
    ) -> Result<(), IoError> {
        self.ensure_role(KeratinRole::Owner, "append")?;
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::One(payload),
                durability,
                completion: completion.into(),
                staged_offset_tx: Some(staged_offset_tx),
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(())
    }

    pub fn append_enqueue_receiver(
        &self,
        payload: Message,
        durability: Option<KDurability>,
    ) -> Result<oneshot::Receiver<Result<AppendResult, IoError>>, IoError> {
        self.ensure_role(KeratinRole::Owner, "append")?;
        let (result_tx, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::One(payload),
                durability,
                completion: AppendCompletionTarget::Oneshot(result_tx),
                staged_offset_tx: None,
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(rx)
    }

    pub async fn append(
        &self,
        payload: Message,
        durability: Option<KDurability>,
    ) -> Result<AppendResult, IoError> {
        let rx = self.append_enqueue_receiver(payload, durability)?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    pub fn append_batch_enqueue(
        &self,
        payloads: Vec<Message>,
        durability: Option<KDurability>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<(), IoError> {
        self.ensure_role(KeratinRole::Owner, "append_batch")?;
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::Many(payloads),
                durability,
                completion: completion.into(),
                staged_offset_tx: None,
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(())
    }

    /// Batch counterpart of [`append_enqueue_staged`]: reports the batch's assigned
    /// base offset as soon as it is staged, before the durability ack.
    pub fn append_batch_enqueue_staged(
        &self,
        payloads: Vec<Message>,
        durability: Option<KDurability>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
        staged_offset_tx: oneshot::Sender<u64>,
    ) -> Result<(), IoError> {
        self.ensure_role(KeratinRole::Owner, "append_batch")?;
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::Many(payloads),
                durability,
                completion: completion.into(),
                staged_offset_tx: Some(staged_offset_tx),
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(())
    }

    pub fn append_batch_enqueue_receiver(
        &self,
        payloads: Vec<Message>,
        durability: Option<KDurability>,
    ) -> Result<oneshot::Receiver<Result<AppendResult, IoError>>, IoError> {
        self.ensure_role(KeratinRole::Owner, "append_batch")?;
        let (result_tx, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::Many(payloads),
                durability,
                completion: AppendCompletionTarget::Oneshot(result_tx),
                staged_offset_tx: None,
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(rx)
    }

    pub async fn append_batch(
        &self,
        payloads: Vec<Message>,
        durability: Option<KDurability>,
    ) -> Result<AppendResult, IoError> {
        let rx = self.append_batch_enqueue_receiver(payloads, durability)?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    pub fn next_offset(&self) -> u64 {
        self.log_state.tail.load(Ordering::Acquire)
    }

    /// Strict sequential disk verification of a quiescent live log. The caller
    /// must finish outstanding writes/fsync, keep this instance alive, and retain
    /// the frozen role for the reader's entire lifetime. Unlike a cold seal, the
    /// live durable frontier can be ahead of the latest manifest checkpoint.
    pub fn frozen_reader(&self) -> io::Result<crate::FrozenLogReader> {
        let next = self.next_offset();
        if self.role() != KeratinRole::Frozen
            || self.log_state.durable.load().first_non_durable() < next
        {
            return Err(io::Error::other("strict live scan requires a frozen, fully durable log"));
        }
        crate::FrozenLogReader::open_live(&self.root, self.current_epoch(), self.head_offset(), next)
    }

    pub fn durable_offset(&self) -> u64 {
        // The watermark is the exclusive durable frontier (count of durable
        // records). Convert back to an inclusive offset for this public accessor.
        // An empty log (frontier `0`) saturates to `0`, matching the long-standing
        // inclusive contract (`0` = empty or offset 0).
        self.log_state
            .durable
            .load()
            .first_non_durable()
            .saturating_sub(1)
    }

    pub fn head_offset(&self) -> u64 {
        self.log_state.head.load(Ordering::Acquire)
    }

    /// Read-only summary of the log's segments, oldest first, for retention
    /// decisions. The last entry is the active segment (`sealed == false`), which
    /// truncation never drops. Builds from the in-memory segment list plus a small
    /// header read per segment, so it does not disturb the writer.
    pub fn segment_infos(&self) -> io::Result<Vec<SegmentInfo>> {
        let bases: Vec<(u64, PathBuf)> = {
            let map = self.segment_mapping.read();
            let mut v: Vec<(u64, PathBuf)> = map.iter().map(|(k, v)| (*k, v.clone())).collect();
            v.sort_unstable_by_key(|(b, _)| *b);
            v
        };
        if bases.is_empty() {
            return Ok(Vec::new());
        }
        let next_offset = self.next_offset();
        let active_base = bases.last().map(|(b, _)| *b).expect("non-empty checked");
        let mut out = Vec::with_capacity(bases.len());
        for i in 0..bases.len() {
            let (base, path) = &bases[i];
            let end_offset = if i + 1 < bases.len() {
                bases[i + 1].0
            } else {
                next_offset
            };
            let bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            let created_ts_ms = read_segment_created_ts_ms(path)?;
            out.push(SegmentInfo {
                base_offset: *base,
                end_offset,
                bytes,
                created_ts_ms,
                sealed: *base != active_base,
            });
        }
        Ok(out)
    }

    pub fn current_epoch(&self) -> u64 {
        self.log_state.epoch.load(Ordering::Acquire)
    }

    pub async fn advance_epoch(&self, epoch: u64) -> std::io::Result<u64> {
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::AdvanceEpoch { epoch, respond_to })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    /// Make everything staged so far durable (fsync). Pair with `AfterWrite`
    /// appends to fsync separately (e.g. two logs concurrently).
    pub async fn sync(&self) -> std::io::Result<()> {
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::Sync { respond_to })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    pub async fn truncate_before(&self, before: u64) -> std::io::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::Truncate {
                before,
                respond_to: tx,
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    pub fn role(&self) -> KeratinRole {
        KeratinRole::from_u8(self.role.load(Ordering::Acquire))
    }

    pub fn become_owner(&self) {
        self.set_role(KeratinRole::Owner, crate::LogControlKind::Owner);
    }

    pub fn become_follower(&self) {
        self.set_role(KeratinRole::Follower, crate::LogControlKind::Follower);
    }

    pub fn freeze(&self) {
        self.set_role(KeratinRole::Frozen, crate::LogControlKind::Frozen);
    }

    fn set_role(&self, role: KeratinRole, kind: crate::LogControlKind) {
        if self.role.swap(role.as_u8(), Ordering::AcqRel) != role.as_u8() {
            self.record_control_event(kind, self.next_offset());
        }
    }

    /// Record rare control operations only. Never called for ordinary appends.
    pub fn record_control_event(&self, kind: crate::LogControlKind, offset: u64) {
        self.log_state
            .diagnostics
            .lock()
            .record(kind, self.current_epoch(), offset);
    }

    fn ensure_role(&self, expected: KeratinRole, op: &str) -> Result<(), IoError> {
        let actual = self.role();
        if actual == expected {
            return Ok(());
        }
        Err(IoError::new(format!(
            "{op} requires Keratin role {expected:?}, current role is {actual:?}"
        )))
    }

    pub async fn shutdown(&self) -> std::io::Result<()> {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let (notify_tx, notify_rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::Shutdown { notify_tx })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        notify_rx
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?;
        tracing::debug!("shutdown command sent to Keratin writer");
        tracing::debug!(
            "releasing Keratin lock at {}",
            self.root.join(".keratin.lock").display()
        );
        self._lock.as_ref().map(|f| f.unlock()).transpose()?;
        self._lock.as_ref().map(|f| f.sync_all()).transpose()?;
        tracing::debug!(
            "released Keratin lock at {}",
            self.root.join(".keratin.lock").display()
        );
        Ok(())
    }

    pub async fn estimate_disk_used(&self) -> std::io::Result<u64> {
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::SizeEstimate { respond_to })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    /// Recovery-only suffix repair, preserving the retained prefix. Caller must
    /// quiesce other operations and readers; the writer drains outstanding I/O.
    /// The epoch is checked in writer order before mutation. A repair I/O failure
    /// stops this writer; reopen resumes the durable journal before normal access.
    pub async fn repair_suffix_at_epoch(&self, next_offset: u64, expected_epoch: u64) -> io::Result<()> {
        self.ensure_role(KeratinRole::Follower, "repair_suffix_at_epoch")
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
        let (respond_to, rx) = oneshot::channel();
        self.tx.send(WriterCmd::RepairSuffix { next_offset, expected_epoch, respond_to })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    async fn reset_checkpoint_inner(
        &self,
        next_offset: u64,
        expected_epoch: Option<u64>,
    ) -> io::Result<()> {
        self.ensure_role(KeratinRole::Follower, "destructive_reset_to_checkpoint")
            .map_err(|err| io::Error::new(io::ErrorKind::PermissionDenied, err))?;
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::ResetToCheckpoint {
                next_offset,
                expected_epoch,
                respond_to,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    /// Force to close without waiting for writer to acknowledge shutdown (for testing)
    /// Do NOT touch in normal operation, as it may cause data loss or corruption if the writer is still processing appends.
    pub async fn force_close(self) -> std::io::Result<()> {
        tracing::warn!("force closing Keratin instance without waiting for writer acknowledgment");
        if let Some(lock) = &self._lock {
            lock.unlock()?;
            lock.sync_all()?;
        }

        std::mem::forget(self); // 💀 Drop will NOT run: the lock was already released above
        Ok(())
    }
}

impl KeratinReplicaExt for Keratin {
    async fn append_replicated_batch(
        &self,
        epoch: u64,
        first_offset: u64,
        records: Vec<Message>,
        durability: Option<KDurability>,
    ) -> Result<ReplicatedAppendOutcome, IoError> {
        self.append_replicated_batch_with_mode(
            epoch,
            first_offset,
            records,
            ReplicatedAppendMode::ExactFit,
            durability,
        )
        .await
    }

    async fn append_replicated_batch_with_mode(
        &self,
        epoch: u64,
        first_offset: u64,
        records: Vec<Message>,
        mode: ReplicatedAppendMode,
        durability: Option<KDurability>,
    ) -> Result<ReplicatedAppendOutcome, IoError> {
        self.ensure_role(KeratinRole::Follower, "append_replicated_batch")?;
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::ReplicatedAppend {
                epoch,
                first_offset,
                records,
                mode,
                durability,
                respond_to,
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    async fn destructive_reset_to_checkpoint(&self, next_offset: u64) -> std::io::Result<()> {
        self.reset_checkpoint_inner(next_offset, None).await
    }

    async fn destructive_reset_to_checkpoint_at_epoch(
        &self,
        next_offset: u64,
        expected_epoch: u64,
    ) -> std::io::Result<()> {
        self.reset_checkpoint_inner(next_offset, Some(expected_epoch))
            .await
    }
}

impl Drop for Keratin {
    fn drop(&mut self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        let (notify_tx, mut notify_rx) = oneshot::channel();
        if let Err(e) = self.tx.send(WriterCmd::Shutdown { notify_tx }) {
            tracing::warn!("failed to send shutdown command to Keratin writer: {e}");
            return;
        } else {
            let started = std::time::Instant::now();
            while let Err(e) = notify_rx.try_recv() {
                if e == tokio::sync::oneshot::error::TryRecvError::Closed {
                    tracing::warn!("Keratin writer shutdown notification channel closed");
                    return;
                }
                if started.elapsed() >= std::time::Duration::from_secs(5) {
                    tracing::warn!(
                        "timed out waiting for Keratin writer shutdown notification for {}",
                        self.root.display()
                    );
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }

        tracing::debug!("shutdown command sent to Keratin writer");
        tracing::debug!(
            "releasing Keratin lock at {}",
            self.root.join(".keratin.lock").display()
        );
        if let Err(e) = self._lock.as_ref().map(|f| f.unlock()).transpose() {
            tracing::warn!("failed to unlock Keratin lock file: {e}");
        }
        if let Err(e) = self._lock.as_ref().map(|f| f.sync_all()).transpose() {
            tracing::warn!("failed to sync Keratin lock file: {e}");
        }
        tracing::debug!(
            "released Keratin lock at {}",
            self.root.join(".keratin.lock").display()
        );
    }
}

#[cfg(test)]
mod writer_buffer_tests {
    use super::*;

    #[tokio::test]
    async fn writer_buffer_factor_rejects_invalid_values_before_open() {
        let dir = crate::test_dir!("invalid_writer_buffer_factor");
        let unopened = dir.root.join("unopened");
        for factor in [0, 129, usize::MAX] {
            let cfg = KeratinConfig {
                writer_buffer_factor: factor,
                ..KeratinConfig::default()
            };
            let error = Keratin::open(&unopened, cfg).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(!unopened.exists());
        }
    }

    #[tokio::test]
    async fn small_writer_buffers_preserve_durable_completions_and_reopen() {
        let dir = crate::test_dir!("small_writer_buffers");
        let cfg = KeratinConfig {
            writer_buffer_factor: 1,
            ..KeratinConfig::default()
        };
        let log = Keratin::open(&dir.root, cfg).await.unwrap();
        assert_eq!(log.tx.capacity(), Some(64));
        // More pending completions than either channel can hold. The notifier
        // must continue draining while the writer applies backpressure.
        let receipts: Vec<_> = (0u64..2048)
            .map(|i| {
                log.append_enqueue_receiver(
                    Message {
                        flags: 0,
                        headers: vec![],
                        payload: i.to_le_bytes().to_vec(),
                    },
                    Some(KDurability::AfterFsync),
                )
                .unwrap()
            })
            .collect();
        for (i, receipt) in receipts.into_iter().enumerate() {
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), receipt)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(result.base_offset, i as u64);
            assert_eq!(result.count, 1);
        }
        log.shutdown().await.unwrap();
        drop(log);

        // Capacity is local startup configuration, not part of the log format.
        let reopened = Keratin::open(&dir.root, KeratinConfig::default())
            .await
            .unwrap();
        assert_eq!(reopened.tx.capacity(), Some(8192));
        assert_eq!(reopened.next_offset(), 2048);
        let records = reopened.reader().scan_from(0, 2048).unwrap();
        assert_eq!(records.len(), 2048);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.offset, i as u64);
            assert_eq!(record.payload, (i as u64).to_le_bytes());
        }
        reopened.shutdown().await.unwrap();
    }
}

#[cfg(test)]
mod checkpoint_epoch_tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_reset_checks_epoch_in_writer_order() {
        let dir = crate::test_dir!("checkpoint_writer_epoch_order");
        let cfg = KeratinConfig::test_default();
        let log = Keratin::open(&dir.root, cfg).await.unwrap();
        log.become_follower();
        log.append_replicated_batch(
            0,
            0,
            vec![Message {
                flags: 0,
                headers: vec![],
                payload: b"preserved".to_vec(),
            }],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
        let observed_epoch = log.current_epoch();

        // The caller saw epoch 0, but an epoch change is ahead of its reset on
        // the writer queue. A caller-side check alone would miss this ordering.
        let (advanced, advanced_rx) = oneshot::channel();
        log.tx
            .send(WriterCmd::AdvanceEpoch {
                epoch: 1,
                respond_to: advanced,
            })
            .unwrap();
        let (reset, reset_rx) = oneshot::channel();
        log.tx
            .send(WriterCmd::ResetToCheckpoint {
                next_offset: 0,
                expected_epoch: Some(observed_epoch),
                respond_to: reset,
            })
            .unwrap();
        assert_eq!(advanced_rx.await.unwrap().unwrap(), 1);
        let error = reset_rx.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(log.head_offset(), 0);
        assert_eq!(log.next_offset(), 1);
        assert_eq!(
            log.reader().scan_from(0, 2).unwrap()[0].payload,
            b"preserved"
        );
        drop(log);

        let log = Keratin::open(&dir.root, cfg).await.unwrap();
        assert_eq!(log.current_epoch(), 1);
        assert_eq!(log.next_offset(), 1);
        assert_eq!(
            log.reader().scan_from(0, 2).unwrap()[0].payload,
            b"preserved"
        );
        log.become_follower();
        assert!(
            log.destructive_reset_to_checkpoint_at_epoch(0, 2)
                .await
                .is_err()
        );
        assert_eq!(log.current_epoch(), 1);
        assert_eq!(log.next_offset(), 1);
        log.destructive_reset_to_checkpoint_at_epoch(0, 1)
            .await
            .unwrap();
        assert_eq!(log.next_offset(), 0);
        assert_eq!(log.current_epoch(), 1);
        drop(log);

        let log = Keratin::open(&dir.root, cfg).await.unwrap();
        assert_eq!(log.next_offset(), 0);
        assert_eq!(log.current_epoch(), 1);
    }
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_reset_drains_prior_fsync_completions() {
    let dir = crate::test_dir!("reset_fsync_drain");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    log.become_follower();
    for _ in 0..16 {
        log.append_replicated_batch(
            0,
            0,
            vec![Message {
                flags: 0,
                headers: vec![],
                payload: vec![7; 4096],
            }],
            Some(KDurability::AfterWrite),
        )
        .await
        .unwrap();
        let mut syncs = Vec::new();
        for _ in 0..16 {
            let (respond_to, rx) = oneshot::channel();
            log.tx.send(WriterCmd::Sync { respond_to }).unwrap();
            syncs.push(rx);
        }
        let (respond_to, reset) = oneshot::channel();
        log.tx
            .send(WriterCmd::ResetToCheckpoint {
                next_offset: 0,
                expected_epoch: Some(0),
                respond_to,
            })
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), reset)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for mut rx in syncs {
            assert!(
                matches!(rx.try_recv(), Ok(Ok(()))),
                "reset returned while an earlier fsync could still publish its old frontier"
            );
        }
        assert_eq!(log.next_offset(), 0);
        assert_eq!(log.log_state.durable.load().first_non_durable(), 0);
    }
    log.shutdown().await.unwrap();
}
