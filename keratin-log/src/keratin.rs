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
use crate::writer::{AppendCompletionTarget, AppendPayload, AppendReq, IoError, WriterHandle};
use crate::{AppendCompletion, KDurability, KeratinConfig};

#[derive(Debug)]
pub struct Keratin {
    root: std::path::PathBuf,
    tx: crossbeam_channel::Sender<WriterCmd>,
    log_state: Arc<LogState>,
    segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
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

    async fn destructive_reset_to_checkpoint(&self, next_offset: u64) -> std::io::Result<()>;
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
    pub async fn open(root: impl AsRef<Path>, cfg: KeratinConfig) -> std::io::Result<Self> {
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

        let log_state = Arc::new(LogState::new(0, 0, 0));

        let (log, segment_mapping) = Log::open(
            &root,
            now,
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            cfg.force_recovery_scan,
            log_state.clone(),
        )?;

        log_state.tail.store(log.next_offset(), Ordering::SeqCst); // add getter or read field
        log_state
            .durable
            .store(log.durable_watermark(), Ordering::SeqCst); // already exists
        log_state
            .head
            .store(log.manifest.head_offset, Ordering::SeqCst);
        log_state.epoch.store(log.current_epoch(), Ordering::SeqCst);

        let WriterHandle { tx } = crate::writer::spawn_writer(log, cfg, log_state.clone());

        Ok(Self {
            root,
            tx,
            log_state,
            segment_mapping,
            _lock: Some(lock_file),
            shutdown_started: AtomicBool::new(false),
            role: AtomicU8::new(KeratinRole::Owner.as_u8()),
        })
    }

    pub fn reader(&self) -> LogReader {
        LogReader::new(&self.root, self.segment_mapping.clone())
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

    pub fn durable_offset(&self) -> u64 {
        self.log_state.durable.load(Ordering::Acquire)
    }

    pub fn head_offset(&self) -> u64 {
        self.log_state.head.load(Ordering::Acquire)
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
        self.role
            .store(KeratinRole::Owner.as_u8(), Ordering::Release);
    }

    pub fn become_follower(&self) {
        self.role
            .store(KeratinRole::Follower.as_u8(), Ordering::Release);
    }

    pub fn freeze(&self) {
        self.role
            .store(KeratinRole::Frozen.as_u8(), Ordering::Release);
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

    /// Force to close without waiting for writer to acknowledge shutdown (for testing)
    /// Do NOT touch in normal operation, as it may cause data loss or corruption if the writer is still processing appends.
    pub async fn force_close(self) -> std::io::Result<()> {
        tracing::warn!("force closing Keratin instance without waiting for writer acknowledgment");
        if let Some(lock) = &self._lock {
            lock.unlock()?;
            lock.sync_all()?;
        }

        std::mem::forget(self); // skip Drop: the lock was already released above
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
        self.ensure_role(KeratinRole::Follower, "destructive_reset_to_checkpoint")
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::PermissionDenied, err))?;
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::ResetToCheckpoint {
                next_offset,
                respond_to,
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
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
