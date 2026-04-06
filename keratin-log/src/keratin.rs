use fs2::FileExt;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

use crate::log::{AppendResult, Log, LogState};
use crate::reader::LogReader;
use crate::record::Message;
use crate::writer::{AppendPayload, AppendReq, IoError, WriterHandle};
use crate::{
    AppendCompletion, CompletionPair, KDurability, KeratinAppendCompletion, KeratinConfig,
};

#[derive(Debug)]
pub struct Keratin {
    root: std::path::PathBuf,
    tx: crossbeam_channel::Sender<WriterCmd>,
    log_state: Arc<LogState>,
    segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
    _lock: Option<File>,
}

pub enum WriterCmd {
    Append(AppendReq),
    Truncate {
        before: u64,
        respond_to: oneshot::Sender<io::Result<u64>>,
    },
    Shutdown {
        notify_tx: oneshot::Sender<()>,
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

        println!("Attempting to acquire lock on {}", lock_path.display());
        // Try to acquire exclusive lock (non-blocking)
        lock_file.try_lock_exclusive().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Keratin already open for {}", root.display()),
            )
        })?;
        println!("Lock acquired on {}", lock_path.display());

        let now = crate::util::unix_millis();

        let log_state = Arc::new(LogState::new(0, 0, 0));

        let (log, segment_mapping) = Log::open(
            &root,
            now,
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            log_state.clone(),
        )?;

        log_state.tail.store(log.next_offset(), Ordering::SeqCst); // add getter or read field
        log_state
            .durable
            .store(log.durable_watermark(), Ordering::SeqCst); // already exists
        log_state
            .head
            .store(log.manifest.head_offset, Ordering::SeqCst);

        let WriterHandle { tx } = crate::writer::spawn_writer(log, cfg, log_state.clone());

        Ok(Self {
            root,
            tx,
            log_state,
            segment_mapping,
            _lock: Some(lock_file),
        })
    }

    pub fn reader(&self) -> LogReader {
        LogReader::new(&self.root, self.segment_mapping.clone())
    }

    pub fn append_enqueue(
        &self,
        payload: Message,
        durability: Option<KDurability>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), IoError> {
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::One(payload),
                durability,
                completion,
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(())
    }

    pub async fn append(
        &self,
        payload: Message,
        durability: Option<KDurability>,
    ) -> Result<AppendResult, IoError> {
        let (completion, rx) = KeratinAppendCompletion::pair();

        let req = crate::writer::AppendReq {
            records: AppendPayload::One(payload),
            durability,
            completion,
        };
        self.tx
            .send(WriterCmd::Append(req))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }

    pub fn append_batch_enqueue(
        &self,
        payloads: Vec<Message>,
        durability: Option<KDurability>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), IoError> {
        self.tx
            .send(WriterCmd::Append(AppendReq {
                records: AppendPayload::Many(payloads),
                durability,
                completion,
            }))
            .map_err(|_| IoError::new("writer channel closed"))?;

        Ok(())
    }

    pub async fn append_batch(
        &self,
        payloads: Vec<Message>,
        durability: Option<KDurability>,
    ) -> Result<AppendResult, IoError> {
        let (completion, rx) = KeratinAppendCompletion::pair();

        let req = crate::writer::AppendReq {
            records: AppendPayload::Many(payloads),
            durability,
            completion,
        };
        self.tx
            .send(WriterCmd::Append(req))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
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

    pub async fn shutdown(&self) -> std::io::Result<()> {
        let (notify_tx, notify_rx) = oneshot::channel();
        self.tx
            .send(WriterCmd::Shutdown { notify_tx })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        notify_rx
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?;
        println!("Shutdown command sent to writer");
        println!(
            "Releasing lock on {}",
            self.root.join(".keratin.lock").display()
        );
        self._lock.as_ref().map(|f| f.unlock()).transpose()?;
        self._lock.as_ref().map(|f| f.sync_all()).transpose()?;
        println!(
            "Lock released on {}",
            self.root.join(".keratin.lock").display()
        );
        Ok(())
    }

    /// Force to close without waiting for writer to acknowledge shutdown (for testing)
    /// Do NOT touch in normal operation, as it may cause data loss or corruption if the writer is still processing appends.
    pub async fn force_close(self) -> std::io::Result<()> {
        println!("Force closing Keratin instance without waiting for writer acknowledgment");
        if let Some(lock) = &self._lock {
            lock.unlock()?;
            lock.sync_all()?;
        }

        std::mem::forget(self); // 💀 Drop will NOT run
        Ok(())
    }
}

impl Drop for Keratin {
    fn drop(&mut self) {
        let (notify_tx, mut notify_rx) = oneshot::channel();
        if let Err(e) = self.tx.send(WriterCmd::Shutdown { notify_tx }) {
            println!("Failed to send shutdown command to writer: {}", e);
            return;
        } else {
            while let Err(e) = notify_rx.try_recv() {
                tracing::warn!("Failed to receive shutdown notification from writer: {}", e);
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }

        println!("Shutdown command sent to writer");
        println!(
            "Releasing lock on {}",
            self.root.join(".keratin.lock").display()
        );
        if let Err(e) = self._lock.as_ref().map(|f| f.unlock()).transpose() {
            println!("Failed to unlock lock file: {}", e);
        }
        if let Err(e) = self._lock.as_ref().map(|f| f.sync_all()).transpose() {
            println!("Failed to sync lock file: {}", e);
        }
        println!(
            "Lock released on {}",
            self.root.join(".keratin.lock").display()
        );
    }
}
