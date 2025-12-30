use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

use crate::log::{AppendResult, Log, LogState};
use crate::reader::LogReader;
use crate::record::Message;
use crate::writer::{AppendReq, IoError, WriterHandle};
use crate::{Durability, KeratinConfig};

pub struct Keratin {
    root: std::path::PathBuf,
    tx: crossbeam_channel::Sender<WriterCmd>,
    log_state: Arc<LogState>,
}

pub enum WriterCmd {
    Append(AppendReq),
    Truncate {
        before: u64,
        respond_to: oneshot::Sender<io::Result<u64>>,
    },
}

impl Keratin {
    pub async fn open(root: impl AsRef<Path>, cfg: KeratinConfig) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let now = crate::util::unix_millis();

        let log_state = Arc::new(LogState::new(0, 0, 0));

        let log = Log::open(
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
        })
    }

    pub fn reader(&self) -> LogReader {
        LogReader::new(&self.root)
    }

    pub async fn append_batch(
        &self,
        payloads: Vec<Message>,
        durability: Option<Durability>,
    ) -> Result<AppendResult, IoError> {
        let (tx, rx) = oneshot::channel();
        let req = crate::writer::AppendReq {
            records: payloads,
            durability,
            respond_to: tx,
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
}
