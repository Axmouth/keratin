use std::path::Path;
use tokio::sync::oneshot;

use crate::log::{AppendResult, Log};
use crate::reader::LogReader;
use crate::record::Message;
use crate::writer::{IoError, WriterHandle};
use crate::{Durability, KeratinConfig};

pub struct Keratin {
    root: std::path::PathBuf,
    tx: crossbeam_channel::Sender<crate::writer::AppendReq>,
}

impl Keratin {
    pub async fn open(root: impl AsRef<Path>, cfg: KeratinConfig) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let now = crate::util::unix_millis();

        let log = Log::open(
            &root,
            now,
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
        )?;

        let WriterHandle { tx } = crate::writer::spawn_writer(log, cfg);

        Ok(Self { root, tx })
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
            .send(req)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer gone"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer dropped"))?
    }
}
