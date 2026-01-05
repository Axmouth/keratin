mod batcher;
mod config;
mod durability;
mod index;
mod keratin;
mod log;
mod manifest;
mod reader;
mod record;
mod recovery;
mod segment;
pub mod util;
mod writer;

pub use config::*;
pub use durability::KDurability;
pub use keratin::Keratin;
pub use log::AppendResult;
pub use reader::{LogReader, OwnedRecord};
pub use record::{Message, ReceivedMessage};

#[derive(Debug)]
pub enum KeratinError {
    // ===== External / environmental =====
    Io(std::io::Error), // kernel / filesystem failure
    OutOfSpace,         // ENOSPC / EDQUOT etc
    PermissionDenied,
    DeviceGone,

    // ===== Data integrity =====
    CorruptSegment,
    CorruptIndex,
    BadMagic,
    BadVersion,
    CrcMismatch,
    TruncatedRecord,

    // ===== Logical invariants =====
    OffsetRegression, // non-monotonic append
    SegmentOverflow,  // would exceed segment max
    SegmentNotFound,
    IndexNotFound,

    // ===== Durability contract =====
    FlushFailed,
    FsyncFailed,
    ManifestCommitFailed,

    // ===== Internal / bug =====
    InvariantViolation(&'static str),
}

pub type KResult<T> = Result<T, KeratinError>;

impl From<std::io::Error> for KeratinError {
    fn from(e: std::io::Error) -> Self {
        use std::io::ErrorKind::*;
        match e.kind() {
            NotFound => KeratinError::DeviceGone,
            PermissionDenied => KeratinError::PermissionDenied,
            WriteZero | UnexpectedEof => KeratinError::OutOfSpace,
            _ => KeratinError::Io(e),
        }
    }
}

pub struct AppendReceipt {
    pub result_rx: tokio::sync::oneshot::Receiver<Result<AppendResult, writer::IoError>>,
}

impl AppendReceipt {
    pub async fn wait(self) -> Result<AppendResult, writer::IoError> {
        self.result_rx.await.unwrap_or_else(|_| Err(writer::IoError::new("writer dropped")))
    }
}