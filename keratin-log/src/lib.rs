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
pub use writer::IoError;

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

#[derive(Debug)]
pub struct KeratinAppendCompletion {
    pub result_tx: tokio::sync::oneshot::Sender<Result<AppendResult, writer::IoError>>,
}

pub trait AppendCompletion<E>: Send + 'static
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn complete(self: Box<Self>, res: Result<AppendResult, E>);
}

pub trait CompletionPair<E> {
    type Receiver;

    fn pair() -> (Box<dyn AppendCompletion<E>>, Self::Receiver);
}

impl AppendCompletion<writer::IoError> for KeratinAppendCompletion {
    fn complete(self: Box<Self>, res: Result<AppendResult, writer::IoError>) {
        let _ = self.result_tx.send(res);
    }
}

impl CompletionPair<writer::IoError> for KeratinAppendCompletion {
    type Receiver = tokio::sync::oneshot::Receiver<Result<AppendResult, writer::IoError>>;

    fn pair() -> (Box<dyn AppendCompletion<writer::IoError>>, Self::Receiver) {
        let (result_tx, rx) = tokio::sync::oneshot::channel();
        (Box::new(KeratinAppendCompletion { result_tx }), rx)
    }
}
