mod config;
mod log;
mod writer;
mod reader;
mod durability;
mod manifest;
mod index;
mod record;
mod recovery;
mod segment;
mod keratin;
pub mod util;

pub use config::*;
pub use durability::Durability;
pub use log::{AppendResult};
pub use keratin::Keratin;
pub use reader::LogReader;
pub use record::{Message, ReceivedMessage};

#[derive(Debug)]
pub enum KeratinError {
    // ===== External / environmental =====
    Io(std::io::Error),               // kernel / filesystem failure
    OutOfSpace,                  // ENOSPC / EDQUOT etc
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
    OffsetRegression,            // non-monotonic append
    SegmentOverflow,             // would exceed segment max
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
