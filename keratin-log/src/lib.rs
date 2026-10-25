mod batcher;
mod config;
#[cfg(feature = "io-fault-injection")]
pub mod io_fault_injection;
mod runtime_config;
mod writer_settings;
pub use writer_settings::{WriterSettings, WriterSettingsSnapshot};
mod durability;
mod sync_status;
pub use sync_status::SyncStatus;
mod diagnostics;
mod frozen_reader;
mod index;
mod keratin;
pub mod lock;
mod log;
mod manifest;
mod manifest_worker;
mod reader;
mod record;
mod recovery;
pub mod reusable_buffer;
mod segment;
mod shared_segment;
mod zero_ahead;
pub use shared_segment::FrozenForkStats;
mod drain_diagnostics;
mod latched_cut;
mod suffix_repair;
mod tail_cache;
pub mod util;
mod writer;
#[cfg(feature = "writer-stage-trace")]
mod writer_stage_trace;
#[cfg(feature = "writer-stage-trace")]
pub use writer_stage_trace::{finish_writer_stage_trace, flush_writer_stage_trace};

pub use config::*;
pub use diagnostics::{
    CONTROL_HISTORY_LIMIT, LogControlEvent, LogControlKind, ReplicationOverlapDiagnostic,
};
pub use durability::{DurableFrontier, DurableWatermark, KDurability, WrittenWatermark};
pub use frozen_reader::{
    FrozenLogCursor, FrozenLogReader, MAX_FROZEN_RECORD_BYTES, create_log_lock_file,
    lock_existing_log,
};
pub use keratin::{Keratin, KeratinReplicaExt, KeratinRole};
pub use log::{AppendResult, ReplicatedAppendMode, ReplicatedAppendOutcome};
pub use reader::{LogReader, OwnedRecord};
pub use record::{Message, ReceivedMessage};
pub use runtime_config::{
    LogRuntimeConfig, LogRuntimeSettings, LogRuntimeSnapshot, LogRuntimeStatus,
};
pub use segment::SegmentInfo;
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

    fn pair() -> (Box<dyn AppendCompletion<E> + Send>, Self::Receiver);
}

impl AppendCompletion<writer::IoError> for KeratinAppendCompletion {
    fn complete(self: Box<Self>, res: Result<AppendResult, writer::IoError>) {
        let _ = self.result_tx.send(res);
    }
}

impl CompletionPair<writer::IoError> for KeratinAppendCompletion {
    type Receiver = tokio::sync::oneshot::Receiver<Result<AppendResult, writer::IoError>>;

    fn pair() -> (
        Box<dyn AppendCompletion<writer::IoError> + Send>,
        Self::Receiver,
    ) {
        let (result_tx, rx) = tokio::sync::oneshot::channel();
        (Box::new(KeratinAppendCompletion { result_tx }), rx)
    }
}
