mod event;
mod state;
mod stroma;

use keratin_log::KDurability;
use thiserror::Error;

pub use stroma::{SnapshotConfig, Stroma};
pub use keratin_log::KeratinConfig;
pub use state::GroupState;

pub type Offset = u64;
pub type UnixMillis = u64;

/// Message supplied by the caller (no offset yet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub flags: u16,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

impl Message {
    #[inline]
    pub fn bytes_len(&self) -> usize {
        // rough, but good for batching decisions
        self.headers.len() + self.payload.len() + 64
    }
}

/// Record returned by the log (offset assigned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub flags: u16,
    pub timestamp_ms: UnixMillis,
    pub offset: Offset,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    AfterWrite,
    AfterFsync,
    AfterReplicated, // for later; can map to AfterFsync initially
}

impl From<Durability> for KDurability {
    fn from(value: Durability) -> Self {
        match value {
            Durability::AfterWrite => KDurability::AfterWrite,
            Durability::AfterFsync => KDurability::AfterFsync,
            Durability::AfterReplicated => KDurability::AfterFsync,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub base_offset: Offset,
    pub count: u32,
}

#[derive(Debug, Error)]
pub enum StromaError {
    #[error("decode: {0}")]
    Decode(String),

    #[error("io: {0}")]
    Io(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("not found")]
    NotFound,

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, StromaError>;
