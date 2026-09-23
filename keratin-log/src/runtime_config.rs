//! Versioned, shared log tuning. Writers sample only when opening/replacing a
//! segment; ordinary append and staging loops use segment-owned values.
use parking_lot::{Mutex, RwLock};
use std::{
    io,
    path::PathBuf,
    sync::{Arc, Weak},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRuntimeConfig {
    pub segment_preallocate_bytes: usize,
}
impl LogRuntimeConfig {
    pub fn validate(self) -> io::Result<()> {
        // Leave room for the write cursor and allocation rounding in signed
        // filesystem offsets. Zero disables preallocation.
        if self.segment_preallocate_bytes as u128 > (i64::MAX as u128 / 2) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation exceeds filesystem offset range",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRuntimeSnapshot {
    pub revision: u64,
    pub config: LogRuntimeConfig,
}
#[derive(Debug, Clone)]
pub struct LogRuntimeStatus {
    pub root: PathBuf,
    pub applied: LogRuntimeSnapshot,
    pub active_segment_base: u64,
    pub effective_preallocate_bytes: u64,
    pub allocation_error: Option<String>,
}

#[derive(Debug)]
pub struct LogRuntimeSettings {
    current: RwLock<LogRuntimeSnapshot>,
    logs: Mutex<Vec<Weak<RwLock<LogRuntimeStatus>>>>,
}
impl LogRuntimeSettings {
    pub fn new(config: LogRuntimeConfig) -> io::Result<Self> {
        config.validate()?;
        Ok(Self {
            current: RwLock::new(LogRuntimeSnapshot {
                revision: 0,
                config,
            }),
            logs: Mutex::new(Vec::new()),
        })
    }
    pub fn current(&self) -> LogRuntimeSnapshot {
        *self.current.read()
    }
    pub fn replace(
        &self,
        expected_revision: u64,
        config: LogRuntimeConfig,
    ) -> io::Result<LogRuntimeSnapshot> {
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| io::Error::other("configuration revision exhausted"))?;
        self.install(expected_revision, LogRuntimeSnapshot { revision, config })
    }
    /// Adopt a revision assigned by an external durable authority, including on
    /// restart. The caller persists first and serializes its update transaction.
    pub fn install(
        &self,
        expected_revision: u64,
        next: LogRuntimeSnapshot,
    ) -> io::Result<LogRuntimeSnapshot> {
        next.config.validate()?;
        let mut current = self.current.write();
        if current.revision != expected_revision {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "stale configuration revision",
            ));
        }
        if next.revision <= current.revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configuration revision must advance",
            ));
        }
        *current = next;
        Ok(next)
    }
    pub fn logs(&self) -> Vec<LogRuntimeStatus> {
        let mut logs = self.logs.lock();
        let mut result = Vec::new();
        logs.retain(|weak| {
            if let Some(status) = weak.upgrade() {
                result.push(status.read().clone());
                true
            } else {
                false
            }
        });
        result.sort_by(|a, b| a.root.cmp(&b.root));
        result
    }
    pub(crate) fn register(&self, status: LogRuntimeStatus) -> Arc<RwLock<LogRuntimeStatus>> {
        let status = Arc::new(RwLock::new(status));
        let mut logs = self.logs.lock();
        logs.retain(|entry| entry.strong_count() > 0);
        logs.push(Arc::downgrade(&status));
        status
    }
}
