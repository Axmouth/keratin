//! Per-log sync reporting shared between the writer and the log handle.
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A point-in-time view of how far durability trails the written records of
/// one log. Read with `Keratin::sync_status`.
///
/// The background sync interval is a target, not a bound on loss. Slow I/O,
/// scheduling delays and failures can stretch the unsynced window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStatus {
    /// Exclusive end of the records written to the OS.
    pub written_next: u64,
    /// Exclusive end of the records an fsync has covered.
    pub durable_next: u64,
    /// `written_next - durable_next`: records that a power loss could drop.
    pub durable_lag_records: u64,
    /// Time since the last successful sync of this log, or since open when
    /// none has run yet. Open leaves every recovered record durable.
    pub since_last_sync: Duration,
    /// Background sync interval the writer is using. Zero means disabled.
    pub background_sync_interval: Duration,
    /// Whether a background sync is in flight.
    pub background_sync_pending: bool,
    /// Background syncs started since open.
    pub background_syncs_started: u64,
    /// The storage failure that stopped this log, if any. A stopped log stays
    /// stopped until reopened.
    pub storage_failure: Option<String>,
}

#[derive(Debug)]
pub(crate) struct SyncTracker {
    opened: Instant,
    // Nanoseconds after `opened` of the last successful sync.
    last_success: AtomicU64,
    interval_nanos: AtomicU64,
    background_pending: AtomicBool,
    background_started: AtomicU64,
    storage_failure: Mutex<Option<String>>,
}

impl SyncTracker {
    pub(crate) fn new() -> Self {
        Self {
            opened: Instant::now(),
            last_success: AtomicU64::new(0),
            interval_nanos: AtomicU64::new(0),
            background_pending: AtomicBool::new(false),
            background_started: AtomicU64::new(0),
            storage_failure: Mutex::new(None),
        }
    }

    pub(crate) fn record_success(&self) {
        let nanos = u64::try_from(self.opened.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last_success.fetch_max(nanos, Ordering::Relaxed);
    }

    pub(crate) fn set_interval(&self, interval: Duration) {
        let nanos = u64::try_from(interval.as_nanos()).unwrap_or(u64::MAX);
        self.interval_nanos.store(nanos, Ordering::Relaxed);
    }

    pub(crate) fn background_started(&self) {
        self.background_started.fetch_add(1, Ordering::Relaxed);
        self.background_pending.store(true, Ordering::Relaxed);
    }

    pub(crate) fn background_finished(&self) {
        self.background_pending.store(false, Ordering::Relaxed);
    }

    pub(crate) fn record_storage_failure(&self, error: &str) {
        self.storage_failure.lock().get_or_insert_with(|| error.to_string());
    }

    pub(crate) fn status(&self, written_next: u64, durable_next: u64) -> SyncStatus {
        let last = Duration::from_nanos(self.last_success.load(Ordering::Relaxed));
        SyncStatus {
            written_next,
            durable_next,
            durable_lag_records: written_next.saturating_sub(durable_next),
            since_last_sync: self.opened.elapsed().saturating_sub(last),
            background_sync_interval: Duration::from_nanos(
                self.interval_nanos.load(Ordering::Relaxed),
            ),
            background_sync_pending: self.background_pending.load(Ordering::Relaxed),
            background_syncs_started: self.background_started.load(Ordering::Relaxed),
            storage_failure: self.storage_failure.lock().clone(),
        }
    }
}
