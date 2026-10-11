//! Per-log writer settings that can change while the log is open. See
//! `Keratin::replace_writer_settings` for when a change takes effect.
use crate::{KDurability, KeratinConfig};
use std::io;

/// The subset of `KeratinConfig` the writer can adopt while open. Fields mean
/// the same as in `KeratinConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterSettings {
    pub default_durability: KDurability,
    pub max_batch_bytes: usize,
    pub max_batch_records: usize,
    pub batch_linger_ms: u64,
    pub fsync_interval_ms: u64,
    pub min_fsync_interval_ms: u64,
}

/// Settings in use and the revision that installed them. The revision
/// starts at 0 on open and goes up by one per applied change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterSettingsSnapshot {
    pub revision: u64,
    pub settings: WriterSettings,
}

impl From<KeratinConfig> for WriterSettings {
    fn from(cfg: KeratinConfig) -> Self {
        Self {
            default_durability: cfg.default_durability,
            max_batch_bytes: cfg.max_batch_bytes,
            max_batch_records: cfg.max_batch_records,
            batch_linger_ms: cfg.batch_linger_ms,
            fsync_interval_ms: cfg.fsync_interval_ms,
            min_fsync_interval_ms: cfg.min_fsync_interval_ms,
        }
    }
}

impl WriterSettings {
    /// Batch limits and the fsync interval must be positive, the minimum
    /// fsync interval must not exceed the fsync interval, and every timer must
    /// fit in an `Instant`.
    pub fn validate(self) -> io::Result<()> {
        if self.max_batch_bytes == 0
            || self.max_batch_records == 0
            || self.fsync_interval_ms == 0
            || self.min_fsync_interval_ms > self.fsync_interval_ms
            || [
                self.batch_linger_ms,
                self.fsync_interval_ms,
                self.min_fsync_interval_ms,
            ]
            .into_iter()
            .any(|ms| {
                std::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(ms))
                    .is_none()
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "writer settings require positive batch limits and fsync ceiling, floor <= ceiling, and representable timers",
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(self, cfg: &mut KeratinConfig) {
        cfg.default_durability = self.default_durability;
        cfg.max_batch_bytes = self.max_batch_bytes;
        cfg.max_batch_records = self.max_batch_records;
        cfg.batch_linger_ms = self.batch_linger_ms;
        cfg.fsync_interval_ms = self.fsync_interval_ms;
        cfg.min_fsync_interval_ms = self.min_fsync_interval_ms;
    }
}
