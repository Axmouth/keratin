use crate::KDurability;

#[derive(Debug, Clone, Copy)]
pub struct KeratinConfig {
    pub segment_max_bytes: u64,
    pub index_stride_bytes: u32,
    pub max_batch_bytes: usize,
    pub max_batch_records: usize,
    pub batch_linger_ms: u64,
    pub default_durability: KDurability,
    pub fsync_interval_ms: u64,
    /// Floor between commits while the fsync worker is idle. With the default
    /// of 0 the writer self-clocks: a commit is issued as soon as durability
    /// acks are pending and no fsync is in flight, so the effective batching
    /// window is the fsync duration itself. `fsync_interval_ms` remains the
    /// ceiling a pending ack can wait while a fsync is already in flight.
    /// Raise this on storage where a high fsync rate is expensive.
    pub min_fsync_interval_ms: u64,
    pub flush_target_bytes: usize,
    /// In-memory tail-read cache budget in bytes, per log (`0` disables). Recent
    /// flush batches are kept in memory and served to tail-following reads so
    /// they avoid scanning the active segment while it is under fsync/writeback
    /// (which otherwise costs ~40% of delivery throughput under mixed load on a
    /// real drive). Node-local: the useful size tracks the fsync-lag window.
    pub tail_cache_bytes: usize,
    pub force_recovery_scan: bool,
}

impl Default for KeratinConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 256 * 1024 * 1024,
            index_stride_bytes: 64 * 1024,
            max_batch_bytes: 1024 * 1024,
            max_batch_records: 4096,
            batch_linger_ms: 5,
            default_durability: KDurability::AfterFsync,
            fsync_interval_ms: 5,
            min_fsync_interval_ms: 0,
            flush_target_bytes: 32 * 1024 * 1024,
            tail_cache_bytes: 64 * 1024 * 1024,
            force_recovery_scan: false,
        }
    }
}

impl KeratinConfig {
    pub fn test_default() -> Self {
        Self {
            segment_max_bytes: 256 * 1024 * 1024,
            index_stride_bytes: 64 * 1024,
            max_batch_bytes: 1024 * 1024,
            max_batch_records: 4096,
            batch_linger_ms: 5,
            default_durability: KDurability::AfterFsync,
            fsync_interval_ms: 5,
            min_fsync_interval_ms: 0,
            flush_target_bytes: 32 * 1024 * 1024,
            tail_cache_bytes: 64 * 1024 * 1024,
            force_recovery_scan: false,
        }
    }
}
