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
    /// Preallocate this many bytes ahead of the active segment's write cursor
    /// (`0` = off). Writes then land in already-allocated blocks, so an fdatasync
    /// skips the block-allocation metadata flush that an extending write pays
    /// (measured ~2.4ms -> ~0.7ms per fdatasync on a consumer nvme). The chunk is
    /// preallocated ahead and grown as the segment fills, and trimmed on clean
    /// shutdown, so disk use tracks written data plus at most one chunk. Set it to
    /// `segment_max_bytes` to preallocate whole segments. Falls back to the extend
    /// path on a filesystem without `fallocate`.
    pub segment_preallocate_bytes: usize,
    /// How many commits may be in flight to the fsync worker at once. Above 1 the
    /// writer keeps issuing small commits while a fsync runs and the worker coalesces
    /// the whole queue into one fdatasync, so low-latency small batches reach
    /// fat-batch throughput. Effectively bounded by the fsync channel capacity.
    pub max_inflight_fsyncs: usize,
    /// Records-per-commit below which the writer pipelines fsyncs: the fixed fsync
    /// cost dominates, so coalescing many small commits into one fdatasync wins. At
    /// or above it the commit is bandwidth-bound and a single fsync stays in flight.
    pub pipeline_commit_records: u64,
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
            segment_preallocate_bytes: 0,
            max_inflight_fsyncs: 8,
            pipeline_commit_records: 2048,
            force_recovery_scan: false,
        }
    }
}

impl KeratinConfig {
    pub fn test_default() -> Self {
        Self::default()
    }
}
