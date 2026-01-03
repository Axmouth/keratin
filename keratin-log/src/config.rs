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
    pub flush_target_bytes: usize,
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
            flush_target_bytes: 32 * 1024 * 1024,
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
            flush_target_bytes: 32 * 1024 * 1024,
        }
    }
}
