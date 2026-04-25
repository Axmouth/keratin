use std::{sync::atomic::{AtomicU64, AtomicUsize, Ordering}, time::Duration};

use dashmap::DashMap;

use crate::state::CommandPrio;


#[inline]
fn current_epoch_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug)]
pub struct RollingCounter {
    buckets: Vec<AtomicU64>,
    last_tick: AtomicU64,
    resolution_secs: u64,
}

impl RollingCounter {
    pub fn new(resolution_secs: u64, bucket_count: usize) -> Self {
        Self {
            buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            last_tick: AtomicU64::new(0),
            resolution_secs,
        }
    }

    #[inline]
    pub fn incr(&self) {
        let now = current_epoch_secs() / self.resolution_secs;
        let idx = (now as usize) % self.buckets.len();

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        let now = current_epoch_secs() / self.resolution_secs;
        let idx = (now as usize) % self.buckets.len();

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(many, Ordering::Relaxed);
    }

    pub fn sum_last(&self, seconds: usize) -> u64 {
        let now = current_epoch_secs() / self.resolution_secs;
        let mut sum = 0;

        for i in 0..seconds.min(self.buckets.len()) {
            let idx =
                ((now as isize - i as isize).rem_euclid(self.buckets.len() as isize)) as usize;
            sum += self.buckets[idx].load(Ordering::Relaxed);
        }

        sum
    }

    pub fn rate_per_sec(&self, window_secs: usize) -> f64 {
        self.sum_last(window_secs) as f64 / window_secs.min(self.buckets.len().max(1)) as f64
    }
}

#[derive(Debug)]
pub struct OpStats {
    pub ops: RollingCounter,
    pub latency: LatencyStats,
    pub total: AtomicU64,
    pub errors: AtomicU64,
}

impl OpStats {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            ops: RollingCounter::new(1, bucket_count),
            latency: LatencyStats::new(),
            total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn incr(&self) {
        self.ops.incr();
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        self.ops.incr_many(many);
        self.total.fetch_add(many, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_result<T, E>(&self, res: &Result<T, E>) {
        self.incr();
        if res.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
pub struct LatencyStats {
    pub count: AtomicU64,
    pub total_micros: AtomicU64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
        }
    }
}

impl LatencyStats {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn observe(&self, d: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_micros
            .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn avg_micros(&self) -> Option<f64> {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            None
        } else {
            Some(self.total_micros.load(Ordering::Relaxed) as f64 / count as f64)
        }
    }
}


#[derive(Debug)]
pub struct BatchStats {
    pub batches: OpStats,
    pub items_total: AtomicU64,
    pub bytes_total: AtomicU64,
}

impl BatchStats {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            batches: OpStats::new(bucket_count),
            items_total: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn observe(&self, items: usize, bytes: usize) {
        self.batches.incr();
        self.items_total.fetch_add(items as u64, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

pub struct StromaMetrics {
    // === Command queue health (per priority lane) ===
    // This is the observability you wanted for command queue depth.
    pub cmd_queue_depth: DashMap<CommandPrio, AtomicUsize>,
    // Counters for commands dispatched per lane
    pub cmd_dispatched: DashMap<CommandPrio, OpStats>,
    // Time commands spent waiting in queue before being processed
    pub cmd_wait_latency: DashMap<CommandPrio, LatencyStats>,
    // Time spent processing a command (after dequeue)
    pub cmd_process_latency: DashMap<CommandPrio, LatencyStats>,
    
    // === Per-command-kind metrics ===
    // Coarser than per-variant, grouped by logical category
    pub enqueue: OpStats,
    pub poll_ready: OpStats,
    pub ack: OpStats,
    pub nack: OpStats,
    pub mark_inflight: OpStats,
    pub collect_expired: OpStats,
    
    // === Snapshot operations — critical for your current investigation ===
    pub snapshot: SnapshotMetrics,
    
    // === Recovery ===
    pub recovery: RecoveryMetrics,
    
    // === Event log / message log I/O ===
    // Appends to either log, differentiated
    pub event_log_appends: BatchStats,
    pub msg_log_appends: BatchStats,
    pub event_log_reads: OpStats,
    pub msg_log_reads: OpStats,
    pub log_truncations: OpStats,
    
    // === Current state gauges ===
    pub queues_active: AtomicUsize,
    pub total_inflight: AtomicUsize,
    pub total_ready: AtomicUsize,
}

pub struct SnapshotMetrics {
    pub attempts: OpStats,
    pub skipped_not_dirty: AtomicU64,
    pub skipped_in_progress: AtomicU64,
    pub clone_latency: LatencyStats,        // time to clone state in the actor
    pub encode_latency: LatencyStats,       // time to serialize in background
    pub write_latency: LatencyStats,        // time to fsync to disk
    pub total_latency: LatencyStats,        // wall clock from trigger to done
    pub bytes_written: AtomicU64,
    pub last_snapshot_size_bytes: AtomicU64,
}

pub struct RecoveryMetrics {
    pub startup_duration: LatencyStats,
    pub snapshot_load_latency: LatencyStats,
    pub events_replayed: AtomicU64,
    pub replay_duration: LatencyStats,
}