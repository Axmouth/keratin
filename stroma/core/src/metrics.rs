use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use serde::Serialize;

use crate::state::CommandPrio;

#[inline]
fn current_epoch_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is set before the UNIX epoch")
        .as_secs()
}

#[derive(Debug, Default)]
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
        if self.buckets.is_empty() {
            return;
        }
        let now = current_epoch_secs() / self.resolution_secs.max(1);
        let idx = (now as usize) % self.buckets.len().max(1);

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        if self.buckets.is_empty() {
            return;
        }
        let now = current_epoch_secs() / self.resolution_secs.max(1);
        let idx = (now as usize) % self.buckets.len().max(1);

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(many, Ordering::Relaxed);
    }

    pub fn sum_last(&self, seconds: usize) -> u64 {
        let now = current_epoch_secs() / self.resolution_secs.max(1);
        let mut sum = 0;

        for i in 0..seconds.min(self.buckets.len()) {
            let idx = ((now as isize - i as isize).rem_euclid(self.buckets.len().max(1) as isize))
                as usize;
            sum += self.buckets[idx].load(Ordering::Relaxed);
        }

        sum
    }

    pub fn rate_per_sec(&self, window_secs: usize) -> f64 {
        self.sum_last(window_secs) as f64 / window_secs.min(self.buckets.len()).max(1) as f64
    }
}

#[derive(Debug, Default)]
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
    pub max_micros: AtomicU64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
            max_micros: AtomicU64::new(0),
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
        self.max_micros
            .fetch_max(d.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn avg_micros(&self) -> Option<f64> {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            None
        } else {
            Some(self.total_micros.load(Ordering::Relaxed) as f64 / count as f64)
        }
    }

    pub fn max_micros(&self) -> Option<u64> {
        let v = self.max_micros.load(Ordering::Relaxed);
        if v == 0 && self.count.load(Ordering::Relaxed) == 0 {
            None
        } else {
            Some(v)
        }
    }
}

#[derive(Debug, Default)]
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

#[derive(Debug, Default)]
pub struct StromaMetrics {
    // TODO: Maybe rework to pass a queue metric per queue with its own depth stats and keep a map here, passing references from it around(Arcs)
    // === Per-priority-lane arrays — one slot per CommandPrio variant ===
    pub cmd_queue_depth: [AtomicUsize; 5],
    pub cmd_dispatched: [OpStats; 5],
    pub cmd_wait_latency: [LatencyStats; 5],
    pub cmd_process_latency: [LatencyStats; 5],

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

    // === Recent replication cache ===
    pub replication_cache: ReplicationCacheMetrics,

    // === Current state gauges ===
    pub queues_active: AtomicUsize,
}

impl StromaMetrics {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            cmd_queue_depth: std::array::from_fn(|_| AtomicUsize::new(0)),
            cmd_dispatched: std::array::from_fn(|_| OpStats::new(bucket_count)),
            cmd_wait_latency: std::array::from_fn(|_| LatencyStats::default()),
            cmd_process_latency: std::array::from_fn(|_| LatencyStats::default()),

            enqueue: OpStats::new(bucket_count),
            poll_ready: OpStats::new(bucket_count),
            ack: OpStats::new(bucket_count),
            nack: OpStats::new(bucket_count),
            mark_inflight: OpStats::new(bucket_count),
            collect_expired: OpStats::new(bucket_count),

            snapshot: SnapshotMetrics::default(),
            recovery: RecoveryMetrics::default(),

            event_log_appends: BatchStats::new(bucket_count),
            msg_log_appends: BatchStats::new(bucket_count),
            event_log_reads: OpStats::new(bucket_count),
            msg_log_reads: OpStats::new(bucket_count),
            log_truncations: OpStats::new(bucket_count),
            replication_cache: ReplicationCacheMetrics::default(),

            queues_active: AtomicUsize::new(0),
        }
    }

    pub fn log_snapshot(&self) -> LogMetricsSnapshot {
        LogMetricsSnapshot {
            event_log: log_kind_snapshot(&self.event_log_appends, &self.event_log_reads),
            message_log: log_kind_snapshot(&self.msg_log_appends, &self.msg_log_reads),
            truncations_total: self.log_truncations.total.load(Ordering::Relaxed),
        }
    }
    pub fn command_snapshot(&self) -> CommandMetricsSnapshot {
        let mut per_lane = HashMap::new();
        for prio in CommandPrio::all() {
            let key = prio.name().to_string();
            let depth = self.cmd_queue_depth[prio.idx()].load(Ordering::Relaxed);
            let dispatched = &self.cmd_dispatched[prio.idx()];
            let wait = &self.cmd_wait_latency[prio.idx()];
            let proc_lat = &self.cmd_process_latency[prio.idx()];

            per_lane.insert(
                key,
                LaneSnapshot {
                    current_depth: depth,
                    dispatched_per_sec_1m: dispatched.ops.rate_per_sec(60),
                    avg_wait_ms: wait.avg_micros().map(|v| v / 1000.0),
                    max_wait_ms: wait.max_micros().map(|v| v as f64 / 1000.0),
                    avg_process_ms: proc_lat.avg_micros().map(|v| v / 1000.0),
                    max_process_ms: proc_lat.max_micros().map(|v| v as f64 / 1000.0),
                    total_dispatched: dispatched.total.load(Ordering::Relaxed),
                },
            );
        }

        let mut per_kind = HashMap::new();
        let kinds = [
            ("enqueue", &self.enqueue),
            ("poll_ready", &self.poll_ready),
            ("ack", &self.ack),
            ("nack", &self.nack),
            ("mark_inflight", &self.mark_inflight),
            ("collect_expired", &self.collect_expired),
        ];
        for (name, ops) in kinds {
            per_kind.insert(
                name.to_string(),
                CmdKindSnapshot {
                    total: ops.total.load(Ordering::Relaxed),
                    per_sec_1m: ops.ops.rate_per_sec(60),
                    avg_latency_ms: ops.latency.avg_micros().map(|v| v / 1000.0),
                },
            );
        }

        CommandMetricsSnapshot { per_lane, per_kind }
    }

    /// Standalone helper for just queue depths, used in `debug_snapshot()`.
    pub fn cmd_queue_depths_snapshot(&self) -> HashMap<String, usize> {
        let mut out = HashMap::new();
        for prio in CommandPrio::all() {
            out.insert(
                format!("{prio:?}"),
                self.cmd_queue_depth[prio.idx()].load(Ordering::Relaxed),
            );
        }
        out
    }
}

#[derive(Debug, Default)]
pub struct ReplicationCacheMetrics {
    pub message_hits: AtomicU64,
    pub message_misses: AtomicU64,
    pub event_hits: AtomicU64,
    pub event_misses: AtomicU64,
    pub evicted_records: AtomicU64,
    pub retained_bytes: AtomicUsize,
}

impl ReplicationCacheMetrics {
    #[inline]
    pub fn record_message_read(&self, hit: bool) {
        let target = if hit {
            &self.message_hits
        } else {
            &self.message_misses
        };
        target.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_event_read(&self, hit: bool) {
        let target = if hit {
            &self.event_hits
        } else {
            &self.event_misses
        };
        target.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_evicted_records(&self, count: usize) {
        self.evicted_records
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_retained_bytes(&self, bytes: usize) {
        self.retained_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReplicationCacheMetricsSnapshot {
        ReplicationCacheMetricsSnapshot {
            message_hits: self.message_hits.load(Ordering::Relaxed),
            message_misses: self.message_misses.load(Ordering::Relaxed),
            event_hits: self.event_hits.load(Ordering::Relaxed),
            event_misses: self.event_misses.load(Ordering::Relaxed),
            evicted_records: self.evicted_records.load(Ordering::Relaxed),
            retained_bytes: self.retained_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ReplicationCacheMetricsSnapshot {
    pub message_hits: u64,
    pub message_misses: u64,
    pub event_hits: u64,
    pub event_misses: u64,
    pub evicted_records: u64,
    pub retained_bytes: usize,
}

fn log_kind_snapshot(appends: &BatchStats, reads: &OpStats) -> LogKindSnapshot {
    let total_batches = appends.batches.total.load(Ordering::Relaxed);
    let total_items = appends.items_total.load(Ordering::Relaxed);

    LogKindSnapshot {
        appends_per_sec_1m: appends.batches.ops.rate_per_sec(60),
        avg_append_latency_ms: appends.batches.latency.avg_micros().map(|v| v / 1000.0),
        max_append_latency_ms: appends
            .batches
            .latency
            .max_micros()
            .map(|v| v as f64 / 1000.0),
        avg_batch_size: if total_batches == 0 {
            None
        } else {
            Some(total_items as f64 / total_batches as f64)
        },
        total_appends: total_batches,
        total_items,
        total_bytes: appends.bytes_total.load(Ordering::Relaxed),
        reads_per_sec_1m: reads.ops.rate_per_sec(60),
        avg_read_latency_ms: reads.latency.avg_micros().map(|v| v / 1000.0),
        total_reads: reads.total.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Serialize)]
pub struct CommandMetricsSnapshot {
    pub per_lane: HashMap<String, LaneSnapshot>,
    pub per_kind: HashMap<String, CmdKindSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct LaneSnapshot {
    pub current_depth: usize,
    pub dispatched_per_sec_1m: f64,
    pub avg_wait_ms: Option<f64>,
    pub max_wait_ms: Option<f64>,
    pub avg_process_ms: Option<f64>,
    pub max_process_ms: Option<f64>,
    pub total_dispatched: u64,
}

#[derive(Debug, Serialize)]
pub struct CmdKindSnapshot {
    pub total: u64,
    pub per_sec_1m: f64,
    pub avg_latency_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct LogMetricsSnapshot {
    pub event_log: LogKindSnapshot,
    pub message_log: LogKindSnapshot,
    pub truncations_total: u64,
}

#[derive(Debug, Serialize)]
pub struct LogKindSnapshot {
    pub appends_per_sec_1m: f64,
    pub avg_append_latency_ms: Option<f64>,
    pub max_append_latency_ms: Option<f64>,
    pub avg_batch_size: Option<f64>,
    pub total_appends: u64,
    pub total_items: u64,
    pub total_bytes: u64,
    pub reads_per_sec_1m: f64,
    pub avg_read_latency_ms: Option<f64>,
    pub total_reads: u64,
}

#[derive(Debug, Default)]
pub struct SnapshotMetrics {
    pub attempts: OpStats,
    pub skipped_not_dirty: AtomicU64,
    pub skipped_in_progress: AtomicU64,
    pub clone_latency: LatencyStats, // time to clone state in the actor
    pub encode_latency: LatencyStats, // time to serialize in background
    pub write_latency: LatencyStats, // time to fsync to disk
    pub total_latency: LatencyStats, // wall clock from trigger to done
    pub bytes_written: AtomicU64,
    pub last_snapshot_size_bytes: AtomicU64,
}

impl SnapshotMetrics {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            attempts: OpStats::new(bucket_count),
            skipped_not_dirty: Default::default(),
            skipped_in_progress: Default::default(),
            clone_latency: Default::default(),
            encode_latency: Default::default(),
            write_latency: Default::default(),
            total_latency: Default::default(),
            bytes_written: Default::default(),
            last_snapshot_size_bytes: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct RecoveryMetrics {
    pub startup_duration: LatencyStats,
    pub snapshot_load_latency: LatencyStats,
    pub events_replayed: AtomicU64,
    pub replay_duration: LatencyStats,
    /// Partitions currently quarantined (recovery found a dangling event->message
    /// reference or a corrupt event record). A gauge: up on quarantine, down on
    /// repair.
    pub quarantined: AtomicU64,
    /// Total times a partition has been quarantined since start (monotonic).
    pub quarantines_total: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct RecoveryMetricsSnapshot {
    pub avg_startup_ms: Option<f64>,
    pub max_startup_ms: Option<f64>,
    pub avg_snapshot_load_ms: Option<f64>,
    pub avg_replay_ms: Option<f64>,
    pub max_replay_ms: Option<f64>,
    pub total_events_replayed: u64,
    pub queues_recovered: u64,
    pub quarantined: u64,
    pub quarantines_total: u64,
}
impl RecoveryMetrics {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn snapshot(&self) -> RecoveryMetricsSnapshot {
        RecoveryMetricsSnapshot {
            avg_startup_ms: self.startup_duration.avg_micros().map(|v| v / 1000.0),
            max_startup_ms: self
                .startup_duration
                .max_micros()
                .map(|v| v as f64 / 1000.0),
            avg_snapshot_load_ms: self.snapshot_load_latency.avg_micros().map(|v| v / 1000.0),
            avg_replay_ms: self.replay_duration.avg_micros().map(|v| v / 1000.0),
            max_replay_ms: self.replay_duration.max_micros().map(|v| v as f64 / 1000.0),
            total_events_replayed: self.events_replayed.load(Ordering::Relaxed),
            queues_recovered: self.startup_duration.count.load(Ordering::Relaxed),
            quarantined: self.quarantined.load(Ordering::Relaxed),
            quarantines_total: self.quarantines_total.load(Ordering::Relaxed),
        }
    }
}

// in stroma metrics module

#[derive(Debug, Serialize)]
pub struct SnapshotMetricsSnapshot {
    pub attempts_total: u64,
    pub skipped_not_dirty: u64,
    pub skipped_in_progress: u64,

    pub avg_clone_ms: Option<f64>,
    pub avg_encode_ms: Option<f64>,
    pub avg_write_ms: Option<f64>,
    pub avg_total_ms: Option<f64>,

    // Useful for understanding outliers
    pub max_clone_ms: Option<f64>, // see note below
    pub max_encode_ms: Option<f64>,
    pub max_total_ms: Option<f64>,

    pub last_snapshot_size_bytes: u64,
    pub total_bytes_written: u64,
}

impl SnapshotMetrics {
    pub fn snapshot(&self) -> SnapshotMetricsSnapshot {
        SnapshotMetricsSnapshot {
            attempts_total: self.attempts.total.load(Ordering::Relaxed),
            skipped_not_dirty: self.skipped_not_dirty.load(Ordering::Relaxed),
            skipped_in_progress: self.skipped_in_progress.load(Ordering::Relaxed),

            avg_clone_ms: self.clone_latency.avg_micros().map(|v| v / 1000.0),
            avg_encode_ms: self.encode_latency.avg_micros().map(|v| v / 1000.0),
            avg_write_ms: self.write_latency.avg_micros().map(|v| v / 1000.0),
            avg_total_ms: self.total_latency.avg_micros().map(|v| v / 1000.0),

            // Your LatencyStats currently doesn't track max — see note below.
            // For now, leaving these as None or removing them.
            max_clone_ms: None,
            max_encode_ms: None,
            max_total_ms: None,

            last_snapshot_size_bytes: self.last_snapshot_size_bytes.load(Ordering::Relaxed),
            total_bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}
