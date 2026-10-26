use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::diagnostics::{LogControlKind, LogDiagnostics, ReplicationOverlapDiagnostic};
use parking_lot::{Mutex, RwLock};

#[cfg(feature = "writer-stage-trace")]
use crate::writer_stage_trace::WriterStageTracer;
use crate::{
    durability::{DurableFrontier, DurableWatermark},
    index::Index,
    manifest::Manifest,
    reader::LogReader,
    record::{Message, Record, encode_record},
    recovery::scan_segment,
    reusable_buffer::ReusableBuffer,
    segment::Segment,
    tail_cache::TailCache,
    util::fsync_dir,
};

// Instrument the same operation in both builds. Only observation is conditional.
#[cfg(feature = "writer-stage-trace")]
macro_rules! observe_stage {
    ($tracer:ident, $stage:expr, $weight:expr, $body:block) => {{
        if let Some((tracer, id)) = $tracer {
            let (records, bytes) = $weight;
            tracer.trace(id, $stage, records, bytes, || $body)
        } else {
            $body
        }
    }};
}
#[cfg(not(feature = "writer-stage-trace"))]
macro_rules! observe_stage {
    ($tracer:ident, $stage:expr, $weight:expr, $body:block) => {{ $body }};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub base_offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicatedAppendOutcome {
    Applied(AppendResult),
    AppliedSuffix {
        requested_first_offset: u64,
        skipped_count: u32,
        result: AppendResult,
    },
    AlreadyPresent {
        first_offset: u64,
        count: u32,
        next_offset: u64,
    },
    Overlap {
        first_offset: u64,
        count: u32,
        next_offset: u64,
    },
    Gap {
        expected_offset: u64,
        first_offset: u64,
    },
    StaleEpoch {
        current_epoch: u64,
        attempted_epoch: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicatedAppendMode {
    ExactFit,
    AppendSuffixAfterKnownPrefix,
}

#[derive(Debug, Clone)]
pub struct LogState {
    pub(crate) diagnostics: Arc<Mutex<LogDiagnostics>>,
    pub head: Arc<AtomicU64>, // inclusive; first available offset (0 initially)
    pub tail: Arc<AtomicU64>, // next offset to assign (exclusive)
    /// Successful local append content bytes in this open instance; scheduling hint only.
    pub(crate) local_append_bytes: Arc<AtomicU64>,
    // Exclusive durable frontier, published through a typed wrapper so the
    // empty-vs-"offset 0 durable" encoding lives in one place. See
    // `DurableWatermark` and `Log::durable_end_exclusive`.
    pub durable: DurableWatermark,
    /// Exclusive written frontier. Published by the log at each write to the
    /// OS, before any fsync job can cover those records, so readers that load
    /// `durable` first and `written` second always see durable <= written.
    pub written: crate::WrittenWatermark,
    pub(crate) sync: Arc<crate::sync_status::SyncTracker>,
    pub epoch: Arc<AtomicU64>,
}

pub(crate) struct FsyncJob {
    durable_end: u64,
    active: File,
    /// Present when the active index is due its lazy sync.
    index: Option<File>,
    /// Work a roll left for the next sync. See `PendingSync`.
    pending: PendingSync,
}

/// Syncs a roll hands to the next fsync, so the writer is not held for them.
/// The sealed segment's index syncs here, and the new segment's directory
/// entry syncs after its data, before any record in it counts as durable.
#[derive(Default)]
pub(crate) struct PendingSync {
    files: Vec<File>,
    dir: Option<PathBuf>,
}

impl PendingSync {
    fn sync(&self) -> io::Result<()> {
        for file in &self.files {
            crate::util::sync_file_data(file)?;
        }
        if let Some(dir) = &self.dir {
            fsync_dir(dir)?;
        }
        Ok(())
    }
}

impl FsyncJob {
    pub(crate) fn durable_end(&self) -> u64 {
        self.durable_end
    }

    /// Only the syncs a roll left for this job. A merged group runs these for
    /// every member before the newest member's full sync.
    pub(crate) fn sync_deferred(&self) -> io::Result<()> {
        self.pending.sync()
    }

    pub(crate) fn sync(&self) -> io::Result<Duration> {
        self.sync_inner(
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub(crate) fn sync_traced(&self, tracer: &WriterStageTracer, id: u64) -> io::Result<Duration> {
        self.sync_inner(Some((tracer, id)))
    }

    fn sync_inner(
        &self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<Duration> {
        crate::drain_diagnostics::fsync(false);
        let started = Instant::now();
        observe_stage!(tracer, "log_fsync", (0, 0), {
            crate::util::sync_file_data(&self.active)
        })?;
        if let Some(index) = &self.index {
            observe_stage!(tracer, "index_fsync", (0, 0), {
                crate::util::sync_file_data(index)
            })?;
        }
        observe_stage!(tracer, "roll_sync", (0, 0), { self.pending.sync() })?;
        Ok(started.elapsed())
    }
}

impl LogState {
    pub fn new(head: u64, tail: u64, durable: DurableFrontier) -> Self {
        Self {
            diagnostics: Arc::new(Mutex::new(LogDiagnostics::default())),
            head: Arc::new(AtomicU64::new(head)),
            tail: Arc::new(AtomicU64::new(tail)),
            local_append_bytes: Arc::new(AtomicU64::new(0)),
            durable: DurableWatermark::new(durable),
            written: crate::WrittenWatermark::new(durable.first_non_durable()),
            sync: Arc::new(crate::sync_status::SyncTracker::new()),
            epoch: Arc::new(AtomicU64::new(0)),
        }
    }
}

// Independent of channel capacity and tail-cache retention.
fn staging_buffer(
    default: usize,
    minimum: usize,
    policy: Option<crate::AdaptiveStagingConfig>,
) -> ReusableBuffer<u8> {
    match policy {
        Some(policy) => {
            ReusableBuffer::adaptive(minimum, policy.decay_interval, policy.idle_release_after)
                .with_empty_resize(policy.empty_resize)
        }
        None => ReusableBuffer::retained(default),
    }
}

pub struct Log {
    // buffers
    write_buf: ReusableBuffer<u8>, // encoded staging
    idx_buf: ReusableBuffer<u8>,   // sparse index buffer

    // Staging uses an inclusive offset; durability uses an exclusive boundary.
    staged_end_offset: u64, // last offset staged into buffers
    durable_end: u64,       // exclusive durable frontier; zero means empty
    /// Exclusive end of the records written to the OS through the staging
    /// buffers. Always `durable_end <= written_end <= next_offset`.
    written_end: u64,
    /// When the last sync of this log started, inline or through the fsync
    /// worker. Paces the writer's background sync.
    pub(crate) last_sync_started: Instant,
    /// Owner records written by appends that did not wait for a sync.
    pub(crate) unclaimed_writes: UnclaimedWrites,
    /// Syncs left by the last roll for the next fsync to take.
    pending_sync: PendingSync,

    root: PathBuf,
    pub manifest: Manifest,
    pub active: Segment,
    pub index: Index,
    pub next_offset: u64,
    last_index_at_log_pos: u64,

    log_state: Arc<LogState>,

    /// In-memory cache of recent flush batches, served to tail-following reads
    /// so they avoid scanning the segment file under active fsync/writeback.
    tail_cache: Arc<TailCache>,
    /// Exclusive next offset not yet pushed to `tail_cache` (= last cached
    /// offset + 1). At flush the batch covers `[flushed_through, staged_end+1)`.
    flushed_through: u64,
    /// EWMA of records per commit. Small commits are fsync-count-bound (the fixed
    /// fsync cost dominates), so the writer pipelines more commits for the fsync
    /// worker to coalesce; fat commits are bandwidth-bound, so it keeps a single
    /// fsync in flight (coalescing bigger writes only makes each fsync slower).
    /// Updated in `commit`.
    pub(crate) recent_commit_records: u64,
    /// `durable_end` of the previous commit, to size the next one.
    pub(crate) last_commit_through: u64,
    /// Bytes to preallocate ahead of the active segment's write cursor (`0` =
    /// off). Applied to the active segment and to each new segment on roll.
    prealloc_chunk: u64,
    runtime: Option<(
        Arc<crate::LogRuntimeSettings>,
        Arc<RwLock<crate::LogRuntimeStatus>>,
    )>,

    // stats
    pub stats: IoStats,
    last_stats_dump: Instant,
    manifest_worker: crate::manifest_worker::ManifestWorker,
    // Drop order keeps the directory locked until the worker has drained.
    _directory_lock: Option<Arc<crate::lock::FileLock>>,
    manifest_flush_interval: Duration,
    last_manifest_flush: Instant,
    /// First flush, sync or manifest failure seen by this handle. After a
    /// failed fsync the kernel may have dropped dirty pages and a later fsync
    /// can still return Ok, so nothing written since the last good sync can be
    /// trusted. Once set, the handle refuses further writes, syncs, frontier
    /// advances and the clean-shutdown mark. Reopening rescans the files.
    storage_failure: Option<String>,

    pub flush_target_bytes: usize, // e.g. 16MB

    segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
}

/// Records an owner wrote with `AfterWrite` or `AfterFlush`, which no append
/// waits to see synced. Only a high-water end and a count are kept, so the
/// count is exact when one sync covers them all, which a background sync
/// always does.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct UnclaimedWrites {
    /// Exclusive end of the highest such record.
    end: u64,
    /// Such records since a sync last covered them all.
    records: u64,
}

impl Log {
    pub(crate) fn note_unclaimed_write(&mut self, end: u64, records: u64) {
        if self.durable_end >= self.unclaimed_writes.end {
            self.unclaimed_writes.records = 0;
        }
        let unclaimed = &mut self.unclaimed_writes;
        unclaimed.end = unclaimed.end.max(end);
        unclaimed.records = unclaimed.records.saturating_add(records);
    }

    /// Unclaimed records not yet covered by any sync. Every sync path moves
    /// `durable_end`, so none of them needs to clear this itself.
    pub(crate) fn unclaimed_unsynced_records(&self) -> u64 {
        if self.durable_end >= self.unclaimed_writes.end {
            0
        } else {
            self.unclaimed_writes.records
        }
    }
}

struct AppendPlanState {
    active_base_offset: u64,
    active_bytes_written: u64,
    index_stride_bytes: u32,
    next_offset: u64,
    last_index_at_log_pos: u64,
}

struct AppendPlanOutcome {
    base_offset: u64,
    end_offset: u64,
    count: u32,
}

#[derive(Default)]
pub struct IoStats {
    pub encode: Duration,
    pub log_write: Duration,
    pub idx_write: Duration,
    pub fsync: Duration,
    pub manifest: Duration,
    pub batches: u64,
    pub records: u64,
    pub bytes: u64,
}

impl IoStats {
    pub fn new() -> Self {
        Self {
            encode: Duration::ZERO,
            log_write: Duration::ZERO,
            idx_write: Duration::ZERO,
            fsync: Duration::ZERO,
            manifest: Duration::ZERO,
            batches: 0u64,
            records: 0u64,
            bytes: 0u64,
        }
    }
}

#[inline(always)]
fn encode_append_payloads_into(
    state: &mut AppendPlanState,
    payloads: &[Message],
    now_ms: u64,
    now: Instant,
    write_buf: &mut ReusableBuffer<u8>,
    idx_buf: &mut ReusableBuffer<u8>,
) -> io::Result<AppendPlanOutcome> {
    debug_assert!(!payloads.is_empty());
    let mut write_buf = write_buf.edit(now);
    let mut idx_buf = idx_buf.edit(now);

    let base_offset = state.next_offset;

    for payload in payloads {
        let offset = state.next_offset;
        let record = Record {
            flags: payload.flags,
            timestamp_ms: now_ms,
            offset,
            headers: &payload.headers,
            payload: &payload.payload,
        };

        let record_start_pos = state.active_bytes_written + write_buf.len() as u64;

        encode_record(&mut write_buf, &record)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        if (record_start_pos - state.last_index_at_log_pos) >= state.index_stride_bytes as u64 {
            let rel = (offset - state.active_base_offset) as u32;

            idx_buf.extend_from_slice(&rel.to_be_bytes());
            idx_buf.extend_from_slice(&0u32.to_be_bytes());
            idx_buf.extend_from_slice(&record_start_pos.to_be_bytes());

            state.last_index_at_log_pos = record_start_pos;
        }

        state.next_offset += 1;
    }

    Ok(AppendPlanOutcome {
        base_offset,
        end_offset: state.next_offset - 1,
        count: payloads.len() as u32,
    })
}

impl Log {
    #[cfg(feature = "writer-stage-trace")]
    pub(crate) fn trace_path(&self) -> &std::path::Path {
        &self.root
    }

    /// Reclaim empty staging allocations without touching durability or caches.
    pub(crate) fn maintain_staging(&mut self, now: Instant) {
        self.write_buf.maintain(now);
        self.idx_buf.maintain(now);
    }

    pub(crate) fn staging_maintenance_deadline(&self) -> Option<Instant> {
        self.write_buf
            .maintenance_deadline()
            .into_iter()
            .chain(self.idx_buf.maintenance_deadline())
            .min()
    }

    /// Retain ownership until outstanding manifest work has drained. Truncation
    /// always runs there, so this holds even with periodic background stores off.
    pub(crate) fn retain_directory_lock(&mut self, lock: Arc<crate::lock::FileLock>) {
        self._directory_lock = Some(lock);
    }

    /// Complete a reset authorized by an external durable installation journal,
    /// before normal log open can encounter partially removed segment files.
    /// The caller must hold the root's exclusive Keratin lock throughout.
    pub(crate) fn rebuild_checkpoint_files(
        root: &Path,
        next_offset: u64,
        expected_epoch: u64,
    ) -> io::Result<()> {
        let mut manifest = Manifest::read_from(&mut File::open(Manifest::path(root))?)?;
        if manifest.epoch != expected_epoch {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint recovery cannot change the persisted epoch",
            ));
        }
        let segments = root.join("segments");
        match fs::remove_dir_all(&segments) {
            Ok(()) => (),
            Err(err) if err.kind() == io::ErrorKind::NotFound => (),
            Err(err) => return Err(err),
        }
        fs::create_dir_all(&segments)?;
        let (segment, index, _) =
            create_segment_pair(root, next_offset, crate::util::unix_millis())?;
        segment.fsync()?;
        index.fsync()?;
        fsync_dir(&segments)?;
        manifest.active_base_offset = next_offset;
        manifest.next_offset = next_offset;
        manifest.head_offset = next_offset;
        manifest.clean_shutdown = false;
        manifest.store_atomic(root)
    }

    // Cohesive open-time parameters, kept as primitives so this low-level layer
    // stays independent of the higher-level KeratinConfig struct.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        root: impl AsRef<Path>,
        now_ms: u64,
        segment_max_bytes: u64,
        index_stride_bytes: u32,
        flush_target_bytes: usize,
        tail_cache_bytes: usize,
        segment_preallocate_bytes: usize,
        force_recovery_scan: bool,
        preserve_history: bool,
        log_state: Arc<LogState>,
        adaptive_staging: Option<crate::AdaptiveStagingConfig>,
    ) -> io::Result<(Self, Arc<RwLock<BTreeMap<u64, PathBuf>>>)> {
        if let Some(policy) = adaptive_staging {
            policy.validate()?;
        }
        let root = root.as_ref().to_path_buf();
        let prealloc_chunk = segment_preallocate_bytes as u64;
        // A cut left by a handle that stopped after a failed sync runs first,
        // before the checks below can mistake its journal for other repair work.
        crate::latched_cut::heal(&root)?;
        // A journaled cut must finish before discovery can mistake an
        // interrupted repair for a normal dirty shutdown or an empty log.
        if preserve_history
            && (root.join(crate::suffix_repair::JOURNAL).try_exists()?
                || !Manifest::path(&root).try_exists()?)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bound history has pending repair or missing manifest",
            ));
        }
        crate::suffix_repair::resume(&root)?;
        // The manifest is created before the first segment, so segments without
        // one mean lost metadata, not a new log. Opening them as an empty log
        // would silently drop their history.
        if !Manifest::path(&root).try_exists()?
            && root.join("segments").try_exists()?
            && !list_segment_bases(&root.join("segments"))?.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "log has segments but no manifest",
            ));
        }
        fs::create_dir_all(root.join("segments"))?;
        fs::create_dir_all(root.join("tmp"))?;

        let mut manifest =
            Manifest::load_or_create(&root, now_ms, segment_max_bytes, index_stride_bytes)?;

        // Discover segments by filename.
        let mut bases = list_segment_bases(&root.join("segments"))?;
        bases.sort_unstable();

        // Validate the entire bound history before any truncation/index repair.
        // The regular recovery pass can then trim zero padding using these scans.
        let mut verified_scans = Vec::new();
        if preserve_history {
            if bases.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bound history has no segments",
                ));
            }
            let mut next = bases[0].0;
            for (base, _) in &bases {
                if *base != next {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bound history has a segment gap",
                    ));
                }
                let file = std::fs::File::open(seg_log_path(&root, *base))?;
                let seg = Segment::open(file, *base)?;
                let scan = crate::recovery::scan_preserving_history(
                    seg.file_ref(),
                    crate::segment::LOG_HEADER_LEN as u64,
                    64 * 1024,
                    *base,
                )?;
                next = match scan.last_offset {
                    Some(offset) => offset.checked_add(1).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "bound history offset overflow")
                    })?,
                    None => *base,
                };
                verified_scans.push(scan);
            }
            if next < manifest.next_offset || bases[0].0 > manifest.head_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bound history does not cover persisted manifest bounds",
                ));
            }
        }
        let mut verified_scans = verified_scans.into_iter();

        // If no segments exist, create first with base=0.
        if bases.is_empty() {
            let (mut seg, idx, seg_path) = create_segment_pair(&root, 0, now_ms)?;
            seg.enable_prealloc(prealloc_chunk);
            manifest.active_base_offset = 0;
            manifest.next_offset = 0;
            manifest.clean_shutdown = false;
            manifest.store_atomic(&root)?;
            let next_offset: u64 = manifest.next_offset;
            let initial: u64 = next_offset.saturating_sub(1);

            let segment_mapping = Arc::new(RwLock::new(BTreeMap::from_iter([(0, seg_path)])));

            return Ok((
                Self {
                    root,
                    last_index_at_log_pos: seg.bytes_written,
                    active: seg,
                    index: idx,
                    next_offset,
                    tail_cache: Arc::new(TailCache::new(
                        log_state.durable.clone(),
                        log_state.written.clone(),
                        tail_cache_bytes,
                    )),
                    flushed_through: next_offset,
                    last_commit_through: next_offset,
                    recent_commit_records: 0,
                    prealloc_chunk,
                    runtime: None,
                    manifest,
                    write_buf: staging_buffer(
                        16 * 1024 * 1024,
                        adaptive_staging.map_or(0, |p| p.write_min_bytes),
                        adaptive_staging,
                    ),
                    idx_buf: staging_buffer(
                        256 * 1024,
                        adaptive_staging.map_or(0, |p| p.index_min_bytes),
                        adaptive_staging,
                    ),
                    stats: IoStats::new(),
                    log_state,
                    last_stats_dump: Instant::now(),
                    manifest_worker: Default::default(),
                    _directory_lock: None,
                    manifest_flush_interval: Duration::from_millis(500),
                    last_manifest_flush: Instant::now(),
                    storage_failure: None,
                    staged_end_offset: initial,
                    durable_end: next_offset,
                    written_end: next_offset,
                    last_sync_started: Instant::now(),
                    unclaimed_writes: UnclaimedWrites::default(),
                    pending_sync: PendingSync::default(),
                    flush_target_bytes,
                    segment_mapping: segment_mapping.clone(),
                },
                segment_mapping,
            ));
        }

        if !preserve_history
            && !force_recovery_scan
            && manifest.clean_shutdown
            && manifest.segment_max_bytes == segment_max_bytes
            && manifest.index_stride_bytes == index_stride_bytes
            && bases
                .last()
                .map(|(base, _)| *base == manifest.active_base_offset)
                .unwrap_or(false)
        {
            let segment_mapping = Arc::new(RwLock::new(BTreeMap::from_iter(bases.clone())));
            let active_base = manifest.active_base_offset;
            let (mut active, index, seg_path) =
                open_or_create_segment_pair(&root, active_base, now_ms)?;
            active.enable_prealloc(prealloc_chunk);
            segment_mapping.write().insert(active_base, seg_path);

            let next_offset = manifest.next_offset;
            manifest.clean_shutdown = false;
            manifest.store_atomic(&root)?;

            let last_index_at_log_pos = active.bytes_written;
            let initial = next_offset.saturating_sub(1);

            return Ok((
                Self {
                    root,
                    manifest,
                    active,
                    index,
                    next_offset,
                    tail_cache: Arc::new(TailCache::new(
                        log_state.durable.clone(),
                        log_state.written.clone(),
                        tail_cache_bytes,
                    )),
                    flushed_through: next_offset,
                    last_commit_through: next_offset,
                    recent_commit_records: 0,
                    prealloc_chunk,
                    runtime: None,
                    last_index_at_log_pos,
                    write_buf: staging_buffer(
                        16 * 1024 * 1024,
                        adaptive_staging.map_or(0, |p| p.write_min_bytes),
                        adaptive_staging,
                    ),
                    idx_buf: staging_buffer(
                        256 * 1024,
                        adaptive_staging.map_or(0, |p| p.index_min_bytes),
                        adaptive_staging,
                    ),
                    log_state,
                    stats: IoStats::new(),
                    last_stats_dump: Instant::now(),
                    manifest_worker: Default::default(),
                    _directory_lock: None,
                    manifest_flush_interval: Duration::from_millis(500),
                    last_manifest_flush: Instant::now(),
                    storage_failure: None,
                    staged_end_offset: initial,
                    durable_end: next_offset,
                    written_end: next_offset,
                    last_sync_started: Instant::now(),
                    unclaimed_writes: UnclaimedWrites::default(),
                    pending_sync: PendingSync::default(),
                    flush_target_bytes,
                    segment_mapping: segment_mapping.clone(),
                },
                segment_mapping,
            ));
        }

        // A segment starts as a renamed spare whose new header is synced with
        // its first records. A crash before that sync can leave the newest
        // files named for a base their header does not carry yet. They hold no
        // acknowledged record, so they go.
        if !preserve_history {
            drop_unstarted_segments(&root, &mut bases)?;
            if bases.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "log has no started segment",
                ));
            }
        }

        // Repair/scan all segments, compute true next_offset.
        let mut computed_next = 0u64;
        let mut previous_end: Option<u64> = None;
        let mut index = 0;
        while index < bases.len() {
            let (base, _) = &bases[index];
            index += 1;
            // A roll hands the outgoing segment's sync to the next fsync, so a
            // crash can leave a sealed segment short of the next one's base.
            // Nothing after such a gap was acknowledged. A checkpoint reset
            // starts at the manifest head, which is the one legitimate gap.
            if let Some(end) = previous_end
                && end < *base
                && *base != manifest.head_offset
            {
                if preserve_history {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("segment {base} starts after a gap at {end}"),
                    ));
                }
                for (dropped, _) in bases.drain(index - 1..) {
                    tracing::warn!(base = dropped, end, "removing a segment after a sealed segment a crash left short");
                    let _ = fs::remove_file(seg_idx_path(&root, dropped));
                    fs::remove_file(seg_log_path(&root, dropped))?;
                }
                fsync_dir(&root.join("segments"))?;
                break;
            }
            let log_path = seg_log_path(&root, *base);
            let f = OpenOptions::new().read(true).write(true).open(&log_path)?;
            // open header to get header_len (fixed in our Segment::open, but we know it)
            // We'll trust Segment::open to validate base.
            let mut seg = Segment::open(f, *base)?;
            // Scan from header_len; our Segment header is fixed size:
            let header_len = (8 + 2 + 2 + 4 + 8 + 8 + 32 + 4) as u64;
            let scan = match verified_scans.next() {
                Some(scan) => scan,
                None => scan_segment(seg.file_ref(), header_len, 64 * 1024, *base)?,
            };
            if scan.last_good_pos < seg.bytes_written {
                // No writable descriptor to a shared inode may survive this barrier.
                drop(seg);
                crate::shared_segment::make_private(&log_path)?;
                let f = OpenOptions::new().read(true).write(true).open(&log_path)?;
                seg = Segment::open(f, *base)?;
                seg.set_len(scan.last_good_pos)?;
            }
            let end = scan.last_offset.map_or(*base, |last| last.saturating_add(1));
            computed_next = computed_next.max(end);
            previous_end = Some(end);

            // The index syncs lazily and may lag the log or be missing.
            let idx_path = seg_idx_path(&root, *base);
            let idxf = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&idx_path)?;
            let mut idx = match Index::open(idxf.try_clone()?, *base) {
                Ok(idx) => idx,
                Err(_) => {
                    idxf.set_len(0)?;
                    Index::create(idxf, *base, now_ms)?
                }
            };
            idx.repair_against_log(
                seg.file_ref(),
                *base,
                scan.last_good_pos,
                index_stride_bytes,
            )?;
            idx.fsync()?;
        }

        let segment_mapping = Arc::new(RwLock::new(BTreeMap::from_iter(bases.clone())));

        // Choose active segment: last base.
        let (active_base, _active_base_path) = bases.last().expect("Already checked empty").clone();
        let (mut active, index, seg_path) =
            open_or_create_segment_pair(&root, active_base, now_ms)?;
        sync_recovered_tail(&root, &active, &index)?;
        active.enable_prealloc(prealloc_chunk);

        segment_mapping.write().insert(active_base, seg_path);

        // Full recovery scan is the source of truth. The manifest may be stale
        // after dirty shutdown, or optimistic if a cleanly written tail is later
        // found corrupt by forced recovery.
        let next_offset = computed_next;
        manifest.active_base_offset = active_base;
        manifest.next_offset = next_offset;
        manifest.segment_max_bytes = segment_max_bytes;
        manifest.index_stride_bytes = index_stride_bytes;
        manifest.clean_shutdown = false;
        manifest.store_atomic(&root)?;

        let last_index_at_log_pos = active.bytes_written;
        let initial: u64 = next_offset.saturating_sub(1);

        Ok((
            Self {
                root,
                manifest,
                active,
                index,
                next_offset,
                tail_cache: Arc::new(TailCache::new(
                    log_state.durable.clone(),
                    log_state.written.clone(),
                    tail_cache_bytes,
                )),
                flushed_through: next_offset,
                last_commit_through: next_offset,
                recent_commit_records: 0,
                prealloc_chunk,
                runtime: None,
                last_index_at_log_pos,
                write_buf: staging_buffer(
                    16 * 1024 * 1024,
                    adaptive_staging.map_or(0, |p| p.write_min_bytes),
                    adaptive_staging,
                ),
                idx_buf: staging_buffer(
                    256 * 1024,
                    adaptive_staging.map_or(0, |p| p.index_min_bytes),
                    adaptive_staging,
                ),
                log_state,
                stats: IoStats::new(),
                last_stats_dump: Instant::now(),
                manifest_worker: Default::default(),
                _directory_lock: None,
                manifest_flush_interval: Duration::from_millis(500),
                last_manifest_flush: Instant::now(),
                storage_failure: None,
                staged_end_offset: initial,
                durable_end: next_offset,
                written_end: next_offset,
                last_sync_started: Instant::now(),
                unclaimed_writes: UnclaimedWrites::default(),
                pending_sync: PendingSync::default(),
                flush_target_bytes,
                segment_mapping: segment_mapping.clone(),
            },
            segment_mapping,
        ))
    }

    #[inline(always)]
    fn append_plan_state(&self, next_offset: u64) -> AppendPlanState {
        AppendPlanState {
            active_base_offset: self.active.base_offset,
            active_bytes_written: self.active.bytes_written,
            index_stride_bytes: self.manifest.index_stride_bytes,
            next_offset,
            last_index_at_log_pos: self.last_index_at_log_pos,
        }
    }

    #[inline(always)]
    fn sync_append_plan_state(&mut self, state: AppendPlanState) {
        self.next_offset = state.next_offset;
        self.last_index_at_log_pos = state.last_index_at_log_pos;
    }

    #[cfg_attr(feature = "writer-stage-trace", allow(dead_code))]
    pub fn stage_append_batch(
        &mut self,
        payloads: &[Message],
        now_ms: u64,
    ) -> io::Result<(AppendResult, u64)> {
        self.stage_append_batch_inner(
            payloads,
            now_ms,
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub fn stage_append_batch_traced(
        &mut self,
        payloads: &[Message],
        now_ms: u64,
        tracer: &WriterStageTracer,
        work_id: u64,
    ) -> io::Result<(AppendResult, u64)> {
        self.stage_append_batch_inner(payloads, now_ms, Some((tracer, work_id)))
    }

    fn stage_append_batch_inner(
        &mut self,
        payloads: &[Message],
        now_ms: u64,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<(AppendResult, u64)> {
        if payloads.is_empty() {
            let end = self.next_offset.saturating_sub(1);
            return Ok((
                AppendResult {
                    base_offset: self.next_offset,
                    count: 0,
                },
                end,
            ));
        }

        // Ensure we have capacity for large sequential writes
        // Estimate worst-case record size; same as before
        let estimated: usize = payloads.iter().map(|p| p.bytes_len()).sum();

        // If staging this would exceed segment capacity once flushed, roll.
        // NOTE: active.bytes_written is on-disk bytes; we also have write_buf pending.
        let pending_bytes = self.write_buf.len() as u64;
        if self.active.bytes_written + pending_bytes + estimated as u64
            > self.manifest.segment_max_bytes
        {
            self.roll(now_ms)?;
        }

        let base_offset = self.next_offset;

        // Reserve to avoid realloc
        let t_reserve = Instant::now();
        self.write_buf.reserve(estimated, t_reserve);
        // Index entries are sparse; reserve modestly
        self.idx_buf.reserve(
            (estimated / (self.manifest.index_stride_bytes as usize).max(1)).max(64),
            t_reserve,
        );

        let t_encode = Instant::now();

        let mut plan_state = self.append_plan_state(base_offset);
        let plan = observe_stage!(tracer, "encode", (payloads.len(), estimated), {
            encode_append_payloads_into(
                &mut plan_state,
                payloads,
                now_ms,
                t_encode,
                &mut self.write_buf,
                &mut self.idx_buf,
            )
        })?;
        self.sync_append_plan_state(plan_state);

        self.stats.encode += t_encode.elapsed();
        self.stats.bytes += payloads.iter().map(|m| m.bytes_len()).sum::<usize>() as u64;
        self.stats.records += payloads.len() as u64;

        let end_offset = plan.end_offset;
        self.staged_end_offset = end_offset;

        Ok((
            AppendResult {
                base_offset: plan.base_offset,
                count: plan.count,
            },
            end_offset,
        ))
    }

    /// `written_overlap` lets an `AfterFlush` retry verify an overlap against
    /// records that are written but not yet durable. Other modes keep the
    /// durable-only check.
    pub fn stage_replicated_append_batch(
        &mut self,
        epoch: u64,
        first_offset: u64,
        payloads: &[Message],
        mode: ReplicatedAppendMode,
        now_ms: u64,
        written_overlap: bool,
    ) -> io::Result<(ReplicatedAppendOutcome, Option<u64>)> {
        let current_epoch = self.manifest.epoch;
        if epoch < current_epoch {
            return Ok((
                ReplicatedAppendOutcome::StaleEpoch {
                    current_epoch,
                    attempted_epoch: epoch,
                },
                None,
            ));
        }
        if epoch > current_epoch {
            self.advance_epoch(epoch)?;
        }

        let count = u32::try_from(payloads.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "replicated batch too large")
        })?;
        let current_next = self.next_offset;

        if payloads.is_empty() {
            return Ok((
                if first_offset == current_next {
                    ReplicatedAppendOutcome::Applied(AppendResult {
                        base_offset: first_offset,
                        count,
                    })
                } else if first_offset < current_next {
                    ReplicatedAppendOutcome::AlreadyPresent {
                        first_offset,
                        count,
                        next_offset: current_next,
                    }
                } else {
                    ReplicatedAppendOutcome::Gap {
                        expected_offset: current_next,
                        first_offset,
                    }
                },
                None,
            ));
        }

        if first_offset > current_next {
            return Ok((
                ReplicatedAppendOutcome::Gap {
                    expected_offset: current_next,
                    first_offset,
                },
                None,
            ));
        }

        let end_offset = first_offset
            .checked_add(payloads.len() as u64)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;

        if first_offset < current_next {
            if end_offset < current_next {
                self.verify_existing_prefix(first_offset, payloads, written_overlap)?;
                return Ok((
                    ReplicatedAppendOutcome::AlreadyPresent {
                        first_offset,
                        count,
                        next_offset: current_next,
                    },
                    None,
                ));
            }

            if mode == ReplicatedAppendMode::ExactFit {
                return Ok((
                    ReplicatedAppendOutcome::Overlap {
                        first_offset,
                        count,
                        next_offset: current_next,
                    },
                    None,
                ));
            }

            let skip = usize::try_from(current_next - first_offset).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replicated suffix skip overflow",
                )
            })?;
            self.verify_existing_prefix(first_offset, &payloads[..skip], written_overlap)?;
            let suffix = &payloads[skip..];
            let (outcome, end_offset) = self.stage_replicated_append_batch(
                epoch,
                current_next,
                suffix,
                ReplicatedAppendMode::ExactFit,
                now_ms,
                written_overlap,
            )?;

            return Ok((
                match outcome {
                    ReplicatedAppendOutcome::Applied(result) => {
                        ReplicatedAppendOutcome::AppliedSuffix {
                            requested_first_offset: first_offset,
                            skipped_count: skip as u32,
                            result,
                        }
                    }
                    _ => outcome,
                },
                end_offset,
            ));
        }

        let estimated: usize = payloads.iter().map(|p| p.bytes_len()).sum();

        let pending_bytes = self.write_buf.len() as u64;
        if self.active.bytes_written + pending_bytes + estimated as u64
            > self.manifest.segment_max_bytes
        {
            self.roll(now_ms)?;
        }

        let t_reserve = Instant::now();
        self.write_buf.reserve(estimated, t_reserve);
        self.idx_buf.reserve(
            (estimated / (self.manifest.index_stride_bytes as usize).max(1)).max(64),
            t_reserve,
        );

        let t_encode = Instant::now();

        let mut plan_state = self.append_plan_state(first_offset);
        let plan = encode_append_payloads_into(
            &mut plan_state,
            payloads,
            now_ms,
            t_encode,
            &mut self.write_buf,
            &mut self.idx_buf,
        )?;
        self.sync_append_plan_state(plan_state);

        self.stats.encode += t_encode.elapsed();
        self.stats.bytes += payloads.iter().map(|m| m.bytes_len()).sum::<usize>() as u64;
        self.stats.records += payloads.len() as u64;
        self.staged_end_offset = plan.end_offset;

        Ok((
            ReplicatedAppendOutcome::Applied(AppendResult {
                base_offset: plan.base_offset,
                count: plan.count,
            }),
            Some(plan.end_offset),
        ))
    }

    #[cfg_attr(feature = "writer-stage-trace", allow(dead_code))]
    pub fn stage_append(
        &mut self,
        payload: &Message,
        now_ms: u64,
    ) -> io::Result<(AppendResult, u64)> {
        self.stage_append_inner(
            payload,
            now_ms,
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub fn stage_append_traced(
        &mut self,
        payload: &Message,
        now_ms: u64,
        tracer: &WriterStageTracer,
        work_id: u64,
    ) -> io::Result<(AppendResult, u64)> {
        self.stage_append_inner(payload, now_ms, Some((tracer, work_id)))
    }

    fn stage_append_inner(
        &mut self,
        payload: &Message,
        now_ms: u64,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<(AppendResult, u64)> {
        // Ensure we have capacity for large sequential writes
        // Estimate worst-case record size; same as before
        let estimated: usize = payload.bytes_len();

        // If staging this would exceed segment capacity once flushed, roll.
        // NOTE: active.bytes_written is on-disk bytes; we also have write_buf pending.
        let pending_bytes = self.write_buf.len() as u64;
        if self.active.bytes_written + pending_bytes + estimated as u64
            > self.manifest.segment_max_bytes
        {
            self.roll(now_ms)?;
        }

        let base_offset = self.next_offset;

        // Reserve to avoid realloc
        let t_reserve = Instant::now();
        self.write_buf.reserve(estimated, t_reserve);
        // Index entries are sparse; reserve modestly
        self.idx_buf.reserve(
            (estimated / (self.manifest.index_stride_bytes as usize).max(1)).max(64),
            t_reserve,
        );

        let t_encode = Instant::now();

        let mut plan_state = self.append_plan_state(base_offset);
        let plan = observe_stage!(tracer, "encode", (1, estimated), {
            encode_append_payloads_into(
                &mut plan_state,
                std::slice::from_ref(payload),
                now_ms,
                t_encode,
                &mut self.write_buf,
                &mut self.idx_buf,
            )
        })?;
        self.sync_append_plan_state(plan_state);

        self.stats.encode += t_encode.elapsed();
        self.stats.bytes += payload.bytes_len() as u64;
        self.stats.records += 1;

        let end_offset = plan.end_offset;
        self.staged_end_offset = end_offset;

        Ok((
            AppendResult {
                base_offset: plan.base_offset,
                count: plan.count,
            },
            end_offset,
        ))
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// The first storage failure recorded by this handle, if any.
    pub(crate) fn storage_failure(&self) -> Option<&str> {
        self.storage_failure.as_deref()
    }

    pub(crate) fn record_storage_failure(&mut self, error: impl Into<String>) {
        if self.storage_failure.is_none() {
            let error = error.into();
            tracing::error!(
                root = %self.root.display(),
                %error,
                "log stopped after storage failure, reopen required"
            );
            self.log_state.sync.record_storage_failure(&error);
            self.storage_failure = Some(error);
            // Records past the durable end may never reach disk and the next
            // open cuts them, so reads that see written records stop there.
            self.written_end = self.durable_end;
            self.log_state.written.reset(self.written_end);
            match self.mark_latched_cut() {
                Ok(cut) => tracing::warn!(
                    root = %self.root.display(),
                    durable_end = cut,
                    "next open cuts the log back to its last successful sync"
                ),
                Err(error) => tracing::error!(
                    root = %self.root.display(),
                    %error,
                    "could not record a cut to the durable end, the next open trusts its scan"
                ),
            }
        }
    }

    /// Record a cut to the end of the last successful sync. Linux reports a
    /// writeback error once, so after a failed sync a later sync or read can
    /// succeed over pages that never reached disk. Nothing past the durable
    /// end was confirmed as durable, so dropping it matches a crash for every
    /// durability tier. `Log::open` carries out the cut, see `latched_cut`.
    fn mark_latched_cut(&mut self) -> io::Result<u64> {
        // A background write of an older manifest must not land after this one.
        if let Err(error) = self.drain_manifest() {
            tracing::warn!(%error, "background manifest write failed before the latched cut");
        }
        let cut = self.durable_end;
        self.manifest.next_offset = self.manifest.next_offset.max(self.next_offset).max(cut);
        self.manifest.clean_shutdown = false;
        self.manifest.store_atomic(&self.root)?;
        crate::latched_cut::write(&self.root, cut)?;
        Ok(cut)
    }

    fn check_storage(&self) -> io::Result<()> {
        match &self.storage_failure {
            Some(error) => Err(io::Error::other(format!(
                "log stopped after storage failure, reopen required: {error}"
            ))),
            None => Ok(()),
        }
    }

    fn latched<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if let Err(error) = &result {
            self.record_storage_failure(error.to_string());
        }
        result
    }

    pub fn flush_buffers(&mut self) -> io::Result<u64> {
        self.flush_buffers_inner(
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub fn flush_buffers_traced(
        &mut self,
        tracer: &WriterStageTracer,
        work_id: u64,
    ) -> io::Result<u64> {
        self.flush_buffers_inner(Some((tracer, work_id)))
    }

    fn flush_buffers_inner(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<u64> {
        self.check_storage()?;
        let result = self.flush_buffers_unguarded(
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        self.latched(result)
    }

    fn flush_buffers_unguarded(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<u64> {
        // write log buffer
        if !self.write_buf.is_empty() {
            let t = Instant::now();
            observe_stage!(tracer, "log_write", (0, self.write_buf.len()), {
                self.active.append_bytes(&self.write_buf)
            })?;
            self.stats.log_write += t.elapsed();
            // Feed the just-written batch to the tail cache so tail-following
            // reads serve it from memory instead of scanning this segment while
            // it is under fsync/writeback. Bytes are the exact on-disk format;
            // ordinary reads are gated on the durable watermark. Experimental
            // replication can opt into these written bytes before fsync.
            if self.tail_cache.enabled() {
                let next = self.staged_end_offset + 1;
                self.tail_cache.push_batch(
                    self.flushed_through,
                    next,
                    Arc::from(self.write_buf.as_slice()),
                );
                self.flushed_through = next;
            }
            self.write_buf.clear();
        }

        // write idx buffer
        if !self.idx_buf.is_empty() {
            let t = Instant::now();
            observe_stage!(tracer, "index_write", (0, self.idx_buf.len()), {
                self.index.append_entries_raw(&self.idx_buf)
            })?;
            self.stats.idx_write += t.elapsed();
            self.idx_buf.clear();
        }
        // Everything staged so far is now with the OS.
        self.written_end = self.next_offset;
        self.log_state.written.advance(self.written_end);
        self.debug_check_frontiers();

        // Periodic stat line (kept here so it measures real IO). A tracing
        // event rather than a raw print so embedders can filter it: one line
        // per second PER ACTIVE WRITER adds up fast with many partitions.
        if self.last_stats_dump.elapsed() > Duration::from_secs(1) {
            tracing::info!(
                "KERATIN IO: batches={} recs={} encode={}ms log={}ms idx={}ms fsync={}ms manifest={}ms bytes={} kbytes/batch={} rec/batch={}",
                self.stats.batches,
                self.stats.records,
                self.stats.encode.as_millis(),
                self.stats.log_write.as_millis(),
                self.stats.idx_write.as_millis(),
                self.stats.fsync.as_millis(),
                self.stats.manifest.as_millis(),
                self.stats.bytes,
                (self.stats.bytes / 1024) / (self.stats.batches.max(1)),
                self.stats.records / self.stats.batches.max(1)
            );
            self.stats = IoStats::default();
            self.last_stats_dump = Instant::now();
        }

        Ok(self.staged_end_offset)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.active.flush()
    }

    #[cfg_attr(feature = "writer-stage-trace", allow(dead_code))]
    pub(crate) fn prepare_fsync_job(&mut self) -> io::Result<FsyncJob> {
        self.prepare_fsync_job_inner(
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub(crate) fn prepare_fsync_job_traced(
        &mut self,
        tracer: &WriterStageTracer,
        work_id: u64,
    ) -> io::Result<FsyncJob> {
        self.prepare_fsync_job_inner(Some((tracer, work_id)))
    }

    fn prepare_fsync_job_inner(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<FsyncJob> {
        self.check_storage()?;
        let result = self.prepare_fsync_job_unguarded(
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        self.latched(result)
    }

    fn prepare_fsync_job_unguarded(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<FsyncJob> {
        self.flush_buffers_inner(
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        )?;

        self.last_sync_started = Instant::now();
        Ok(FsyncJob {
            durable_end: self.next_offset,
            active: self.active.try_clone_file()?,
            index: if self.index.sync_due() {
                Some(self.index.try_clone_file()?)
            } else {
                None
            },
            pending: std::mem::take(&mut self.pending_sync),
        })
    }

    #[cfg_attr(feature = "writer-stage-trace", allow(dead_code))]
    pub(crate) fn finish_fsync_job(
        &mut self,
        durable_end: u64,
        elapsed: Duration,
    ) -> io::Result<()> {
        self.finish_fsync_job_inner(
            durable_end,
            elapsed,
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub(crate) fn finish_fsync_job_traced(
        &mut self,
        durable_end: u64,
        elapsed: Duration,
        tracer: &WriterStageTracer,
        work_id: u64,
    ) -> io::Result<()> {
        self.finish_fsync_job_inner(durable_end, elapsed, Some((tracer, work_id)))
    }

    fn finish_fsync_job_inner(
        &mut self,
        durable_end: u64,
        elapsed: Duration,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        self.check_storage()?;
        let result = self.finish_fsync_job_unguarded(
            durable_end,
            elapsed,
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        self.latched(result)
    }

    fn finish_fsync_job_unguarded(
        &mut self,
        durable_end: u64,
        elapsed: Duration,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        self.poll_manifest()?;
        self.stats.fsync += elapsed;
        self.durable_end = self.durable_end.max(durable_end);
        self.log_state.sync.record_success();
        self.debug_check_frontiers();
        self.manifest.next_offset = self.manifest.next_offset.max(durable_end);
        self.manifest.active_base_offset = self.active.base_offset;

        // manifest is a hint; persist it on interval
        if self.last_manifest_flush.elapsed() >= self.manifest_flush_interval {
            observe_stage!(tracer, "manifest_write", (0, 0), {
                self.store_periodic_manifest()
            })?;
        }

        Ok(())
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        self.fsync_inner(
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    #[allow(dead_code)]
    pub fn fsync_traced(&mut self, tracer: &WriterStageTracer, work_id: u64) -> io::Result<()> {
        self.fsync_inner(Some((tracer, work_id)))
    }

    fn fsync_inner(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        self.check_storage()?;
        let result = self.fsync_unguarded(
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        self.latched(result)
    }

    fn fsync_unguarded(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        crate::drain_diagnostics::fsync(true);
        self.poll_manifest()?;
        observe_stage!(tracer, "fsync", (0, 0), {
            self.fsync_files_and_update_manifest_inner(
                #[cfg(feature = "writer-stage-trace")]
                tracer,
            )
        })?;

        // manifest is a hint; persist it on interval
        if self.last_manifest_flush.elapsed() >= self.manifest_flush_interval {
            observe_stage!(tracer, "manifest_write", (0, 0), {
                self.store_periodic_manifest()
            })?;
        }

        Ok(())
    }

    fn fsync_files_and_update_manifest(&mut self) -> io::Result<()> {
        self.fsync_files_and_update_manifest_inner(
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    fn fsync_files_and_update_manifest_inner(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        self.check_storage()?;
        let result = self.fsync_files_and_update_manifest_unguarded(
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        self.latched(result)
    }

    fn fsync_files_and_update_manifest_unguarded(
        &mut self,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        let t = Instant::now();
        self.last_sync_started = t;
        // Records still in the staging buffers are not covered by this sync,
        // so every caller writes them out first.
        debug_assert!(
            self.write_buf.is_empty(),
            "inline fsync with records still staged"
        );

        observe_stage!(tracer, "log_fsync", (0, 0), { self.active.fsync() })?;
        if self.index.sync_due() {
            observe_stage!(tracer, "index_fsync", (0, 0), { self.index.fsync() })?;
        }
        observe_stage!(tracer, "roll_sync", (0, 0), {
            std::mem::take(&mut self.pending_sync).sync()
        })?;

        self.stats.fsync += t.elapsed();

        // mark durable
        self.durable_end = self.next_offset;
        self.log_state.sync.record_success();
        self.debug_check_frontiers();

        self.manifest.next_offset = self.next_offset;
        self.manifest.active_base_offset = self.active.base_offset;

        Ok(())
    }

    fn poll_manifest(&mut self) -> io::Result<()> {
        if let Some(elapsed) = self.manifest_worker.poll()? {
            self.stats.manifest += elapsed;
        }
        Ok(())
    }

    fn drain_manifest(&mut self) -> io::Result<()> {
        if let Some(elapsed) = self.manifest_worker.drain()? {
            self.stats.manifest += elapsed;
        }
        Ok(())
    }

    fn store_periodic_manifest(&mut self) -> io::Result<()> {
        if self.manifest_worker.submit(&self.root, &self.manifest)? {
            self.last_manifest_flush = Instant::now();
        }
        Ok(())
    }

    fn store_manifest(&mut self) -> io::Result<()> {
        self.drain_manifest()?;
        let t = Instant::now();
        self.manifest.store_atomic(&self.root)?;
        self.stats.manifest += t.elapsed();
        self.last_manifest_flush = Instant::now();
        Ok(())
    }

    /// The exclusive durable frontier published to readers. It comes from the
    /// fsync job's captured end, so an empty job cannot cover a later append.
    pub fn durable_end_exclusive(&self) -> DurableFrontier {
        DurableFrontier::from_exclusive(self.durable_end)
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Exclusive end of the records written to the OS.
    pub(crate) fn written_end(&self) -> u64 {
        self.written_end
    }

    #[inline]
    fn debug_check_frontiers(&self) {
        debug_assert!(
            self.durable_end <= self.written_end && self.written_end <= self.next_offset,
            "frontiers out of order: durable {} written {} next {}",
            self.durable_end,
            self.written_end,
            self.next_offset
        );
    }

    /// The shared tail cache, for handing to `LogReader`s (they read it, the
    /// writer populates it at flush).
    pub fn tail_cache(&self) -> Arc<TailCache> {
        self.tail_cache.clone()
    }

    pub fn current_epoch(&self) -> u64 {
        self.manifest.epoch
    }

    pub fn advance_epoch(&mut self, epoch: u64) -> io::Result<u64> {
        let current = self.manifest.epoch;
        if epoch < current {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("stale epoch {epoch}, current epoch is {current}"),
            ));
        }
        if epoch == current {
            return Ok(current);
        }

        self.flush_buffers()?;
        self.flush()?;
        self.fsync()?;
        self.drain_manifest()?;

        // Publish the in-memory fence only after persistence succeeds. Otherwise
        // a failed store makes a same-epoch retry return early without writing.
        let mut manifest = self.manifest.clone();
        manifest.epoch = epoch;
        manifest.next_offset = self.next_offset;
        manifest.active_base_offset = self.active.base_offset;
        manifest.store_atomic(&self.root)?;
        self.manifest = manifest;
        self.log_state.epoch.store(epoch, Ordering::Release);
        self.log_state.diagnostics.lock().record(
            LogControlKind::EpochAdvanced,
            epoch,
            self.next_offset,
        );

        Ok(epoch)
    }

    #[inline]
    pub fn should_flush(&self) -> bool {
        self.write_buf.len() >= self.flush_target_bytes
    }

    /// See `LogRuntimeSettings::set_loaded_fsync_spacing`. Zero for a log
    /// without shared runtime settings.
    pub(crate) fn loaded_fsync_spacing(&self) -> Duration {
        self.runtime
            .as_ref()
            .map_or(Duration::ZERO, |(settings, _)| {
                settings.loaded_fsync_spacing()
            })
    }

    pub(crate) fn attach_runtime(
        &mut self,
        settings: Arc<crate::LogRuntimeSettings>,
        snapshot: crate::LogRuntimeSnapshot,
    ) {
        let status = settings.register(crate::LogRuntimeStatus {
            root: self.root.clone(),
            applied: snapshot,
            active_segment_base: self.active.base_offset,
            effective_preallocate_bytes: 0,
            allocation_error: None,
        });
        self.active.attach_runtime_status(status.clone(), snapshot);
        self.runtime = Some((settings, status));
    }

    fn prepare_segment_policy(&self, segment: &mut Segment) -> Option<crate::LogRuntimeSnapshot> {
        let snapshot = self
            .runtime
            .as_ref()
            .map(|(settings, _)| settings.current());
        segment.enable_prealloc(snapshot.map_or(self.prealloc_chunk, |s| {
            s.config.segment_preallocate_bytes as u64
        }));
        snapshot
    }

    fn publish_segment_policy(&mut self, snapshot: Option<crate::LogRuntimeSnapshot>) {
        if let (Some(snapshot), Some((_, status))) = (snapshot, &self.runtime) {
            self.active.attach_runtime_status(status.clone(), snapshot);
        }
    }

    fn roll(&mut self, now_ms: u64) -> io::Result<()> {
        let started = Instant::now();
        self.flush_buffers()?;
        let seal_started = Instant::now();
        self.seal_active()?;
        let seal = seal_started.elapsed();

        // Reclaim preallocated padding so it does not stay on disk for the
        // segment's lifetime.
        self.active.trim_to_written()?;
        let sealed_index = self.index.try_clone_file()?;

        // The next segment is not synced here. Its header goes with its first
        // data sync and its directory entry right after, both before any of its
        // records count as durable. Recovery drops a segment a crash caught
        // before that.
        let new_base = self.next_offset;
        let (mut seg, idx, new_seg_path) = start_segment_pair(&self.root, new_base, now_ms)?;
        self.pending_sync.files.push(sealed_index);
        self.pending_sync.dir = Some(self.root.join("segments"));
        let policy = self.prepare_segment_policy(&mut seg);
        self.active = seg;
        self.index = idx;

        self.segment_mapping.write().insert(new_base, new_seg_path);

        self.last_index_at_log_pos = self.active.bytes_written;

        // The periodic store records the new active base. Recovery does not
        // depend on it.
        self.manifest.active_base_offset = new_base;
        self.publish_segment_policy(policy);
        crate::drain_diagnostics::roll(started.elapsed(), seal);

        Ok(())
    }

    /// Hand the outgoing segment's data sync to the next fsync instead of
    /// holding the writer for it. Its records count as durable only once that
    /// fsync finishes, and an fsync covering any later record syncs this file
    /// first, so nothing acknowledged can sit past an unsynced segment.
    /// Recovery trims a sealed segment a crash left short, and every segment
    /// after it.
    fn seal_active(&mut self) -> io::Result<()> {
        self.check_storage()?;
        let sealed = self.active.file_ref().try_clone();
        let sealed = self.latched(sealed)?;
        self.pending_sync.files.insert(0, sealed);
        self.manifest.next_offset = self.next_offset;
        Ok(())
    }

    /// Ordered writer barrier: retire the writable tail before linking closed files.
    /// The caller owns an unreferenced destination and must keep this log sealed.
    pub(crate) fn fork_frozen(
        &mut self,
        destination: &Path,
        epoch: u64,
        head: u64,
        next: u64,
    ) -> io::Result<crate::FrozenForkStats> {
        if self.manifest.epoch != epoch
            || self.manifest.head_offset != head
            || self.next_offset != next
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frozen fork boundary changed",
            ));
        }
        if destination.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "frozen fork destination exists",
            ));
        }
        self.flush_buffers()?;
        self.fsync()?;
        self.drain_manifest()?;
        if self.active.base_offset != next {
            self.roll(crate::util::unix_millis())?;
        }
        self.manifest.next_offset = next;
        // Persist the downgrade fence before the first shared inode is exposed.
        #[cfg(unix)]
        {
            self.manifest.shared_segments |=
                self.segment_mapping.read().keys().any(|base| *base < next);
        }
        self.store_manifest()?;
        crate::shared_segment::boundary("source-sealed");
        let mut cursor =
            crate::FrozenLogReader::open(&self.root, epoch, head, next)?.into_retained_cursor()?;
        while cursor
            .next_record(crate::MAX_FROZEN_RECORD_BYTES)?
            .is_some()
        {}

        std::fs::create_dir(destination)?;
        std::fs::create_dir(destination.join("segments"))?;
        std::fs::create_dir(destination.join("tmp"))?;
        let mut stats = crate::FrozenForkStats::default();
        for (&base, source) in self.segment_mapping.read().iter() {
            if base == next {
                continue;
            }
            let target = seg_log_path(destination, base);
            let file = File::open(source)?;
            crate::util::sync_file_all(&file)?;
            let length = file.metadata()?.len();
            #[cfg(unix)]
            let shared = std::fs::hard_link(source, &target).is_ok();
            #[cfg(not(unix))]
            let shared = false;
            if shared {
                stats.shared_segments += 1;
                stats.shared_bytes += length;
                crate::shared_segment::boundary("linked");
            } else {
                std::fs::copy(source, &target)?;
                crate::util::sync_file_all(&File::open(&target)?)?;
                stats.copied_bytes += length;
            }
            let target_index = seg_idx_path(destination, base);
            stats.copied_bytes += std::fs::copy(seg_idx_path(&self.root, base), &target_index)?;
            crate::util::sync_file_all(&File::open(target_index)?)?;
        }
        create_segment_pair(destination, next, crate::util::unix_millis())?;
        crate::shared_segment::boundary("target-tail");
        crate::util::fsync_dir(&destination.join("segments"))?;
        let mut manifest = self.manifest.clone();
        manifest.clean_shutdown = true;
        manifest.store_atomic(destination)?;
        crate::shared_segment::boundary("target-manifest");
        if let Some(parent) = destination.parent() {
            crate::util::fsync_dir(parent)?;
        }
        Ok(stats)
    }

    /// Delete whole sealed segments whose max offset < `before`.
    /// NOTE: v0 is *segment-granular* retention. It will not trim inside the active segment.
    /// Advance an exact logical floor covered by an external durable checkpoint.
    /// The caller must serialize checkpoint publication and retain its snapshot.
    pub(crate) fn advance_retained_head(&mut self, before: u64, epoch: u64) -> io::Result<u64> {
        if epoch != self.current_epoch()
            || before > self.next_offset
            || before < self.manifest.head_offset
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint floor differs from current epoch or retained bounds",
            ));
        }
        self.flush_buffers()?;
        self.fsync()?;
        self.drain_manifest()?;
        self.manifest.next_offset = self.next_offset;
        // Publish the durable logical floor before any physical removal. Reopen
        // must never resurrect a discarded prefix after interrupted reclamation.
        self.manifest.head_offset = before;
        self.manifest.store_atomic(&self.root)?;
        self.log_state.head.store(before, Ordering::Release);
        self.truncate_before_inner(before, true)
    }

    #[cfg(test)]
    pub fn truncate_before(&mut self, before: u64) -> io::Result<u64> {
        self.truncate_before_inner(before, false)
    }

    fn truncate_before_inner(&mut self, before: u64, reclaim: bool) -> io::Result<u64> {
        let Some(job) = self.plan_truncate(before, reclaim)? else {
            return Ok(self.manifest.head_offset);
        };
        // Make everything written so far durable before removing history.
        self.flush_buffers()?;
        self.fsync()?;
        self.drain_manifest()?;
        job.run()?;
        Ok(job.manifest.head_offset)
    }

    /// `truncate_before` with the file removal on a background job, so the
    /// writer is not held for its metadata syncs. `done` gets the new head once
    /// the files are gone. Readers stop at the new head straight away. A crash
    /// before the job lands only keeps more history.
    pub(crate) fn truncate_before_deferred(
        &mut self,
        before: u64,
        done: impl FnOnce(io::Result<u64>) + Send + 'static,
    ) -> io::Result<()> {
        let job = match self.plan_truncate(before, false) {
            Ok(Some(job)) => job,
            Ok(None) => {
                done(Ok(self.manifest.head_offset));
                return Ok(());
            }
            Err(e) => {
                done(Err(e));
                return Ok(());
            }
        };
        self.manifest_worker.submit_after(move || {
            let result = job.run().map(|()| job.manifest.head_offset);
            if let Err(error) = &result {
                tracing::error!(log_root = %job.root.display(), %error, "removing truncated segments failed");
            }
            done(result);
        })
    }

    /// Move the head past every sealed segment below `before` in memory, and
    /// return the file work that leaves. None when no segment can go.
    fn plan_truncate(&mut self, before: u64, reclaim: bool) -> io::Result<Option<Reclaim>> {
        let cur_head = self.manifest.head_offset;
        if before <= cur_head && !reclaim {
            return Ok(None);
        }

        let mut bases = self
            .segment_mapping
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect::<Vec<(u64, PathBuf)>>();
        bases.sort_unstable();

        if bases.is_empty() {
            // degenerate; keep head consistent
            self.manifest.head_offset = before;
            self.drain_manifest()?;
            self.manifest.store_atomic(&self.root)?;
            return Ok(None);
        }

        // Only sealed segments whose last offset is below `before` can go. The
        // active segment is the last base and is never trimmed.
        // A sealed segment [base_i, base_{i+1}) ends at base_{i+1} - 1.
        let deletable = bases
            .windows(2)
            .take_while(|w| w[1].0.saturating_sub(1) < before)
            .map(|w| w[0].0)
            .collect::<Vec<u64>>();
        // Without a deletion the first readable offset does not change.
        if deletable.is_empty() {
            return Ok(None);
        }

        let new_head = bases[deletable.len()].0.max(cur_head);
        self.manifest.head_offset = new_head;
        self.log_state.head.store(new_head, Ordering::Release);
        {
            let mut mapping = self.segment_mapping.write();
            for base in &deletable {
                mapping.remove(base);
            }
        }
        self.log_state.diagnostics.lock().record(
            LogControlKind::Truncated,
            self.manifest.epoch,
            new_head,
        );
        Ok(Some(Reclaim {
            root: self.root.clone(),
            manifest: self.manifest.clone(),
            bases: deletable,
        }))
    }

    /// Preserve all retained records below `next_offset`, with restartable
    /// metadata/file updates. The writer must drain fsync completions first and
    /// stop on any error after beginning the repair.
    pub(crate) fn repair_suffix(&mut self, next_offset: u64) -> io::Result<()> {
        self.flush_buffers()?;
        self.flush()?;
        self.fsync()?;
        self.drain_manifest()?;
        self.manifest.next_offset = self.next_offset;
        self.manifest.store_atomic(&self.root)?;
        crate::suffix_repair::prepare(&self.root, &self.manifest, next_offset)?;
        crate::suffix_repair::resume(&self.root)?;
        self.manifest = Manifest::read_from(&mut File::open(Manifest::path(&self.root))?)?;
        let base = self.manifest.active_base_offset;
        let (mut active, index, _) =
            open_or_create_segment_pair(&self.root, base, crate::util::unix_millis())?;
        let policy = self.prepare_segment_policy(&mut active);
        self.active = active;
        self.index = index;
        self.publish_segment_policy(policy);
        *self.segment_mapping.write() = list_segment_bases(&self.root.join("segments"))?
            .into_iter()
            .collect();
        self.write_buf.clear();
        self.idx_buf.clear();
        self.tail_cache.clear();
        self.next_offset = next_offset;
        self.staged_end_offset = next_offset.saturating_sub(1);
        self.durable_end = self.next_offset;
        self.written_end = self.next_offset;
        // The fsync above covered every unclaimed record.
        self.unclaimed_writes = UnclaimedWrites::default();
        self.flushed_through = next_offset;
        self.last_commit_through = next_offset;
        self.recent_commit_records = 0;
        self.last_index_at_log_pos = self.active.bytes_written;
        // Lower the frontiers before the tail so readers never see
        // durable > written > tail while the log moves back.
        self.log_state.durable.reset(self.durable_end_exclusive());
        self.log_state.written.reset(self.written_end);
        self.log_state.tail.store(next_offset, Ordering::Release);
        self.log_state.diagnostics.lock().record(
            LogControlKind::SuffixTruncated,
            self.manifest.epoch,
            next_offset,
        );
        Ok(())
    }

    pub fn reset_to_checkpoint(&mut self, next_offset: u64, now_ms: u64) -> io::Result<()> {
        self.flush_buffers()?;
        self.flush()?;
        self.fsync()?;
        self.drain_manifest()?;
        // Once files start changing, a failure leaves the files and the
        // in-memory state out of step, so the handle must stop.
        let result = self.replace_files_for_checkpoint(next_offset, now_ms);
        self.latched(result)
    }

    fn replace_files_for_checkpoint(&mut self, next_offset: u64, now_ms: u64) -> io::Result<()> {
        let seg_dir = self.root.join("segments");
        for ent in fs::read_dir(&seg_dir)? {
            let ent = ent?;
            let path = ent.path();
            let is_log_or_idx = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "log" || ext == "idx")
                .unwrap_or(false);
            if is_log_or_idx {
                fs::remove_file(path)?;
            }
        }

        self.segment_mapping.write().clear();

        let (mut seg, idx, seg_path) = create_segment_pair(&self.root, next_offset, now_ms)?;
        let policy = self.prepare_segment_policy(&mut seg);
        self.active = seg;
        self.index = idx;
        self.segment_mapping.write().insert(next_offset, seg_path);

        self.write_buf.clear();
        self.idx_buf.clear();
        self.next_offset = next_offset;
        self.staged_end_offset = next_offset.saturating_sub(1);
        self.durable_end = self.next_offset;
        self.written_end = self.next_offset;
        // The fsync above covered every unclaimed record.
        self.unclaimed_writes = UnclaimedWrites::default();
        self.flushed_through = next_offset;
        // The log rewound past everything the cache held; drop it so no stale
        // (rewound-away) offset is ever served from memory.
        self.tail_cache.clear();
        self.last_index_at_log_pos = self.active.bytes_written;

        self.manifest.active_base_offset = next_offset;
        self.manifest.next_offset = next_offset;
        self.manifest.head_offset = next_offset;
        self.active.fsync()?;
        self.index.fsync()?;
        fsync_dir(&seg_dir)?;
        self.manifest.store_atomic(&self.root)?;

        self.publish_segment_policy(policy);
        // Truncation/checkpoint reset: the durable set can legitimately shrink.
        // Lower the frontiers before the tail, as in `repair_suffix`.
        self.log_state.durable.reset(self.durable_end_exclusive());
        self.log_state.written.reset(self.written_end);
        self.log_state.head.store(next_offset, Ordering::Release);
        self.log_state.tail.store(next_offset, Ordering::Release);

        self.log_state.diagnostics.lock().record(
            LogControlKind::CheckpointReset,
            self.manifest.epoch,
            next_offset,
        );
        Ok(())
    }

    fn verify_existing_prefix(
        &self,
        first_offset: u64,
        payloads: &[Message],
        written_overlap: bool,
    ) -> io::Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        let head_offset = self.manifest.head_offset;
        let readable_first_offset = first_offset.max(head_offset);
        let skip =
            usize::try_from(readable_first_offset.saturating_sub(first_offset)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replicated overlap skip overflow",
                )
            })?;
        if skip >= payloads.len() {
            return Ok(());
        }
        let payloads = &payloads[skip..];

        let reader = LogReader::new(
            &self.root,
            self.segment_mapping.clone(),
            self.tail_cache.clone(),
        );
        let requested_end = readable_first_offset
            .checked_add(payloads.len() as u64)
            .ok_or_else(|| io::Error::other("replicated overlap range overflow"))?;
        let got = if written_overlap && !self.durable_end_exclusive().covers(requested_end - 1) {
            // An AfterFlush retry can overlap a written prefix that is not yet
            // durable. Raw reads under this writer's exclusive access check the
            // exact bytes. This does not advance durability or count as
            // recovery evidence.
            (readable_first_offset..requested_end)
                .map(|offset| {
                    reader.fetch(offset)?.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "replicated written overlap is not fully readable",
                        )
                    })
                })
                .collect::<io::Result<Vec<_>>>()?
        } else {
            reader.scan_from(readable_first_offset, payloads.len())?
        };
        if got.len() != payloads.len() {
            // Durable-only scans stop at the frontier. Say so, so a retry that
            // raced a pending sync does not read like diverged content.
            let message = if self.durable_end_exclusive().covers(requested_end - 1) {
                "replicated overlap prefix is not fully readable"
            } else {
                "replicated overlap prefix is not durable yet, retry after it syncs"
            };
            return Err(io::Error::new(io::ErrorKind::InvalidData, message));
        }

        for (idx, (existing, incoming)) in got.iter().zip(payloads).enumerate() {
            let expected_offset = readable_first_offset + idx as u64;
            if existing.offset != expected_offset
                || existing.flags != incoming.flags
                || existing.headers != incoming.headers
                || existing.payload != incoming.payload
            {
                let (report_id, emit_details, suppressed_reports, control_history) =
                    self.log_state.diagnostics.lock().capture(Instant::now());
                let diagnostic = ReplicationOverlapDiagnostic {
                    report_id,
                    offset: expected_offset,
                    existing_offset: existing.offset,
                    epoch: self.manifest.epoch,
                    local_head: head_offset,
                    local_next: self.next_offset,
                    local_durable_next: self.durable_end_exclusive().first_non_durable(),
                    compared_first: readable_first_offset,
                    compared_count: payloads.len(),
                    flags_differ: existing.flags != incoming.flags,
                    headers_differ: existing.headers != incoming.headers,
                    payloads_differ: existing.payload != incoming.payload,
                    emit_details,
                    suppressed_reports,
                    control_history,
                };
                if emit_details {
                    tracing::error!(log_root = %self.root.display(), diagnostic = ?diagnostic,
                        "replication overlap diagnostic");
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, diagnostic));
            }
        }

        Ok(())
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        // A failed handle must not trim, sync or mark the log clean. Leaving
        // the dirty flag forces the recovery scan on the next open.
        self.check_storage()?;
        self.drain_manifest()?;
        self.flush_buffers()?;
        self.flush()?;
        self.fsync_files_and_update_manifest()?;
        // Trim the preallocated padding so the next clean open sees file size ==
        // data size and needs no scan. Made durable before the clean flag, so a
        // crash in between just falls back to the recovery scan.
        self.active.trim_to_written()?;
        self.active.fsync()?;
        // A clean open trusts the index without rebuilding it.
        self.index.fsync()?;
        self.manifest.clean_shutdown = true;
        self.store_manifest()?;
        Ok(())
    }

    pub fn estimate_disk_used(&self) -> io::Result<u64> {
        let seg_dir = self.root.join("segments");
        let mut total = 0u64;
        for ent in fs::read_dir(&seg_dir)? {
            let ent = ent?;
            let meta = ent.metadata()?;
            if meta.is_file() {
                total += meta.len();
            }
        }
        let manifest_path = self.root.join("manifest.bin");
        if let Ok(manifest_meta) = fs::metadata(&manifest_path) {
            total += manifest_meta.len();
        }
        // Also iterate the index files
        for ent in fs::read_dir(seg_dir)? {
            let ent = ent?;
            let name = ent.file_name();
            let Some(s) = name.to_str() else { continue };
            if s.ends_with(".idx") {
                let meta = ent.metadata()?;
                if meta.is_file() {
                    total += meta.len();
                }
            }
        }
        Ok(total)
    }
}

// ---- helpers

#[test]
fn adaptive_staging_release_preserves_written_cache_fsync_and_next_append() {
    let dir = crate::test_dir!("adaptive_staging_durability");
    let cfg = crate::KeratinConfig {
        adaptive_staging: Some(crate::AdaptiveStagingConfig {
            write_min_bytes: 64,
            index_min_bytes: 16,
            empty_resize: crate::reusable_buffer::EmptyBufferResize::Replace,
            ..Default::default()
        }),
        ..crate::KeratinConfig::test_default()
    };
    let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
    let (mut log, mapping) = Log::open(
        &dir.root,
        0,
        cfg.segment_max_bytes,
        1,
        cfg.flush_target_bytes,
        cfg.tail_cache_bytes,
        0,
        false,
        false,
        state,
        cfg.adaptive_staging,
    )
    .unwrap();
    let reader = LogReader::new(&dir.root, mapping, log.tail_cache.clone());
    assert_eq!(log.write_buf.capacity(), 0);
    assert_eq!(log.idx_buf.capacity(), 0);
    let message = Message {
        headers: vec![9; 17],
        payload: vec![42; 128 * 1024],
        flags: 0,
    };
    log.stage_append(&message, 1).unwrap();
    let staged_len = log.write_buf.len();
    let later = Instant::now() + Duration::from_secs(120);
    log.maintain_staging(later);
    assert_eq!(
        log.write_buf.len(),
        staged_len,
        "maintenance must preserve unflushed bytes"
    );
    // Preparing the job writes and clears staging, but does not acknowledge fsync.
    let job = log.prepare_fsync_job().unwrap();
    log.maintain_staging(later + Duration::from_secs(10));
    assert_eq!(log.write_buf.capacity(), 0);
    assert_eq!(log.idx_buf.capacity(), 0);
    assert_eq!(log.staging_maintenance_deadline(), None);
    assert_eq!(log.durable_end_exclusive().first_non_durable(), 0);
    assert!(reader.scan_from(0, 2).unwrap().is_empty());
    let elapsed = job.sync().unwrap();
    log.finish_fsync_job(job.durable_end(), elapsed).unwrap();
    log.log_state.durable.advance(log.durable_end_exclusive());
    // Both cache and physical file outlive the released encoding allocation.
    for record in [
        reader.scan_from(0, 2).unwrap(),
        reader.scan_from_disk(0, 2).unwrap(),
    ] {
        assert_eq!(record.len(), 1);
        assert_eq!(record[0].payload, message.payload);
        assert_eq!(record[0].headers, message.headers);
    }
    assert_eq!(log.stage_append(&message, 2).unwrap().0.base_offset, 1);
    let job = log.prepare_fsync_job().unwrap();
    let elapsed = job.sync().unwrap();
    log.finish_fsync_job(job.durable_end(), elapsed).unwrap();
    log.log_state.durable.advance(log.durable_end_exclusive());
    assert_eq!(reader.scan_from_disk(0, 3).unwrap().len(), 2);
    log.shutdown().unwrap();
}

fn seg_log_path(root: &Path, base: u64) -> PathBuf {
    root.join("segments").join(format!("{:020}.log", base))
}

#[test]
fn empty_fsync_does_not_invent_a_record_or_cover_a_later_append() {
    let dir = crate::test_dir!("empty_fsync_frontier");
    let cfg = crate::KeratinConfig::test_default();
    let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
    let (mut log, _) = Log::open(
        &dir.root,
        0,
        cfg.segment_max_bytes,
        cfg.index_stride_bytes,
        cfg.flush_target_bytes,
        cfg.tail_cache_bytes,
        cfg.segment_preallocate_bytes,
        false,
        false,
        state,
        cfg.adaptive_staging,
    )
    .unwrap();
    log.manifest_flush_interval = Duration::ZERO;
    let empty = log.prepare_fsync_job().unwrap();
    let elapsed = empty.sync().unwrap();
    log.finish_fsync_job(empty.durable_end(), elapsed).unwrap();
    let persisted =
        Manifest::load_or_create(&dir.root, 0, cfg.segment_max_bytes, cfg.index_stride_bytes)
            .unwrap();
    assert_eq!(
        persisted.next_offset, 0,
        "an empty fsync must preserve the empty manifest"
    );
    let empty = log.prepare_fsync_job().unwrap();
    let elapsed = empty.sync().unwrap();
    log.stage_append(
        &Message {
            headers: vec![],
            payload: vec![42],
            flags: 0,
        },
        1,
    )
    .unwrap();
    log.finish_fsync_job(empty.durable_end(), elapsed).unwrap();
    assert_eq!(
        log.durable_end_exclusive().first_non_durable(),
        0,
        "earlier empty fsync cannot cover the later offset-zero append"
    );
    let actual = log.prepare_fsync_job().unwrap();
    let elapsed = actual.sync().unwrap();
    log.finish_fsync_job(actual.durable_end(), elapsed).unwrap();
    assert_eq!(log.durable_end_exclusive().first_non_durable(), 1);
}

#[test]
fn failed_epoch_persistence_must_not_make_retry_a_successful_noop() {
    let dir = crate::test_dir!("epoch_persistence_retry");
    let cfg = crate::KeratinConfig::test_default();
    let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
    let (mut log, _) = Log::open(
        &dir.root,
        0,
        cfg.segment_max_bytes,
        cfg.index_stride_bytes,
        cfg.flush_target_bytes,
        cfg.tail_cache_bytes,
        cfg.segment_preallocate_bytes,
        false,
        false,
        state.clone(),
        cfg.adaptive_staging,
    )
    .unwrap();
    // Isolate the explicit epoch-store failure from periodic manifest updates.
    log.manifest_flush_interval = Duration::MAX;
    let blocked = Manifest::tmp_path(&dir.root);
    fs::create_dir(&blocked).unwrap();
    assert!(log.advance_epoch(3).is_err());
    assert!(
        log.advance_epoch(3).is_err(),
        "retry must still attempt persistence"
    );
    assert_eq!(log.current_epoch(), 0);
    assert_eq!(state.epoch.load(Ordering::Acquire), 0);
    fs::remove_dir(&blocked).unwrap();
    assert_eq!(log.advance_epoch(3).unwrap(), 3);
    let stored =
        Manifest::load_or_create(&dir.root, 0, cfg.segment_max_bytes, cfg.index_stride_bytes)
            .unwrap();
    assert_eq!(stored.epoch, 3);
    assert_eq!(state.epoch.load(Ordering::Acquire), 3);
}

fn seg_idx_path(root: &Path, base: u64) -> PathBuf {
    root.join("segments").join(format!("{:020}.idx", base))
}

fn list_segment_bases(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        let name = ent.file_name();
        let Some(s) = name.to_str() else { continue };
        if let Some(stem) = s.strip_suffix(".log")
            && let Ok(base) = stem.parse::<u64>()
        {
            out.push((base, ent.path()));
        }
    }
    Ok(out)
}

/// Freed segment files kept per log for reuse. Writing over a file's existing
/// blocks needs no allocation, so its data syncs skip the journal commit an
/// extending write pays, and nothing has to zero-fill ahead.
const MAX_SPARE_SEGMENTS: usize = 4;

fn spare_path(root: &Path, base: u64) -> PathBuf {
    root.join("segments").join(format!("{base:020}.spare"))
}

fn list_spares(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for ent in fs::read_dir(root.join("segments"))? {
        let path = ent?.path();
        if path.extension().is_some_and(|ext| ext == "spare") {
            out.push(path);
        }
    }
    Ok(out)
}

/// Keep a freed segment as a spare when the pool has room. A file still
/// linked from a frozen fork is never reused, since writing over it would
/// change the fork's copy too. The caller syncs the directory.
fn keep_as_spare(root: &Path, base: u64, log_path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let metadata = match fs::metadata(log_path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    if metadata.nlink() > 1 || list_spares(root)?.len() >= MAX_SPARE_SEGMENTS {
        return Ok(false);
    }
    fs::rename(log_path, spare_path(root, base))?;
    Ok(true)
}

/// Open the next active segment pair without syncing, in a spare file when
/// one is free. See `Log::roll` for how it becomes durable.
fn start_segment_pair(
    root: &Path,
    base: u64,
    now_ms: u64,
) -> io::Result<(Segment, Index, PathBuf)> {
    let log_path = seg_log_path(root, base);
    let idx_path = seg_idx_path(root, base);
    crate::shared_segment::make_private(&log_path)?;
    let seg = match list_spares(root)?.into_iter().next() {
        Some(spare) => {
            fs::rename(&spare, &log_path)?;
            let file = OpenOptions::new().read(true).write(true).open(&log_path)?;
            Segment::create_recycled(file, base, now_ms)?
        }
        None => {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&log_path)?;
            Segment::create_unsynced(file, base, now_ms)?
        }
    };
    let idxf = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&idx_path)?;
    let idx = Index::create_unsynced(idxf, base, now_ms)?;
    Ok((seg, idx, log_path))
}

/// Removal of the segments below a new head. The manifest with that head is
/// stored first, so an interrupted reclaim never reopens with a head that
/// points at a missing segment. Removal tolerates files already gone, so an
/// interrupted reclaim can finish.
struct Reclaim {
    root: PathBuf,
    manifest: Manifest,
    bases: Vec<u64>,
}

impl Reclaim {
    fn run(&self) -> io::Result<()> {
        self.manifest.store_atomic(&self.root)?;
        for base in &self.bases {
            remove_if_present(&seg_idx_path(&self.root, *base))?;
            let log_path = seg_log_path(&self.root, *base);
            if !keep_as_spare(&self.root, *base, &log_path)? {
                remove_if_present(&log_path)?;
            }
        }
        fsync_dir(&self.root.join("segments"))
    }
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn create_segment_pair(
    root: &Path,
    base: u64,
    now_ms: u64,
) -> io::Result<(Segment, Index, PathBuf)> {
    let log_path = seg_log_path(root, base);
    let idx_path = seg_idx_path(root, base);
    crate::shared_segment::make_private(&log_path)?;

    let logf = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&log_path)?;
    let idxf = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&idx_path)?;

    let seg = Segment::create(logf, base, now_ms)?;
    let idx = Index::create(idxf, base, now_ms)?;
    Ok((seg, idx, log_path))
}

/// Sync the active segment, its index and the segments directory before a
/// scanned or preserved tail becomes the durable frontier.
///
/// The scan reads through the page cache, which can hold records that were
/// written but never synced: AfterWrite or AfterFlush appends, or a follower
/// batch that died before its sync. Publishing them unsynced would let a
/// follower resume past them or answer an AfterFsync retry with
/// AlreadyPresent, and power loss would then drop acknowledged data. Sealed
/// segments need no sync here because roll syncs a segment before sealing it.
///
/// This is not enough after a latched sync failure. Linux reports a writeback
/// error once, so the failed sync already consumed it and this one can succeed
/// over pages that never reached disk. It only catches an error nobody has
/// seen yet, such as one from background writeback after a crash. Healing the
/// latched case needs the range rewritten into a fresh segment or a cut back
/// to the frontier the manifest recorded.
fn sync_recovered_tail(root: &Path, active: &Segment, index: &Index) -> io::Result<()> {
    active.fsync()?;
    index.fsync()?;
    fsync_dir(&root.join("segments"))
}

/// Remove trailing segment files whose header was never written: a new file
/// still empty or zero at its start, or a recycled one still carrying an older
/// segment's header. A damaged header still fails the open, since it could
/// hide acknowledged records.
fn drop_unstarted_segments(root: &Path, bases: &mut Vec<(u64, PathBuf)>) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let mut dropped = false;
    while let Some((base, _)) = bases.last() {
        let base = *base;
        let log_path = seg_log_path(root, base);
        let file = File::open(&log_path)?;
        let mut start = vec![0u8; crate::segment::LOG_HEADER_LEN as usize];
        let n = file.read_at(&mut start, 0)?;
        let unwritten = start[..n].iter().all(|b| *b == 0);
        let older = crate::segment::read_header_base(&file)?.is_some_and(|old| old < base);
        if !unwritten && !older {
            break;
        }
        tracing::warn!(base, "removing a segment that never started");
        let _ = fs::remove_file(seg_idx_path(root, base));
        fs::remove_file(&log_path)?;
        bases.pop();
        dropped = true;
    }
    if dropped {
        fsync_dir(&root.join("segments"))?;
    }
    Ok(())
}

fn open_or_create_segment_pair(
    root: &Path,
    base: u64,
    now_ms: u64,
) -> io::Result<(Segment, Index, PathBuf)> {
    let log_path = seg_log_path(root, base);
    let idx_path = seg_idx_path(root, base);
    crate::shared_segment::make_private(&log_path)?;

    // TODO: reeval truncate
    let logf = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&log_path)?;
    let idxf = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&idx_path)?;

    // If files were empty, create headers; otherwise open.
    let seg = if logf.metadata()?.len() == 0 {
        Segment::create(logf, base, now_ms)?
    } else {
        Segment::open(logf, base)?
    };

    let idx = if idxf.metadata()?.len() == 0 {
        Index::create(idxf, base, now_ms)?
    } else {
        Index::open(idxf, base)?
    };

    Ok((seg, idx, log_path))
}

#[cfg(test)]
mod background_manifest_tests {
    use super::*;
    use crossbeam_channel::{Receiver, Sender, bounded};

    fn open(root: &Path) -> Log {
        let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
        let (mut log, _) = Log::open(
            root,
            0,
            1024 * 1024,
            64 * 1024,
            4096,
            0,
            0,
            false,
            false,
            state,
            None,
        )
        .unwrap();
        log
    }

    // Deterministic stalled snapshot: reproduce an old periodic write that
    // would overwrite newer metadata unless the control operation waits.
    fn stall_snapshot(log: &mut Log) -> (Sender<()>, Receiver<()>) {
        assert!(log.manifest_worker.pending.is_none());
        let (release, gate) = bounded(1);
        let (done, observed) = bounded(1);
        let (result, rx) = bounded(1);
        let snapshot = log.manifest.clone();
        let root = log.root.clone();
        log.manifest_worker.pending = Some(rx);
        std::thread::spawn(move || {
            gate.recv().unwrap();
            let outcome = snapshot.store_atomic(&root).map(|()| Duration::ZERO);
            result.send(outcome).unwrap();
            done.send(()).unwrap();
        });
        (release, observed)
    }

    #[test]
    fn durable_fsync_completion_does_not_wait_for_periodic_manifest() {
        let dir = crate::test_dir!("manifest_completion");
        let mut log = open(&dir.root);
        log.stage_append(
            &Message {
                flags: 0,
                headers: vec![],
                payload: vec![7],
            },
            1,
        )
        .unwrap();
        log.flush_buffers().unwrap();
        log.fsync_files_and_update_manifest().unwrap();
        let (release, observed) = stall_snapshot(&mut log);
        log.last_manifest_flush = Instant::now() - Duration::from_secs(1);
        // If completion waited, this test would deadlock before release.
        let (tx, rx) = bounded(1);
        let thread = std::thread::spawn(move || {
            log.finish_fsync_job(1, Duration::ZERO).unwrap();
            tx.send(log)
                .unwrap_or_else(|_| panic!("test receiver lost"));
        });
        let result = rx.recv_timeout(Duration::from_secs(2));
        release.send(()).unwrap();
        let mut log = result.expect("durable completion waited on a metadata hint");
        assert!(log.durable_end_exclusive().covers(0));
        observed.recv().unwrap();
        log.shutdown().unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn follower_fsync_does_not_wait_for_periodic_manifest() {
        let dir = crate::test_dir!("manifest_follower_completion");
        let mut log = open(&dir.root);
        let (release, observed) = stall_snapshot(&mut log);
        log.stage_append(
            &Message {
                flags: 0,
                headers: vec![],
                payload: vec![7],
            },
            1,
        )
        .unwrap();
        log.flush_buffers().unwrap();
        log.last_manifest_flush = Instant::now() - Duration::from_secs(1);
        let (tx, rx) = bounded(1);
        let thread = std::thread::spawn(move || {
            log.fsync().unwrap();
            tx.send(log)
                .unwrap_or_else(|_| panic!("test receiver lost"));
        });
        let result = rx.recv_timeout(Duration::from_secs(2));
        release.send(()).unwrap();
        let mut log = result.expect("follower persistence waited on a periodic hint");
        assert!(log.durable_end_exclusive().covers(0));
        observed.recv().unwrap();
        log.shutdown().unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn critical_metadata_and_drop_wait_for_old_snapshot() {
        for action in [
            "epoch", "reset", "truncate", "head", "repair", "fork", "shutdown", "drop",
        ] {
            let dir = crate::test_dir!(action);
            let mut log = open(&dir.root);
            log.stage_append(
                &Message {
                    flags: 0,
                    headers: vec![],
                    payload: vec![9],
                },
                1,
            )
            .unwrap();
            log.flush_buffers().unwrap();
            log.fsync_files_and_update_manifest().unwrap();
            if action == "truncate" {
                log.roll(2).unwrap();
            }
            let (release, observed) = stall_snapshot(&mut log);
            let (tx, rx) = bounded(1);
            let thread = std::thread::spawn(move || {
                match action {
                    "epoch" => {
                        log.advance_epoch(3).unwrap();
                    }
                    "reset" => log.reset_to_checkpoint(0, 2).unwrap(),
                    "truncate" => {
                        log.truncate_before(1).unwrap();
                    }
                    "head" => {
                        log.advance_retained_head(1, 0).unwrap();
                    }
                    "repair" => log.repair_suffix(0).unwrap(),
                    "fork" => {
                        let destination = log.root.join("frozen-copy");
                        log.fork_frozen(&destination, 0, 0, 1).unwrap();
                    }
                    "shutdown" => log.shutdown().unwrap(),
                    "drop" => (),
                    _ => unreachable!(),
                }
                drop(log);
                tx.send(()).unwrap();
            });
            let premature = rx.recv_timeout(Duration::from_millis(30));
            release.send(()).unwrap();
            assert!(
                premature.is_err(),
                "{action} passed an unfinished old snapshot"
            );
            rx.recv_timeout(Duration::from_secs(2)).unwrap();
            observed.recv().unwrap();
            thread.join().unwrap();
            let m = Manifest::load_or_create(&dir.root, 0, 1024 * 1024, 64 * 1024).unwrap();
            match action {
                "epoch" => assert_eq!(m.epoch, 3),
                "reset" | "repair" => assert_eq!(m.next_offset, 0),
                "truncate" | "head" => assert_eq!(m.head_offset, 1),
                "shutdown" => assert!(m.clean_shutdown),
                _ => assert!(!m.clean_shutdown),
            }
            let reopened = open(&dir.root);
            assert_eq!(
                reopened.next_offset,
                if action == "reset" || action == "repair" {
                    0
                } else {
                    1
                }
            );
        }
    }

    #[test]
    fn abrupt_exit_recovers_confirmed_data_with_stale_periodic_hint() {
        const CHILD_ROOT: &str = "KERATIN_MANIFEST_CRASH_TEST_ROOT";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let mut log = open(Path::new(&root));
            log.advance_epoch(7).unwrap();
            let preallocate = std::env::var("KERATIN_MANIFEST_CRASH_PREALLOC")
                .unwrap()
                .parse()
                .unwrap();
            log.active.enable_prealloc(preallocate);
            let (_release, _observed) = stall_snapshot(&mut log);
            log.stage_append(
                &Message {
                    flags: 3,
                    headers: vec![8],
                    payload: vec![4; 1024],
                },
                1,
            )
            .unwrap();
            log.flush_buffers().unwrap();
            let job = log.prepare_fsync_job().unwrap();
            let elapsed = job.sync().unwrap();
            log.finish_fsync_job(job.durable_end(), elapsed).unwrap();
            assert!(log.durable_end_exclusive().covers(0));
            // No destructors or clean-shutdown store; the manifest still says next=0.
            std::process::exit(0);
        }
        for preallocate in [0u64, 1024 * 1024] {
            let dir = crate::test_dir!("manifest_abrupt_exit");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "log::background_manifest_tests::abrupt_exit_recovers_confirmed_data_with_stale_periodic_hint", "--nocapture"])
            .env(CHILD_ROOT, &dir.root).env("KERATIN_MANIFEST_CRASH_PREALLOC",preallocate.to_string()).status().unwrap();
            assert!(status.success());
            let m = Manifest::load_or_create(&dir.root, 0, 1, 1).unwrap();
            assert!(!m.clean_shutdown);
            assert_eq!(m.next_offset, 0);
            let mut log = open(&dir.root);
            assert_eq!(log.current_epoch(), 7);
            assert_eq!(log.next_offset, 1);
            let (result, _) = log
                .stage_append(
                    &Message {
                        flags: 0,
                        headers: vec![],
                        payload: vec![5],
                    },
                    2,
                )
                .unwrap();
            assert_eq!(result.base_offset, 1);
            log.shutdown().unwrap();
        }
    }

    #[test]
    fn failed_old_snapshot_does_not_advance_epoch() {
        let dir = crate::test_dir!("manifest_error_epoch");
        let mut log = open(&dir.root);
        let (result_tx, result_rx) = bounded(1);
        log.manifest_worker.pending = Some(result_rx);
        let (done_tx, done_rx) = bounded(1);
        let thread = std::thread::spawn(move || {
            let outcome = log.advance_epoch(3);
            done_tx
                .send((outcome, log))
                .unwrap_or_else(|_| panic!("test receiver lost"));
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
        result_tx
            .send(Err(io::Error::other("injected old checkpoint failure")))
            .unwrap();
        let (outcome, mut log) = done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(outcome.is_err());
        assert_eq!(log.current_epoch(), 0);
        assert_eq!(log.advance_epoch(3).unwrap(), 3);
        assert_eq!(
            Manifest::load_or_create(&dir.root, 0, 1, 1).unwrap().epoch,
            3
        );
        thread.join().unwrap();
    }

    #[test]
    fn directory_lock_outlives_pending_manifest_on_drop() {
        use crate::lock::{FileLock, log_lock_path};
        let dir = crate::test_dir!("manifest_lock_lifetime");
        let mut log = open(&dir.root);
        let path = log_lock_path(&dir.root);
        let lock = Arc::new(FileLock::try_exclusive_or_create(&path).unwrap());
        log.retain_directory_lock(lock.clone());
        let (tx, rx) = bounded(1);
        log.manifest_worker.pending = Some(rx);
        drop(lock);
        let thread = std::thread::spawn(move || drop(log));
        assert!(FileLock::try_exclusive(&path).is_err());
        tx.send(Ok(Duration::ZERO)).unwrap();
        thread.join().unwrap();
        FileLock::try_exclusive(&path).unwrap();
    }

    #[test]
    fn real_worker_failure_is_observed_and_checkpoint_can_retry() {
        let dir = crate::test_dir!("manifest_failure");
        let mut log = open(&dir.root);
        // A directory at the temporary-file path deterministically rejects writes.
        fs::create_dir(Manifest::tmp_path(&dir.root)).unwrap();
        log.store_periodic_manifest().unwrap();
        assert!(log.drain_manifest().is_err());
        assert!(log.manifest_worker.pending.is_none());
        fs::remove_dir(Manifest::tmp_path(&dir.root)).unwrap();
        log.manifest.epoch = 7;
        log.store_periodic_manifest().unwrap();
        log.drain_manifest().unwrap();
        assert_eq!(
            Manifest::load_or_create(&dir.root, 0, 1, 1).unwrap().epoch,
            7
        );
        log.shutdown().unwrap();
    }
}
