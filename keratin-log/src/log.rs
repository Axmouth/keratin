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

use parking_lot::RwLock;

#[cfg(feature = "writer-stage-trace")]
use crate::writer_stage_trace::WriterStageTracer;
use crate::{
    index::Index,
    manifest::Manifest,
    reader::LogReader,
    record::{Message, Record, encode_record},
    recovery::scan_last_good,
    segment::Segment,
    tail_cache::TailCache,
    util::fsync_dir,
};

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
    pub head: Arc<AtomicU64>, // inclusive; first available offset (0 initially)
    pub tail: Arc<AtomicU64>, // next offset to assign (exclusive)
    // Inclusive last fsynced offset. Empty logs currently report 0, matching
    // existing public behavior rather than using a nullable/sentinel value.
    pub durable: Arc<AtomicU64>,
    pub epoch: Arc<AtomicU64>,
}

pub(crate) struct FsyncJob {
    through_offset: u64,
    active: File,
    index: File,
}

impl FsyncJob {
    pub(crate) fn through_offset(&self) -> u64 {
        self.through_offset
    }

    pub(crate) fn sync(&self) -> io::Result<Duration> {
        let started = Instant::now();
        self.active.sync_data()?;
        self.index.sync_data()?;
        Ok(started.elapsed())
    }
}

impl LogState {
    pub fn new(head: u64, tail: u64, durable: u64) -> Self {
        Self {
            head: Arc::new(AtomicU64::new(head)),
            tail: Arc::new(AtomicU64::new(tail)),
            durable: Arc::new(AtomicU64::new(durable)),
            epoch: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub struct Log {
    // buffers
    write_buf: Vec<u8>, // 16-64MB ideally
    idx_buf: Vec<u8>,   // sparse index buffer

    // watermarks (inclusive)
    staged_end_offset: u64, // last offset staged into buffers
    durable_offset: u64,    // inclusive last fsynced offset, 0 for an empty log

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
    /// `through_offset` of the previous commit, to size the next one.
    pub(crate) last_commit_through: u64,
    /// Bytes to preallocate ahead of the active segment's write cursor (`0` =
    /// off). Applied to the active segment and to each new segment on roll.
    prealloc_chunk: u64,

    // stats
    pub stats: IoStats,
    last_stats_dump: Instant,
    manifest_flush_interval: Duration,
    last_manifest_flush: Instant,

    pub flush_target_bytes: usize, // e.g. 16MB

    segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
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
    write_buf: &mut Vec<u8>,
    idx_buf: &mut Vec<u8>,
) -> io::Result<AppendPlanOutcome> {
    debug_assert!(!payloads.is_empty());

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

        encode_record(write_buf, &record)
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
        log_state: Arc<LogState>,
    ) -> io::Result<(Self, Arc<RwLock<BTreeMap<u64, PathBuf>>>)> {
        let root = root.as_ref().to_path_buf();
        let prealloc_chunk = segment_preallocate_bytes as u64;
        fs::create_dir_all(root.join("segments"))?;
        fs::create_dir_all(root.join("tmp"))?;

        let mut manifest =
            Manifest::load_or_create(&root, now_ms, segment_max_bytes, index_stride_bytes)?;

        // Discover segments by filename.
        let mut bases = list_segment_bases(&root.join("segments"))?;
        bases.sort_unstable();

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
                    tail_cache: Arc::new(TailCache::new(log_state.durable.clone(), tail_cache_bytes)),
                    flushed_through: next_offset,
                    last_commit_through: next_offset,
                    recent_commit_records: 0,
                    prealloc_chunk,
                    manifest,
                    write_buf: Vec::with_capacity(16 * 1024 * 1024),
                    idx_buf: Vec::with_capacity(256 * 1024),
                    stats: IoStats::new(),
                    log_state,
                    last_stats_dump: Instant::now(),
                    manifest_flush_interval: Duration::from_millis(500),
                    last_manifest_flush: Instant::now(),
                    staged_end_offset: initial,
                    durable_offset: initial,
                    flush_target_bytes,
                    segment_mapping: segment_mapping.clone(),
                },
                segment_mapping,
            ));
        }

        if !force_recovery_scan
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
                    tail_cache: Arc::new(TailCache::new(log_state.durable.clone(), tail_cache_bytes)),
                    flushed_through: next_offset,
                    last_commit_through: next_offset,
                    recent_commit_records: 0,
                    prealloc_chunk,
                    last_index_at_log_pos,
                    write_buf: Vec::with_capacity(16 * 1024 * 1024),
                    idx_buf: Vec::with_capacity(256 * 1024),
                    log_state,
                    stats: IoStats::new(),
                    last_stats_dump: Instant::now(),
                    manifest_flush_interval: Duration::from_millis(500),
                    last_manifest_flush: Instant::now(),
                    staged_end_offset: initial,
                    durable_offset: initial,
                    flush_target_bytes,
                    segment_mapping: segment_mapping.clone(),
                },
                segment_mapping,
            ));
        }

        // Repair/scan all segments, compute true next_offset.
        let mut computed_next = 0u64;
        for (base, _base_path) in &bases {
            let log_path = seg_log_path(&root, *base);
            let f = OpenOptions::new().read(true).write(true).open(&log_path)?;
            // open header to get header_len (fixed in our Segment::open, but we know it)
            // We'll trust Segment::open to validate base.
            let mut seg = Segment::open(f, *base)?;
            // Scan from header_len; our Segment header is fixed size:
            let header_len = (8 + 2 + 2 + 4 + 8 + 8 + 32 + 4) as u64;
            let scan = scan_last_good(seg.file_ref(), header_len, 64 * 1024)?;
            if scan.last_good_pos < seg.bytes_written {
                // truncate partial tail
                seg.set_len(scan.last_good_pos)?;
            }
            if let Some(last) = scan.last_offset {
                computed_next = computed_next.max(last.saturating_add(1));
            } else {
                computed_next = computed_next.max(*base);
            }

            // Repair idx length too (best-effort).
            let idx_path = seg_idx_path(&root, *base);
            if idx_path.exists() {
                let idxf = OpenOptions::new().read(true).write(true).open(&idx_path)?;
                let mut idx = Index::open(idxf, *base)?;
                idx.repair_truncate_to_entries()?;
            }
        }

        let segment_mapping = Arc::new(RwLock::new(BTreeMap::from_iter(bases.clone())));

        // Choose active segment: last base.
        let (active_base, _active_base_path) = bases.last().expect("Already checked empty").clone();
        let (mut active, index, seg_path) =
            open_or_create_segment_pair(&root, active_base, now_ms)?;
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
                tail_cache: Arc::new(TailCache::new(log_state.durable.clone(), tail_cache_bytes)),
                flushed_through: next_offset,
                last_commit_through: next_offset,
                recent_commit_records: 0,
                prealloc_chunk,
                last_index_at_log_pos,
                write_buf: Vec::with_capacity(16 * 1024 * 1024),
                idx_buf: Vec::with_capacity(256 * 1024),
                log_state,
                stats: IoStats::new(),
                last_stats_dump: Instant::now(),
                manifest_flush_interval: Duration::from_millis(500),
                last_manifest_flush: Instant::now(),
                staged_end_offset: initial,
                durable_offset: initial,
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
        self.write_buf.reserve(estimated);
        // Index entries are sparse; reserve modestly
        self.idx_buf
            .reserve((estimated / (self.manifest.index_stride_bytes as usize).max(1)).max(64));

        let t_encode = Instant::now();

        let mut plan_state = self.append_plan_state(base_offset);
        #[cfg(not(feature = "writer-stage-trace"))]
        let plan = encode_append_payloads_into(
            &mut plan_state,
            payloads,
            now_ms,
            &mut self.write_buf,
            &mut self.idx_buf,
        )?;
        #[cfg(feature = "writer-stage-trace")]
        let plan = if let Some((tracer, work_id)) = tracer {
            tracer.trace(work_id, "encode", payloads.len(), estimated, || {
                encode_append_payloads_into(
                    &mut plan_state,
                    payloads,
                    now_ms,
                    &mut self.write_buf,
                    &mut self.idx_buf,
                )
            })?
        } else {
            encode_append_payloads_into(
                &mut plan_state,
                payloads,
                now_ms,
                &mut self.write_buf,
                &mut self.idx_buf,
            )?
        };
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

    pub fn stage_replicated_append_batch(
        &mut self,
        epoch: u64,
        first_offset: u64,
        payloads: &[Message],
        mode: ReplicatedAppendMode,
        now_ms: u64,
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
                self.verify_existing_prefix(first_offset, payloads)?;
                return Ok((
                    ReplicatedAppendOutcome::AlreadyPresent {
                        first_offset,
                        count,
                        next_offset: current_next,
                    },
                    None,
                ));
            }

            if end_offset == current_next.saturating_sub(1) {
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
            self.verify_existing_prefix(first_offset, &payloads[..skip])?;
            let suffix = &payloads[skip..];
            let (outcome, end_offset) = self.stage_replicated_append_batch(
                epoch,
                current_next,
                suffix,
                ReplicatedAppendMode::ExactFit,
                now_ms,
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

        self.write_buf.reserve(estimated);
        self.idx_buf
            .reserve((estimated / (self.manifest.index_stride_bytes as usize).max(1)).max(64));

        let t_encode = Instant::now();

        let mut plan_state = self.append_plan_state(first_offset);
        let plan = encode_append_payloads_into(
            &mut plan_state,
            payloads,
            now_ms,
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
        self.write_buf.reserve(estimated);
        // Index entries are sparse; reserve modestly
        self.idx_buf
            .reserve((estimated / (self.manifest.index_stride_bytes as usize).max(1)).max(64));

        let t_encode = Instant::now();

        let mut plan_state = self.append_plan_state(base_offset);
        #[cfg(not(feature = "writer-stage-trace"))]
        let plan = encode_append_payloads_into(
            &mut plan_state,
            std::slice::from_ref(payload),
            now_ms,
            &mut self.write_buf,
            &mut self.idx_buf,
        )?;
        #[cfg(feature = "writer-stage-trace")]
        let plan = if let Some((tracer, work_id)) = tracer {
            tracer.trace(work_id, "encode", 1, estimated, || {
                encode_append_payloads_into(
                    &mut plan_state,
                    std::slice::from_ref(payload),
                    now_ms,
                    &mut self.write_buf,
                    &mut self.idx_buf,
                )
            })?
        } else {
            encode_append_payloads_into(
                &mut plan_state,
                std::slice::from_ref(payload),
                now_ms,
                &mut self.write_buf,
                &mut self.idx_buf,
            )?
        };
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
        // write log buffer
        if !self.write_buf.is_empty() {
            let t = Instant::now();
            #[cfg(feature = "writer-stage-trace")]
            let bytes = self.write_buf.len();
            #[cfg(not(feature = "writer-stage-trace"))]
            self.active.append_bytes(&self.write_buf)?;
            #[cfg(feature = "writer-stage-trace")]
            if let Some((tracer, work_id)) = tracer {
                tracer.trace(work_id, "log_write", 0, bytes, || {
                    self.active.append_bytes(&self.write_buf)
                })?;
            } else {
                self.active.append_bytes(&self.write_buf)?;
            }
            self.stats.log_write += t.elapsed();
            // Feed the just-written batch to the tail cache so tail-following
            // reads serve it from memory instead of scanning this segment while
            // it is under fsync/writeback. Bytes are the exact on-disk format;
            // the cache gates reads on the durable watermark, so pushing here
            // (pre-fsync) never exposes a non-durable record.
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
            #[cfg(feature = "writer-stage-trace")]
            let bytes = self.idx_buf.len();
            #[cfg(not(feature = "writer-stage-trace"))]
            self.index.append_entries_raw(&self.idx_buf)?;
            #[cfg(feature = "writer-stage-trace")]
            if let Some((tracer, work_id)) = tracer {
                tracer.trace(work_id, "index_write", 0, bytes, || {
                    self.index.append_entries_raw(&self.idx_buf)
                })?;
            } else {
                self.index.append_entries_raw(&self.idx_buf)?;
            }
            self.stats.idx_write += t.elapsed();
            self.idx_buf.clear();
        }

        // periodic stat print (keep it here so it measures real IO)
        if self.last_stats_dump.elapsed() > Duration::from_secs(1) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards");

            let total_secs = now.as_secs();
            let millis = now.subsec_millis();

            println!(
                "{}.{:03} : KERATIN IO: batches={} recs={} encode={}ms log={}ms idx={}ms fsync={}ms manifest={}ms bytes={} kbytes/batch={} rec/batch={}",
                total_secs,
                millis,
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
        #[cfg(feature = "writer-stage-trace")]
        self.flush_buffers_inner(tracer)?;
        #[cfg(not(feature = "writer-stage-trace"))]
        self.flush_buffers()?;

        Ok(FsyncJob {
            through_offset: self.staged_end_offset,
            active: self.active.try_clone_file()?,
            index: self.index.try_clone_file()?,
        })
    }

    #[cfg_attr(feature = "writer-stage-trace", allow(dead_code))]
    pub(crate) fn finish_fsync_job(
        &mut self,
        through_offset: u64,
        elapsed: Duration,
    ) -> io::Result<()> {
        self.finish_fsync_job_inner(
            through_offset,
            elapsed,
            #[cfg(feature = "writer-stage-trace")]
            None,
        )
    }

    #[cfg(feature = "writer-stage-trace")]
    pub(crate) fn finish_fsync_job_traced(
        &mut self,
        through_offset: u64,
        elapsed: Duration,
        tracer: &WriterStageTracer,
        work_id: u64,
    ) -> io::Result<()> {
        self.finish_fsync_job_inner(through_offset, elapsed, Some((tracer, work_id)))
    }

    fn finish_fsync_job_inner(
        &mut self,
        through_offset: u64,
        elapsed: Duration,
        #[cfg(feature = "writer-stage-trace")] tracer: Option<(&WriterStageTracer, u64)>,
    ) -> io::Result<()> {
        self.stats.fsync += elapsed;
        self.durable_offset = self.durable_offset.max(through_offset);
        self.manifest.next_offset = self
            .manifest
            .next_offset
            .max(through_offset.saturating_add(1));
        self.manifest.active_base_offset = self.active.base_offset;

        // manifest is a hint; persist it on interval
        if self.last_manifest_flush.elapsed() >= self.manifest_flush_interval {
            #[cfg(not(feature = "writer-stage-trace"))]
            self.store_manifest()?;
            #[cfg(feature = "writer-stage-trace")]
            if let Some((tracer, work_id)) = tracer {
                tracer.trace(work_id, "manifest_write", 0, 0, || self.store_manifest())?;
            } else {
                self.store_manifest()?;
            }
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
        #[cfg(not(feature = "writer-stage-trace"))]
        self.fsync_files_and_update_manifest()?;
        #[cfg(feature = "writer-stage-trace")]
        if let Some((tracer, work_id)) = tracer {
            tracer.trace(work_id, "fsync", 0, 0, || {
                self.fsync_files_and_update_manifest()
            })?;
        } else {
            self.fsync_files_and_update_manifest()?;
        }

        // manifest is a hint; persist it on interval
        if self.last_manifest_flush.elapsed() >= self.manifest_flush_interval {
            #[cfg(not(feature = "writer-stage-trace"))]
            self.store_manifest()?;
            #[cfg(feature = "writer-stage-trace")]
            if let Some((tracer, work_id)) = tracer {
                tracer.trace(work_id, "manifest_write", 0, 0, || self.store_manifest())?;
            } else {
                self.store_manifest()?;
            }
        }

        Ok(())
    }

    fn fsync_files_and_update_manifest(&mut self) -> io::Result<()> {
        let t = Instant::now();

        self.active.fsync()?;
        self.index.fsync()?;

        self.stats.fsync += t.elapsed();

        // mark durable
        self.durable_offset = self.staged_end_offset;

        self.manifest.next_offset = self.next_offset;
        self.manifest.active_base_offset = self.active.base_offset;

        Ok(())
    }

    fn store_manifest(&mut self) -> io::Result<()> {
        let t = Instant::now();
        self.manifest.store_atomic(&self.root)?;
        self.stats.manifest += t.elapsed();
        self.last_manifest_flush = Instant::now();
        Ok(())
    }

    pub fn durable_watermark(&self) -> u64 {
        self.durable_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
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

        self.manifest.epoch = epoch;
        self.manifest.next_offset = self.next_offset;
        self.manifest.active_base_offset = self.active.base_offset;
        self.manifest.store_atomic(&self.root)?;
        self.log_state.epoch.store(epoch, Ordering::Release);

        Ok(epoch)
    }

    #[inline]
    pub fn should_flush(&self) -> bool {
        self.write_buf.len() >= self.flush_target_bytes
    }

    fn roll(&mut self, now_ms: u64) -> io::Result<()> {
        // 1) flush current buffers to current segment files
        self.flush_buffers()?;

        // 2) fsync to seal it durably (conservative)
        self.fsync()?;

        // The outgoing segment is sealed: reclaim any preallocated padding so it
        // is not left on disk for the segment's lifetime.
        self.active.trim_to_written()?;

        let new_base = self.next_offset;
        let (mut seg, idx, new_seg_path) = create_segment_pair(&self.root, new_base, now_ms)?;
        seg.enable_prealloc(self.prealloc_chunk);
        self.active = seg;
        self.index = idx;

        self.segment_mapping.write().insert(new_base, new_seg_path);

        self.last_index_at_log_pos = self.active.bytes_written;

        self.manifest.active_base_offset = new_base;
        let t = Instant::now();
        self.manifest.store_atomic(&self.root)?;
        self.stats.manifest += t.elapsed();
        self.last_manifest_flush = Instant::now();

        Ok(())
    }

    /// Delete whole sealed segments whose max offset < `before`.
    /// NOTE: v0 is *segment-granular* retention. It will not trim inside the active segment.
    pub fn truncate_before(&mut self, before: u64) -> io::Result<u64> {
        // Nothing to do
        let cur_head = self.manifest.head_offset; // add this to manifest
        if before <= cur_head {
            return Ok(cur_head);
        }

        // Must not delete data that might still be needed for durability semantics.
        // We force durability before deleting anything.
        self.flush_buffers()?;
        self.fsync()?;

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
            self.manifest.store_atomic(&self.root)?;
            return Ok(before);
        }

        // Active segment is the last base (by construction: you always roll to new_base=next_offset).
        let (active_base, _) = *bases.last().expect("Already checked if empty");

        // Identify deletable sealed bases (everything except active), using the property:
        // sealed segment [base_i, base_{i+1}) => last_offset = base_{i+1}-1
        let mut deletable: Vec<u64> = Vec::new();
        for w in bases.windows(2) {
            let (base, _) = w[0];
            let (next_base, _) = w[1];
            let last_offset = next_base.saturating_sub(1);

            if last_offset < before {
                deletable.push(base);
            } else {
                // since bases sorted and last_offset grows, we can stop early
                break;
            }
        }

        // Never delete the active segment in v0 (no mid-segment trim).
        deletable.retain(|&b| b != active_base);

        // If nothing deletable, we can still advance head only up to the first base >= before? No.
        // In v0 we keep head truthful to "first readable". That is:
        // if we didn't delete any segment, head doesn't change.
        if deletable.is_empty() {
            return Ok(cur_head);
        }

        // Delete files (best-effort: delete idx + log; ignore NotFound for idempotence)
        for base in &deletable {
            let lp = seg_log_path(&self.root, *base);
            let ip = seg_idx_path(&self.root, *base);
            self.segment_mapping.write().remove(base);

            match fs::remove_file(&ip) {
                Ok(_) => {
                    tracing::info!("{} removed", ip.display())
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    tracing::warn!("{} not found during truncating", ip.display());
                }
                Err(e) => return Err(e),
            }
            match fs::remove_file(&lp) {
                Ok(_) => {
                    tracing::info!("{} removed", lp.display())
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    tracing::warn!("{} not found during truncating", lp.display());
                }
                Err(e) => return Err(e),
            }
        }

        // Compute new head = smallest remaining base (after deletions)
        // let mut remaining = list_segment_bases(&seg_dir)?;
        let mut remaining = self
            .segment_mapping
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect::<Vec<(u64, PathBuf)>>();
        remaining.sort_unstable();

        let new_head = remaining
            .first()
            .map(|(v, _)| v)
            .copied()
            .unwrap_or(self.next_offset);

        // Persist + publish head
        self.manifest.head_offset = new_head;
        self.manifest.store_atomic(&self.root)?;
        self.log_state.head.store(new_head, Ordering::Release);

        Ok(new_head)
    }

    pub fn reset_to_checkpoint(&mut self, next_offset: u64, now_ms: u64) -> io::Result<()> {
        self.flush_buffers()?;
        self.flush()?;
        self.fsync()?;

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
        seg.enable_prealloc(self.prealloc_chunk);
        self.active = seg;
        self.index = idx;
        self.segment_mapping.write().insert(next_offset, seg_path);

        self.write_buf.clear();
        self.idx_buf.clear();
        self.next_offset = next_offset;
        self.staged_end_offset = next_offset.saturating_sub(1);
        self.durable_offset = self.staged_end_offset;
        self.flushed_through = next_offset;
        // The log rewound past everything the cache held; drop it so no stale
        // (rewound-away) offset is ever served from memory.
        self.tail_cache.clear();
        self.last_index_at_log_pos = self.active.bytes_written;

        self.manifest.active_base_offset = next_offset;
        self.manifest.next_offset = next_offset;
        self.manifest.head_offset = next_offset;
        self.manifest.store_atomic(&self.root)?;
        fsync_dir(&seg_dir)?;

        self.log_state.head.store(next_offset, Ordering::Release);
        self.log_state.tail.store(next_offset, Ordering::Release);
        self.log_state
            .durable
            .store(self.durable_offset, Ordering::Release);

        Ok(())
    }

    fn verify_existing_prefix(&self, first_offset: u64, payloads: &[Message]) -> io::Result<()> {
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
        let got = reader.scan_from(readable_first_offset, payloads.len())?;
        if got.len() != payloads.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "replicated overlap prefix is not fully readable",
            ));
        }

        for (idx, (existing, incoming)) in got.iter().zip(payloads).enumerate() {
            let expected_offset = readable_first_offset + idx as u64;
            if existing.offset != expected_offset
                || existing.flags != incoming.flags
                || existing.headers != incoming.headers
                || existing.payload != incoming.payload
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("replicated overlap mismatch at offset {expected_offset}"),
                ));
            }
        }

        Ok(())
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        self.flush_buffers()?;
        self.flush()?;
        self.fsync_files_and_update_manifest()?;
        // Trim the preallocated padding so the next clean open sees file size ==
        // data size and needs no scan. Made durable before the clean flag, so a
        // crash in between just falls back to the recovery scan.
        self.active.trim_to_written()?;
        self.active.fsync()?;
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

fn seg_log_path(root: &Path, base: u64) -> PathBuf {
    root.join("segments").join(format!("{:020}.log", base))
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

fn create_segment_pair(
    root: &Path,
    base: u64,
    now_ms: u64,
) -> io::Result<(Segment, Index, PathBuf)> {
    let log_path = seg_log_path(root, base);
    let idx_path = seg_idx_path(root, base);

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

fn open_or_create_segment_pair(
    root: &Path,
    base: u64,
    now_ms: u64,
) -> io::Result<(Segment, Index, PathBuf)> {
    let log_path = seg_log_path(root, base);
    let idx_path = seg_idx_path(root, base);

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
