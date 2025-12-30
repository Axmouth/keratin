use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    index::Index,
    manifest::Manifest,
    record::{Message, Record, encode_record},
    recovery::scan_last_good,
    segment::Segment,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub base_offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct LogState {
    pub head: Arc<AtomicU64>, // inclusive; first available offset (0 initially)
    pub tail: Arc<AtomicU64>, // next offset to assign (exclusive)
    pub durable: Arc<AtomicU64>, // inclusive; last fsynced offset (u64::MAX if none)
}

impl LogState {
    pub fn new(head: u64, tail: u64, durable: u64) -> Self {
        Self {
            head: Arc::new(AtomicU64::new(head)),
            tail: Arc::new(AtomicU64::new(tail)),
            durable: Arc::new(AtomicU64::new(durable)),
        }
    }
}

pub struct Log {
    // buffers
    write_buf: Vec<u8>, // 16–64MB ideally
    idx_buf: Vec<u8>,   // sparse index buffer

    // watermarks (inclusive)
    staged_end_offset: u64, // last offset staged into buffers
    durable_offset: u64,    // last offset fsynced (inclusive)

    root: PathBuf,
    pub manifest: Manifest,
    pub active: Segment,
    pub index: Index,
    pub next_offset: u64,
    last_index_at_log_pos: u64,

    log_state: Arc<LogState>,

    // stats
    pub stats: IoStats,
    last_stats_dump: Instant,
    manifest_flush_interval: Duration,
    last_manifest_flush: Instant,

    pub flush_target_bytes: usize, // e.g. 16MB
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
        }
    }
}

impl Log {
    pub fn open(
        root: impl AsRef<Path>,
        now_ms: u64,
        segment_max_bytes: u64,
        index_stride_bytes: u32,
        flush_target_bytes: usize,
        log_state: Arc<LogState>,
    ) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("segments"))?;
        fs::create_dir_all(root.join("tmp"))?;

        let mut manifest =
            Manifest::load_or_create(&root, now_ms, segment_max_bytes, index_stride_bytes)?;

        // Discover segments by filename.
        let mut bases = list_segment_bases(&root.join("segments"))?;
        bases.sort_unstable();

        // If no segments exist, create first with base=0.
        if bases.is_empty() {
            let (seg, idx) = create_segment_pair(&root, 0, now_ms)?;
            manifest.active_base_offset = 0;
            manifest.next_offset = 0;
            manifest.store_atomic(&root)?;
            let next_offset: u64 = manifest.next_offset;
            let initial: u64 = next_offset.saturating_sub(1);

            return Ok(Self {
                root,
                last_index_at_log_pos: seg.bytes_written,
                active: seg,
                index: idx,
                next_offset,
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
            });
        }

        // Repair/scan all segments, compute true next_offset.
        let mut computed_next = 0u64;
        for &base in &bases {
            let log_path = seg_log_path(&root, base);
            let f = OpenOptions::new().read(true).write(true).open(&log_path)?;
            // open header to get header_len (fixed in our Segment::open, but we know it)
            // We'll trust Segment::open to validate base.
            let mut seg = Segment::open(f, base)?;
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
                computed_next = computed_next.max(base);
            }

            // Repair idx length too (best-effort).
            let idx_path = seg_idx_path(&root, base);
            if idx_path.exists() {
                let idxf = OpenOptions::new().read(true).write(true).open(&idx_path)?;
                let mut idx = Index::open(idxf, base)?;
                idx.repair_truncate_to_entries()?;
            }
        }

        // Choose active segment: last base.
        let active_base = *bases.last().unwrap();
        let (active, index) = open_or_create_segment_pair(&root, active_base, now_ms)?;

        // Recompute next_offset from computed_next; reconcile with manifest (prefer computed).
        let next_offset = computed_next.max(manifest.next_offset);
        manifest.active_base_offset = active_base;
        manifest.next_offset = next_offset;
        manifest.segment_max_bytes = segment_max_bytes;
        manifest.index_stride_bytes = index_stride_bytes;
        manifest.store_atomic(&root)?;

        let last_index_at_log_pos = active.bytes_written;
        let initial: u64 = next_offset.saturating_sub(1);

        Ok(Self {
            root,
            manifest,
            active,
            index,
            next_offset,
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
        })
    }

    pub fn stage_append_batch(
        &mut self,
        payloads: &[Message],
        now_ms: u64,
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

        for payload in payloads {
            let offset = self.next_offset;

            let r = Record {
                flags: payload.flags,
                timestamp_ms: now_ms,
                offset,
                headers: &payload.headers,
                payload: &payload.payload,
            };

            // record starts at: on-disk bytes + pending bytes + current buffer len
            let record_start_pos = self.active.bytes_written + self.write_buf.len() as u64;

            encode_record(&mut self.write_buf, &r)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            // Maybe emit an idx entry (encode into idx_buf, not to file)
            if (record_start_pos - self.last_index_at_log_pos)
                >= self.manifest.index_stride_bytes as u64
            {
                let rel = (offset - self.active.base_offset) as u32;

                // idx entry layout: rel_offset(4) reserved0(4) file_pos(8)
                self.idx_buf.extend_from_slice(&rel.to_be_bytes());
                self.idx_buf.extend_from_slice(&0u32.to_be_bytes());
                self.idx_buf
                    .extend_from_slice(&record_start_pos.to_be_bytes());

                self.last_index_at_log_pos = record_start_pos;
            }

            self.next_offset += 1;
        }

        self.stats.encode += t_encode.elapsed();
        self.stats.records += payloads.len() as u64;
        self.stats.batches += 1;

        let end_offset = self.next_offset - 1;
        self.staged_end_offset = end_offset;

        Ok((
            AppendResult {
                base_offset,
                count: payloads.len() as u32,
            },
            end_offset,
        ))
    }

    pub fn flush_buffers(&mut self) -> io::Result<u64> {
        // write log buffer
        if !self.write_buf.is_empty() {
            let t = Instant::now();
            self.active.append_bytes(&self.write_buf)?;
            self.stats.log_write += t.elapsed();
            self.write_buf.clear();
        }

        // write idx buffer
        if !self.idx_buf.is_empty() {
            let t = Instant::now();
            self.index.append_entries_raw(&self.idx_buf)?;
            self.stats.idx_write += t.elapsed();
            self.idx_buf.clear();
        }

        // periodic stat print (keep it here so it measures real IO)
        if self.last_stats_dump.elapsed() > Duration::from_secs(1) {
            println!(
                "KERATIN IO: batches={} recs={} encode={}ms log={}ms idx={}ms fsync={}ms manifest={}ms",
                self.stats.batches,
                self.stats.records,
                self.stats.encode.as_millis(),
                self.stats.log_write.as_millis(),
                self.stats.idx_write.as_millis(),
                self.stats.fsync.as_millis(),
                self.stats.manifest.as_millis(),
            );
            self.stats = IoStats::default();
            self.last_stats_dump = Instant::now();
        }

        Ok(self.staged_end_offset)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.active.flush()
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        let t = Instant::now();

        self.active.fsync()?;
        self.index.fsync()?;

        self.stats.fsync += t.elapsed();

        // mark durable
        self.durable_offset = self.staged_end_offset;

        // manifest is a hint; persist it on interval
        self.manifest.next_offset = self.next_offset;
        self.manifest.active_base_offset = self.active.base_offset;

        if self.last_manifest_flush.elapsed() >= self.manifest_flush_interval {
            let t2 = Instant::now();
            self.manifest.store_atomic(&self.root)?;
            self.stats.manifest += t2.elapsed();
            self.last_manifest_flush = Instant::now();
        }

        Ok(())
    }

    pub fn durable_watermark(&self) -> u64 {
        self.durable_offset
    }

    pub fn readable_watermark(&self) -> u64 {
        self.durable_offset
    }

    pub fn flushed_watermark(&self) -> u64 {
        self.durable_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
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

        let new_base = self.next_offset;
        let (seg, idx) = create_segment_pair(&self.root, new_base, now_ms)?;
        self.active = seg;
        self.index = idx;

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

        let seg_dir = self.root.join("segments");
        let mut bases = list_segment_bases(&seg_dir)?;
        bases.sort_unstable();

        if bases.is_empty() {
            // degenerate; keep head consistent
            self.manifest.head_offset = before;
            self.manifest.store_atomic(&self.root)?;
            return Ok(before);
        }

        // Active segment is the last base (by construction: you always roll to new_base=next_offset).
        let active_base = *bases.last().unwrap();

        // Identify deletable sealed bases (everything except active), using the property:
        // sealed segment [base_i, base_{i+1}) => last_offset = base_{i+1}-1
        let mut deletable: Vec<u64> = Vec::new();
        for w in bases.windows(2) {
            let base = w[0];
            let next_base = w[1];
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
        // - if we didn't delete any segment, head doesn't change.
        if deletable.is_empty() {
            return Ok(cur_head);
        }

        // Delete files (best-effort: delete idx + log; ignore NotFound for idempotence)
        for base in &deletable {
            let lp = seg_log_path(&self.root, *base);
            let ip = seg_idx_path(&self.root, *base);

            match fs::remove_file(&ip) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            match fs::remove_file(&lp) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        // Compute new head = smallest remaining base (after deletions)
        let mut remaining = list_segment_bases(&seg_dir)?;
        remaining.sort_unstable();

        let new_head = remaining.first().copied().unwrap_or(self.next_offset);

        // Persist + publish head
        self.manifest.head_offset = new_head;
        self.manifest.store_atomic(&self.root)?;
        self.log_state.head.store(new_head, Ordering::Release);

        Ok(new_head)
    }

    fn cleanup_orphans(seg_dir: &Path) -> io::Result<()> {
        // remove .idx with no .log and vice versa
        // optional: keep it conservative
        Ok(())
    }
}

// ---- helpers

fn seg_log_path(root: &Path, base: u64) -> PathBuf {
    root.join("segments").join(format!("{:020}.log", base))
}
fn seg_idx_path(root: &Path, base: u64) -> PathBuf {
    root.join("segments").join(format!("{:020}.idx", base))
}

fn list_segment_bases(dir: &Path) -> io::Result<Vec<u64>> {
    let mut out = Vec::new();
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        let name = ent.file_name();
        let Some(s) = name.to_str() else { continue };
        if let Some(stem) = s.strip_suffix(".log")
            && let Ok(base) = stem.parse::<u64>()
        {
            out.push(base);
        }
    }
    Ok(out)
}

fn create_segment_pair(root: &Path, base: u64, now_ms: u64) -> io::Result<(Segment, Index)> {
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
    Ok((seg, idx))
}

fn open_or_create_segment_pair(
    root: &Path,
    base: u64,
    now_ms: u64,
) -> io::Result<(Segment, Index)> {
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

    Ok((seg, idx))
}
