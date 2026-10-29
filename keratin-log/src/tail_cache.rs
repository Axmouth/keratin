//! In-memory tail cache for tail-following reads.
//!
//! Delivery reads the records the writer has just appended. Reading them back
//! from the active segment contends with that segment's fsync and writeback on
//! real drives, while the bytes are still in memory. The cache keeps a bounded
//! ring of the most recent flush batches, as the exact encoded on-disk bytes,
//! and serves reads near the tail from it. Older offsets miss and go to the
//! file.
//!
//! Rules the cache keeps:
//!
//! 1. A miss is always correct. The cache is never a source of truth. A read
//!    below the window, a disabled cache, a gap or a decode error returns a
//!    miss and the caller scans the file. A hit covers the whole requested
//!    range up to its frontier, never part of it, so callers do not stitch.
//! 2. Each read names its frontier. Ordinary reads (`scan_from`) stop at the
//!    durable frontier, so they never see a record whose fsync has not
//!    completed. `scan_written_with_byte_budget` stops at the written
//!    frontier, for logs whose readers accept records that reached the OS but
//!    not the drive, such as buffered queues. Replication reads
//!    (`scan_for_replication`) may return any cached batch, which can be ahead
//!    of both frontiers. Their callers fence the history and gate confirmation
//!    on owner durability. That path misses while the cache is over budget.
//!    Batches are pushed after the file write and before fsync, so the
//!    frontier check is what keeps the first two kinds correct.
//! 3. Clear on every backward move. `Log::reset_to_checkpoint` and
//!    `Log::repair_suffix` clear the cache. `truncate_before` only moves the
//!    head forward and readers never ask below the head, so stale entries
//!    there are never read and need no clear. As a backstop, `push_batch`
//!    drops everything when a batch does not continue the previous one.
//! 4. Batches are contiguous and ascending. Each base equals the previous
//!    batch's next offset, tracked by `Log::flushed_through`. Reads rely on
//!    this to binary search the covering batch and return a contiguous run.
//! 5. Cached bytes are decoded with the same `decode_record_prefix` as the
//!    file path, so cache hits and file reads return identical records.
//!    Recovery verification reads the disk explicitly (`scan_from_disk`).
//! 6. Only the owner fills the cache. Followers and frozen logs serve no tail
//!    reads, so caching there would only hold memory. Demotion drops the
//!    contents and promotion starts a new run from the next flush.
//! 7. `tail_cache_bytes` is a node-local setting, not replicated, since it
//!    trades memory for read latency on that node's hardware. `0` disables
//!    the cache. A queue's single log takes the message log setting. Fibril
//!    runs the separate event logs (stream cursors and metadata) at `0`.
//!    `push_batch` keeps the newest batch even when it alone exceeds the
//!    budget, so the cap is soft when a flush is larger than the budget.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::durability::{DurableFrontier, DurableWatermark, WrittenWatermark};
use crate::reader::{OwnedRecord, ScanByteBudget, to_owned};
use crate::record::{decode_record_prefix, peek_record_prefix};

/// One flush worth of encoded records, covering `[base_offset, next_offset)`.
#[derive(Debug)]
struct CachedBatch {
    base_offset: u64,
    next_offset: u64,
    bytes: Arc<[u8]>,
}

#[derive(Debug)]
struct Inner {
    /// Ordered oldest -> newest, contiguous: each batch's `base_offset` equals
    /// the previous batch's `next_offset`.
    batches: VecDeque<CachedBatch>,
    bytes: usize,
}

/// Bounded in-memory ring of recent flush batches. Shared (via `Arc`) between the
/// writer (which pushes at flush) and every `LogReader` (which reads).
#[derive(Debug)]
pub(crate) struct TailCache {
    inner: RwLock<Inner>,
    /// The durable frontier (a clone of `LogState.durable`). Ordinary reads never
    /// serve an offset at or above it; tentative replication opts in separately.
    durable: DurableWatermark,
    /// The written frontier (a clone of `LogState.written`). Bounds reads that
    /// opt into records not yet covered by an fsync.
    written_next: WrittenWatermark,
    /// Total cached bytes cap. `0` disables the cache (all reads go to the file).
    byte_budget: usize,
    /// Off while the log is not the owner. Followers serve no reads from
    /// their own tail, so caching there only holds memory.
    active: std::sync::atomic::AtomicBool,
    written: tokio::sync::watch::Sender<()>,
}

impl TailCache {
    pub(crate) fn new(
        durable: DurableWatermark,
        written_next: WrittenWatermark,
        byte_budget: usize,
    ) -> Self {
        Self {
            inner: RwLock::new(Inner {
                batches: VecDeque::new(),
                bytes: 0,
            }),
            durable,
            written_next,
            byte_budget,
            active: std::sync::atomic::AtomicBool::new(true),
            written: tokio::sync::watch::channel(()).0,
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.byte_budget > 0
    }

    /// The durable frontier the cache and file reader bound their scans by, so
    /// neither ever serves a written-but-not-yet-durable record or reads the
    /// preallocated zero padding past the durable data.
    pub(crate) fn durable_frontier(&self) -> DurableFrontier {
        self.durable.load()
    }

    /// The exclusive written frontier, for reads that serve written records.
    pub(crate) fn written_frontier(&self) -> u64 {
        self.written_next.load()
    }

    /// Append a just-flushed batch. Called once per flush (~fsync rate). Evicts
    /// from the front while over budget, always keeping at least one batch.
    pub(crate) fn push_batch(&self, base_offset: u64, next_offset: u64, bytes: Arc<[u8]>) {
        if !self.active.load(std::sync::atomic::Ordering::Acquire) {
            // Readers still wait on new writes, from the file.
            self.written.send_replace(());
            return;
        }
        if self.byte_budget == 0 || bytes.is_empty() || next_offset <= base_offset {
            return;
        }
        let len = bytes.len();
        let mut inner = self.inner.write();
        // Contiguity: a gap would break the "serve a contiguous run" invariant.
        // Drop the whole cache rather than serve across a hole (e.g. after a
        // rewind that skipped clear()); the file path stays correct.
        if let Some(back) = inner.batches.back()
            && back.next_offset != base_offset
        {
            inner.batches.clear();
            inner.bytes = 0;
        }
        inner.batches.push_back(CachedBatch {
            base_offset,
            next_offset,
            bytes,
        });
        inner.bytes += len;
        while inner.bytes > self.byte_budget && inner.batches.len() > 1 {
            if let Some(front) = inner.batches.pop_front() {
                inner.bytes -= front.bytes.len();
            }
        }
        drop(inner);
        self.written.send_replace(());
    }

    /// Start or stop caching. Stopping drops what is cached. The next push
    /// after a restart begins a new contiguous run.
    pub(crate) fn set_active(&self, active: bool) {
        if !self.active.swap(active, std::sync::atomic::Ordering::AcqRel) || active {
            return;
        }
        self.clear();
    }

    pub(crate) fn subscribe_written(&self) -> tokio::sync::watch::Receiver<()> {
        self.written.subscribe()
    }

    /// Drop everything. Must be called on any backward move of the log
    /// (truncate, reset, epoch change, snapshot install, rewind).
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.write();
        inner.batches.clear();
        inner.bytes = 0;
        drop(inner);
        self.written.send_replace(());
    }

    /// Serve records `[from, from + max)` from the cache, gated on the durable
    /// watermark.
    ///
    /// - `None`: the requested durable range is not completely cached, the cache
    ///   is disabled, or decoding/offset validation failed. Read from the file.
    /// - `Some(recs)`: every record in `[from, min(from + max, durable))` at the
    ///   captured frontier. This can be shorter than `max`, or empty when `from`
    ///   is at/past the frontier; a cache hit never returns a partial range.
    #[cfg(test)]
    pub(crate) fn read_from(&self, from: u64, max: usize) -> Option<Vec<OwnedRecord>> {
        self.read_from_with_byte_budget(from, max, usize::MAX)
    }

    pub(crate) fn read_from_with_byte_budget(
        &self,
        from: u64,
        max: usize,
        max_bytes: usize,
    ) -> Option<Vec<OwnedRecord>> {
        self.read_bounded(
            from,
            max,
            max_bytes,
            Some(self.durable_frontier().first_non_durable()),
        )
    }

    #[cfg(test)]
    pub(crate) fn read_written(&self, from: u64, max: usize) -> Option<Vec<OwnedRecord>> {
        self.read_written_with_byte_budget(from, max, usize::MAX)
    }

    /// Records below the written frontier, durable or not. Same hit and miss
    /// contract as `read_from_with_byte_budget`.
    pub(crate) fn read_written_visible_with_byte_budget(
        &self,
        from: u64,
        max: usize,
        max_bytes: usize,
    ) -> Option<Vec<OwnedRecord>> {
        self.read_bounded(from, max, max_bytes, Some(self.written_frontier()))
    }

    /// Tentative replication only. Oversized cached flushes miss this path.
    pub(crate) fn read_written_with_byte_budget(
        &self,
        from: u64,
        max: usize,
        max_bytes: usize,
    ) -> Option<Vec<OwnedRecord>> {
        self.read_bounded(from, max, max_bytes, None)
    }

    fn read_bounded(
        &self,
        from: u64,
        max: usize,
        max_bytes: usize,
        durable: Option<u64>,
    ) -> Option<Vec<OwnedRecord>> {
        if self.byte_budget == 0 || max == 0 {
            return None;
        }
        let upper;
        // Hold the lock only long enough to clone the covering batches' byte
        // handles (cheap Arc refcounts). Decoding runs after the guard drops so a
        // long read never blocks the writer's `push_batch`.
        let bufs: Vec<(u64, u64, Arc<[u8]>)> = {
            let inner = self.inner.read();
            if durable.is_none() && inner.bytes > self.byte_budget {
                return None;
            }
            let frontier = durable.unwrap_or(inner.batches.back()?.next_offset);
            upper = from.saturating_add(max as u64).min(frontier);
            let oldest = inner.batches.front()?.base_offset;
            if from < oldest {
                return None; // below the window: lagging consumer reads the file
            }
            if upper <= from {
                return Some(Vec::new()); // in-window but nothing durable at/after `from` yet
            }
            // Batches are contiguous and ascending by offset, so binary-search to
            // the first one that can cover `from` instead of scanning the ring
            // (which grows to thousands of entries under small group-commit
            // flushes). `take_while` then stops at the first batch past `upper`.
            let start = inner.batches.partition_point(|b| b.next_offset <= from);
            if inner.batches.get(start)?.base_offset > from
                || inner.batches.back()?.next_offset < upper
            {
                return None;
            }
            inner
                .batches
                .range(start..)
                .take_while(|b| b.base_offset < upper)
                .map(|b| (b.base_offset, b.next_offset, b.bytes.clone()))
                .collect()
        };
        // Records are contiguous and ascending, so the loop returns as soon as it
        // has the `upper - from` records requested.
        let mut out = Vec::with_capacity((upper - from) as usize);
        let mut expected = from;
        let mut budget = ScanByteBudget::new(max_bytes);
        for (base, next, buf) in &bufs {
            let mut decoded_next = *base;
            let mut pos = 0usize;
            while pos < buf.len() {
                // A damaged cache must miss, even after a valid prefix. The file
                // path performs its normal validation of the durable records.
                // Records before `from` are stepped over by length. A full
                // decode of each would make a read near the end of a large
                // flush batch cost a CRC pass over the whole batch.
                let (offset, used) = peek_record_prefix(&buf[pos..]).ok()?;
                if offset != decoded_next || offset >= *next {
                    return None;
                }
                decoded_next += 1;
                if offset < from {
                    pos += used;
                    continue;
                }
                let (rec, _) = decode_record_prefix(&buf[pos..]).ok()?;
                if rec.offset != expected {
                    return None;
                }
                budget.observe(&rec);
                out.push(to_owned(rec));
                expected += 1;
                if expected == upper || budget.finished {
                    return Some(out);
                }
                pos += used;
            }
            if decoded_next != *next {
                return None;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Record, encode_record};

    fn encode_batch(base: u64, count: u64, payload_len: usize) -> (u64, u64, Arc<[u8]>) {
        let mut buf = Vec::new();
        for i in 0..count {
            let rec = Record {
                flags: 0,
                timestamp_ms: 1000 + i,
                offset: base + i,
                headers: b"h",
                payload: &vec![(base + i) as u8; payload_len],
            };
            encode_record(&mut buf, &rec).unwrap();
        }
        (base, base + count, buf.into())
    }

    /// `durable_excl` is the exclusive durable frontier (count of durable
    /// records): `0` = nothing durable, `N` = offsets `0..N` durable.
    fn cache(budget: usize, durable_excl: u64) -> TailCache {
        TailCache::new(
            DurableWatermark::new(DurableFrontier::from_exclusive(durable_excl)),
            WrittenWatermark::new(durable_excl),
            budget,
        )
    }

    /// A follower caches nothing, demotion drops what the owner cached, and
    /// promotion starts a fresh run from the next batch.
    #[test]
    fn inactive_cache_holds_nothing_until_reactivated() {
        let c = cache(1 << 20, 30);
        let (b, n, bytes) = encode_batch(0, 10, 16);
        c.push_batch(b, n, bytes);
        assert_eq!(c.read_from(0, 10).map(|r| r.len()), Some(10));

        c.set_active(false);
        assert!(c.read_from(0, 10).is_none());
        let (b, n, bytes) = encode_batch(10, 10, 16);
        c.push_batch(b, n, bytes);
        assert!(c.read_from(10, 10).is_none());
        assert_eq!(c.inner.read().bytes, 0);

        c.set_active(true);
        let (b, n, bytes) = encode_batch(20, 10, 16);
        c.push_batch(b, n, bytes);
        assert_eq!(c.read_from(20, 10).map(|r| r.len()), Some(10));
        assert!(c.read_from(10, 10).is_none());
    }

    #[test]
    fn records_before_from_are_stepped_over_by_length() {
        let (b, n, bytes) = encode_batch(0, 10, 32);
        let mut record_starts = vec![0usize];
        while record_starts.len() < 10 {
            let (_, used) =
                crate::record::decode_record_prefix(&bytes[*record_starts.last().unwrap()..])
                    .unwrap();
            record_starts.push(record_starts.last().unwrap() + used);
        }
        // A damaged payload is only detected when that record is returned.
        let mut payload_damage = bytes.to_vec();
        payload_damage[record_starts[2] + 40] ^= 0xff;
        let c = cache(1 << 20, 10);
        c.push_batch(b, n, payload_damage.into());
        let records = c.read_from(5, 5).unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [5, 6, 7, 8, 9]
        );
        assert!(c.read_from(2, 1).is_none());
        // A damaged length misaligns every later frame, so the read misses.
        let mut length_damage = bytes.to_vec();
        length_damage[record_starts[2] + 12] ^= 0x01;
        let c = cache(1 << 20, 10);
        c.push_batch(b, n, length_damage.into());
        assert!(c.read_from(5, 5).is_none());
    }

    #[test]
    fn disabled_cache_always_misses() {
        let c = cache(0, 1000);
        let (b, n, bytes) = encode_batch(0, 10, 8);
        c.push_batch(b, n, bytes); // no-op when disabled
        assert!(c.read_from(0, 10).is_none());
    }

    #[tokio::test]
    async fn tentative_reads_are_bounded_and_wakes_survive_an_unpolled_read() {
        let (_, _, bytes) = encode_batch(0, 2, 16);
        let c = cache(bytes.len(), 0);
        let mut wake = c.subscribe_written();
        for base in [0, 2] {
            let (b, n, bytes) = encode_batch(base, 2, 16);
            c.push_batch(b, n, bytes);
            // The write happened before changed() was polled.
            tokio::time::timeout(std::time::Duration::from_secs(1), wake.changed())
                .await
                .unwrap()
                .unwrap();
            assert!(c.read_from(base, 2).unwrap().is_empty());
            let records = c.read_written(base, 10).unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(records[0].offset, base);
        }
        assert!(c.read_written(0, 4).is_none(), "evicted range must miss");
        c.clear();
        assert!(
            c.read_written(2, 2).is_none(),
            "reset discards tentative history"
        );
        assert!(wake.has_changed().unwrap());
        let (b, n, bytes) = encode_batch(0, 10, 16);
        c.push_batch(b, n, bytes);
        assert!(
            c.read_written(0, 10).is_none(),
            "oversized batch uses durable fallback"
        );
        c.durable.advance(DurableFrontier::from_exclusive(10));
        assert_eq!(c.read_from(0, 10).unwrap().len(), 10);
        let disabled = cache(0, 0);
        let (b, n, bytes) = encode_batch(0, 1, 16);
        disabled.push_batch(b, n, bytes);
        assert!(disabled.read_written(0, 1).is_none());
    }

    #[test]
    fn tentative_byte_budget_keeps_lookahead_and_ordinary_visibility() {
        let c = cache(1 << 20, 0);
        let (b, n, bytes) = encode_batch(0, 10, 128);
        c.push_batch(b, n, bytes);
        let record_bytes = crate::record::RECORD_HEADER_LEN + 1 + 128;
        let bounded = c
            .read_written_with_byte_budget(0, 10, record_bytes * 2)
            .unwrap();
        assert_eq!(
            bounded.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let oversized = c.read_written_with_byte_budget(3, 10, 1).unwrap();
        assert_eq!(
            oversized.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(c.read_written_with_byte_budget(3, 1, 1).unwrap().len(), 1);
        assert!(
            c.read_from_with_byte_budget(0, 10, record_bytes * 2)
                .unwrap()
                .is_empty()
        );
        c.durable.advance(DurableFrontier::from_exclusive(10));
        let durable = c
            .read_from_with_byte_budget(0, 10, record_bytes * 2)
            .unwrap();
        assert_eq!(
            durable.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        c.clear();
        assert!(
            c.read_written_with_byte_budget(0, 10, record_bytes * 2)
                .is_none()
        );
    }

    #[test]
    fn hit_serves_durable_tail() {
        let c = cache(1 << 20, 10); // offsets 0..10 durable (frontier 10)
        let (b, n, bytes) = encode_batch(0, 10, 16);
        c.push_batch(b, n, bytes);
        let recs = c.read_from(0, 10).expect("in window");
        assert_eq!(recs.len(), 10);
        assert_eq!(recs[0].offset, 0);
        assert_eq!(recs[9].offset, 9);
        assert_eq!(recs[3].payload, vec![3u8; 16]);
    }

    #[test]
    fn respects_max() {
        let c = cache(1 << 20, 100);
        let (b, n, bytes) = encode_batch(0, 50, 8);
        c.push_batch(b, n, bytes);
        let recs = c.read_from(10, 5).expect("in window");
        assert_eq!(recs.len(), 5);
        assert_eq!(recs[0].offset, 10);
        assert_eq!(recs[4].offset, 14);
    }

    #[test]
    fn durable_gate_caps_the_range() {
        let c = cache(1 << 20, 5); // only offsets 0..5 durable (frontier 5)
        let (b, n, bytes) = encode_batch(0, 20, 8);
        c.push_batch(b, n, bytes);
        let recs = c.read_from(0, 20).expect("in window");
        assert_eq!(recs.len(), 5); // offsets 0..=4
        assert_eq!(recs.last().unwrap().offset, 4);
    }

    #[test]
    fn empty_frontier_serves_nothing_but_offset_zero_alone_is_serveable() {
        // Frontier `0` means nothing durable: offset 0 is pushed pre-fsync but must
        // not be served yet.
        let c = cache(1 << 20, 0);
        let (b, n, bytes) = encode_batch(0, 5, 8);
        c.push_batch(b, n, bytes);
        assert!(c.read_from(0, 5).expect("in window").is_empty());

        // Frontier `1` means offset 0 alone is durable, and the exclusive encoding
        // (unlike a bare inclusive `0`) can say so: offset 0 is served.
        let c = cache(1 << 20, 1);
        let (b, n, bytes) = encode_batch(0, 5, 8);
        c.push_batch(b, n, bytes);
        let recs = c.read_from(0, 5).expect("in window");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].offset, 0);
    }

    #[test]
    fn nothing_durable_yet_returns_empty_not_miss() {
        let c = cache(1 << 20, 0); // frontier 0: nothing durable
        let (b, n, bytes) = encode_batch(5, 10, 8); // window starts at 5
        c.push_batch(b, n, bytes);
        // from=5 is in the window, but the frontier is 0, so nothing at/after 5.
        let recs = c.read_from(5, 10).expect("in window");
        assert!(recs.is_empty());
    }

    #[test]
    fn below_window_misses_to_file() {
        let c = cache(1 << 20, 1000);
        let (b, n, bytes) = encode_batch(100, 10, 8); // window 100..110
        c.push_batch(b, n, bytes);
        assert!(c.read_from(50, 10).is_none()); // older than the cached tail
    }

    #[test]
    fn eviction_by_byte_budget_advances_the_window() {
        // Each batch ~ 10 records * (fixed 32 + hlen 1 + plen 64 + crc 4) bytes.
        let per_batch = {
            let (_, _, bytes) = encode_batch(0, 10, 64);
            bytes.len()
        };
        let c = cache(per_batch * 2 + 1, 1_000_000); // hold ~2 batches
        for k in 0..5u64 {
            let (b, n, bytes) = encode_batch(k * 10, 10, 64);
            c.push_batch(b, n, bytes);
        }
        // Oldest batches evicted: reading offset 0 now misses (below window).
        assert!(c.read_from(0, 5).is_none());
        // The recent tail is still served.
        let recs = c.read_from(45, 5).expect("recent tail cached");
        assert_eq!(recs[0].offset, 45);
    }

    #[test]
    fn clear_empties_the_cache() {
        let c = cache(1 << 20, 1000);
        let (b, n, bytes) = encode_batch(0, 10, 8);
        c.push_batch(b, n, bytes);
        c.clear();
        assert!(c.read_from(0, 10).is_none());
    }

    #[test]
    fn read_spans_multiple_contiguous_batches() {
        let c = cache(1 << 20, 1000);
        for k in 0..3u64 {
            let (b, n, bytes) = encode_batch(k * 10, 10, 8);
            c.push_batch(b, n, bytes);
        }
        let recs = c.read_from(5, 20).expect("in window");
        assert_eq!(recs.len(), 20);
        assert_eq!(recs[0].offset, 5);
        assert_eq!(recs[19].offset, 24);
    }

    #[test]
    fn noncontiguous_push_drops_stale_cache() {
        let c = cache(1 << 20, 1000);
        let (b, n, bytes) = encode_batch(0, 10, 8);
        c.push_batch(b, n, bytes);
        // A push that does not start at the previous next_offset (a rewind that
        // forgot to clear) drops everything rather than serve across a hole.
        let (b2, n2, bytes2) = encode_batch(100, 10, 8);
        c.push_batch(b2, n2, bytes2);
        assert!(c.read_from(0, 10).is_none()); // old batch gone
        let recs = c.read_from(100, 10).expect("new window");
        assert_eq!(recs[0].offset, 100);
    }
    #[test]
    fn short_durable_tail_is_served_without_a_segment_file() {
        let c = Arc::new(cache(1 << 20, 1));
        let (base, next, bytes) = encode_batch(0, 3, 8);
        c.push_batch(base, next, bytes);
        let reader = crate::reader::LogReader::new(
            "unused-cache-only-root",
            Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            c.clone(),
        );
        let records = reader.scan_from(0, 2048).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].payload, vec![0; 8]);
        assert!(reader.scan_from(1, 2048).unwrap().is_empty());
        c.durable.advance(DurableFrontier::from_exclusive(3));
        assert_eq!(reader.scan_from(0, 2048).unwrap().len(), 3);
    }

    #[test]
    fn incomplete_cached_tail_is_a_miss() {
        let c = cache(1 << 20, 20);
        let (base, next, bytes) = encode_batch(0, 3, 8);
        c.push_batch(base, next, bytes);
        assert!(c.read_from(0, 20).is_none());
        assert!(c.read_from(3, 20).is_none());
        assert_eq!(c.read_from(0, 3).unwrap().len(), 3);
    }

    #[test]
    fn corrupt_or_truncated_cached_record_is_a_miss() {
        for truncate in [false, true] {
            let c = cache(1 << 20, 3);
            let (base, next, bytes) = encode_batch(0, 3, 8);
            let mut bytes = bytes.to_vec();
            if truncate {
                bytes.pop();
            } else {
                *bytes.last_mut().unwrap() ^= 1;
            }
            c.push_batch(base, next, bytes.into());
            assert!(c.read_from(0, 2048).is_none());
        }
    }

    #[test]
    fn incorrect_cached_offsets_are_a_miss() {
        for second_offset in [0, 2] {
            let c = cache(1 << 20, 2);
            let (_, _, first) = encode_batch(0, 1, 8);
            let (_, _, second) = encode_batch(second_offset, 1, 8);
            let bytes: Vec<_> = first.iter().chain(second.iter()).copied().collect();
            c.push_batch(0, 2, bytes.into());
            assert!(c.read_from(0, 2).is_none());
        }
    }
}
