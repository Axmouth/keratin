//! In-memory tail cache for hot-path (tail-following) reads.
//!
//! Under mixed publish+deliver on one partition, delivery `scan_from`s the tail
//! segment that the writer is concurrently appending and fsyncing. On a real
//! drive the fsync/writeback contends with the read at the page-cache/inode
//! level (measured ~48% delivery loss on nvme, ~8% on tmpfs where fsync is a
//! no-op). Fibril delivery is tail-following, so the records it reads are still
//! in memory when written.
//!
//! This cache keeps a bounded, offset-indexed ring of the most recent flush
//! batches (encoded record bytes, the exact on-disk wire format). Tail-following
//! reads decode from memory and never touch the file under writeback; older
//! offsets (lagging consumers) miss and fall back to the file scan.
//!
//! It is a pure cache: a miss is always correct, and it only ever serves offsets
//! at or below the durable watermark (with `durable == 0` treated as nothing
//! durable, since it doubles as the empty-log sentinel), so durability semantics
//! are unchanged.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::reader::{OwnedRecord, to_owned};
use crate::record::decode_record_prefix;

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
    /// Inclusive last-fsynced offset (cloned from `LogState.durable`). Reads never
    /// serve an offset above this, so nothing non-durable is ever delivered.
    durable: Arc<AtomicU64>,
    /// Total cached bytes cap. `0` disables the cache (all reads go to the file).
    byte_budget: usize,
}

impl TailCache {
    pub(crate) fn new(durable: Arc<AtomicU64>, byte_budget: usize) -> Self {
        Self {
            inner: RwLock::new(Inner {
                batches: VecDeque::new(),
                bytes: 0,
            }),
            durable,
            byte_budget,
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.byte_budget > 0
    }

    /// Append a just-flushed batch. Called once per flush (~fsync rate). Evicts
    /// from the front while over budget, always keeping at least one batch.
    pub(crate) fn push_batch(&self, base_offset: u64, next_offset: u64, bytes: Arc<[u8]>) {
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
    }

    /// Drop everything. Must be called on any backward move of the log
    /// (truncate, reset, epoch change, snapshot install, rewind).
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.write();
        inner.batches.clear();
        inner.bytes = 0;
    }

    /// Serve records `[from, from + max)` from the cache, gated on the durable
    /// watermark.
    ///
    /// - `None`: `from` is below the cached window (or the cache is empty /
    ///   disabled). The caller must read from the file.
    /// - `Some(recs)`: the cache covers `from`. `recs` holds the contiguous
    ///   durable records at/after `from` (possibly empty if nothing at/after
    ///   `from` is durable yet), capped at `max`.
    pub(crate) fn read_from(&self, from: u64, max: usize) -> Option<Vec<OwnedRecord>> {
        if self.byte_budget == 0 || max == 0 {
            return None;
        }
        // `durable` is the inclusive last fsynced offset. `0` is the shared
        // empty-log / nothing-durable sentinel, indistinguishable from "offset 0
        // is durable", so we treat it as nothing durable: the exclusive upper
        // bound is `0`, and that single first record falls to the file until
        // offset 1 becomes durable. This keeps the "never serve a non-durable
        // offset" invariant true even though `push_batch` runs pre-fsync.
        let durable_excl = match self.durable.load(Ordering::Acquire) {
            0 => 0,
            d => d.saturating_add(1),
        };
        let upper = from.saturating_add(max as u64).min(durable_excl);
        // Hold the lock only long enough to clone the covering batches' byte
        // handles (cheap Arc refcounts). Decoding runs after the guard drops so a
        // long read never blocks the writer's `push_batch`.
        let bufs: Vec<Arc<[u8]>> = {
            let inner = self.inner.read();
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
            inner
                .batches
                .range(start..)
                .take_while(|b| b.base_offset < upper)
                .map(|b| b.bytes.clone())
                .collect()
        };
        // Records are contiguous and ascending, so the loop returns as soon as it
        // has the `upper - from` records requested.
        let mut out = Vec::with_capacity((upper - from) as usize);
        for buf in &bufs {
            let buf: &[u8] = buf;
            let mut pos = 0usize;
            while pos < buf.len() {
                match decode_record_prefix(&buf[pos..]) {
                    Ok((rec, used)) => {
                        if rec.offset >= from {
                            out.push(to_owned(rec));
                            if from + out.len() as u64 >= upper {
                                return Some(out);
                            }
                        }
                        pos += used;
                    }
                    // Cached batches are complete and valid; a decode error means
                    // corruption in memory. Stop and let the caller re-request
                    // (the file path re-validates).
                    Err(_) => break,
                }
            }
        }
        Some(out)
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

    fn cache(budget: usize, durable: u64) -> TailCache {
        TailCache::new(Arc::new(AtomicU64::new(durable)), budget)
    }

    #[test]
    fn disabled_cache_always_misses() {
        let c = cache(0, 1000);
        let (b, n, bytes) = encode_batch(0, 10, 8);
        c.push_batch(b, n, bytes); // no-op when disabled
        assert!(c.read_from(0, 10).is_none());
    }

    #[test]
    fn hit_serves_durable_tail() {
        let c = cache(1 << 20, 9); // durable through offset 9 inclusive
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
        let c = cache(1 << 20, 4); // only 0..=4 durable
        let (b, n, bytes) = encode_batch(0, 20, 8);
        c.push_batch(b, n, bytes);
        let recs = c.read_from(0, 20).expect("in window");
        assert_eq!(recs.len(), 5); // offsets 0..=4
        assert_eq!(recs.last().unwrap().offset, 4);
    }

    #[test]
    fn durable_zero_never_serves_offset_zero() {
        // `durable == 0` is the empty-log / nothing-durable sentinel. Offset 0 is
        // pushed to the cache pre-fsync, but must not be served until it is
        // actually durable (offset 1+ advancing durable past the sentinel).
        let c = cache(1 << 20, 0);
        let (b, n, bytes) = encode_batch(0, 5, 8);
        c.push_batch(b, n, bytes);
        let recs = c.read_from(0, 5).expect("in window");
        assert!(recs.is_empty());
    }

    #[test]
    fn nothing_durable_yet_returns_empty_not_miss() {
        let c = cache(1 << 20, 0); // durable = 0 (only offset 0 durable)
        let (b, n, bytes) = encode_batch(5, 10, 8); // window starts at 5
        c.push_batch(b, n, bytes);
        // from=5 is in the window, but durable_excl=1, so nothing at/after 5.
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
}
