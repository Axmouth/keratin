# Tail-read cache for keratin-log

## Why

Measured: under mixed publish+deliver on the **same partition**, delivery throughput
drops ~48% on an nvme drive but only ~8% on tmpfs. The cause is **fsync/writeback
contention on the active segment file**: the writer appends to the tail segment and
fsyncs it (dirty-page writeback to the drive) while delivery `scan_from`s that same
tail (the just-published, now-durable records). Same inode -> writeback and reads
contend at the page-cache/I/O level. Different topic (different file) costs only ~9%;
tmpfs (no writeback) removes it entirely.

Fibril delivery is **tail-following**: consumers read offsets just behind the writer.
Those records are already in memory when written. So serve them from memory instead of
re-reading the segment file under active writeback.

This is the existing `reader.rs:47` TODO ("Memory cache X entries for hot path of
reading"). It touches only keratin-log. It is NOT a runtime split and NOT lock-free
queue state (both ruled out by measurement).

## Approach

Keep a bounded, offset-indexed **in-memory ring of recent flush batches** (encoded
record bytes, same wire format as the segment file). `scan_from` checks the ring first;
for offsets in the cached window it decodes from memory and never touches the file; for
older offsets (lagging consumers) it falls through to the existing file scan. The ring
only serves offsets at/below the `durable` watermark, so semantics are unchanged.

Cache **encoded bytes per flush batch** (not decoded records): near-zero extra write
cost (move the write buffer into the ring instead of copying), and reads reuse the
existing record decoder. It is a pure cache: a miss is always correct.

## Data structure (new)

New module `keratin-log/src/tail_cache.rs`:

```
struct CachedBatch { base_offset: u64, next_offset: u64, bytes: Arc<[u8]> }

pub struct TailCache {
    inner: parking_lot::RwLock<VecDeque<CachedBatch>>, // ordered, contiguous
    durable: Arc<AtomicU64>,   // cloned from LogState.durable; gate reads
    byte_budget: usize,        // 0 = disabled
    cur_bytes: AtomicUsize,    // for O(1) eviction decisions
}
```

- `push_batch(base, next, bytes)`: write-lock, push_back, evict from front while
  `cur_bytes > byte_budget`. Called once per flush (~fsync rate, ~hundreds/s).
- `read_into(from, max, out) -> ReadOutcome`: read-lock. If `from < oldest.base_offset`
  or ring empty -> `Miss` (caller does file scan). Else decode contiguous records from
  the batch(es) covering `[from, min(from+max, durable+1))` into `out`; return how far
  it got. `durable` gate: never return an offset `> durable.load()`.
- `clear()`: write-lock, drop all batches. For truncation/epoch/snapshot.

Reads take a short read-lock + decode (microseconds); the old path took a
file read racing writeback (milliseconds). Concurrent readers proceed together; the
writer's per-flush write-lock is brief. (A later refinement could go lock-free via an
`arc_swap` snapshot; RwLock is the correct-and-simple first cut and matches the
codebase.)

## Parts needing intervention

### 1. `LogState` / `Log` (log.rs)
- `Log` already holds `log_state: Arc<LogState>` (has `durable: Arc<AtomicU64>`) and
  `segment_mapping`. Add `tail_cache: Arc<TailCache>`.
- Add writer bookkeeping: `write_buf_base_offset: u64` set when `write_buf` transitions
  empty->non-empty (the offset of the batch's first record), so the flush knows the
  batch's `[base, next)` range.
- Construct `TailCache` in all 3 `Log` constructors (log.rs ~265, ~311, ~387), sharing
  `log_state.durable.clone()` and the configured `byte_budget`. Reader clones the same
  `Arc<TailCache>`.

### 2. Append site (log.rs ~187-201)
- `base_offset` is already captured. When `write_buf` was empty before this append set
  `write_buf_base_offset = base_offset`. No per-record work (we cache bytes, not records).

### 3. Flush point (log.rs `flush_buffers_inner`, ~824-840)
- Today: `append_bytes(&write_buf)` (file) then `write_buf.clear()`.
- Change: after `append_bytes`, hand the batch to the cache with **no extra copy** by
  moving the buffer: `let batch = std::mem::take(&mut self.write_buf);` then
  `self.tail_cache.push_batch(self.write_buf_base_offset, state.next_offset, batch.into());`
  and re-provision `self.write_buf` (fresh `Vec::with_capacity(cap)`, or a recycled
  buffer pool - v2). `append_bytes` already consumed the bytes for the file write, so
  the move is O(1). (If a buffer pool is not added, this is one 16MB alloc per flush -
  acceptable; note as a follow-up to recycle evicted batch buffers back into the pool.)
- Durability: the batch is pushed pre-fsync, but `TailCache::read_into` gates on
  `durable`, so nothing non-durable is ever served. The `durable` store already happens
  post-fsync in the commit path - no change needed there.

### 4. `LogReader` (reader.rs)
- Add `tail_cache: Arc<TailCache>` field + `LogReader::new(..., tail_cache)` param.
- Update both call sites: `log.rs:1293` and `keratin.rs:183` (`Keratin::reader()`) to
  pass `self.tail_cache.clone()`.

### 5. `scan_from` / `fetch` (reader.rs:64, and `fetch`)
- `scan_from(from, max)`: first `tail_cache.read_into(from, max, &mut out)`. If it
  returns `Hit` covering the whole request, done (no file I/O). If `Miss` or a partial
  (cache started above `from`, or ran past the cached tail), scan the file for the
  uncovered prefix/suffix via the existing `scan_forward_exact`, and stitch. Simplest
  correct v1: if `from` is in the cache window, serve the cached contiguous run and let
  the caller's loop re-request the remainder; else file scan. The stroma `poll_ready`
  loop already groups contiguous runs and re-polls, so partial returns are fine.
- `fetch(offset)`: check cache (gated on durable) before file.
- Decoding from the cached slice reuses `decode_record_prefix` (the same decoder
  `scan_forward_exact` uses), so record parsing is identical to the file path.

### 6. Invalidation (correctness-critical)
- `tail_cache.clear()` on: truncation (`log.rs` truncate paths ~1180-1266), `reset`,
  epoch change (`log.rs:1083`), snapshot install / `InstallSnapshotState`, and
  become-follower / log rewind. Grep `QueueCommand::Reset`, `truncate`, `epoch.store`,
  `head.store`. Missing an invalidation point = serving stale/rewound offsets, so this
  is the highest-risk area - enumerate every place `head`/`tail`/`epoch` move backward
  or segments are rewritten, and clear there.

### 7. Config (config.rs `KeratinConfig`)
- Add `tail_cache_bytes: usize` (default e.g. 64 MiB; `0` disables). Node-local memory
  knob per settings-discipline. Thread from `KeratinConfig` into `Log` construction
  (via `StromaKeratinConfig::from_message_log` -> the log open path). Size guidance:
  must exceed the fsync-lag window (records written but not yet drained by delivery);
  64 MiB at 1 KB payloads ~= 64k messages of tail, comfortably above a few ms of lag.

### 8. Stroma (stroma.rs)
- No change. `scan_messages_from` -> `reader.scan_from` already; the cache is internal
  to the reader.

### 9. Tests
- Unit (tail_cache.rs): push/evict by byte budget; `read_into` hit / miss (below window)
  / partial (past durable) / durable gate; `clear`.
- Integration (keratin-log tests): write N, `scan_from` the tail == same records as a
  cache-disabled (`tail_cache_bytes=0`) file scan; read an old offset (below window)
  falls to file and matches; after truncate/epoch-change the cache is cleared and reads
  stay correct.
- End-to-end (fibril): re-run the same-topic mixed vs drain-alone bench on nvme; expect
  the drown to fall from ~48% toward the tmpfs ~8%. Also confirm drain-alone and
  cross-topic are unchanged, and correctness/order intact.

## Correctness gotchas

- **Serve only `offset <= durable`** - the gate is the whole reason reads stay correct
  while the batch is pushed pre-fsync.
- **Cache is best-effort** - any miss falls to the file; never a source of truth.
- **Invalidate on every backward move** of head/tail/epoch or segment rewrite (item 6).
- **Contiguity** - batches are contiguous and ordered; `read_into` must not skip a gap
  (there shouldn't be one on the durable tail, but assert base==prev.next on push).
- **Bytes format** - cache holds the exact encoded record bytes, so the decoder is
  shared with the file path; no divergent parsing.
- **Disabled path** - `tail_cache_bytes=0` must behave exactly like today (all reads go
  to file); keep that as the correctness reference in tests.

## Rollout / risk

- Behind the `tail_cache_bytes` knob; `0` = current behavior, so it ships dark and is
  A/B-able against the file path.
- Extra memory: one budget-bounded ring per log (per partition). At 64 MiB x many
  partitions this adds up - the budget is per-log; consider a global cap later.
- Biggest risk is invalidation completeness (item 6); mitigate with the truncate/epoch
  integration tests and the disabled-path reference.

## Expected payoff

Removes the fsync/writeback read contention for tail-following delivery: mixed
publish+deliver throughput recovers toward the drain-alone ceiling (~48% -> ~8% drown
on nvme measured as the gap to close). No change to the runtime, the actor, or the
queue state.
