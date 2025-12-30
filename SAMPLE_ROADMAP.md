
## Phase 0: Repo & crate boundaries

Create a workspace:

* `keratin` (storage substrate: segmented log)
* `stroma` (derived state materializer: group/inflight/acked)
* `nexus-storage` (your trait adapter + glue; can keep current trait here)
* optional: `keratin-tools` (inspect/dump/repair CLI)

Keep `keratin` and `stroma` **dependency-light** (no tokio required inside them, unless you want). They can expose:

* a **blocking core API**
* and an **async wrapper** behind a feature flag (tokio) if you want

That keeps them reusable.

---

# Phase 1: Keratin v0 (single partition, single writer)

### Goal

Append records + read by offset + scan forward, crash-safe, bounded memory, decent throughput.

### 1.1 Data model (minimal)

Start with one log (no topic/partition hierarchy yet), just:

* `segments/`
* `manifest.bin`

Once it works, wrap it with `(topic, partition)` directories.

### 1.2 Implement file formats with *repair*

Implement:

* `.log` header + record framing + CRC
* `.idx` header + entries (optional in first pass; but I’d include it early)
* `manifest.bin` atomic replace

**Recovery rules:**

* scan `.log`, truncate to last good record
* scan `.idx`, truncate to last complete entry
* compute `next_offset` from last record

Make this rock-solid first. Correctness before speed.

### 1.3 Writer loop (micro-batching)

Implement an internal writer thread:

* `KeratinHandle` exposes async-ish API via channels:

  * `append_batch(payloads) -> (base_offset, count)`
* writer thread:

  * batches by size/count/linger
  * assigns offsets
  * writes `.log` (and `.idx`)
  * optionally fsync (config)
  * replies to all waiters

**Backpressure:** bounded channel. If full, caller awaits.

### 1.4 Read path

Implement:

* `fetch(offset) -> Vec<u8>`:

  * find segment by base offset
  * use `.idx` to seek near it
  * scan until record.offset == offset
* `scan(from, max_msgs|max_bytes)`:

  * start at from offset, stream forward

No mmap. Use reusable read buffers.

### 1.5 Segment rolling + retention (simple)

* roll on `segment_max_bytes`
* retention: delete whole segments below a cutoff offset
* update manifest atomically

### 1.6 “Correctness suite”

Before performance tuning, add:

* fuzz-ish crash simulation: append records, kill mid-write (simulate by truncating file randomly), reopen, ensure no partial records exposed.
* property tests:

  * offsets contiguous
  * fetch returns what was appended
  * scan order correct
  * after recovery, next_offset correct

This is where log stores usually fail.

---

## Phase 2: Keratin v0 performance passes (still simple)

Once correctness is nailed:

* switch encoding to write into a single contiguous `Vec<u8>` for the whole batch (or use vectored writes)
* reduce syscalls:

  * one write per batch
  * fsync only per batch based on config
* tune index stride (e.g. every 64KB appended)
* reuse buffers (`BytesMut` or your own pool)

At this point you’ll already beat RocksDB on your workload.

---

# Phase 3: Stroma v0 (group state + deadlines)

### Goal

Have a derived state engine that supports your broker semantics without touching Keratin internals.

### 3.1 In-memory model first

Implement per `(topic,partition,group)`:

* `acked_until: u64`
* `inflight: HashMap<u64, Deadline>` (or BTreeMap if you prefer)
* `heap: BinaryHeap` min-heap (store `(Reverse(deadline), offset, epoch)`)

Expose operations:

* `mark_inflight(offset, deadline, epoch)`
* `ack(offset)` → advance `acked_until` if possible (more below)
* `clear_inflight(offset)`
* `is_acked(offset)`
* `is_inflight(offset)`
* `next_expiry_hint()`
* `pop_expired(now) -> Vec<offset>`

### 3.2 The *important* correctness detail: advancing acked_until

Rabbit-like semantics aren’t necessarily “contiguous ack only”. You need:

* you can ACK offsets out of order
* but you still want a fast “lowest unacked”

So model:

* `acked_until` frontier
* `acked_set` for out-of-order acks above frontier (bitset/HashSet)
* when ack arrives:

  * if `offset == acked_until`: advance frontier while `acked_set` contains next
  * else if `offset > acked_until`: insert into `acked_set`

This makes “lowest unacked” essentially `acked_until` (plus inflight check).

### 3.3 Persistence: snapshot + delta

Implement `.delta` event log + `.snap` snapshot per group.

* On restart: load snapshot, replay delta, rebuild heap.
* On corruption: truncate delta like Keratin does.
* Compact periodically.

Keep encoding fixed-width and CRC’d.

---

# Phase 4: Connect them (adapter crate)

Create `nexus-storage` that implements your existing `Storage` trait using:

* Keratin for messages
* Stroma for ack/inflight/group state

Mapping:

* `append_batch` → Keratin append_batch
* `fetch_by_offset` → Keratin fetch
* `fetch_available`:

  * Keratin scan from `from_offset` (take more than needed)
  * for each candidate offset:

    * if stroma says acked/inflight skip
    * else return and broker calls `mark_inflight`
* `mark_inflight` → stroma + persist delta
* `ack` → stroma + persist delta
* `list_expired(now)` → stroma pop expired, then Keratin fetch those offsets

Important: `list_expired` should not scan the whole world anymore.

---

## A few pushbacks (to keep you from accidental pain)

1. **“Rebuild from scratch always”**
   Still keep it possible, but don’t rely on it. Snap+delta is the difference between “cool project” and “operationally usable”.

2. **Per-partition writer thread**
   Works great initially, but long-term you may want *sharded writers* (N partitions per writer) to avoid 10k threads. Start simple.

3. **Don’t bake topics/groups into Keratin**
   Keratin should be “log per namespace.” Let the wrapper decide directory layout.

---

TODO: Page-aligned segment preallocation (Avoid growth stalls)

TODO: Background compactor threads

TODO: Direct I/O option?

TODO: proper error handling (a way to propagate and retry? start by logging at least. return error on failed writes?)

TODO: Single writer safety lock per log (+ regression test)

TODO: Asyncify scan

TODO: Option to get stream instead of vec?

TODO: Batch inputs coming in smaller sets better?

TODO: Memory benchmarks. Launch X Keratin instances, write a bunch of messages, measure peak memory