
# Keratin v0: file formats

Everything is **big-endian** on disk (network order). Fixed-width structs are stable.

## 1) `manifest.bin`

Purpose: authoritative, tiny, atomically replaceable pointer to the current state.

**Update rule:** write `tmp/manifest.new` → flush+fsync → atomic rename/replace to `manifest.bin`.

### Header

```
MANIFEST := {
  magic        [u8;8]  = b"KERATIN\0"
  version      u16     = 1
  flags        u16     // reserved
  header_len   u32     // bytes including this header (for forward compat)
  crc32c       u32     // of bytes after crc field through end of file
  // followed by payload
}
```

### Payload (v1)

```
payload := {
  created_ts_ms        u64
  segment_max_bytes    u64
  index_stride_bytes   u32        // write an idx entry at least every N bytes of log append
  max_open_segments    u16        // resident handles cap (bounded memory)
  reserved0            u16
  active_base_offset   u64
  next_offset          u64        // next to assign (monotonic)
  // sealed segments list (optional v0):
  segment_count        u32
  segments[segment_count] := { base_offset u64, last_offset u64, log_bytes u64, idx_bytes u64 }
}
```

**Note:** you can skip embedding the segment list initially and discover segments from filenames; but persisting it makes recovery and validation faster and more deterministic.

---

## 2) Segment log: `<base>.log`

### File header (fixed)

```
LOG_FILE_HEADER := {
  magic      [u8;8] = b"KLOG\0\0\0\0"
  version    u16    = 1
  flags      u16
  header_len u32    // allows extending header later
  base_offset u64
  created_ts_ms u64
  reserved   [u8;32]
  header_crc32c u32 // crc of all header bytes except this field
}
```

### Record framing

**Key property:** You can detect incomplete writes and truncate safely.

```
RECORD := {
  magic        u16 = 0x4B52        // "KR"
  version      u16 = 1
  flags        u16
  reserved0    u16
  header_len   u32
  payload_len  u32
  timestamp_ms u64
  offset       u64
  headers      [u8;header_len]
  payload      [u8;payload_len]
  crc32c       u32                 // crc over (version..payload), excluding magic and excluding crc field
}
```

**Write rule:** write record bytes up to payload, then crc last.

**Recovery rule:** scan records; stop at first of:

* EOF mid-record
* bad magic/version
* crc mismatch
  Then truncate file to last-good record boundary.

---

## 3) Sparse index: `<base>.idx`

Append-only mapping: offset → file position.

### Header

```
IDX_FILE_HEADER := {
  magic      [u8;8] = b"KIDX\0\0\0\0"
  version    u16 = 1
  flags      u16
  header_len u32
  base_offset u64
  created_ts_ms u64
  entry_len  u16 = 16            // v1 entry size
  reserved0  u16
  reserved   [u8;32]
  header_crc32c u32
}
```

### Entry v1 (16 bytes)

```
IDX_ENTRY := {
  rel_offset u32     // offset - base_offset
  reserved0  u32     // could become "record_len" or "flags" later
  file_pos   u64     // byte position in .log where RECORD.magic begins
}
```

**Index write policy:** after appending records, if `log_bytes_since_last_index >= index_stride_bytes`, append an index entry for the *current* record.

**Index recovery:** if `.idx` ends mid-entry, truncate to last complete entry. Optionally validate monotonic rel_offset.

---

## 4) Optional time index: `<base>.tidx` (later)

Header similar to idx (magic `KTIX...`), entries:

```
TIDX_ENTRY := {
  timestamp_ms u64
  rel_offset   u32
  reserved0    u32
}
```

This enables “seek by time” and time-based TTL/retention without scanning.

---

# Collagen (or Stroma) v0: state persistence

You’re right that “log shouldn’t grow too far”, but operationally you still want bounded restart time. The best compromise that keeps your philosophy:

* **always rebuildable from Keratin**
* **normally restart from snapshot+delta**

Per `(topic,partition,group)`:

```
groups/
  <tp>_<group>.snap
  <tp>_<group>.delta
```

## 1) Snapshot file `.snap`

### Header

```
SNAP_HEADER := {
  magic      [u8;8] = b"KSNAP\0\0\0"
  version    u16 = 1
  flags      u16
  header_len u32
  created_ts_ms u64
  tp_hash    u64      // optional sanity check
  group_hash u64
  reserved   [u8;32]
  header_crc32c u32
}
```

### Payload v1

```
acked_until u64
inflight_count u32
reserved0 u32
inflight[inflight_count] := { offset u64, deadline_ts_ms u64, epoch u32, reserved u32 }
min_deadline_hint u64  // optional quick hint
payload_crc32c u32
```

Snapshot is written as:

* write temp file
* fsync
* atomic rename over old snapshot

## 2) Delta log `.delta` (append-only)

This is the “state journal.” Not a WAL chain; it’s just replayable materialization events.

### Header

```
DELTA_HEADER := {
  magic [u8;8] = b"KDELTA\0\0"
  version u16 = 1
  flags u16
  header_len u32
  created_ts_ms u64
  reserved [u8;32]
  header_crc32c u32
}
```

### Event framing

```
EVENT := {
  magic u16 = 0x4B45          // "KE"
  version u16 = 1
  event_type u16
  flags u16
  len u32                      // bytes of event payload
  payload [u8;len]
  crc32c u32                    // crc over (version..payload)
}
```

Event types (payloads are fixed-width where possible):

* `1 MarkInflight` : `{ offset u64, deadline_ts_ms u64, epoch u32 }`
* `2 Ack`          : `{ offset u64 }`
* `3 ClearInflight`: `{ offset u64 }`
* `4 BumpAckedUntil` (optional optimization): `{ acked_until u64 }`
* `5 Reset` (rare): `{}`

**Delta recovery:** same truncation rule as segment logs—scan until invalid/incomplete, truncate.

**Compaction policy:** when delta exceeds N bytes or inflight count big:

* write new snapshot from in-memory state
* rotate delta (replace with empty header)

---

# Keratin recovery algorithm (deterministic)

For each `(topic,partition)`:

1. Read `manifest.bin`. If missing/corrupt: rebuild manifest by scanning `segments/`.
2. Enumerate segment bases from filenames; sort.
3. For each segment:

   * validate `.log` header and base_offset matches filename
   * scan `.log` records to last good boundary; truncate if needed
   * scan `.idx` entries; truncate to last complete; optionally validate entries <= last record offset
4. Determine `next_offset`:

   * if manifest has it and it’s consistent, trust it
   * else compute from last segment’s last record offset + 1
5. Open active segment:

   * if none exists, create base = next_offset rounded (or equal next_offset)
   * ensure `.idx` exists
6. Update manifest atomically if you had to repair anything.

**Trust model:** records are authoritative; indexes are derived acceleration structures.

---

# Writer state machine (micro-batching + durability)

One writer loop per partition (or per shard) receives requests:

Request:

* `AppendBatch { payloads: Vec<Bytes>, headers: Vec<Bytes>, ack_mode, respond_to }`

Loop:

1. dequeue first request (blocking wait)
2. start batch window
3. keep draining until any stop condition:

   * total_bytes > max_batch_bytes
   * total_msgs > max_batch_msgs
   * linger deadline reached
4. assign offsets sequentially from `next_offset`
5. encode records into a contiguous buffer (or vectored IO slices)
6. write to active `.log`
7. maybe append `.idx` entries
8. flush:

   * `Ack::AfterWrite` → respond now
   * `Ack::AfterFsync` → flush file buffers + fsync `.log` (and maybe `.idx`) then respond
9. update in-memory `next_offset`, and periodically persist manifest (or on segment roll)
10. roll segment if size limit hit

**Backpressure:** channel bounded; if full, append requests await.

---

# Trait API: keep it, but add two “log-native” primitives

You can keep your current trait for now, but I’d add these ASAP:

1. **Append range return**

* `append_batch(...) -> (base_offset, count)`
  You can always derive the Vec lazily in callers if needed.

2. **Streaming scan**

* `scan_from(topic, partition, from_offset, max_bytes|max_msgs) -> Stream<Item=RecordRef>`
  This avoids allocating big Vecs and matches log physics.

You can keep your existing `fetch_available` convenience method at the broker layer on top of scan + in-memory group state.

---

# Raft/replication readiness

Your “durability levels” map cleanly to:

* after local write
* after local fsync
* after quorum replicated (Raft commit index)

Keratin being append-only makes Raft integration much easier: replication is literally “ship log entries,” apply in order.

┌──────────────────────────┐
│   Broker / Stream API    │   <-- control records, EOS, headers
├──────────────────────────┤
│     Stroma (future)      │   <-- cursors, groups, inflight
├──────────────────────────┤
│         Keratin          │   <-- raw durable append & scan
└──────────────────────────┘
