use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use keratin_log::{
    AppendCompletion, CompletionPair, IoError, KDurability, Keratin, KeratinAppendCompletion,
    KeratinConfig, Message,
};
use tokio::sync::RwLock;

use crate::{
    Result, StromaError,
    event::StromaEvent,
    sequencer::{PublishIntent, run_msg_sequencer},
    state::{Offset, QueueState, UnixMillis},
};

fn io_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Io(e.to_string())
}

fn decode_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Decode(e.to_string())
}

pub(crate) fn event_msg(ev: &StromaEvent) -> Result<Message> {
    let payload = ev.encode().map_err(io_err)?;
    Ok(Message {
        flags: 0,
        headers: vec![],
        payload,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    tp: String,
    part: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyRef<'a> {
    pub tp: &'a str,
    pub part: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotConfig {
    /// Take snapshot every N durable events per (tp,part) (best-effort).
    pub every_events: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            every_events: 50_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GlobalDLQ {
    pub tp: String,
    pub part: u32,
}

impl GlobalDLQ {
    pub async fn new(tp: &str, part: u32) -> Result<Self> {
        Ok(Self {
            tp: tp.to_string(),
            part,
        })
    }

    // TODO: Helper to create DLQ message, with metadata about original message. (stabilize headers format first)

    pub fn to_custom_dlq(&self) -> crate::state::CustomDLQ {
        crate::state::CustomDLQ {
            tp: self.tp.clone(),
            part: self.part,
        }
    }
}

#[derive(Debug)]
pub struct Stroma {
    pub(crate) root: PathBuf,
    pub(crate) keratin_cfg: KeratinConfig,
    pub(crate) snap_cfg: SnapshotConfig,

    // One Keratin per (tp,part) for message payloads.
    pub(crate) msg_logs_by_tp_part: Arc<DashMap<(Box<str>, u32), Arc<Keratin>>>,

    // One Keratin per (tp,part), events log
    pub(crate) logs_by_tp_part: Arc<DashMap<(Box<str>, u32), Arc<Keratin>>>,

    pub(crate) msg_sequencers:
        Arc<DashMap<(Box<str>, u32), tokio::sync::mpsc::Sender<PublishIntent>>>,

    // Materialized queue state
    pub(crate) queues: Arc<DashMap<(Box<str>, u32), QueueState>>,

    // Global DLQ topic
    pub(crate) global_dlq: Arc<RwLock<Option<GlobalDLQ>>>,

    // For each (tp,part): highest Keratin offset we have applied into memory
    pub(crate) applied_upto: DashMap<(Box<str>, u32), AtomicU64>,
}

impl Stroma {
    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg: KeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("events")).map_err(io_err)?;
        fs::create_dir_all(root.join("messages")).map_err(io_err)?;
        fs::create_dir_all(root.join("snapshots")).map_err(io_err)?;
        fs::create_dir_all(root.join("tmp")).map_err(io_err)?;

        let st = Self {
            root,
            keratin_cfg,
            snap_cfg,
            msg_logs_by_tp_part: Arc::new(DashMap::new()),
            logs_by_tp_part: Arc::new(DashMap::new()),
            msg_sequencers: Arc::new(DashMap::new()),
            queues: Arc::new(DashMap::new()),
            applied_upto: DashMap::new(),
            global_dlq: Arc::new(RwLock::new(None)),
        };

        // Recover from existing snapshot files + replay events.
        st.recover_all().await?;

        Ok(st)
    }

    // ---------------- Paths / naming ----------------

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    fn messages_root(&self) -> PathBuf {
        self.root.join("messages")
    }

    fn events_root(&self) -> PathBuf {
        self.root.join("events")
    }

    fn snapshots_root(&self) -> PathBuf {
        self.root.join("snapshots")
    }

    /// Encode a string into a path-safe component (stable & reversible-ish).
    /// Only [A-Za-z0-9._-] are left as-is; everything else becomes %HH.
    fn enc_component(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.as_bytes() {
            match *b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                    out.push(*b as char)
                }
                _ => out.push_str(&format!("%{:02X}", b)),
            }
        }
        out
    }

    fn msg_tp_part_dir(&self, tp: &str, part: u32) -> PathBuf {
        self.messages_root()
            .join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn tp_part_dir(&self, tp: &str, part: u32) -> PathBuf {
        self.events_root()
            .join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn snap_dir(&self, tp: &str, part: u32) -> PathBuf {
        self.snapshots_root()
            .join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn snap_file(&self, tp: &str, part: u32) -> PathBuf {
        self.snap_dir(tp, part)
            .join(format!("{}.snap", Self::enc_component(tp)))
    }

    fn snap_tmp_file(&self, tp: &str, part: u32) -> PathBuf {
        self.root
            .join("tmp")
            .join(format!("{}_{}.snap.new", Self::enc_component(tp), part,))
    }

    // ---------------- Core accessors ----------------

    async fn log(&self, tp: &str, part: u32) -> Result<Arc<Keratin>> {
        if let Some(v) = self.logs_by_tp_part.get(&(tp.into(), part)) {
            return Ok(v.value().clone());
        }

        let dir = self.tp_part_dir(tp, part);
        fs::create_dir_all(&dir).map_err(io_err)?;

        let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
        let k = Arc::new(k);

        self.logs_by_tp_part.insert((tp.into(), part), k.clone());
        self.applied_upto
            .entry((tp.into(), part))
            .or_insert_with(|| AtomicU64::new(0));

        Ok(k)
    }

    async fn msg_log(&self, tp: &str, part: u32) -> Result<Arc<Keratin>> {
        if let Some(v) = self.msg_logs_by_tp_part.get(&(tp.into(), part)) {
            return Ok(v.value().clone());
        }

        let dir = self.msg_tp_part_dir(tp, part);
        fs::create_dir_all(&dir).map_err(io_err)?;

        let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
        let k = Arc::new(k);

        self.msg_logs_by_tp_part
            .insert((tp.into(), part), k.clone());
        Ok(k)
    }

    async fn msg_sequencer(
        &self,
        tp: &str,
        part: u32,
    ) -> Result<tokio::sync::mpsc::Sender<PublishIntent>> {
        let _ = self.msg_log(tp, part).await?;
        let _ = self.log(tp, part).await?;
        let _ = self.queue_entry(tp, part);
        if let Some(v) = self.msg_sequencers.get(&(tp.into(), part)) {
            return Ok(v.value().clone());
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let msg_log_map = self.msg_logs_by_tp_part.clone();
        let event_log_map = self.logs_by_tp_part.clone();
        let queue_state_map = self.queues.clone();

        let tp_box: Box<str> = tp.into();
        tokio::spawn(async move {
            tracing::info!("queue task STARTED for {} {}", tp_box, part);
            let res = run_msg_sequencer(
                rx,
                &tp_box,
                part,
                msg_log_map,
                event_log_map,
                queue_state_map,
            )
            .await;
            tracing::error!("queue task EXITED for {} {}: {:?}", tp_box, part, res);
        });

        self.msg_sequencers.insert((tp.into(), part), tx.clone());
        Ok(tx)
    }

    fn queue_entry(
        &self,
        tp: &str,
        part: u32,
    ) -> dashmap::mapref::one::RefMut<'_, (Box<str>, u32), QueueState> {
        self.queues
            .entry((tp.into(), part))
            .or_insert_with(|| QueueState::new(tp.into(), part))
    }

    fn applied_upto_entry(
        &self,
        tp: &str,
        part: u32,
    ) -> dashmap::mapref::one::RefMut<'_, (Box<str>, u32), AtomicU64> {
        self.applied_upto
            .entry((tp.into(), part))
            .or_insert_with(|| AtomicU64::new(0))
    }

    // ---------------- Event apply rules ----------------

    async fn apply_event_inmem(&self, ev: &StromaEvent) -> Result<()> {
        match ev {
            StromaEvent::Enqueue {
                tp,
                part,
                off,
                retries,
            } => {
                let mut gs = self.queue_entry(tp, *part);
                gs.enqueue(*off, *retries);
            }
            StromaEvent::MarkInflight {
                tp,
                part,
                off,
                deadline,
            } => {
                let mut gs = self.queue_entry(tp, *part);
                gs.mark_inflight(*off, *deadline);
            }
            StromaEvent::Ack { tp, part, off } => {
                let mut gs = self.queue_entry(tp, *part);
                // ✅ Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                gs.ack(*off);
            }
            StromaEvent::Nack {
                tp,
                part,
                off,
                requeue,
            } => {
                let mut gs = self.queue_entry(tp, *part);
                // ✅ Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
                gs.nack(*off, *requeue);
            }
            StromaEvent::DeadLetter { tp, part, off } => {
                let mut gs = self.queue_entry(tp, *part);
                gs.dead_letter(*off);
            }
            StromaEvent::ClearInflight { tp, part, off } => {
                let mut gs = self.queue_entry(tp, *part);
                gs.clear_inflight(*off);
            }
            StromaEvent::ResetQueue { tp, part } => {
                self.queues.remove(&(tp.clone(), *part));
            }
            StromaEvent::Snapshot { .. } => {
                // If you keep Snapshot events inside the event log later, you'd load it here.
                // With file snapshots, we don't emit Snapshot events, so this is unused.
                return Err(StromaError::Decode(
                    "Snapshot event unsupported in v0".into(),
                ));
            }
        }
        Ok(())
    }

    async fn append_events_durable(
        &self,
        tp: &str,
        part: u32,
        evs: &[StromaEvent],
        durability: KDurability,
    ) -> Result<Offset> {
        if evs.is_empty() {
            return Ok(self.applied_upto_entry(tp, part).load(Ordering::Acquire));
        }

        let log = self.log(tp, part).await?;
        let mut msgs = Vec::with_capacity(evs.len());
        for ev in evs {
            msgs.push(event_msg(ev)?);
        }

        // Durable append first.
        let ar = log
            .append_batch(msgs, Some(durability))
            .await
            .map_err(io_err)?;

        // Apply in memory after durable accept.
        for ev in evs {
            self.apply_event_inmem(ev).await?;
        }

        // Update applied watermark:
        let new_upto = ar.base_offset + ar.count as u64;
        self.applied_upto_entry(tp, part)
            .store(new_upto, Ordering::Release);

        Ok(new_upto)
    }

    // ---------------- Public API used by Storage shim ----------------

    pub async fn mark_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        entries: &[(Offset, UnixMillis)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut evs = Vec::with_capacity(entries.len());
        for &(off, deadline) in entries {
            evs.push(StromaEvent::MarkInflight {
                tp: tp.into(),
                part,
                off,
                deadline,
            });
        }

        let upto = self
            .append_events_durable(tp, part, &evs, KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, upto).await?;

        Ok(())
    }

    pub async fn ack_batch(&self, tp: &str, part: u32, offs: &[Offset]) -> Result<()> {
        if offs.is_empty() {
            return Ok(());
        }

        let mut evs = Vec::with_capacity(offs.len());
        for &off in offs {
            evs.push(StromaEvent::Ack {
                tp: tp.into(),
                part,
                off,
            });
        }

        let upto = self
            .append_events_durable(tp, part, &evs, KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, upto).await?;

        Ok(())
    }

    pub async fn clear_inflight(&self, tp: &str, part: u32, off: Offset) -> Result<()> {
        let ev = StromaEvent::ClearInflight {
            tp: tp.into(),
            part,
            off,
        };
        let upto = self
            .append_events_durable(tp, part, std::slice::from_ref(&ev), KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, upto).await?;
        Ok(())
    }

    pub fn lowest_unacked_offset(&self, tp: &str, part: u32) -> Result<Offset> {
        Ok(self
            .queues
            .get(&(tp.into(), part))
            .map(|g| g.lowest_unacked_offset())
            .unwrap_or(0))
    }

    pub fn is_inflight_or_acked(&self, tp: &str, part: u32, off: Offset) -> Result<bool> {
        Ok(self
            .queues
            .get(&(tp.into(), part))
            .map(|g| g.is_inflight_or_acked(off))
            .unwrap_or(false))
    }

    pub fn is_enqueued(&self, tp: &str, part: u32, off: Offset) -> Result<bool> {
        Ok(self
            .queues
            .get(&(tp.into(), part))
            .map(|q| q.is_enqueued(off))
            .unwrap_or(false))
    }

    pub fn filter_not_enqueued<T>(&self, tp: &str, part: u32, items: &mut Vec<(Offset, T)>) {
        if let Some(q) = self.queues.get(&(tp.into(), part)) {
            q.filter_not_enqueued(items);
        } else {
            items.clear();
        }
    }

    pub fn next_expiry_hint(&self) -> Result<Option<UnixMillis>> {
        Ok(self
            .queues
            .iter_mut()
            .filter_map(|mut e| e.value_mut().next_expiry_hint())
            .min())
    }

    pub fn list_expired(&self, now: UnixMillis, max: usize) -> Result<Vec<(String, u32, Offset)>> {
        let mut out = Vec::new();

        for mut kv in self.queues.iter_mut() {
            if out.len() >= max {
                break;
            }
            let want = max - out.len();
            for off in kv.value_mut().pop_expired(now, want) {
                let (tp, part) = kv.key();
                out.push((tp.to_string(), *part, off));
                if out.len() >= max {
                    break;
                }
            }
        }

        Ok(out)
    }

    // ---------------- Snapshotting ----------------
    //
    // Snapshot files make restart fast:
    // - durable event log = Keratin partition log
    // - snapshot per (tp,part): { last_applied_event_offset, queue_state_blob }
    //
    // Recovery loads snapshots, then replays events AFTER the minimum snapshot offset,
    // skipping events already covered by each queue's snapshot.

    async fn maybe_snapshot(&self, tp: &str, part: u32, applied_upto: Offset) -> Result<()> {
        let every = self.snap_cfg.every_events.max(1);
        if !applied_upto.is_multiple_of(every) {
            return Ok(());
        }
        self.write_snapshots_for_partition(tp, part, applied_upto)
            .await
    }

    async fn write_snapshots_for_partition(
        &self,
        tp: &str,
        part: u32,
        applied_upto: Offset,
    ) -> Result<()> {
        let dir = self.snap_dir(tp, part);
        fs::create_dir_all(&dir).map_err(io_err)?;

        // snapshot all queues for this partition (simple v0; later you can do incremental / max-bytes)
        for e in self.queues.iter() {
            let (key_tp, key_part) = e.key();
            if key_tp.as_ref() == tp && *key_part == part {
                let blob = e.value().encode_snapshot();
                self.write_queue_snapshot(tp, part, applied_upto, &blob)?;
            }
        }

        // Future: after snapshotting, you can truncate events log safely (once Keratin supports it).
        // self.maybe_truncate_partition(tp, part).await?;

        Ok(())
    }

    fn write_queue_snapshot(
        &self,
        tp: &str,
        part: u32,
        last_applied_event_offset: Offset,
        blob: &[u8],
    ) -> Result<()> {
        let tmp = self.snap_tmp_file(tp, part);
        let final_path = self.snap_file(tp, part);

        if let Some(parent) = tmp.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }

        // file format (big endian):
        // magic 8: b"SSNAP\0\0\0"
        // ver u16: 1
        // reserved u16
        // last_applied_event_offset u64
        // blob_len u32
        // blob bytes
        // crc32c u32 over (ver..blob)
        const MAGIC: &[u8; 8] = b"SSNAP\0\0\0";
        const VER: u16 = 1;

        let mut payload = Vec::with_capacity(2 + 2 + 8 + 4 + blob.len());
        payload.extend_from_slice(&VER.to_be_bytes());
        payload.extend_from_slice(&0u16.to_be_bytes());
        payload.extend_from_slice(&last_applied_event_offset.to_be_bytes());
        payload.extend_from_slice(&(blob.len() as u32).to_be_bytes());
        payload.extend_from_slice(blob);

        let crc = crc32c::crc32c(&payload);

        let mut out = Vec::with_capacity(8 + payload.len() + 4);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&payload);
        out.extend_from_slice(&crc.to_be_bytes());

        // write temp + fsync + rename
        {
            let mut f = io::BufWriter::new(
                fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&tmp)
                    .map_err(io_err)?,
            );
            use io::Write;
            f.write_all(&out).map_err(io_err)?;
            f.flush().map_err(io_err)?;
            let inner = f.into_inner().map_err(io_err)?;
            inner.sync_all().map_err(io_err)?;
        }

        fs::rename(&tmp, &final_path).map_err(io_err)?;
        Ok(())
    }

    fn read_queue_snapshot(&self, path: &Path) -> Result<Option<(Offset, Vec<u8>)>> {
        if !path.exists() {
            return Ok(None);
        }

        const MAGIC: &[u8; 8] = b"SSNAP\0\0\0";
        const VER: u16 = 1;

        let bytes = fs::read(path).map_err(io_err)?;
        if bytes.len() < 8 + 2 + 2 + 8 + 4 + 4 {
            return Err(StromaError::Decode("snapshot too small".into()));
        }
        if &bytes[0..8] != MAGIC {
            return Err(StromaError::Decode("bad snapshot magic".into()));
        }

        // crc check
        let want = u32::from_be_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        let payload = &bytes[8..bytes.len() - 4];
        let got = crc32c::crc32c(payload);
        if got != want {
            return Err(StromaError::Decode("snapshot crc mismatch".into()));
        }

        let ver = u16::from_be_bytes(payload[0..2].try_into().unwrap());
        if ver != VER {
            return Err(StromaError::Decode("snapshot version mismatch".into()));
        }

        let last_applied = u64::from_be_bytes(payload[4..12].try_into().unwrap());
        let blob_len = u32::from_be_bytes(payload[12..16].try_into().unwrap()) as usize;

        if 16 + blob_len > payload.len() {
            return Err(StromaError::Decode("snapshot blob truncated".into()));
        }
        let blob = payload[16..16 + blob_len].to_vec();
        Ok(Some((last_applied, blob)))
    }

    // ---------------- Recovery ----------------

    async fn recover_all(&self) -> Result<()> {
        // discover existing partitions by walking root/events
        let events_root = self.events_root();
        if !events_root.exists() {
            return Ok(());
        }

        let mut partitions: Vec<(String, u32)> = Vec::new();

        for tp_ent in fs::read_dir(&events_root).map_err(io_err)? {
            let tp_ent = tp_ent.map_err(io_err)?;
            if !tp_ent.file_type().map_err(io_err)?.is_dir() {
                continue;
            }
            let tp_dirname = tp_ent.file_name().to_string_lossy().to_string();
            let tp_dir = tp_ent.path();

            for part_ent in fs::read_dir(&tp_dir).map_err(io_err)? {
                let part_ent = part_ent.map_err(io_err)?;
                if !part_ent.file_type().map_err(io_err)?.is_dir() {
                    continue;
                }
                let part_str = part_ent.file_name().to_string_lossy().to_string();
                let part = part_str
                    .parse::<u32>()
                    .map_err(|_| StromaError::Decode(format!("bad partition dir: {part_str}")))?; // directory name is our own format
                partitions.push((tp_dirname.clone(), part));
            }
        }

        // NOTE: tp_dirname is encoded; we don't need the original for recovery of state
        // because events carry real tp strings. We'll just open each partition dir and replay.
        for (tp_enc, part) in partitions {
            let dir = events_root.join(tp_enc).join(format!("{:010}", part));
            let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
            let k = Arc::new(k);
            // We don't know real topic string here; replay will populate from event payload.
            // We'll store this Keratin under the first real tp seen during replay.
            self.recover_one_log(k, part).await?;
        }

        Ok(())
    }

    async fn recover_one_log(&self, k: Arc<Keratin>, part: u32) -> Result<()> {
        // 1) Load all snapshot files for this partition into queues map + remember per-queue snapshot offsets
        //    We cannot pre-know tp; snapshot directory is keyed by encoded tp, but the snapshot file
        //    itself does not embed tp. So: in v0, we load snapshots only when we know tp.
        //
        // Practical approach v0:
        // - Don’t "discover snapshots by scanning filesystem blindly".
        // - Instead, do replay from 0 once per partition (still OK early).
        //
        // For fast restarts now:
        // - store snapshots under snapshots/<enc(tp)>/<part>/<enc(tp)>.snap
        // - and during replay, when we see tp, we attempt to load its snapshot once.

        // We'll cache loaded snapshot offsets here:
        let snap_applied_for: DashMap<Box<str>, Offset> = DashMap::new();
        // tp -> last_applied_event_offset

        let reader = k.reader();
        let tail = k.next_offset();

        // replay from 0 (or from head once you have truncation)
        let mut cur = 0u64;

        while cur < tail {
            let batch = reader.scan_from(cur, 10_000).map_err(io_err)?;
            if batch.is_empty() {
                break;
            }

            for rec in batch {
                cur = rec.offset + 1;

                let ev = StromaEvent::decode(&rec.payload).map_err(decode_err)?;

                // Ensure the Keratin instance is registered under the real topic string.
                let tp = ev.tp();
                self.logs_by_tp_part
                    .entry((tp.into(), part))
                    .or_insert_with(|| k.clone());

                // Best-effort: load snapshot for (tp) once, and if snapshot's last_applied >= rec.offset, skip.
                let key = tp;

                if !snap_applied_for.contains_key(key) {
                    // try read snapshot file
                    let sp = self.snap_file(&tp, part);
                    if let Some((snap_upto, blob)) = self.read_queue_snapshot(&sp)? {
                        {
                            let mut gs = self.queue_entry(&tp, part);
                            gs.load_snapshot(&blob).map_err(io_err)?;
                        }
                        snap_applied_for.insert(key.into(), snap_upto);
                    } else {
                        snap_applied_for.insert(key.into(), 0);
                    }
                }

                let snap_upto = *snap_applied_for.get(key).unwrap().value();
                if rec.offset <= snap_upto {
                    continue; // covered by snapshot
                }

                self.apply_event_inmem(&ev).await?;
                self.applied_upto_entry(&tp, part)
                    .store(rec.offset + 1, Ordering::Release);
            }
        }

        Ok(())
    }

    // ---------------- Future: truncation hook ----------------
    //
    // Once Keratin supports truncate_before(before_offset),
    // we can compute a safe cutoff:
    //   cutoff = min(last_applied_event_offset in all snapshots for this partition)
    // and call log.truncate_before(cutoff).
    //
    // Until then, snapshots give fast startup even if the event log grows.
}

impl Stroma {
    /// Append a batch of message payloads and return assigned offsets (like Rocks).
    pub async fn append_messages_batch(
        &self,
        tp: &str,
        part: u32,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let log = self.msg_log(tp, part).await?;
        let mut msgs = Vec::with_capacity(payloads.len());
        for p in payloads {
            msgs.push(Message {
                flags: 0,
                headers: vec![],
                payload: p.clone(),
            });
        }

        let ar = log.append_batch(msgs, None).await.map_err(io_err)?;

        let mut out = Vec::with_capacity(ar.count as usize);
        let mut o = ar.base_offset;
        for _ in 0..ar.count {
            out.push(o);
            o += 1;
        }
        Ok(out)
    }

    pub async fn append_message(
        &self,
        tp: &str,
        part: u32,
        payload: &[u8],
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let (inner_completion, rx) = KeratinAppendCompletion::pair();
        let msg_log = self.msg_log(tp, part).await?;
        let _ = self.queue_entry(tp, part);
        msg_log
            .append_enqueue(
                Message {
                    flags: 0,
                    headers: vec![],
                    payload: payload.to_vec(),
                },
                None,
                inner_completion,
            )
            .map_err(io_err)?;
        let log = self.log(tp, part).await?;
        let tp: Box<str> = tp.into();
        let queues = self.queues.clone();
        tokio::spawn(async move {
            let res = rx.await;

            let append_result = match res {
                Ok(Ok(offset)) => offset,
                Ok(Err(err)) => {
                    println!("Derp err 3 {}", err);
                    completion.complete(Err(err));
                    return;
                }
                Err(err) => {
                    println!("Derp err 4 {}", err);
                    completion.complete(Err(IoError::new("Channel closed")));
                    return;
                }
            };
            let offset = append_result.base_offset;

            // TODO: emit Enqueue event too in some form
            let ev = StromaEvent::Enqueue {
                retries: 0,
                tp: tp.clone(),
                part,
                off: offset,
            };

            let msg = match event_msg(&ev) {
                Ok(msg) => msg,
                Err(err) => {
                    println!("Derp err 5 {}", err);
                    completion.complete(Err(IoError::new(err)));
                    return;
                }
            };
            match log.append_enqueue(msg, None, completion).map_err(io_err) {
                Ok(()) => {
                    if let Some(mut q) = queues.get_mut(&(tp, part)) {
                        q.enqueue(offset, 0);
                    }
                }
                Err(_err) => {}
            };
        });

        // let intent = PublishIntent {
        //     tp: tp.into(),
        //     part,
        //     payload: payload.to_vec(),
        //     outer_completion: completion,
        // };
        // let sequencer = self.msg_sequencer(tp, part).await.map_err(|e| {
        //     tracing::error!("Errrr {}", e);
        //     e
        // })?;
        // sequencer
        //     .send(intent)
        //     .await
        //     .map_err(|e| {
        //         tracing::error!("{} derp failed to send intent..", e);
        //         StromaError::Io("Failed to enqueue message".into())})?;

        Ok(())
    }

    pub async fn ack_enqueue(
        &self,
        tp: &str,
        part: u32,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let ev = StromaEvent::Ack {
            tp: tp.into(),
            part,
            off: offset,
        };

        let log = self.log(tp, part).await?;
        let msg = event_msg(&ev)?;
        log.append_enqueue(msg, None, completion).map_err(io_err)?;

        Ok(())
    }

    pub async fn nack_enqueue(
        &self,
        tp: &str,
        part: u32,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let ev = StromaEvent::Nack {
            tp: tp.into(),
            part,
            off: offset,
            requeue,
        };

        let log = self.log(tp, part).await?;
        let msg = event_msg(&ev)?;
        log.append_enqueue(msg, None, completion).map_err(io_err)?;

        Ok(())
    }

    pub async fn fetch_message_by_offset(
        &self,
        tp: &str,
        part: u32,
        off: Offset,
    ) -> Result<Option<Message>> {
        let log = self.msg_log(tp, part).await?;
        let reader = log.reader();
        let rec = reader.fetch(off).map_err(io_err)?.map(|r| r.to_message());
        Ok(rec)
    }

    pub async fn scan_messages_from(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>)>> {
        let log = self
            .msg_logs_by_tp_part
            .get(&(tp.into(), part))
            .ok_or(StromaError::NotFound)?;
        let reader = log.reader();
        let got = reader.scan_from(from, max).map_err(io_err)?;
        Ok(got.into_iter().map(|r| (r.offset, r.payload)).collect())
    }

    pub async fn current_next_offset(&self, tp: &str, part: u32) -> Result<Offset> {
        let log = self.msg_log(tp, part).await?;
        Ok(log.next_offset())
    }

    /// Optional (used by cleanup_topic): truncate message log.
    pub async fn truncate_messages_before(
        &self,
        tp: &str,
        part: u32,
        before: Offset,
    ) -> Result<u64> {
        let log = self.msg_log(tp, part).await?;
        log.truncate_before(before).await.map_err(io_err)
    }

    pub async fn cleanup_topic_partition(&self, tp: &str, part: u32) -> Result<()> {
        let cutoff = self.safe_truncate_before(tp, part);
        if cutoff == 0 {
            return Ok(());
        }

        self.snapshot_partition(tp, part).await?;
        self.truncate_partition_log(tp, part, cutoff).await?;
        Ok(())
    }

    /// Only offsets < min(acked_until of every queue) are globally deletable.
    fn safe_truncate_before(&self, tp: &str, part: u32) -> Offset {
        self.queues
            .iter()
            .filter(|g| {
                let (k_tp, k_part) = g.key();
                k_tp.as_ref() == tp && *k_part == part
            })
            .map(|g| g.value().acked_until())
            .min()
            .unwrap_or(0)
    }

    pub fn list_queues(&self) -> Vec<(Box<str>, u32)> {
        self.queues
            .iter()
            .map(|e| {
                let (tp, part) = e.key();
                (tp.clone(), *part)
            })
            .collect()
    }

    pub fn is_acked(&self, tp: &str, part: u32, off: Offset) -> Result<bool> {
        Ok(self
            .queues
            .get(&(tp.into(), part))
            .map(|g| g.is_acked(off))
            .unwrap_or(false))
    }

    pub fn count_inflight(&self, tp: &str, part: u32) -> Result<usize> {
        Ok(self
            .queues
            .get(&(tp.into(), part))
            .map(|g| g.inflight_len())
            .unwrap_or(0))
    }

    pub fn list_topics(&self) -> Vec<Box<str>> {
        self.queues
            .iter()
            .map(|g| {
                let (tp, _) = g.key();
                tp.clone()
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }
}

// TODO: add flags to avoid in release builds or such? with default (Currently used by tests)
impl Stroma {
    pub async fn mark_inflight_one(
        &self,
        tp: &str,
        part: u32,
        off: Offset,
        deadline: UnixMillis,
    ) -> Result<()> {
        self.mark_inflight_batch(tp, part, &[(off, deadline)]).await
    }

    pub async fn ack_one(&self, tp: &str, part: u32, off: Offset) -> Result<()> {
        self.ack_batch(tp, part, &[off]).await
    }

    pub async fn snapshot_partition(&self, tp: &str, part: u32) -> Result<()> {
        let upto = self.applied_upto_entry(tp, part).load(Ordering::Acquire);
        self.write_snapshots_for_partition(tp, part, upto).await
    }

    pub async fn truncate_partition_log(&self, tp: &str, part: u32, before: Offset) -> Result<u64> {
        let log = self.log(tp, part).await?;
        log.truncate_before(before).await.map_err(io_err)
    }

    pub fn debug_dump_queue(&self, tp: &str, part: u32) -> String {
        self.queues
            .get(&(tp.into(), part))
            .map(|g| format!("{:#?}", g.canonical()))
            .unwrap_or_else(|| "<empty>".into())
    }

    pub fn validate(&self) -> Result<()> {
        for g in self.queues.iter() {
            let gs = g.value();

            for (off, _) in gs.dump_inflight() {
                if off < gs.acked_until() {
                    return Err(StromaError::Decode("inflight < ack frontier".into()));
                }
            }

            if gs.ack_window_base() > gs.acked_until() {
                return Err(StromaError::Decode("ack window base > frontier".into()));
            }
        }
        Ok(())
    }
}

// ---- Small helpers on event ----
// (Add these methods on StromaEvent; they make stroma.rs cleaner.)

trait EventView {
    fn tp(&self) -> &str;
    fn part(&self) -> u32;
}

impl EventView for StromaEvent {
    fn tp(&self) -> &str {
        match self {
            StromaEvent::Enqueue { tp, .. } => tp,
            StromaEvent::MarkInflight { tp, .. } => tp,
            StromaEvent::Ack { tp, .. } => tp,
            StromaEvent::Nack { tp, .. } => tp,
            StromaEvent::DeadLetter { tp, .. } => tp,
            StromaEvent::ClearInflight { tp, .. } => tp,
            StromaEvent::ResetQueue { tp, .. } => tp,
            StromaEvent::Snapshot { tp, .. } => tp,
        }
    }
    fn part(&self) -> u32 {
        match self {
            StromaEvent::Enqueue { part, .. } => *part,
            StromaEvent::MarkInflight { part, .. } => *part,
            StromaEvent::Ack { part, .. } => *part,
            StromaEvent::Nack { part, .. } => *part,
            StromaEvent::DeadLetter { part, .. } => *part,
            StromaEvent::ClearInflight { part, .. } => *part,
            StromaEvent::ResetQueue { part, .. } => *part,
            StromaEvent::Snapshot { part, .. } => *part,
        }
    }
}
