use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use hashbrown::{HashMap, HashSet};
use keratin_log::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KDurability, Keratin,
    KeratinAppendCompletion, KeratinConfig, Message,
};
use tokio::sync::{OnceCell, RwLock};

use crate::{
    Result, StromaError,
    event::{self, StromaEvent},
    state::{Offset, QueueHandle, UnixMillis},
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
            every_events: 5_000_000,
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

type Registry = HashMap<(Box<str>, u32, Option<Box<str>>), Arc<OnceCell<QueueHandle>>>;

#[derive(Debug, Clone)]
pub struct Stroma {
    pub(crate) root: PathBuf,
    pub(crate) keratin_cfg: KeratinConfig,
    pub(crate) snap_cfg: SnapshotConfig,

    // Materialized queue state
    queue_handles: Arc<ArcSwap<Registry>>,

    // Global DLQ topic
    pub(crate) global_dlq: Arc<RwLock<Option<GlobalDLQ>>>,

    pub(crate) msg_count: Arc<AtomicU64>,

    pub(crate) event_count: Arc<AtomicU64>,
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
            queue_handles: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
            global_dlq: Arc::new(RwLock::new(None)),
            msg_count: Arc::new(AtomicU64::new(0)),
            event_count: Arc::new(AtomicU64::new(0)),
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

    fn msg_tp_part_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let mut p = self.messages_root();
        if let Some(g) = group {
            p = p.join(Self::enc_component(g))
        }
        p.join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn tp_part_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let mut p = self.events_root();
        if let Some(g) = group {
            p = p.join(Self::enc_component(g))
        }
        p.join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn snap_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let mut p = self.snapshots_root();
        if let Some(g) = group {
            p = p.join(Self::enc_component(g))
        }
        p.join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn snap_file(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.snap_dir(tp, part, group)
            .join(format!("{}.snap", Self::enc_component(tp)))
    }

    fn snap_tmp_file(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let p = self.root.join("tmp");
        if let Some(g) = group {
            p.join(format!(
                "{}_{}_{}.snap.new",
                Self::enc_component(g),
                Self::enc_component(tp),
                part,
            ))
        } else {
            p.join(format!("{}_{}.snap.new", Self::enc_component(tp), part,))
        }
    }

    // ---------------- Core accessors ----------------

    async fn event_log_init(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Arc<Keratin>> {
        let dir = self.tp_part_dir(tp, part, group);
        fs::create_dir_all(&dir).map_err(io_err)?;

        tracing::info!("Initializing event log: (`{tp}` `{part}` `{group:?}`)");
        let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
        tracing::info!("Initialized event log: (`{tp}` `{part}` `{group:?}`)");

        Ok(Arc::new(k))
    }

    async fn msg_log_init(&self, tp: &str, part: u32, group: Option<&str>) -> Result<Arc<Keratin>> {
        let dir = self.msg_tp_part_dir(tp, part, group);
        fs::create_dir_all(&dir).map_err(io_err)?;

        tracing::info!("Initializing message log: (`{tp}` `{part}` `{group:?}`)");
        let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
        tracing::info!("Initialized message log: (`{tp}` `{part}` `{group:?}`)");

        Ok(Arc::new(k))
    }

    pub async fn queue_handle(&self, tp: &str, part: u32, group: Option<&str>) -> Result<QueueHandle> {
        let cell = loop {
            let current = self.queue_handles.load();

            let key = (tp.into(), part, group.map(|s| s.into()));
            if let Some(cell) = current.get(&key) {
                break cell.clone();
            }

            let new_cell = Arc::new(OnceCell::new());
            let mut next = (**current).clone();
            next.insert(key.clone(), new_cell.clone());

            // swap in the new map only if snapshot is still current
            let prev = self.queue_handles.compare_and_swap(&current, Arc::new(next));

            if Arc::ptr_eq(&prev, &current) {
                break new_cell;
            }

            // lost race; retry
        };

        let q = cell
            .get_or_try_init(|| async {
                let msg_log = self.msg_log_init(tp, part, group).await?;
                let event_log = self.event_log_init(tp, part, group).await?;
                Ok(QueueHandle::init(
                    tp.into(),
                    part,
                    msg_log,
                    event_log,
                ))
            })
            .await?;

        Ok(q.clone())
    }

    async fn ensure_queue(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        self.queue_handle(tp, part, group).await?;

        Ok(())
    }

    async fn applied_upto_entry(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Arc<AtomicU64>> {
        let queue = self.queue_handle(tp, part, group).await?;
        Ok(queue.applied_upto())
    }

    // ---------------- Event apply rules ----------------

    async fn apply_event_inmem(&self, ev: &StromaEvent) -> Result<()> {
        match ev {
            StromaEvent::Enqueue {
                tp,
                part,
                group,
                off,
                retries,
            } => {
                let q = self.queue_handle(tp, *part, group.as_deref()).await?;
                q.enqueue(*off, *retries).await;
            }
            StromaEvent::MarkInflight {
                tp,
                part,
                group,
                off,
                deadline,
            } => {
                let q = self.queue_handle(tp, *part, group.as_deref()).await?;
                q.mark_inflight(*off, *deadline).await;
            }
            StromaEvent::Ack {
                tp,
                part,
                group,
                off,
            } => {
                let q = self.queue_handle(tp, *part, group.as_deref()).await?;
                // ✅ Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                q.ack(*off).await;
            }
            StromaEvent::Nack {
                tp,
                part,
                group,
                off,
                requeue,
            } => {
                let q = self.queue_handle(tp, *part, group.as_deref()).await?;
                // ✅ Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
                q.nack(*off, *requeue).await;
            }
            StromaEvent::DeadLetter {
                tp,
                part,
                group,
                off,
            } => {
                let q = self.queue_handle(tp, *part, group.as_deref()).await?;
                q.dead_letter(*off).await;
            }
            StromaEvent::ClearInflight {
                tp,
                part,
                group,
                off,
            } => {
                let q = self.queue_handle(tp, *part, group.as_deref()).await?;
                q.clear_inflight(*off).await;
            }
            StromaEvent::ResetQueue { tp, part, group } => {
                self.remove_queue(tp, *part, group.as_deref());
                // TODO: More cleanup?
            }
            StromaEvent::Snapshot { .. } => {
                // If you keep Snapshot events inside the event log later, you'd load it here.
                // With file snapshots, we don't emit Snapshot events, so this is unused.
                return Err(StromaError::Decode(
                    "Snapshot event unsupported in v0".into(),
                ));
            }
        }
        tracing::debug!("Applied event: {ev:?}");
        Ok(())
    }

    async fn append_events_durable(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        evs: &[StromaEvent],
        durability: KDurability,
    ) -> Result<Offset> {
        if evs.is_empty() {
            return Ok(self
                .applied_upto_entry(tp, part, group).await?
                .load(Ordering::Acquire));
        }

        let log = self.queue_handle(tp, part, group).await?.event_log();
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
        self.applied_upto_entry(tp, part, group).await?
            .store(new_upto, Ordering::Release);

        Ok(new_upto)
    }

    // ---------------- Public API used by Storage shim ----------------

    pub async fn mark_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
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
                group: group.map(|s| s.into()),
                off,
                deadline,
            });
        }

        let upto = self
            .append_events_durable(tp, part, group, &evs, KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, group, upto).await?;

        Ok(())
    }

    pub async fn ack_batch(
        &self,
        tp: Box<str>,
        part: u32,
        group: Option<&str>,
        offs: &[Offset],
    ) -> Result<()> {
        if offs.is_empty() {
            return Ok(());
        }

        let mut evs = Vec::with_capacity(offs.len());
        for &off in offs {
            evs.push(StromaEvent::Ack {
                tp: tp.clone(),
                part,
                group: group.map(|s| s.into()),
                off,
            });
        }

        let upto = self
            .append_events_durable(&tp, part, group, &evs, KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(&tp, part, group, upto).await?;

        Ok(())
    }

    pub async fn clear_inflight(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<()> {
        let ev = StromaEvent::ClearInflight {
            tp: tp.into(),
            part,
            group: group.map(|s| s.into()),
            off,
        };
        let upto = self
            .append_events_durable(
                tp,
                part,
                group,
                std::slice::from_ref(&ev),
                KDurability::AfterFsync,
            )
            .await?;
        self.maybe_snapshot(tp, part, group, upto).await?;
        Ok(())
    }

    pub async fn add_to_redelivery(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<()> {
        let ev1 = StromaEvent::ClearInflight {
            tp: tp.into(),
            part,
            group: group.map(|s| s.into()),
            off,
        };
        let ev2 = StromaEvent::Nack {
            tp: tp.into(),
            part,
            group: group.map(|s| s.into()),
            off,
            requeue: true,
        };
        let upto = self
            .append_events_durable(tp, part, group, &[ev1, ev2], KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, group, upto).await?;
        Ok(())
    }

    pub async fn requeue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<()> {
        let ev = StromaEvent::Nack {
            tp: tp.into(),
            part,
            group: group.map(|s| s.into()),
            off,
            requeue: true,
        };
        let upto = self
            .append_events_durable(tp, part, group, &[ev], KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, group, upto).await?;
        Ok(())
    }

    pub async fn lowest_unacked_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(q.lowest_unacked_offset().await)
    }

    pub async fn is_inflight_or_acked(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<bool> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(q.is_inflight_or_acked(off).await)
    }

    pub async fn is_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<bool> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(q.is_ready(off).await)
    }

    pub async fn filter_not_enqueued(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<(Offset, Vec<u8>)>,
    ) -> Result<Vec<(Offset, Vec<u8>)>> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(q.filter_not_enqueued(items).await)
    }

    fn queue_keys_snapshot(&self) -> Vec<(Box<str>, u32, Option<Box<str>>)> {
        let map = self.queue_handles.load();
        map.keys().cloned().collect()
    }

    pub async fn next_expiry_hint(&self) -> Result<Option<UnixMillis>> {
        let mut min: Option<UnixMillis> = None;
        let keys = self.queue_keys_snapshot();
        for (t, p, g) in keys {
            let qh = self.queue_handle(&t, p, g.as_deref()).await?;
            if let Some(hint) = qh.next_expiry_hint().await {
                min = Some(match min {
                    Some(m) => m.min(hint),
                    None => hint,
                });
            }
        }

        Ok(min)
    }

    pub async fn next_deliverable(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        from: u64,
        upper: u64,
    ) -> Result<Option<Offset>> {
        let q = self.queue_handle(topic, part, group).await?;
        Ok(q.next_deliverable(from, upper).await)
    }

    pub async fn list_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<Vec<(String, u32, Option<String>, Offset)>> {
        let mut out = Vec::new();

        let keys = self.queue_keys_snapshot();
        for key in keys {
            let (tp, part, group) = key;
            let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
            if out.len() >= max {
                break;
            }
            let want = max - out.len();
            for off in qh.collect_expired(now, want).await {
                out.push((
                    tp.to_string(),
                    part,
                    group.clone().map(|s| s.to_string()),
                    off,
                ));
                if out.len() >= max {
                    break;
                }
            }
        }

        Ok(out)
    }

    pub async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>> {
        let expired = self.list_expired(now, max).await?;
        let expired_set: HashSet<(String, u32, Option<String>, u64)> =
            HashSet::from_iter(expired.clone().into_iter());

        for (tp, part, group, off) in expired {
            let evs = [
                StromaEvent::ClearInflight {
                    tp: tp.clone().into(),
                    part,
                    group: group.clone().map(|s| s.into()),
                    off,
                },
                StromaEvent::Nack {
                    tp: tp.clone().into(),
                    part,
                    group: group.clone().map(|s| s.into()),
                    off,
                    requeue: true,
                },
            ];

            self.append_events_durable(&tp, part, group.as_deref(), &evs, KDurability::AfterFsync)
                .await?;
        }

        Ok(expired_set)
    }

    // ---------------- Snapshotting ----------------
    //
    // Snapshot files make restart fast:
    // - durable event log = Keratin partition log
    // - snapshot per (tp,part): { last_applied_event_offset, queue_state_blob }
    //
    // Recovery loads snapshots, then replays events AFTER the minimum snapshot offset,
    // skipping events already covered by each queue's snapshot.

    async fn maybe_snapshot(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        applied_upto: Offset,
    ) -> Result<()> {
        let every = self.snap_cfg.every_events.max(1);
        if !applied_upto.is_multiple_of(every) {
            return Ok(());
        }
        self.write_snapshots_for_partition(tp, part, group, applied_upto)
            .await
    }

    async fn write_snapshots_for_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        applied_upto: Offset,
    ) -> Result<()> {
        let dir = self.snap_dir(tp, part, group);
        fs::create_dir_all(&dir).map_err(io_err)?;

        // snapshot all queues for this partition (simple v0; later you can do incremental / max-bytes)
        let keys = self.queue_keys_snapshot();
        for (key_tp, key_part, key_group) in keys {
            let qh = self
                .queue_handle(&key_tp, key_part, key_group.as_deref())
                .await?;
            if key_tp.as_ref() == tp && key_part == part && key_group.as_deref() == group {
                let blob = qh.encode_snapshot().await;
                self.write_queue_snapshot(tp, part, group, applied_upto, &blob)?;
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
        group: Option<&str>,
        last_applied_event_offset: Offset,
        blob: &[u8],
    ) -> Result<()> {
        let tmp = self.snap_tmp_file(tp, part, group);
        let final_path = self.snap_file(tp, part, group);

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

        let mut partitions: Vec<(Option<String>, String, u32)> = Vec::new();

        for lvl1_ent in fs::read_dir(&events_root).map_err(io_err)? {
            let lvl1_ent = lvl1_ent.map_err(io_err)?;
            if !lvl1_ent.file_type().map_err(io_err)?.is_dir() {
                continue;
            }

            let lvl1_name = lvl1_ent.file_name().to_string_lossy().to_string();
            let lvl1_path = lvl1_ent.path();

            // Peek inside: are there numeric dirs directly? If yes → legacy (no group)
            let mut has_partition = false;
            for e in fs::read_dir(&lvl1_path).map_err(io_err)? {
                let e = e.map_err(io_err)?;
                if e.file_type().map_err(io_err)?.is_dir()
                    && e.file_name().to_string_lossy().parse::<u32>().is_ok()
                {
                    has_partition = true;
                    break;
                }
            }

            if has_partition {
                // legacy: lvl1 is topic
                collect_parts(None, lvl1_name, &lvl1_path, &mut partitions)?;
            } else {
                // grouped: lvl1 is group, next level is topic
                for tp_ent in fs::read_dir(&lvl1_path).map_err(io_err)? {
                    let tp_ent = tp_ent.map_err(io_err)?;
                    if !tp_ent.file_type().map_err(io_err)?.is_dir() {
                        continue;
                    }
                    let tp_name = tp_ent.file_name().to_string_lossy().to_string();
                    collect_parts(
                        Some(lvl1_name.clone()),
                        tp_name,
                        &tp_ent.path(),
                        &mut partitions,
                    )?;
                }
            }
        }

        // NOTE: tp_dirname is encoded; we don't need the original for recovery of state
        // because events carry real tp strings. We'll just open each partition dir and replay.
        for (group, tp_enc, part) in partitions {
            let q = self.queue_handle(&tp_enc, part, group.as_deref()).await?;
            let event_log = q.event_log();
            self.ensure_queue(&tp_enc, part, group.as_deref()).await?;

            let mut dir = events_root.clone();
            if let Some(ref g) = group {
                dir = dir.join(g);
            }
            dir = dir.join(&tp_enc).join(format!("{:010}", part));

            // let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
            // let k = Arc::new(k);
            // We don't know real topic string here; replay will populate from event payload.
            // We'll store this Keratin under the first real tp seen during replay.
            self.recover_one_log(event_log, part).await?;
        }

        Ok(())
    }

    async fn recover_one_log(&self, k: Arc<Keratin>, part: u32) -> Result<()> {
        // 1) Load all snapshot files for this partition into queues map + remember per-queue snapshot offsets
        //    We cannot pre-know tp; snapshot directory is keyed by encoded tp, but the snapshot file
        //    itself does not embed tp. So: in v0, we load snapshots only when we know tp.
        //
        // Practical approach v0:
        // - Do not "discover snapshots by scanning filesystem blindly".
        // - Instead, do replay from 0 once per partition (still OK early).
        //
        // For fast restarts now:
        // - store snapshots under snapshots/<enc(tp)>/<part>/<enc(tp)>.snap
        // - and during replay, when we see tp, we attempt to load its snapshot once.

        // We'll cache loaded snapshot offsets here:
        let mut snap_applied_for: HashMap<Box<str>, Offset> = HashMap::new();
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
                let group = ev.group();
                // let cell = self.log(tp, part, group.as_deref()).await?;
                // let cell = self
                //     .logs_by_tp_part
                //     .entry((tp.into(), part, group.clone()))
                //     .or_insert_with(|| Arc::new(OnceCell::new()))
                //     .clone();

                // Best-effort: initialize if empty
                // if let Err(err) = cell.set(k.clone()) {
                //     tracing::error!("Error initializing log: {err}");
                // }

                // Best-effort: load snapshot for (tp) once, and if snapshot's last_applied >= rec.offset, skip.
                let key = tp;

                if !snap_applied_for.contains_key(key) {
                    // try read snapshot file
                    let sp = self.snap_file(tp, part, group.as_deref());
                    if let Some((snap_upto, blob)) = self.read_queue_snapshot(&sp)? {
                        {
                            let gs = self.queue_handle(tp, part, group.as_deref()).await?;
                            gs.load_snapshot(blob).await.map_err(io_err)?;
                        }
                        snap_applied_for.insert(key.into(), snap_upto);
                    } else {
                        snap_applied_for.insert(key.into(), 0);
                    }
                }

                let snap_upto = *snap_applied_for.get(key).unwrap();

                // Zero with one event causes edge case
                if rec.offset <= snap_upto && snap_upto > 0 {
                    continue; // covered by snapshot
                }

                self.apply_event_inmem(&ev).await?;
                self.applied_upto_entry(tp, part, group.as_deref()).await?
                    .store(rec.offset + 1, Ordering::Release);
            }
        }

        Ok(())
    }

    fn remove_queue(&self, tp: &str, part: u32, group: Option<&str>) -> Option<Arc<OnceCell<QueueHandle>>> {
        let key = (tp.into(), part, group.map(|s| s.into()));
        loop {
            let current = self.queue_handles.load();

            if !current.contains_key(&key) {
                return None;
            }

            let mut next = (**current).clone();
            let removed = next.remove(&key);

            let new = Arc::new(next);
            let prev = self.queue_handles.compare_and_swap(&current, new);

            if Arc::ptr_eq(&prev, &current) {
                return removed;
            }

            // lost race -> retry
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Step 1: atomically take ownership of all queues
        let old = self.queue_handles.swap(Arc::new(HashMap::new()));

        // Step 2: shutdown everything from the old snapshot
        for (_key, cell) in old.iter() {
            if let Some(q) = cell.get() {
                q.event_log().shutdown().await.map_err(io_err)?;
                q.msg_log().shutdown().await.map_err(io_err)?;
                q.shutdown().await;
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
        group: Option<&str>,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let log = self.queue_handle(tp, part, group).await?.msg_log();
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
        group: Option<&str>,
        payload: &[u8],
        event_completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let (msg_completion, msg_rx) = KeratinAppendCompletion::pair();
        let msg_log = self.queue_handle(tp, part, group).await?.msg_log();
        self.ensure_queue(tp, part, group).await?;
        msg_log
            .append_enqueue(
                Message {
                    flags: 0,
                    headers: vec![],
                    payload: payload.to_vec(),
                },
                None,
                msg_completion,
            )
            .map_err(io_err)?;
        let tp: Box<str> = tp.into();
        let group: Option<Box<str>> = group.map(|s| s.into());
        let stroma = self.clone();
        tokio::spawn(async move {
            let msg_res = msg_rx.await;

            let msg_append_result = match msg_res {
                Ok(Ok(ar)) => ar,
                Ok(Err(err)) => {
                    tracing::error!("Got res for write to msg log: {err:?}");
                    event_completion.complete(Err(err));
                    return;
                }
                Err(_err) => {
                    event_completion.complete(Err(IoError::new("Channel closed")));
                    return;
                }
            };
            let msg_offset = msg_append_result.base_offset;

            // TODO: emit Enqueue event too in some form
            let ev = StromaEvent::Enqueue {
                retries: 0,
                tp: tp.clone(),
                part,
                group: group.clone(),
                off: msg_offset,
            };

            match stroma
                .append_events_durable(
                    &tp,
                    part,
                    group.as_deref(),
                    &[ev],
                    stroma.keratin_cfg.default_durability,
                )
                .await
                .map_err(io_err)
            {
                Ok(_event_offset) => {
                    event_completion.complete(Ok(AppendResult {
                        base_offset: msg_offset,
                        count: 1,
                    }));
                }
                Err(err) => {
                    tracing::error!("Got res for write to event log: {err:?}");
                    event_completion.complete(Err(IoError::new(err)));
                }
            };
        });

        Ok(())
    }

    pub async fn ack_enqueue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let ev = StromaEvent::Ack {
            tp: tp.into(),
            part,
            group: group.map(|s| s.into()),
            off: offset,
        };

        let log = self.queue_handle(tp, part, group).await?.event_log();
        let msg = event_msg(&ev)?;
        // TODO: Create a separate completion for the event log, so that I use the original one, once the event is also applied in memory.
        log.append_enqueue(msg, None, completion).map_err(io_err)?;
        self.apply_event_inmem(&ev).await?;
        self.maybe_snapshot(tp, part, group, offset).await?;

        Ok(())
    }

    pub async fn nack_enqueue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let ev = StromaEvent::Nack {
            tp: tp.into(),
            part,
            group: group.map(|s| s.into()),
            off: offset,
            requeue,
        };

        let log = self.queue_handle(tp, part, group).await?.event_log();
        let msg = event_msg(&ev)?;
        log.append_enqueue(msg, None, completion).map_err(io_err)?;
        self.apply_event_inmem(&ev).await?;
        self.maybe_snapshot(tp, part, group, offset).await?;

        Ok(())
    }

    pub async fn fetch_message_by_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<Option<Message>> {
        let log = self.queue_handle(tp, part, group).await?.msg_log();
        let reader = log.reader();
        let rec = reader.fetch(off).map_err(io_err)?.map(|r| r.to_message());
        Ok(rec)
    }

    pub async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Result<Vec<(Offset, Vec<u8>)>> {
        let qs = self.queue_handle(tp, part, group).await?;

        // Offsets are now already marked inflight inside queue
        let offs = qs.poll_ready_and_mark(max, lease_deadline).await;

        if offs.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(offs.len());

        let mut i = 0;
        while i < offs.len() {
            let start = offs[i];
            let mut len = 1;

            // ---- group contiguous offsets ----
            while i + len < offs.len() && offs[i + len] == start + len as u64 {
                len += 1;
            }

            // ---- batch fetch ----
            let batch = self.scan_messages_from(tp, part, group, start, len).await?;

            // ---- fast path: perfect match ----
            if batch.len() == len {
                for (off, payload) in batch {
                    out.push((off, payload));
                }
            } else {
                // ---- slow path: handle holes (rare but important) ----
                // build small lookup map
                let mut map = hashbrown::HashMap::with_capacity(batch.len());
                for (off, payload) in batch {
                    map.insert(off, payload);
                }

                for j in 0..len {
                    let off = start + j as u64;
                    if let Some(payload) = map.remove(&off) {
                        out.push((off, payload));
                    } else {
                        // extremely rare: log inconsistency or race
                        tracing::warn!(
                            "Missing payload for offset {} in batch fetch (tp={}, part={}, group={:?})",
                            off,
                            tp,
                            part,
                            group
                        );
                    }
                }
            }

            i += len;
        }

        Ok(out)
    }

    pub async fn scan_messages_from(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>)>> {
        let log = self.queue_handle(tp, part, group).await?.msg_log();
        let reader = log.reader();
        let got = reader.scan_from(from, max).map_err(io_err)?;
        Ok(got.into_iter().map(|r| (r.offset, r.payload)).collect())
    }

    pub async fn current_next_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let log = self.queue_handle(tp, part, group).await?.msg_log();
        Ok(log.next_offset())
    }

    /// Optional (used by cleanup_topic): truncate message log.
    pub async fn truncate_messages_before(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        before: Offset,
    ) -> Result<u64> {
        let log = self.queue_handle(tp, part, group).await?.msg_log();
        log.truncate_before(before).await.map_err(io_err)
    }

    pub async fn cleanup_topic_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        let cutoff = self.safe_truncate_before(tp, part, group).await?;
        tracing::warn!("cleanup cutoff: {}", cutoff);
        if cutoff == 0 {
            return Ok(());
        }

        self.snapshot_partition(tp, part, group).await?;
        self.truncate_partition_log(tp, part, group, cutoff).await?;
        Ok(())
    }

    /// Only offsets < min(acked_until of every queue) are globally deletable.
    async fn safe_truncate_before(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let mut min: Option<UnixMillis> = None;
        let keys = self.queue_keys_snapshot();
        for (k_tp, k_part, k_group) in keys {
            if k_tp.as_ref() == tp && k_part == part && k_group.as_deref() == group {
                let qh = self.queue_handle(&k_tp, k_part, k_group.as_deref()).await?;
                let settled_until = qh.settled_until().await;
                min = Some(match min {
                    Some(m) => m.min(settled_until),
                    None => settled_until,
                });
            }
        }

        Ok(min.unwrap_or(0))
    }

    pub fn list_queues(&self) -> Vec<(Box<str>, u32, Option<Box<str>>)> {
        self.queue_keys_snapshot()
            .iter()
            .map(|k| {
                let (tp, part, group) = k;
                (tp.clone(), *part, group.clone())
            })
            .collect()
    }

    pub async fn is_acked(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<bool> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(q.is_acked(off).await)
    }

    pub async fn count_inflight(&self, tp: &str, part: u32, group: Option<&str>) -> Result<usize> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(q.inflight_len().await)
    }

    pub fn list_topics(&self) -> Vec<Box<str>> {
        // TODO: Should return groups too
        self.queue_keys_snapshot()
            .iter()
            .map(|k| {
                let (tp, _, _) = k;
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
        group: Option<&str>,
        off: Offset,
        deadline: UnixMillis,
    ) -> Result<()> {
        self.mark_inflight_batch(tp, part, group, &[(off, deadline)])
            .await
    }

    pub async fn ack_one(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<()> {
        self.ack_batch(tp.into(), part, group, &[off]).await
    }

    pub async fn snapshot_partition(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        let upto = self
            .applied_upto_entry(tp, part, group).await?
            .load(Ordering::Acquire);
        self.write_snapshots_for_partition(tp, part, group, upto)
            .await
    }

    pub async fn truncate_partition_log(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        before: Offset,
    ) -> Result<u64> {
        let log = self.queue_handle(tp, part, group).await?.event_log();
        log.truncate_before(before).await.map_err(io_err)
    }

    pub async fn debug_dump_queue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<String> {
        let q = self.queue_handle(tp, part, group).await?;
        Ok(format!("{:#?}", q.canonical().await))
    }

    pub async fn validate(&self) -> Result<()> {
        let keys = self.queue_keys_snapshot();
        for (k_tp, k_part, k_group) in keys {
            let qh = self.queue_handle(&k_tp, k_part, k_group.as_deref()).await?;
            for (off, _) in qh.dump_inflight().await {
                if off < qh.settled_until().await {
                    return Err(StromaError::Decode("inflight < ack frontier".into()));
                }
            }

            if qh.ack_window_base().await > qh.settled_until().await {
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
    fn group(&self) -> &Option<Box<str>>;
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
    fn group(&self) -> &Option<Box<str>> {
        match self {
            StromaEvent::Enqueue { group, .. } => group,
            StromaEvent::MarkInflight { group, .. } => group,
            StromaEvent::Ack { group, .. } => group,
            StromaEvent::Nack { group, .. } => group,
            StromaEvent::DeadLetter { group, .. } => group,
            StromaEvent::ClearInflight { group, .. } => group,
            StromaEvent::ResetQueue { group, .. } => group,
            StromaEvent::Snapshot { group, .. } => group,
        }
    }
}

fn collect_parts(
    group: Option<String>,
    tp_enc: String,
    tp_dir: &Path,
    out: &mut Vec<(Option<String>, String, u32)>,
) -> Result<()> {
    for part_ent in fs::read_dir(tp_dir).map_err(io_err)? {
        let part_ent = part_ent.map_err(io_err)?;
        if !part_ent.file_type().map_err(io_err)?.is_dir() {
            continue;
        }
        let part_str = part_ent.file_name().to_string_lossy().to_string();
        let part = part_str
            .parse::<u32>()
            .map_err(|_| StromaError::Decode(format!("bad partition dir: {part_str}")))?;
        out.push((group.clone(), tp_enc.clone(), part));
    }
    Ok(())
}
