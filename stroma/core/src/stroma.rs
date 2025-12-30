use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use keratin_log::{Durability as KDurability, Keratin, KeratinConfig, Message as KMessage};

use crate::{
    event::StromaEvent,
    state::{GroupState, Offset, UnixMillis},
};

#[derive(thiserror::Error, Debug)]
pub enum StromaError {
    #[error("io: {0}")]
    Io(String),
    #[error("decode: {0}")]
    Decode(String),
}

type Result<T> = std::result::Result<T, StromaError>;

fn io_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Io(e.to_string())
}

fn decode_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Decode(e.to_string())
}

fn event_msg(ev: &StromaEvent) -> Result<KMessage> {
    let payload = ev.encode().map_err(io_err)?;
    Ok(KMessage {
        flags: 0,
        headers: vec![],
        payload,
    })
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    tp: String,
    part: u32,
    group: String,
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

pub struct Stroma {
    root: PathBuf,
    keratin_cfg: KeratinConfig,
    snap_cfg: SnapshotConfig,

    // One Keratin per (tp,part)
    logs: DashMap<(String, u32), Arc<Keratin>>,

    // Materialized group state
    groups: DashMap<Key, GroupState>,

    // For each (tp,part): highest Keratin offset we have applied into memory
    applied_upto: DashMap<(String, u32), AtomicU64>,
}

impl Stroma {
    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg: KeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("events")).map_err(io_err)?;
        fs::create_dir_all(root.join("snapshots")).map_err(io_err)?;
        fs::create_dir_all(root.join("tmp")).map_err(io_err)?;

        let st = Self {
            root,
            keratin_cfg,
            snap_cfg,
            logs: DashMap::new(),
            groups: DashMap::new(),
            applied_upto: DashMap::new(),
        };

        // Recover from existing snapshot files + replay events.
        st.recover_all().await?;

        Ok(st)
    }

    // ---------------- Paths / naming ----------------

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

    fn snap_file(&self, tp: &str, part: u32, group: &str) -> PathBuf {
        self.snap_dir(tp, part)
            .join(format!("{}.snap", Self::enc_component(group)))
    }

    fn snap_tmp_file(&self, tp: &str, part: u32, group: &str) -> PathBuf {
        self.root.join("tmp").join(format!(
            "{}_{}_{}.snap.new",
            Self::enc_component(tp),
            part,
            Self::enc_component(group),
        ))
    }

    // ---------------- Core accessors ----------------

    async fn log(&self, tp: &str, part: u32) -> Result<Arc<Keratin>> {
        let key = (tp.to_string(), part);

        if let Some(v) = self.logs.get(&key) {
            return Ok(v.value().clone());
        }

        let dir = self.tp_part_dir(tp, part);
        fs::create_dir_all(&dir).map_err(io_err)?;

        let k = Keratin::open(dir, self.keratin_cfg).await.map_err(io_err)?;
        let k = Arc::new(k);

        self.logs.insert(key.clone(), k.clone());
        self.applied_upto
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0));

        Ok(k)
    }

    fn group_entry(
        &self,
        tp: &str,
        part: u32,
        group: &str,
    ) -> dashmap::mapref::one::RefMut<'_, Key, GroupState> {
        let key = Key {
            tp: tp.into(),
            part,
            group: group.into(),
        };
        self.groups.entry(key).or_insert_with(GroupState::new)
    }

    fn applied_upto_entry(
        &self,
        tp: &str,
        part: u32,
    ) -> dashmap::mapref::one::RefMut<'_, (String, u32), AtomicU64> {
        self.applied_upto
            .entry((tp.to_string(), part))
            .or_insert_with(|| AtomicU64::new(0))
    }

    // ---------------- Event apply rules ----------------

    fn apply_event_inmem(&self, ev: &StromaEvent) -> Result<()> {
        match ev {
            StromaEvent::MarkInflight {
                tp,
                part,
                group,
                off,
                deadline,
            } => {
                let mut gs = self.group_entry(tp, *part, group);
                gs.mark_inflight(*off, *deadline);
            }
            StromaEvent::Ack {
                tp,
                part,
                group,
                off,
            } => {
                let mut gs = self.group_entry(tp, *part, group);
                // ✅ Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                gs.ack(*off);
            }
            StromaEvent::ClearInflight {
                tp,
                part,
                group,
                off,
            } => {
                let mut gs = self.group_entry(tp, *part, group);
                gs.clear_inflight(*off);
            }
            StromaEvent::ResetGroup { tp, part, group } => {
                let k = Key {
                    tp: tp.clone(),
                    part: *part,
                    group: group.clone(),
                };
                self.groups.remove(&k);
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
            self.apply_event_inmem(ev)?;
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
        group: &str,
        entries: &[(Offset, UnixMillis)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut evs = Vec::with_capacity(entries.len());
        for &(off, deadline) in entries {
            evs.push(StromaEvent::MarkInflight {
                tp: tp.to_string(),
                part,
                group: group.to_string(),
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

    pub async fn ack_batch(&self, tp: &str, part: u32, group: &str, offs: &[Offset]) -> Result<()> {
        if offs.is_empty() {
            return Ok(());
        }

        let mut evs = Vec::with_capacity(offs.len());
        for &off in offs {
            evs.push(StromaEvent::Ack {
                tp: tp.to_string(),
                part,
                group: group.to_string(),
                off,
            });
        }

        let upto = self
            .append_events_durable(tp, part, &evs, KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, upto).await?;

        Ok(())
    }

    pub async fn clear_inflight(
        &self,
        tp: &str,
        part: u32,
        group: &str,
        off: Offset,
    ) -> Result<()> {
        let ev = StromaEvent::ClearInflight {
            tp: tp.to_string(),
            part,
            group: group.to_string(),
            off,
        };
        let upto = self
            .append_events_durable(tp, part, std::slice::from_ref(&ev), KDurability::AfterFsync)
            .await?;
        self.maybe_snapshot(tp, part, upto).await?;
        Ok(())
    }

    pub fn lowest_unacked_offset(&self, tp: &str, part: u32, group: &str) -> Result<Offset> {
        let k = Key {
            tp: tp.to_string(),
            part,
            group: group.to_string(),
        };
        Ok(self
            .groups
            .get(&k)
            .map(|g| g.lowest_unacked_offset())
            .unwrap_or(0))
    }

    pub fn is_inflight_or_acked(
        &self,
        tp: &str,
        part: u32,
        group: &str,
        off: Offset,
    ) -> Result<bool> {
        let k = Key {
            tp: tp.to_string(),
            part,
            group: group.to_string(),
        };
        Ok(self
            .groups
            .get(&k)
            .map(|g| g.is_inflight_or_acked(off))
            .unwrap_or(false))
    }

    pub fn next_expiry_hint(&self) -> Result<Option<UnixMillis>> {
        Ok(self
            .groups
            .iter_mut()
            .filter_map(|mut e| e.value_mut().next_expiry_hint())
            .min())
    }

    pub fn list_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<Vec<(String, u32, String, Offset)>> {
        let mut out = Vec::new();

        for mut kv in self.groups.iter_mut() {
            if out.len() >= max {
                break;
            }
            let want = max - out.len();
            for off in kv.value_mut().pop_expired(now, want) {
                out.push((
                    kv.key().tp.clone(),
                    kv.key().part,
                    kv.key().group.clone(),
                    off,
                ));
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
    // - snapshot per (tp,part,group): { last_applied_event_offset, groupstate_blob }
    //
    // Recovery loads snapshots, then replays events AFTER the minimum snapshot offset,
    // skipping events already covered by each group's snapshot.

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

        // snapshot all groups for this partition (simple v0; later you can do incremental / max-bytes)
        for e in self.groups.iter() {
            if e.key().tp == tp && e.key().part == part {
                let group = e.key().group.clone();
                let blob = e.value().encode_snapshot();
                self.write_group_snapshot(tp, part, &group, applied_upto, &blob)?;
            }
        }

        // Future: after snapshotting, you can truncate events log safely (once Keratin supports it).
        // self.maybe_truncate_partition(tp, part).await?;

        Ok(())
    }

    fn write_group_snapshot(
        &self,
        tp: &str,
        part: u32,
        group: &str,
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

    fn read_group_snapshot(&self, path: &Path) -> Result<Option<(Offset, Vec<u8>)>> {
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
                let part = part_str.parse::<u32>().unwrap_or(0); // directory name is our own format
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
        // 1) Load all snapshot files for this partition into groups map + remember per-group snapshot offsets
        //    We cannot pre-know tp; snapshot directory is keyed by encoded tp, but the snapshot file
        //    itself does not embed tp/group. So: in v0, we load snapshots only when we know tp.
        //
        // Practical approach v0:
        // - Don’t “discover snapshots by scanning filesystem blindly”.
        // - Instead, do replay from 0 once per partition (still OK early).
        //
        // If you want fast restarts now, we do it properly:
        // - store snapshots under snapshots/<enc(tp)>/<part>/<enc(group)>.snap
        // - and during replay, when we see tp/group, we attempt to load its snapshot once.

        // We'll cache loaded snapshot offsets here:
        let snap_applied_for: DashMap<(String, String), Offset> = DashMap::new();
        // (tp, group) -> last_applied_event_offset

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
                let tp = ev.tp().to_string();
                self.logs
                    .entry((tp.clone(), part))
                    .or_insert_with(|| k.clone());

                // Best-effort: load snapshot for (tp,group) once, and if snapshot's last_applied >= rec.offset, skip.
                let group = ev.group().to_string();
                let key = (tp.clone(), group.clone());

                if !snap_applied_for.contains_key(&key) {
                    // try read snapshot file
                    let sp = self.snap_file(&tp, part, &group);
                    if let Some((snap_upto, blob)) = self.read_group_snapshot(&sp)? {
                        {
                            let mut gs = self.group_entry(&tp, part, &group);
                            gs.load_snapshot(&blob).map_err(io_err)?;
                        }
                        snap_applied_for.insert(key.clone(), snap_upto);
                    } else {
                        snap_applied_for.insert(key.clone(), 0);
                    }
                }

                let snap_upto = *snap_applied_for.get(&key).unwrap().value();
                if rec.offset <= snap_upto {
                    continue; // covered by snapshot
                }

                self.apply_event_inmem(&ev)?;
                self.applied_upto_entry(&tp, part)
                    .store(rec.offset + 1, Ordering::Release);
            }
        }

        Ok(())
    }

    // ---------------- Future: truncation hook ----------------
    //
    // Once Keratin supports truncate_before(before_offset),
    // you can compute a safe cutoff:
    //   cutoff = min(last_applied_event_offset in all snapshots for this partition)
    // and call log.truncate_before(cutoff).
    //
    // Until then, snapshots give fast startup even if the event log grows.
}

// TODO: add flags to avoid in release builds or such? with default
impl Stroma {
    pub async fn mark_inflight_one(
        &self,
        tp: &str,
        part: u32,
        group: &str,
        off: Offset,
        deadline: UnixMillis,
    ) -> Result<()> {
        self.mark_inflight_batch(tp, part, group, &[(off, deadline)])
            .await
    }

    pub async fn ack_one(&self, tp: &str, part: u32, group: &str, off: Offset) -> Result<()> {
        self.ack_batch(tp, part, group, &[off]).await
    }

    pub async fn snapshot_partition(&self, tp: &str, part: u32) -> Result<()> {
        let upto = self.applied_upto_entry(tp, part).load(Ordering::Acquire);
        self.write_snapshots_for_partition(tp, part, upto).await
    }

    pub async fn truncate_partition_log(&self, tp: &str, part: u32, before: Offset) -> Result<u64> {
        let log = self.log(tp, part).await?;
        log.truncate_before(before).await.map_err(io_err)
    }

    pub fn debug_dump_group(&self, tp: &str, part: u32, group: &str) -> String {
        let k = Key {
            tp: tp.into(),
            part,
            group: group.into(),
        };
        self.groups
            .get(&k)
            .map(|g| format!("{:#?}", g.canonical()))
            .unwrap_or_else(|| "<empty>".into())
    }

    pub fn validate(&self) -> Result<()> {
        for g in self.groups.iter() {
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
    fn group(&self) -> &str;
}

impl EventView for StromaEvent {
    fn tp(&self) -> &str {
        match self {
            StromaEvent::MarkInflight { tp, .. } => tp,
            StromaEvent::Ack { tp, .. } => tp,
            StromaEvent::ClearInflight { tp, .. } => tp,
            StromaEvent::ResetGroup { tp, .. } => tp,
            StromaEvent::Snapshot { tp, .. } => tp,
        }
    }
    fn part(&self) -> u32 {
        match self {
            StromaEvent::MarkInflight { part, .. } => *part,
            StromaEvent::Ack { part, .. } => *part,
            StromaEvent::ClearInflight { part, .. } => *part,
            StromaEvent::ResetGroup { part, .. } => *part,
            StromaEvent::Snapshot { part, .. } => *part,
        }
    }
    fn group(&self) -> &str {
        match self {
            StromaEvent::MarkInflight { group, .. } => group,
            StromaEvent::Ack { group, .. } => group,
            StromaEvent::ClearInflight { group, .. } => group,
            StromaEvent::ResetGroup { group, .. } => group,
            StromaEvent::Snapshot { group, .. } => group,
        }
    }
}
