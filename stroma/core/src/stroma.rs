use std::{
    fs,
    hash::{Hash, Hasher},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use keratin_log::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KDurability, Keratin,
    KeratinAppendCompletion, KeratinConfig, Message,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::{
    sync::{Notify, OnceCell, RwLock, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    DeclareMeta, Result, StromaError,
    event::{
        AckEventMeta, DeadLetterMeta, EnqueueDelayedEventMeta, EnqueueEventMeta, NackEventMeta,
        StromaEvent,
    },
    metrics::StromaMetrics,
    partition::Partition,
    state::{
        CustomDLQ, NackOutcome, Offset, QueueCommand, QueueHandle, QueueSharedBundle,
        QueueStatusReport, StromaDebugSnapshot, UnixMillis,
    },
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

pub struct PublishItem {
    pub headers: MessageHeaders,
    pub payload: Vec<u8>,
    pub not_before: Option<UnixMillis>,
    pub completion: Box<dyn AppendCompletion<IoError>>,
}

struct ItemMeta {
    not_before: Option<UnixMillis>,
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotConfig {
    /// Take snapshot every N durable events per (tp,part) (best-effort).
    pub every_events: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            every_events: 500_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GlobalDLQ {
    pub tp: String,
    pub part: u32,
    pub group: Option<String>,
}

impl GlobalDLQ {
    pub async fn new(tp: &str, part: u32, group: Option<&str>) -> Result<Self> {
        Ok(Self {
            tp: tp.to_string(),
            part,
            group: group.map(|s| s.into()),
        })
    }

    // TODO: Helper to create DLQ message, with metadata about original message. (stabilize headers format first)

    pub fn to_custom_dlq(&self) -> CustomDLQ {
        CustomDLQ {
            tp: self.tp.clone(),
            part: self.part,
            group: self.group.clone(),
        }
    }
}

pub struct ApplyThenComplete {
    stroma: Stroma,
    ev: StromaEvent,
    qh: QueueHandle,
    inner: Box<dyn AppendCompletion<IoError>>,
}

impl AppendCompletion<IoError> for ApplyThenComplete {
    fn complete(self: Box<Self>, res: std::result::Result<AppendResult, IoError>) {
        match res {
            Ok(ar) => {
                let stroma = self.stroma.clone();
                let ev = self.ev.clone();
                let inner = self.inner;

                match stroma.enqueue_event_inmem(ev, &self.qh) {
                    Ok(()) => {
                        // let _ = tx.send(Ok(ar));
                        self.qh
                            .applied_upto()
                            .fetch_max(ar.base_offset + ar.count as u64, Ordering::Relaxed);
                        inner.complete(Ok(ar));
                    }
                    Err(e) => inner.complete(Err(IoError::new(e.to_string()))),
                }
            }
            Err(e) => {
                self.inner.complete(Err(IoError::new(e.to_string())));
            }
        }
    }
}

impl ApplyThenComplete {
    pub fn new(
        stroma: Stroma,
        ev: StromaEvent,
        qh: QueueHandle,
        inner: Box<dyn AppendCompletion<IoError>>,
    ) -> Box<Self> {
        Box::new(Self {
            stroma,
            ev,
            qh,
            inner,
        })
    }
}

struct CompletionItem {
    meta: ItemMeta,
    completion: Box<dyn AppendCompletion<IoError>>,
}

/// Completion for the msg_log batch in append_message_batch.
/// Once msg-log durability is reached, emits one EnqueueMany event_log entry,
/// then fans out per client completions with assigned offsets.
struct MsgBatchCompletion {
    stroma: Stroma,
    tp: Box<str>,
    part: u32,
    group: Option<Box<str>>,
    items: Vec<CompletionItem>,
    durability: KDurability,
    runtime: tokio::runtime::Handle,
}

impl MsgBatchCompletion {
    fn new(
        stroma: Stroma,
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        items: Vec<CompletionItem>,
        durability: KDurability,
    ) -> Box<Self> {
        Box::new(Self {
            stroma,
            tp,
            part,
            group,
            items,
            durability,
            runtime: tokio::runtime::Handle::current(),
        })
    }
}

impl AppendCompletion<IoError> for MsgBatchCompletion {
    fn complete(self: Box<Self>, res: std::result::Result<AppendResult, IoError>) {
        let Self {
            stroma,
            tp,
            part,
            group,
            items,
            durability,
            runtime,
        } = *self;

        let ar = match res {
            Ok(ar) => ar,
            Err(err) => {
                let err_msg = err.to_string();
                for CompletionItem {
                    meta: _,
                    completion: c,
                } in items
                {
                    c.complete(Err(IoError::new(err_msg.clone())));
                }
                return;
            }
        };

        let base = ar.base_offset;
        let count = ar.count as u64;

        if count != items.len() as u64 {
            tracing::error!(
                "msg-log batch returned count={} but we had {} completions",
                count,
                items.len()
            );
            // Continue anyway with whichever is smaller, fan out will not go past either
        }

        // Build EnqueueMany events for the event log
        let mut immediate = Vec::new();
        let mut delayed = Vec::new();
        for (i, CompletionItem { meta, .. }) in items.iter().enumerate() {
            let off = base + i as u64;
            match meta.not_before {
                None => immediate.push(EnqueueEventMeta { off, retries: 0 }),
                Some(nb) => delayed.push(EnqueueDelayedEventMeta {
                    off,
                    not_before: nb,
                }),
            }
        }

        let mut events = Vec::with_capacity(2);
        if !immediate.is_empty() {
            events.push(StromaEvent::EnqueueMany { reqs: immediate });
        }
        if !delayed.is_empty() {
            events.push(StromaEvent::EnqueueDelayedMany { reqs: delayed });
        }

        // Spawn the event_log append + fan-out. We are inside a sync completion
        // callback (called from the writer thread), so we cannot await directly.
        // The completion thread should not block, we hand off to the runtime
        runtime.spawn(async move {
            match stroma
                .append_events_durable(&tp, part, group.as_deref(), events, durability)
                .await
            {
                Ok(_) => {
                    for (
                        i,
                        CompletionItem {
                            meta: _,
                            completion: c,
                        },
                    ) in items.into_iter().enumerate()
                    {
                        c.complete(Ok(AppendResult {
                            base_offset: base + i as u64,
                            count: 1,
                        }));
                    }
                }
                Err(err) => {
                    let err_msg = err.to_string();
                    for CompletionItem {
                        meta: _,
                        completion: c,
                    } in items
                    {
                        c.complete(Err(IoError::new(err_msg.clone())));
                    }
                }
            }
        });
    }
}

#[derive(Debug)]
pub struct TaskGroup {
    token: CancellationToken,
    tracker: tokio_util::task::TaskTracker,
}

impl TaskGroup {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            tracker: tokio_util::task::TaskTracker::new(),
        }
    }

    /// Spawn with automatic cancellation on shutdown.
    /// Future is dropped at its current await point when shutdown fires.
    pub fn spawn<F>(&self, name: &'static str, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.token.is_cancelled() {
            return;
        }
        let cancel = self.token.child_token();
        self.spawn_raw(name, async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = fut => {}
            }
        });
    }

    /// Spawn with the cancellation token exposed.
    /// Use when the task needs to clean up at a safe point on shutdown.
    pub fn spawn_with_cancel<F, Fut>(&self, name: &'static str, make_fut: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.token.is_cancelled() {
            return;
        }
        let cancel = self.token.child_token();
        self.spawn_raw(name, make_fut(cancel));
    }

    fn spawn_raw<F>(&self, _name: &'static str, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        #[cfg(tokio_unstable)]
        let _ = self.tracker.build_task().name(_name).spawn(fut);
        #[cfg(not(tokio_unstable))]
        {
            let _ = _name;
            self.tracker.spawn(fut);
        }
    }

    pub async fn shutdown(&self) {
        self.token.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}

impl Drop for TaskGroup {
    fn drop(&mut self) {
        self.token.cancel();
        self.tracker.close();
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageHeaders {
    pub published: u64,
    pub publish_received: u64,
    pub extra: HashMap<String, String>,
}

impl MessageHeaders {
    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(|err| StromaError::Decode(err.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(|err| StromaError::Decode(err.to_string()))
    }
}

type Registry = HashMap<(Box<str>, u32, Option<Box<str>>), Arc<OnceCell<QueueHandle>>>;

#[derive(Debug, Clone)]
pub struct Stroma {
    pub(crate) start_time: Instant,
    pub(crate) root: PathBuf,
    pub(crate) keratin_cfg_msg: KeratinConfig,
    pub(crate) keratin_cfg_event: KeratinConfig,
    pub(crate) snap_cfg: SnapshotConfig,

    pub(crate) task_group: Arc<TaskGroup>,

    // Materialized queue state
    queue_handles: Arc<ArcSwap<Registry>>,

    // TODO: Consider using parking lot
    // Global DLQ topic
    pub(crate) global_dlq: Arc<RwLock<Option<GlobalDLQ>>>,

    pub(crate) msg_count: Arc<AtomicU64>,

    pub(crate) event_count: Arc<AtomicU64>,

    pub(crate) metrics: Arc<StromaMetrics>,

    earliest_pending_deadline_sender: tokio::sync::watch::Sender<Option<UnixMillis>>,
    earliest_pending_deadline_receiver: tokio::sync::watch::Receiver<Option<UnixMillis>>,
    pub(crate) deadline_waker: Arc<Notify>,
    initial_recovery_complete: Arc<AtomicBool>,
}

impl Stroma {
    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg_msg: KeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self> {
        let start_time = Instant::now();
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("events")).map_err(io_err)?;
        fs::create_dir_all(root.join("messages")).map_err(io_err)?;
        fs::create_dir_all(root.join("snapshots")).map_err(io_err)?;
        fs::create_dir_all(root.join("tmp")).map_err(io_err)?;

        let metrics = Arc::new(StromaMetrics::new(60));
        let keratin_cfg_event = KeratinConfig {
            flush_target_bytes: keratin_cfg_msg.flush_target_bytes / 8,
            max_batch_bytes: keratin_cfg_msg.max_batch_bytes / 8,
            index_stride_bytes: keratin_cfg_msg.index_stride_bytes / 8,
            segment_max_bytes: keratin_cfg_msg.segment_max_bytes / 8,
            ..keratin_cfg_msg
        };

        let (earliest_pending_deadline_sender, earliest_pending_deadline_receiver) =
            tokio::sync::watch::channel(None);

        let st = Self {
            start_time,
            root,
            keratin_cfg_msg,
            keratin_cfg_event,
            snap_cfg,
            task_group: Arc::new(TaskGroup::new()),
            queue_handles: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
            global_dlq: Arc::new(RwLock::new(None)),
            msg_count: Arc::new(AtomicU64::new(0)),
            event_count: Arc::new(AtomicU64::new(0)),
            metrics: metrics.clone(),
            earliest_pending_deadline_sender,
            earliest_pending_deadline_receiver,
            deadline_waker: Arc::new(Notify::new()),
            initial_recovery_complete: Arc::new(AtomicBool::new(false)),
        };

        // Recover from existing snapshot files + replay events.
        // TODO: do it lazily
        st.recover_all().await?;

        st.initial_recovery_complete.store(true, Ordering::Release);
        st.mark_all_queue_recoveries_complete();

        let st_metrics = st.clone();
        st.task_group.spawn("debug report", async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;

                let res = st_metrics.debug_report().await;
                if let Ok(report) = res {
                    println!("{report}");
                } else if let Err(err) = res {
                    eprintln!("{err}");
                }
            }
        });

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

    pub fn metrics(&self) -> Arc<StromaMetrics> {
        self.metrics.clone()
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

    fn dec_component(s: &str) -> Result<String> {
        let bytes = s.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;

        while i < bytes.len() {
            match bytes[i] {
                b'%' if i + 2 < bytes.len() => {
                    let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                        .map_err(|e| StromaError::Decode(e.to_string()))?;
                    let b = u8::from_str_radix(hex, 16)
                        .map_err(|e| StromaError::Decode(e.to_string()))?;
                    out.push(b);
                    i += 3;
                }
                b => {
                    out.push(b);
                    i += 1;
                }
            }
        }

        String::from_utf8(out).map_err(|e| StromaError::Decode(e.to_string()))
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
        let k = Keratin::open(dir, self.keratin_cfg_event)
            .await
            .map_err(io_err)?;
        tracing::info!("Initialized event log: (`{tp}` `{part}` `{group:?}`)");

        Ok(Arc::new(k))
    }

    async fn msg_log_init(&self, tp: &str, part: u32, group: Option<&str>) -> Result<Arc<Keratin>> {
        let dir = self.msg_tp_part_dir(tp, part, group);
        fs::create_dir_all(&dir).map_err(io_err)?;

        tracing::info!("Initializing message log: (`{tp}` `{part}` `{group:?}`)");
        let k = Keratin::open(dir, self.keratin_cfg_msg)
            .await
            .map_err(io_err)?;
        tracing::info!("Initialized message log: (`{tp}` `{part}` `{group:?}`)");

        Ok(Arc::new(k))
    }

    pub fn deadline_waker(&self) -> Arc<Notify> {
        self.deadline_waker.clone()
    }

    pub async fn queue_handle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueHandle> {
        let cell = loop {
            let current = self.queue_handles.load();

            let key = (tp.into(), part, group.map(|s| s.into()));
            if let Some(cell) = current.get(&key) {
                break cell.clone();
            }

            let new_cell = Arc::new(OnceCell::new());
            let mut next = (**current).clone();
            next.insert(key.clone(), new_cell.clone());

            tracing::debug!("Attempting to insert queue handle for ({tp}, {part}, {group:?})...");
            // swap in the new map only if snapshot is still current
            let prev = self
                .queue_handles
                .compare_and_swap(&current, Arc::new(next));
            tracing::debug!(
                "compare_and_swap result for ({tp}, {part}, {group:?}): {}",
                if Arc::ptr_eq(&prev, &current) {
                    "success"
                } else {
                    "failure"
                }
            );

            if Arc::ptr_eq(&prev, &current) {
                break new_cell;
            }

            // lost race; retry
            tracing::debug!(
                "Lost race to insert queue handle for ({tp}, {part}, {group:?}), retrying..."
            );
            // make numeric hash of topic/part/group and sleep that long to reduce contention in high concurrency scenarios.
            let hash = {
                use std::collections::hash_map::DefaultHasher;
                let mut hasher = DefaultHasher::new();
                tp.hash(&mut hasher);
                part.hash(&mut hasher);
                group.hash(&mut hasher);
                hasher.finish()
            };
            let sleep_us = hash % 1000;
            tracing::debug!("Sleeping for {sleep_us} microseconds before retrying...");
            tokio::time::sleep(tokio::time::Duration::from_micros(sleep_us)).await;
        };

        let qh = cell
            .get_or_try_init(|| async {
                let msg_log = self.msg_log_init(tp, part, group).await?;
                let event_log = self.event_log_init(tp, part, group).await?;
                let global_dlq = self.global_dlq.clone();
                let bundle = QueueSharedBundle {
                    event_log,
                    msg_log,
                    task_group: self.task_group.clone(),
                    metrics: self.metrics.clone(),
                    global_dlq,
                    deadline_waker: self.deadline_waker.clone(),
                };

                let qh = QueueHandle::init(tp.into(), part, group.map(|s| s.into()), bundle);

                if self.initial_recovery_complete.load(Ordering::Acquire) {
                    qh.mark_recovery_complete();
                }

                self.periodic_snapshot(qh.clone());

                Ok(qh)
            })
            .await?;

        Ok(qh.clone())
    }

    async fn ensure_queue(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        self.queue_handle(tp, part, group).await?;

        Ok(())
    }

    fn mark_all_queue_recoveries_complete(&self) {
        let current = self.queue_handles.load();

        for cell in current.values() {
            if let Some(qh) = cell.get() {
                qh.mark_recovery_complete();
            }
        }
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

    fn queue_handle_sync(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> std::io::Result<QueueHandle> {
        let current = self.queue_handles.load();
        let key = (tp.into(), part, group.map(|s| s.into()));
        let cell = current.get(&key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "queue not found for event: tp={} part={} group={:?}",
                    tp, part, group
                ),
            )
        })?;
        cell.get()
            .cloned()
            .ok_or_else(|| io::Error::other("queue handle not initialized"))
    }

    fn enqueue_event_inmem(&self, ev: StromaEvent, qh: &QueueHandle) -> std::io::Result<()> {
        tracing::debug!("Applying event: {ev:?}");
        match ev {
            StromaEvent::Enqueue { off, retries } => {
                let command = QueueCommand::Enqueue {
                    offset: off,
                    retries,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::EnqueueMany { reqs } => {
                let command = QueueCommand::EnqueueMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::EnqueueDelayed { off, not_before } => {
                let command = QueueCommand::EnqueueDelayed {
                    offset: off,
                    not_before,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::EnqueueDelayedMany { reqs } => {
                let command = QueueCommand::EnqueueDelayedMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::MarkInflight { off, deadline } => {
                let command = QueueCommand::MarkInflight {
                    offset: off,
                    deadline,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::MarkInflightMany { reqs } => {
                let command = QueueCommand::MarkInflightMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::Ack { off } => {
                let command = QueueCommand::Ack {
                    offset: off,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
            }
            StromaEvent::AckMany { reqs } => {
                let command = QueueCommand::AckMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
            }
            StromaEvent::Nack { off, requeue } => {
                let command = QueueCommand::Nack {
                    offset: off,
                    requeue,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
            }
            StromaEvent::NackMany { reqs } => {
                let command = QueueCommand::NackMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
            }
            StromaEvent::DeadLetter { reqs } => {
                // On replay we just mark pending; recovery scan will re-issue copies.
                let offsets: Vec<Offset> = reqs.iter().map(|r| r.off).collect();
                // We need state.mark_pending_dlq, OR fold via nack(_, false)+pending insert.
                // Cleanest: add an explicit MarkPendingDlq command for replay.
                qh.blocking_command_enqueue(QueueCommand::MarkPendingDlq {
                    offsets,
                    response: None,
                })?;
            }
            StromaEvent::DeadLetterCommit { offs } => {
                qh.blocking_command_enqueue(QueueCommand::DeadLetterCommit {
                    offsets: offs,
                    response: None,
                })?;
            }
            StromaEvent::Declare(m) => {
                qh.blocking_command_enqueue(QueueCommand::Declare {
                    meta: m,
                    response: None,
                })?;
            }
            StromaEvent::ResetQueue { tp, part, group } => {
                self.remove_queue(&tp, part, group.as_deref());
                // TODO: More cleanup?
            }
            StromaEvent::Snapshot { .. } => {
                // If you keep Snapshot events inside the event log later, you'd load it here.
                // With file snapshots, we don't emit Snapshot events, so this is unused.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Snapshot event unsupported in v0",
                ));
            }
        }
        Ok(())
    }

    async fn apply_event_inmem(&self, ev: StromaEvent, qh: &QueueHandle) -> Result<()> {
        tracing::debug!("Applying event: {ev:?}");
        match ev {
            StromaEvent::Enqueue { off, retries } => {
                qh.enqueue(off, retries).await;
            }
            StromaEvent::EnqueueMany { reqs } => {
                qh.enqueue_many(reqs).await;
            }
            StromaEvent::EnqueueDelayed { off, not_before } => {
                qh.enqueue_delayed(off, not_before).await;
            }
            StromaEvent::EnqueueDelayedMany { reqs } => {
                qh.enqueue_delayed_many(reqs).await;
            }
            StromaEvent::MarkInflight { off, deadline } => {
                qh.mark_inflight(off, deadline).await;
            }
            StromaEvent::MarkInflightMany { reqs } => {
                qh.mark_inflight_batch(reqs).await;
            }
            StromaEvent::Ack { off } => {
                // Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                qh.ack(off).await;
            }
            StromaEvent::AckMany { reqs } => {
                // Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                qh.ack_many(reqs).await;
            }
            StromaEvent::Nack { off, requeue } => {
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
                qh.nack(off, requeue).await;
            }
            StromaEvent::NackMany { reqs } => {
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
                qh.nack_many(reqs).await;
            }
            StromaEvent::DeadLetter { reqs } => {
                // On replay we just mark pending; recovery scan will re-issue copies.
                let offsets: Vec<Offset> = reqs.iter().map(|r| r.off).collect();
                // We need state.mark_pending_dlq, OR fold via nack(_, false)+pending insert.
                // Cleanest: add an explicit MarkPendingDlq command for replay.
                qh.mark_pending_dlq_many(offsets).await;
            }
            StromaEvent::DeadLetterCommit { offs } => {
                qh.dead_letter_commit(offs).await;
            }
            StromaEvent::Declare(meta) => {
                qh.declare(meta).await;
            }
            StromaEvent::ResetQueue { tp, part, group } => {
                self.remove_queue(&tp, part, group.as_deref());
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
        Ok(())
    }

    async fn append_events_durable(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        evs: Vec<StromaEvent>,
        durability: KDurability,
    ) -> Result<Offset> {
        if evs.is_empty() {
            return Ok(self
                .applied_upto_entry(tp, part, group)
                .await?
                .load(Ordering::Acquire));
        }

        // let _timer = Timer::new(&self.metrics.event_log_appends.batches.latency);
        let start = Instant::now();

        let qh = self.queue_handle(tp, part, group).await?;
        let event_log = qh.event_log();
        let mut msgs = Vec::with_capacity(evs.len());
        let msgs_count = msgs.len();
        for ev in &evs {
            msgs.push(event_msg(ev)?);
        }
        let bytes_count: usize = msgs.iter().map(|m| m.bytes_len()).sum();

        // Durable append first.
        let ar = event_log
            .append_batch(msgs, Some(durability))
            .await
            .map_err(io_err)?;

        self.metrics
            .event_log_appends
            .observe(msgs_count, bytes_count);
        qh.applied_upto()
            .fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        qh.set_dirty_snapshot(true);

        // Apply in memory after durable accept.
        for ev in evs.into_iter() {
            self.apply_event_inmem(ev, &qh).await?;
        }

        // Update applied watermark:
        let new_upto = event_log.head_offset();
        // TODO:
        // self.applied_upto_entry(tp, part, group)
        //     .await?
        //     .store(new_upto, Ordering::Release);

        self.metrics
            .event_log_appends
            .batches
            .latency
            .observe(start.elapsed());

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
            evs.push(StromaEvent::MarkInflight { off, deadline });
        }

        let _upto = self
            .append_events_durable(tp, part, group, evs, KDurability::AfterFsync)
            .await?;

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
            evs.push(StromaEvent::Ack { off });
        }

        let _upto = self
            .append_events_durable(&tp, part, group, evs, KDurability::AfterFsync)
            .await?;

        Ok(())
    }

    pub async fn add_to_redelivery(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<()> {
        let ev1 = StromaEvent::Nack { off, requeue: true };
        let _upto = self
            .append_events_durable(tp, part, group, vec![ev1], KDurability::AfterFsync)
            .await?;

        Ok(())
    }

    pub async fn requeue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<()> {
        let ev = StromaEvent::Nack { off, requeue: true };
        let _upto = self
            .append_events_durable(tp, part, group, vec![ev], KDurability::AfterFsync)
            .await?;

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

    fn signal_earlier_deadline(&self, deadline_ms: UnixMillis) {
        self.earliest_pending_deadline_sender
            .send_replace(Some(deadline_ms));
    }

    async fn wait_for_earlier_deadline(&mut self) -> Result<Option<UnixMillis>> {
        Ok(*self.earliest_pending_deadline_receiver.borrow_and_update())
    }

    pub async fn collect_expired(
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
        let expired = self.collect_expired(now, max).await?;
        let expired_set: HashSet<(String, u32, Option<String>, u64)> =
            HashSet::from_iter(expired.clone().into_iter());

        let mut events_per_queue =
            HashMap::<(String, u32, Option<String>), Vec<NackEventMeta>>::new();

        for (tp, part, group, off) in expired {
            let meta = NackEventMeta { off, requeue: true };

            let entry = events_per_queue.entry((tp, part, group)).or_default();
            entry.push(meta);
        }

        let mut awaiters = Vec::new();
        for ((tp, part, group), reqs) in events_per_queue {
            let (completion, rx) = KeratinAppendCompletion::pair();
            self.nack_enqueue_many(&tp, part, group.as_deref(), reqs, completion)
                .await?;
            awaiters.push(rx);
        }

        for awaiter in awaiters {
            awaiter
                .await
                .map_err(|_err| StromaError::Io("Broken pipe".into()))?
                .map_err(|err| StromaError::Io(err.to_string()))?;
        }

        Ok(expired_set)
    }

    /// ---------------- Snapshotting ----------------
    ///
    /// Snapshot files make restart fast:
    /// - durable event log = Keratin partition log
    /// - snapshot per (tp,part): { last_applied_event_offset, queue_state_blob }
    ///
    /// Recovery loads snapshots, then replays events AFTER the minimum snapshot offset,
    /// skipping events already covered by each queue's snapshot.
    async fn periodic_snapshot_step(stroma: &Stroma, qh: &QueueHandle) -> Result<()> {
        if qh.creating_snapshot() {
            tracing::info!("Snapshot already in progress, skipping..");
            return Ok::<(), StromaError>(());
        }
        if !qh.dirty_snapshot() {
            tracing::info!(
                "Snapshot for {}: {} {} is not dirty, skipping..",
                qh.topic(),
                qh.group().unwrap_or("Default"),
                qh.partition()
            );
            return Ok::<(), StromaError>(());
        }
        let msg_log = qh.msg_log();
        let safe_msg_truncate = qh.lowest_not_acked_offset().await;
        let applied_upto = qh.applied_upto().load(Ordering::Relaxed);
        // let every = stroma.snap_cfg.every_events.max(1);
        // let last = qh.last_snapshot_event_offset();
        // if applied_upto - last < every {
        //     return Ok(());
        // }
        let tp = qh.topic();
        let part = qh.partition();
        let group = qh.group();
        tracing::info!("Writing log snapshot until {applied_upto}..");
        stroma
            .write_snapshots_for_partition(qh.clone(), applied_upto)
            .await?;
        let event_log = qh.event_log();
        let event_head = event_log
            .truncate_before(applied_upto)
            .await
            .map_err(io_err)?;
        tracing::info!(
            "event truncate tp={} part={} group={:?} before={} -> new_head={}",
            tp,
            part,
            group,
            applied_upto,
            event_head
        );
        let msg_head = msg_log
            .truncate_before(safe_msg_truncate)
            .await
            .map_err(io_err)?;
        tracing::info!(
            "message truncate tp={} part={} group={:?} before={} -> new_head={}",
            tp,
            part,
            group,
            safe_msg_truncate,
            msg_head
        );
        qh.set_dirty_snapshot(false);
        Ok(())
    }

    fn periodic_snapshot(&self, qh: QueueHandle) {
        if !qh.try_start_snapshot_task() {
            return;
        }

        let stroma = self.clone();

        self.task_group.spawn("periodic snapshot", async move {
            qh.wait_recovery_complete().await;

            tracing::info!(
                "Starting periodic snapshot service for tp={} part={} group={}",
                qh.topic(),
                qh.partition(),
                qh.group().unwrap_or("Default")
            );

            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            ticker.tick().await;

            loop {
                ticker.tick().await;

                let res = Self::periodic_snapshot_step(&stroma, &qh).await;
                if let Err(err) = res {
                    tracing::error!("Error during periodic snapshot: {err}");
                }
            }
        });
    }

    async fn write_snapshots_for_partition(
        &self,
        qh: QueueHandle,
        applied_upto: Offset,
    ) -> Result<()> {
        let tp = qh.topic();
        let part = qh.partition();
        let group = qh.group();
        let qh: QueueHandle = self.queue_handle(tp, part, group).await?;
        let blob = if let Some(blob) = qh.encode_snapshot(applied_upto).await {
            blob
        } else {
            return Ok(());
        };

        let dir = self.snap_dir(tp, part, group);
        let (tp, group) = (tp.to_string(), group.map(|s| s.to_string()));
        let stroma = self.clone();
        tokio::task::spawn_blocking(move || {
            fs::create_dir_all(&dir).map_err(io_err)?;
            stroma.write_queue_snapshot(&tp, part, group.as_deref(), applied_upto, &blob)
        })
        .await
        .map_err(|e| StromaError::Io(e.to_string()))??;

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
        let partitions = self.discover_partitions()?; // decoded names
        let max_parallel = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);

        let permits = Arc::new(Semaphore::new(max_parallel));
        let mut tasks = JoinSet::new();

        for (group, tp, part) in partitions {
            let stroma = self.clone();
            let permits = permits.clone();

            tasks.spawn(async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|err| StromaError::Io(err.to_string()))?;

                let qh = stroma.queue_handle(&tp, part, group.as_deref()).await?;
                let event_log = qh.event_log();

                stroma
                    .recover_one_log(&tp, part, group.as_deref(), event_log)
                    .await?;

                qh.mark_recovery_complete();

                Ok::<(), StromaError>(())
            });
        }

        while let Some(joined) = tasks.join_next().await {
            joined.map_err(|err| StromaError::Io(err.to_string()))??;
        }

        Ok(())
    }

    async fn recover_one_log(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        event_log: Arc<Keratin>,
    ) -> Result<()> {
        let start = Instant::now();

        tracing::info!(
            "Recovering log tp: {tp}, partition: {part}, group: {}",
            group.unwrap_or("Default")
        );
        let qh = self.queue_handle(tp, part, group).await?;

        let snap_load_start = Instant::now();
        // load snapshot...

        let mut cur = 0u64;

        if let Some((applied_upto, blob)) =
            self.read_queue_snapshot(&self.snap_file(tp, part, group))?
        {
            qh.load_snapshot(blob).await.map_err(io_err)?;
            qh.applied_upto().store(applied_upto, Ordering::Release);
            cur = applied_upto;
        }

        let reader = event_log.reader();
        let tail = event_log.next_offset();

        self.metrics
            .recovery
            .snapshot_load_latency
            .observe(snap_load_start.elapsed());

        let replay_start = Instant::now();
        let mut events_count = 0;

        let qh_clone = qh.clone();
        let events = tokio::task::spawn_blocking(move || {
            let mut events = Vec::new();
            let _: Result<()> = {
                while cur < tail {
                    let batch = reader.scan_from(cur, 10_000).map_err(io_err)?;
                    if batch.is_empty() {
                        break;
                    }

                    for rec in batch {
                        cur = rec.offset + 1;
                        let ev = StromaEvent::decode(&rec.payload).map_err(decode_err)?;
                        events.push(ev);
                        // self.apply_event_inmem(ev, &qh).await?;
                        // stroma
                        //     .enqueue_event_inmem(ev, &qh_clone)
                        //     .map_err(|err| StromaError::Io(err.to_string()))?;
                        qh_clone.applied_upto().store(cur, Ordering::Release);
                        events_count += 1;
                    }
                }

                Ok(())
            };
            Ok(events)
        })
        .await
        .map_err(|err| StromaError::Io(err.to_string()))??;

        for ev in events {
            self.apply_event_inmem(ev, &qh).await?;
        }

        let pending = qh.pending_dlq().await;
        let source_tp = tp;
        let source_part = part;
        let source_group = group;
        let target = qh.get_dlq_target().await;
        let src = (
            source_tp.to_string(),
            source_part,
            source_group.map(|s| s.into()),
        );
        for (off, _target) in pending {
            // We don't have the resolved target stored in state — only in the DeadLetter event.
            // Two options:
            //   (a) walk the event log backward to find the matching DeadLetter event for this offset
            //   (b) re-resolve via current dlq_policy
            // (b) is simpler and matches "policy is mutable", (a) is more faithful to original intent.
            // oon recovery, the *current* policy wins.
            match target {
                Some((ref tp, part, ref grp)) => {
                    let stroma = self.clone();
                    let qh2 = qh.clone();
                    let meta = DeadLetterMeta {
                        off,
                        target_tp: tp.clone().into(),
                        target_part: part,
                        target_group: grp.clone().map(Into::into),
                    };
                    let src = src.clone();
                    tokio::spawn(async move {
                        stroma.dlq_copy_then_commit(src, qh2, meta).await;
                    });
                }
                None => {
                    // Policy is now Discard -> ack locally.
                    self.commit_dlq_event(&qh, vec![off]).await;
                }
            }
        }

        let elapsed = start.elapsed();
        tracing::info!(
            "Recovered log tp: {tp}, partition: {part}, group: {} after {:.3} seconds",
            group.unwrap_or("Default"),
            elapsed.as_secs_f64(),
        );
        self.metrics
            .recovery
            .events_replayed
            .fetch_add(events_count as u64, Ordering::Relaxed);
        self.metrics
            .recovery
            .replay_duration
            .observe(replay_start.elapsed());

        self.metrics.recovery.startup_duration.observe(elapsed);

        Ok(())
    }

    pub fn discover_partitions(&self) -> Result<Vec<(Option<String>, String, u32)>> {
        let root = self.events_root();

        if !root.exists() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        for lvl1 in fs::read_dir(&root).map_err(io_err)? {
            let lvl1 = lvl1.map_err(io_err)?;
            if !lvl1.file_type().map_err(io_err)?.is_dir() {
                continue;
            }

            let lvl1_name_enc = lvl1.file_name().to_string_lossy().to_string();
            let lvl1_path = lvl1.path();

            // --- detect layout ---
            let mut has_partition_dirs = false;

            for e in fs::read_dir(&lvl1_path).map_err(io_err)? {
                let e = e.map_err(io_err)?;
                if e.file_type().map_err(io_err)?.is_dir()
                    && e.file_name().to_string_lossy().parse::<u32>().is_ok()
                {
                    has_partition_dirs = true;
                    break;
                }
            }

            if has_partition_dirs {
                // ---- legacy: lvl1 = topic ----
                let tp = Self::dec_component(&lvl1_name_enc)?;
                collect_parts_decoded(None, tp, &lvl1_path, &mut out)?;
            } else {
                // ---- grouped: lvl1 = group ----
                let group = Self::dec_component(&lvl1_name_enc)?;

                for tp_ent in fs::read_dir(&lvl1_path).map_err(io_err)? {
                    let tp_ent = tp_ent.map_err(io_err)?;
                    if !tp_ent.file_type().map_err(io_err)?.is_dir() {
                        continue;
                    }

                    let tp_enc = tp_ent.file_name().to_string_lossy().to_string();
                    let tp = Self::dec_component(&tp_enc)?;

                    collect_parts_decoded(Some(group.clone()), tp, &tp_ent.path(), &mut out)?;
                }
            }
        }

        Ok(out)
    }

    fn remove_queue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Option<Arc<OnceCell<QueueHandle>>> {
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
                q.shutdown().await;
                q.event_log().shutdown().await.map_err(io_err)?;
                q.msg_log().shutdown().await.map_err(io_err)?;
            }
        }
        self.task_group.shutdown();
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

    pub async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot> {
        let keys = self.queue_keys_snapshot();

        use futures::stream::{FuturesUnordered, StreamExt};

        let mut futs = FuturesUnordered::new();
        for (tp, part, group) in keys {
            let stroma = self.clone();
            futs.push(async move {
                let qh = stroma.queue_handle(&tp, part, group.as_deref()).await?;
                Ok::<_, StromaError>(qh.full_debug_info().await)
            });
        }

        let mut queues = Vec::with_capacity(futs.len());
        while let Some(result) = futs.next().await {
            queues.push(result?);
        }

        Ok(StromaDebugSnapshot {
            queue_count: queues.len(),
            queues,
            cmd_queue_depths: self.metrics.cmd_queue_depths_snapshot(),
            snapshot_metrics: self.metrics.snapshot.snapshot(),
            recovery_metrics: self.metrics.recovery.snapshot(),
            log_metrics: self.metrics.log_snapshot(),
            command_metrics: self.metrics.command_snapshot(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
        })
    }

    pub async fn debug_report(&self) -> Result<String> {
        let snap = self.debug_snapshot().await?;
        let mut out = String::new();
        use std::fmt::Write;

        writeln!(out, "=== Stroma debug report ===").unwrap();
        writeln!(out, "Uptime: {}s", snap.uptime_seconds).unwrap();
        writeln!(out, "Active queues: {}", snap.queue_count).unwrap();
        writeln!(out).unwrap();

        writeln!(out, "Command queue depths:").unwrap();
        for (lane, depth) in &snap.cmd_queue_depths {
            writeln!(out, "  {}: {}", lane, depth).unwrap();
        }
        writeln!(out).unwrap();

        writeln!(out, "Snapshots:").unwrap();
        writeln!(out, "  attempts: {}", snap.snapshot_metrics.attempts_total).unwrap();
        writeln!(
            out,
            "  skipped (not dirty): {}",
            snap.snapshot_metrics.skipped_not_dirty
        )
        .unwrap();
        if let Some(avg) = snap.snapshot_metrics.avg_clone_ms {
            writeln!(out, "  avg clone: {:.1}ms", avg).unwrap();
        }
        if let Some(avg) = snap.snapshot_metrics.avg_total_ms {
            writeln!(out, "  avg total: {:.1}ms", avg).unwrap();
        }
        writeln!(out).unwrap();

        writeln!(out, "Queues:").unwrap();
        for q in &snap.queues {
            let g = q.group.as_deref().unwrap_or("Default");
            writeln!(
                out,
                "  {}/{}/{}: ready={} inflight={} settled={} dirty={}",
                q.topic,
                q.partition,
                g,
                q.state.ready_count,
                q.state.inflight_count,
                q.state.settled_until,
                q.dirty_since_snapshot
            )
            .unwrap();
        }

        Ok(out)
    }
}

impl Stroma {
    /// Append a batch of messages with one msg-log batch and one event-log batch.
    /// Each client gets its own completion that fires with its assigned offset
    /// after both the msg-log AND event-log batches complete durably.
    pub async fn append_message_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        self.ensure_queue(tp, part, group).await?;
        let qh = self.queue_handle(tp, part, group).await?;
        let msg_log = qh.msg_log();

        // Build msg_log batch and extract per client completions.
        // Per message header encode failures fail just that one completion.
        let mut messages = Vec::with_capacity(items.len());
        let mut completion_items = Vec::with_capacity(items.len());
        for item in items {
            let PublishItem {
                headers,
                payload,
                completion,
                not_before,
            } = item;
            let header_bytes = match headers.encode() {
                Ok(b) => b,
                Err(err) => {
                    completion.complete(Err(IoError::new(err.to_string())));
                    continue;
                }
            };
            messages.push(Message {
                flags: 0,
                headers: header_bytes,
                payload,
            });
            completion_items.push(CompletionItem {
                meta: ItemMeta { not_before },
                completion,
            });
        }

        if messages.is_empty() {
            return Ok(());
        }

        // Custom completion that:
        //   1. fires when the msg_log batch is durably accepted
        //   2. emits ONE event_log batch with EnqueueMany
        //   3. fans out per client completions with their assigned offsets
        let stroma = self.clone();
        let tp_box: Box<str> = tp.into();
        let group_box: Option<Box<str>> = group.map(|s| s.into());

        let msg_completion = MsgBatchCompletion::new(
            stroma,
            tp_box,
            part,
            group_box,
            completion_items,
            self.keratin_cfg_msg.default_durability,
        );

        msg_log
            .append_batch_enqueue(
                messages,
                Some(self.keratin_cfg_msg.default_durability),
                msg_completion,
            )
            .map_err(io_err)?;

        Ok(())
    }

    pub async fn append_message(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        event_completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let (msg_completion, msg_rx) = KeratinAppendCompletion::pair();
        let msg_log = self.queue_handle(tp, part, group).await?.msg_log();
        self.ensure_queue(tp, part, group).await?;
        msg_log
            .append_enqueue(
                Message {
                    flags: 0,
                    headers: headers.encode()?,
                    payload,
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
                off: msg_offset,
            };

            match stroma
                .append_events_durable(
                    &tp,
                    part,
                    group.as_deref(),
                    vec![ev],
                    stroma.keratin_cfg_msg.default_durability,
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
        let ev = StromaEvent::Ack { off: offset };

        let qh = self.queue_handle(tp, part, group).await?;
        let event_log = qh.event_log();
        let event_msg = event_msg(&ev)?;
        let outter_completion = ApplyThenComplete::new(self.clone(), ev, qh, completion);
        event_log
            .append_enqueue(event_msg, None, outter_completion)
            .map_err(io_err)?;

        // let applied_up_to = qh.applied_upto().load(Ordering::Relaxed);
        // self.maybe_snapshot(tp, part, group, applied_up_to).await?;

        Ok(())
    }

    pub async fn ack_enqueue_many(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        reqs: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let ev = StromaEvent::AckMany { reqs };

        let qh = self.queue_handle(tp, part, group).await?;
        let event_log = qh.event_log();
        let event_msg = event_msg(&ev)?;
        let outter_completion = ApplyThenComplete::new(self.clone(), ev, qh, completion);
        event_log
            .append_enqueue(event_msg, None, outter_completion)
            .map_err(io_err)?;

        // let applied_up_to = qh.applied_upto().load(Ordering::Relaxed);
        // self.maybe_snapshot(tp, part, group, applied_up_to).await?;

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
        self.nack_enqueue_many(
            tp,
            part,
            group,
            vec![NackEventMeta {
                off: offset,
                requeue,
            }],
            completion,
        )
        .await
    }

    pub async fn nack_enqueue_many(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        reqs: Vec<NackEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<()> {
        let qh = self.queue_handle(tp, part, group).await?;
        let event_log = qh.event_log();

        // Phase 1: durable Nack write
        let nack_event = StromaEvent::NackMany { reqs: reqs.clone() };
        let m = event_msg(&nack_event)?;
        let ar = event_log
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
            .map_err(io_err)?;
        qh.applied_upto()
            .fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        qh.set_dirty_snapshot(true);

        // Apply -> get outcomes
        let outcomes = qh.nack_many(reqs).await;
        let dl_offsets: Vec<Offset> = outcomes
            .iter()
            .filter_map(|(o, oc)| matches!(oc, NackOutcome::DeadLetterRequested).then_some(*o))
            .collect();

        if dl_offsets.is_empty() {
            completion.complete(Ok(ar));
            return Ok(());
        }

        // Phase 2: resolve policy, decide per-offset
        let (to_dlq, to_discard) = self.resolve_dlq_targets(&qh, &dl_offsets).await;

        // Discards: ack-locally directly, no DLQ event needed.
        if !to_discard.is_empty() {
            let ev = StromaEvent::DeadLetterCommit {
                offs: to_discard.clone(),
            };
            // (DeadLetterCommit on replay = ack, same effect.)
            // Could also use a distinct DiscardPending event; using commit keeps event types minimal.
            let m = event_msg(&ev)?;
            let _ = event_log
                .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
                .await
                .map_err(io_err)?;
            qh.discard_pending_dlq(to_discard).await;
        }

        // DLQ-bound: emit DeadLetter event with resolved targets.
        if !to_dlq.is_empty() {
            let ev = StromaEvent::DeadLetter {
                reqs: to_dlq.clone(),
            };
            let event_msg = event_msg(&ev)?;
            let _ = event_log
                .append_batch(
                    vec![event_msg],
                    Some(self.keratin_cfg_event.default_durability),
                )
                .await
                .map_err(io_err)?;
            // (Apply already done at phase-1 nack, which moved them to pending_dlq.
            //  No second apply needed, DeadLetter event is for replay durability only.)

            // Spawn background copy.
            for meta in to_dlq {
                let stroma = self.clone();
                let src = (tp.to_string(), part, group.map(String::from));
                let qh2 = qh.clone();
                tokio::spawn(async move {
                    stroma.dlq_copy_then_commit(src, qh2, meta).await;
                });
            }
        }

        completion.complete(Ok(ar));
        Ok(())
    }

    pub async fn declare(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        meta: DeclareMeta,
    ) -> Result<()> {
        self.ensure_queue(tp, part, group).await?;

        let _upto = self
            .append_events_durable(
                tp,
                part,
                group,
                vec![StromaEvent::Declare(meta)],
                KDurability::AfterFsync,
            )
            .await?;

        Ok(())
    }

    async fn resolve_dlq_targets(
        &self,
        qh: &QueueHandle,
        offsets: &[Offset],
    ) -> (Vec<DeadLetterMeta>, Vec<Offset>) {
        let resolved = qh.get_dlq_target().await;
        match resolved {
            Some((tp, part, grp)) => {
                let metas = offsets
                    .iter()
                    .map(|&off| DeadLetterMeta {
                        off,
                        target_tp: tp.clone().into(),
                        target_part: part,
                        target_group: grp.clone().map(Into::into),
                    })
                    .collect();
                (metas, Vec::new())
            }
            None => (Vec::new(), offsets.to_vec()),
        }
    }

    async fn dlq_copy_then_commit(
        &self,
        src: (String, u32, Option<String>),
        src_qh: QueueHandle,
        meta: DeadLetterMeta,
    ) {
        const MAX_ATTEMPTS: u32 = 5;
        let (src_tp, src_part, src_group) = src;

        // Fetch source message.
        let msg = match self
            .fetch_message_by_offset(&src_tp, src_part, src_group.as_deref(), meta.off)
            .await
        {
            Ok(Some(m)) => m,
            Ok(None) => {
                tracing::error!(
                    "DLQ copy: source message {} missing in {}/{}/{:?} — discarding",
                    meta.off,
                    src_tp,
                    src_part,
                    src_group
                );
                self.commit_dlq_event(&src_qh, vec![meta.off]).await;
                return;
            }
            Err(e) => {
                tracing::error!("DLQ copy: fetch failed: {e}");
                self.commit_dlq_event(&src_qh, vec![meta.off]).await; // give up, ack-locally
                return;
            }
        };

        // Decode original headers, augment with DLQ metadata.
        let mut headers = MessageHeaders::decode(&msg.headers).unwrap_or_else(|_| MessageHeaders {
            published: 0,
            publish_received: 0,
            extra: HashMap::new(),
        });
        headers
            .extra
            .insert("x-dlq-source-tp".into(), src_tp.clone());
        headers
            .extra
            .insert("x-dlq-source-part".into(), src_part.to_string());
        if let Some(g) = &src_group {
            headers.extra.insert("x-dlq-source-group".into(), g.clone());
        }
        headers
            .extra
            .insert("x-dlq-source-offset".into(), meta.off.to_string());

        // Append to target with bounded retries.
        let mut attempt = 0u32;
        let target_group = meta.target_group.as_deref();
        loop {
            let (cmp, rx) = KeratinAppendCompletion::pair();
            let res = self
                .append_message(
                    &meta.target_tp,
                    meta.target_part,
                    target_group,
                    &headers,
                    msg.payload.clone(),
                    cmp,
                )
                .await;

            let durable = match res {
                Ok(()) => rx.await.ok().and_then(|r| r.ok()),
                Err(_) => None,
            };

            if durable.is_some() {
                break;
            }

            attempt += 1;
            if attempt >= MAX_ATTEMPTS {
                tracing::error!(
                    "DLQ copy permanently failed for {}/{}/{:?}@{} after {} attempts; ack-locally",
                    src_tp,
                    src_part,
                    src_group,
                    meta.off,
                    MAX_ATTEMPTS
                );
                break; // fall through to commit (= local ack)
            }
            tokio::time::sleep(Duration::from_millis(100 * (1 << attempt.min(5)))).await;
        }

        self.commit_dlq_event(&src_qh, vec![meta.off]).await;
    }

    async fn commit_dlq_event(&self, qh: &QueueHandle, offs: Vec<Offset>) {
        let ev = StromaEvent::DeadLetterCommit { offs: offs.clone() };
        let Ok(m) = event_msg(&ev) else {
            return;
        };

        if let Err(e) = qh
            .event_log()
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
        {
            tracing::error!("DeadLetterCommit append failed: {e}");
            return;
        }
        qh.dead_letter_commit(offs).await;
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
    ) -> Result<Vec<(Offset, MessageHeaders, Vec<u8>, u32)>> {
        let qs = self.queue_handle(tp, part, group).await?;

        // Offsets are now already marked inflight inside queue
        let offs = qs.poll_ready_and_mark(max, lease_deadline).await;

        if offs.is_empty() {
            return Ok(Vec::new());
        }

        let mut i = 0;

        let qh = self.queue_handle(tp, part, group).await?;

        let stroma = self.clone();
        let closure = move || {
            let res: Result<Vec<(u64, MessageHeaders, Vec<u8>, u32)>> = {
                let mut out: Vec<(u64, MessageHeaders, Vec<u8>, u32)> =
                    Vec::with_capacity(offs.len());
                let retries_map: HashMap<u64, u32> = HashMap::from_iter(offs.clone().into_iter());
                while i < offs.len() {
                    let (start, _retries) = offs[i];
                    let mut len = 1;

                    // ---- group contiguous offsets ----
                    // TODO: we might be able to skip since we now save ranges?
                    while i + len < offs.len() && offs[i + len].0 == start + len as u64 {
                        len += 1;
                    }

                    // ---- batch fetch ----
                    let batch: Vec<(u64, Vec<u8>, MessageHeaders)> =
                        stroma.scan_messages_from(&qh, start, len)?;

                    // ---- fast path: perfect match ----
                    if batch.len() == len {
                        for (off, payload, headers) in batch {
                            out.push((off, headers, payload, retries_map[&off]));
                        }
                    } else {
                        // ---- slow path: handle holes (rare but important) ----
                        // build small lookup map
                        let mut map = HashMap::with_capacity(batch.len());
                        for (off, payload, headers) in batch {
                            map.insert(off, (headers, payload));
                        }

                        for j in 0..len {
                            let off = start + j as u64;
                            if let Some((headers, payload)) = map.remove(&off) {
                                out.push((off, headers, payload, retries_map[&off]));
                            } else {
                                // extremely rare: log inconsistency or race
                                tracing::warn!(
                                    "Missing payload for offset {} in batch fetch (tp={}, part={}, group={:?})",
                                    off,
                                    qh.topic(),
                                    qh.partition(),
                                    qh.group(),
                                );
                            }
                        }
                    }

                    i += len;
                }
                Ok(out)
            };
            res
        };

        let out = tokio::task::spawn_blocking(closure)
            .await
            .map_err(|err| StromaError::Io(err.to_string()))??;

        Ok(out)
    }

    pub fn scan_messages_from(
        &self,
        qh: &QueueHandle,
        from: Offset,
        max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>, MessageHeaders)>> {
        let log = qh.msg_log();
        let reader = log.reader();
        let got = reader.scan_from(from, max).map_err(io_err)?;
        got.into_iter()
            .map(|r| MessageHeaders::decode(&r.headers).map(|h| (r.offset, r.payload, h)))
            .collect::<Result<Vec<(Offset, Vec<u8>, MessageHeaders)>>>()
    }

    pub async fn current_next_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let msg_log = self.queue_handle(tp, part, group).await?.msg_log();
        Ok(msg_log.next_offset())
    }

    /// Optional (used by cleanup_topic): truncate message log.
    pub async fn truncate_messages_before(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        before: Offset,
    ) -> Result<u64> {
        let msg_log = self.queue_handle(tp, part, group).await?.msg_log();
        msg_log.truncate_before(before).await.map_err(io_err)
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
        let qh = self.queue_handle(tp, part, group).await?;
        self.truncate_partition_log(qh, cutoff).await?;
        Ok(())
    }

    /// Only offsets < min(acked_until of every queue) are globally deletable.
    async fn safe_truncate_before(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let qh = self.queue_handle(tp, part, group).await?;
        let settled_until = qh.settled_until().await;
        let min = settled_until.min(qh.lowest_not_acked_offset().await);

        Ok(min)
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

    pub async fn estimate_disk_used(&self) -> Result<u64> {
        let mut total = 0;
        let keys = self.queue_keys_snapshot();
        for (tp, part, group) in keys {
            let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
            total += qh.event_log().estimate_disk_used().await.map_err(io_err)?;
            total += qh.msg_log().estimate_disk_used().await.map_err(io_err)?;
        }
        Ok(total)
    }

    pub async fn get_queues_stats(
        &self,
    ) -> Result<HashMap<(Box<str>, Option<Box<str>>), QueueStatusReport>> {
        let mut stats = HashMap::new();
        for (tp, part, group) in self.queue_keys_snapshot() {
            let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
            let status = qh.status_report().await.map_err(io_err)?;
            stats.insert((tp, group), status);
        }
        Ok(stats)
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

    pub async fn nack_one(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
        requeue: bool,
    ) -> Result<()> {
        let (cmp, rx) = KeratinAppendCompletion::pair();
        self.nack_enqueue(tp, part, group, off, requeue, cmp)
            .await?;
        let _ = rx.await; // wait for durability + DLQ resolution
        Ok(())
    }

    pub async fn snapshot_partition(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        let upto = self
            .applied_upto_entry(tp, part, group)
            .await?
            .load(Ordering::Acquire);
        let qh = self.queue_handle(tp, part, group).await?;
        self.write_snapshots_for_partition(qh, upto).await
    }

    pub async fn truncate_partition_log(
        &self,
        qh: QueueHandle,
        before_event: Offset,
    ) -> Result<u64> {
        let tp = qh.topic();
        let part = qh.partition();
        let group = qh.group();
        let event_log = qh.event_log();
        let event_head = event_log
            .truncate_before(before_event)
            .await
            .map_err(io_err)?;
        tracing::info!(
            "event truncate tp={} part={} group={:?} before={} -> new_head={}",
            tp,
            part,
            group,
            before_event,
            event_head
        );
        let msg_log = qh.msg_log();
        // let safe_msg_truncate = self.safe_truncate_before(tp, part, group).await?;
        let safe_msg_truncate = qh.lowest_not_acked_offset().await;
        let msg_head = msg_log
            .truncate_before(safe_msg_truncate)
            .await
            .map_err(io_err)?;
        tracing::info!(
            "message truncate tp={} part={} group={:?} before={} -> new_head={}",
            tp,
            part,
            group,
            safe_msg_truncate,
            msg_head
        );

        Ok(event_head)
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

fn collect_parts_decoded(
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

fn is_send<T: Send>(_: T) {}

#[cfg(test)]
mod tests {
    use keratin_log::test_dir;

use super::*;

    #[tokio::test]
    async fn queue_handle_starts_snapshot_task_once() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::default(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();

        assert!(qh.snapshot_task_started());
        assert!(qh.recovery_complete());

        stroma.periodic_snapshot(qh.clone());

        assert!(qh.snapshot_task_started());
    }

    #[tokio::test]
    async fn new_queue_after_empty_recovery_is_marked_recovered() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::default(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        assert!(stroma.initial_recovery_complete.load(Ordering::Acquire));

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();

        assert!(qh.recovery_complete());
        assert!(qh.snapshot_task_started());
    }

    #[tokio::test]
    async fn mark_all_queue_recoveries_completes_existing_waiters() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::default(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();

        qh.recovery_complete.store(false, Ordering::Release);

        let qh_waiter = qh.clone();
        let waiter = tokio::spawn(async move {
            qh_waiter.wait_recovery_complete().await;
        });

        stroma.mark_all_queue_recoveries_complete();

        waiter.await.unwrap();

        assert!(qh.recovery_complete());
    }

    #[tokio::test(start_paused = true)]
    async fn newly_created_queue_writes_periodic_snapshot() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::default(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();

        qh.enqueue(0, 0).await;

        tokio::time::advance(Duration::from_secs(21)).await;
        // should find better way to ensure it runs ideally
        for _ in 0..10000 {
            tokio::task::yield_now().await;
        }

        assert!(stroma.snap_file("new-topic", 0, None).exists());
    }
}
