use std::{
    fs,
    hash::{BuildHasher, Hash, Hasher},
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use keratin_log::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KDurability, Keratin,
    KeratinAppendCompletion, KeratinConfig, Message, ReplicatedAppendOutcome,
    util::unix_millis,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OnceCell, RwLock, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    DeclareMeta, Result, StromaError,
    event::{
        AckEventMeta, DeadLetterMeta, DeadLetterReason, EnqueueDelayedEventMeta, EnqueueEventMeta,
        NackEventMeta, StromaEvent,
    },
    global::{GlobalKey, GlobalStore, GlobalValue, PutOutcome},
    group,
    metrics::StromaMetrics,
    replication_cache::{
        RecentReplicationCache, ReplicationCacheKey, ReplicationCacheMutation, ReplicationCacheRead,
    },
    state::{
        CustomDLQ, InspectMode, NackOutcome, Offset, OwnerOperationLease, QueueCommand,
        QueueDebugInfo, QueueHandle, QueueHandleInner, QueueInspectionSnapshot, QueueInspectionState,
        QueueInternalDebugInfo, QueueRole, QueueSharedBundle,
        QueueStatusReport, StromaDebugSnapshot, UnixMillis,
    },
    topic,
};

// Replication data types + owner-read helpers now live in `replication.rs`;
// re-export so existing `stroma_core::` and `crate::stroma::` paths keep resolving
// (clustering-module separation).
pub use crate::replication::*;

pub(crate) fn io_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Io(e.to_string())
}

pub(crate) fn decode_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Decode(e.to_string())
}

fn encode_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Encode(e.to_string())
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
    pub completion: Box<dyn AppendCompletion<IoError> + Send>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueuePromotionOutcome {
    Promoted {
        message_next_offset: Offset,
        event_next_offset: Offset,
        applied_event_offset: Option<Offset>,
    },
    MessageLogBehind {
        local_next_offset: Offset,
        expected_next_offset: Offset,
    },
    MessageLogAhead {
        local_next_offset: Offset,
        expected_next_offset: Offset,
    },
    EventLogBehind {
        local_next_offset: Offset,
        expected_next_offset: Offset,
    },
    EventLogAhead {
        local_next_offset: Offset,
        expected_next_offset: Offset,
    },
    EventsNotApplied {
        applied_event_offset: Option<Offset>,
        event_next_offset: Offset,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDemotionOutcome {
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
    pub applied_event_offset: Option<Offset>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationCacheConfig {
    pub max_bytes: usize,
}

impl ReplicationCacheConfig {
    pub const fn disabled() -> Self {
        Self { max_bytes: 0 }
    }

    pub const fn enabled(max_bytes: usize) -> Self {
        Self { max_bytes }
    }

    pub const fn is_enabled(&self) -> bool {
        self.max_bytes > 0
    }
}

impl Default for ReplicationCacheConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StromaOptions {
    pub replication_cache: ReplicationCacheConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct StromaKeratinConfig {
    pub message_log: KeratinConfig,
    pub event_log: KeratinConfig,
}

impl StromaKeratinConfig {
    pub fn from_message_log(message_log: KeratinConfig) -> Self {
        Self {
            message_log,
            event_log: derived_event_log_config(message_log),
        }
    }
}

fn derived_event_log_config(message_log: KeratinConfig) -> KeratinConfig {
    KeratinConfig {
        flush_target_bytes: message_log.flush_target_bytes / 8,
        max_batch_bytes: message_log.max_batch_bytes / 8,
        index_stride_bytes: message_log.index_stride_bytes / 8,
        segment_max_bytes: message_log.segment_max_bytes / 8,
        ..message_log
    }
}

const GLOBAL_DLQ_NAMESPACE: &str = "stroma.settings";
const GLOBAL_DLQ_KEY: &str = "global_dlq";
const DEFAULT_GROUP_ALIAS: &str = "default";

fn normalize_group(group: Option<&str>) -> Option<&str> {
    match group {
        Some(DEFAULT_GROUP_ALIAS) | None => None,
        Some(group) => Some(group),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalDLQ {
    pub tp: String,
    pub part: u32,
    pub group: Option<String>,
}

impl GlobalDLQ {
    pub async fn new(tp: &str, part: u32, group: Option<&str>) -> Result<Self> {
        let group = normalize_group(group);
        let dlq = Self {
            tp: tp.to_string(),
            part,
            group: group.map(|s| s.into()),
        };
        dlq.validate()?;
        Ok(dlq)
    }

    pub fn validate(&self) -> Result<()> {
        topic::Topic::parse(&self.tp)
            .map_err(|err| StromaError::InvalidArgument(err.to_string()))?;
        if let Some(group) = &self.group {
            if group == DEFAULT_GROUP_ALIAS {
                return Err(StromaError::InvalidArgument(
                    "group \"default\" is reserved for the ungrouped queue".to_string(),
                ));
            }
            group::Group::parse(group)
                .map_err(|err| StromaError::InvalidArgument(err.to_string()))?;
        }
        Ok(())
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalDlqSnapshot {
    pub version: u64,
    pub target: Option<GlobalDLQ>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalDlqUpdateOutcome {
    Stored(GlobalDlqSnapshot),
    Conflict(GlobalDlqSnapshot),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobalDlqRecord {
    target: Option<GlobalDLQ>,
}

impl GlobalDlqRecord {
    fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(encode_err)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(decode_err)
    }
}

fn global_dlq_key() -> Result<GlobalKey> {
    GlobalKey::new(GLOBAL_DLQ_NAMESPACE, GLOBAL_DLQ_KEY)
}

fn global_dlq_snapshot_from_value(value: Option<GlobalValue>) -> Result<GlobalDlqSnapshot> {
    match value {
        Some(value) => {
            let record = GlobalDlqRecord::decode(&value.bytes)?;
            if let Some(target) = &record.target {
                target.validate()?;
            }
            Ok(GlobalDlqSnapshot {
                version: value.version,
                target: record.target,
            })
        }
        None => Ok(GlobalDlqSnapshot {
            version: 0,
            target: None,
        }),
    }
}

pub struct ApplyThenComplete {
    stroma: Stroma,
    ev: StromaEvent,
    qh: QueueHandle,
    _owner_operation: OwnerOperationLease,
    inner: Box<dyn AppendCompletion<IoError> + Send>,
}

impl AppendCompletion<IoError> for ApplyThenComplete {
    fn complete(self: Box<Self>, res: std::result::Result<AppendResult, IoError>) {
        match res {
            Ok(ar) => {
                let stroma = self.stroma.clone();
                let ev = self.ev.clone();
                let inner = self.inner;

                let qh = match self.qh.resolve() {
                    Ok(qh) => qh,
                    Err(e) => {
                        inner.complete(Err(IoError::new(e.to_string())));
                        return;
                    }
                };

                match stroma.enqueue_event_inmem(ev, &qh) {
                    Ok(()) => {
                        // let _ = tx.send(Ok(ar));
                        qh.applied_upto()
                            .fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
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
        owner_operation: OwnerOperationLease,
        inner: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Box<Self> {
        Box::new(Self {
            stroma,
            ev,
            qh,
            _owner_operation: owner_operation,
            inner,
        })
    }
}

struct CompletionItem {
    meta: ItemMeta,
    completion: Box<dyn AppendCompletion<IoError> + Send>,
}

/// Completion for the msg_log batch in append_message_batch.
/// Once msg-log durability is reached, emits one EnqueueMany event_log entry,
/// then fans out per client completions with assigned offsets.
struct MsgBatchCompletion {
    stroma: Stroma,
    items: Vec<CompletionItem>,
    cache_messages: Vec<Message>,
    durability: KDurability,
    runtime: tokio::runtime::Handle,
    qh: QueueHandle,
    owner_operation: OwnerOperationLease,
}

impl MsgBatchCompletion {
    fn new(
        stroma: Stroma,
        items: Vec<CompletionItem>,
        cache_messages: Vec<Message>,
        durability: KDurability,
        qh: QueueHandle,
        owner_operation: OwnerOperationLease,
    ) -> Box<Self> {
        Box::new(Self {
            stroma,
            items,
            cache_messages,
            durability,
            runtime: tokio::runtime::Handle::current(),
            qh,
            owner_operation,
        })
    }
}

impl AppendCompletion<IoError> for MsgBatchCompletion {
    fn complete(self: Box<Self>, res: std::result::Result<AppendResult, IoError>) {
        let Self {
            stroma,
            items,
            cache_messages,
            durability,
            runtime,
            qh,
            owner_operation,
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
                .append_events_durable_leased(qh.clone(), events, durability, owner_operation)
                .await
            {
                Ok(_) => {
                    if let Ok(qh) = qh.resolve() {
                        stroma.cache_owner_messages(&qh, base, cache_messages);
                    }
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
pub enum MessageContentType {
    MsgPack,
    Json,
    Text,
    Custom(Box<str>),
}

impl MessageContentType {
    pub fn from_header(value: impl Into<String>) -> Self {
        let value = value.into();
        match value.split(';').next().map(str::trim) {
            Some("application/msgpack") => MessageContentType::MsgPack,
            Some("application/json") => MessageContentType::Json,
            Some("text/plain") if value == "text/plain; charset=utf-8" => MessageContentType::Text,
            _ => MessageContentType::Custom(value.into_boxed_str()),
        }
    }

    pub fn as_header(&self) -> &str {
        match self {
            MessageContentType::MsgPack => "application/msgpack",
            MessageContentType::Json => "application/json",
            MessageContentType::Text => "text/plain; charset=utf-8",
            MessageContentType::Custom(value) => value,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageHeaders {
    pub published: u64,
    pub publish_received: u64,
    #[serde(default)]
    pub content_type: Option<MessageContentType>,
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageInspectionPage {
    pub next_offset_hint: Offset,
    pub items: Vec<MessageInspectionItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageInspectionItem {
    pub state: QueueInspectionState,
    pub headers: Option<MessageHeaders>,
    pub payload_len: Option<usize>,
    pub payload: Option<Vec<u8>>,
    pub payload_truncated: bool,
    pub missing_payload: bool,
}

impl MessageHeaders {
    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(|err| StromaError::Decode(err.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(|err| StromaError::Decode(err.to_string()))
    }
}

fn validate_user_message_headers(headers: &MessageHeaders) -> Result<()> {
    if let Some(key) = headers.extra.keys().find(|key| key.starts_with("stroma.")) {
        return Err(StromaError::InvalidArgument(format!(
            "header {key:?} uses reserved stroma.* namespace"
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct QueueSlot {
    /// The strong, log-owning incarnation. The registry slot is the SOLE strong
    /// owner; handed-out `QueueHandle` tickets hold only a `Weak` to this.
    pub(crate) handle: OnceCell<Arc<QueueHandleInner>>,
    exists_on_disk: bool,
    eviction_state: Arc<EvictionState>,
}

#[derive(Debug)]
struct EvictionState {
    evicting: AtomicBool,
    done: Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictOutcome {
    NotPresent,
    NotMaterialized,
    HasInflight,
    RaceLost,
    Evicted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestroyOutcome {
    /// The partition's registry entry and on-disk storage are gone (or were
    /// already absent). A subsequent materialize starts from an empty dir.
    Destroyed,
    /// The partition still had inflight (leased, un-acked) work. Nothing was
    /// removed. Callers must drain before destroying.
    HasInflight,
}

#[derive(Debug)]
struct EvictionGuard {
    state: Arc<EvictionState>,
    completed: bool,
}

impl EvictionGuard {
    fn new(state: Arc<EvictionState>) -> Self {
        Self {
            state,
            completed: false,
        }
    }

    fn complete(mut self) {
        self.completed = true;
        self.state.finish_eviction();
    }
}

impl Drop for EvictionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.state.finish_eviction();
        }
    }
}

impl EvictionState {
    fn new() -> Self {
        Self {
            evicting: AtomicBool::new(false),
            done: Notify::new(),
        }
    }

    pub async fn wait_until_not_evicting(&self) {
        loop {
            // Create the notification future first — this registers us as a waiter.
            let notified = self.done.notified();
            // Then check the flag. If it's clear, we're done.
            if !self.evicting.load(Ordering::Acquire) {
                return;
            }
            // Flag was set when we checked. Wait. If the evictor fires
            // notify_waiters between the check and the .await, we're already
            // registered so the wakeup hits us.
            notified.await;
        }
    }

    pub fn start_eviction(&self) {
        self.evicting.store(true, Ordering::Release);
    }

    pub fn finish_eviction(&self) {
        self.evicting.store(false, Ordering::Release);
        self.done.notify_waiters();
    }

    pub fn is_evicting(&self) -> bool {
        self.evicting.load(Ordering::Acquire)
    }
}

impl QueueSlot {
    pub fn new() -> Self {
        Self {
            handle: OnceCell::new(),
            exists_on_disk: false,
            eviction_state: Arc::new(EvictionState::new()),
        }
    }

    pub fn new_existing() -> Self {
        Self {
            handle: OnceCell::new(),
            exists_on_disk: true,
            eviction_state: Arc::new(EvictionState::new()),
        }
    }

    pub fn wait_until_ready(&self) -> impl Future<Output = Arc<QueueHandleInner>> + '_ {
        async {
            loop {
                if let Some(handle) = self.handle.get() {
                    return handle.clone();
                }
                self.eviction_state.done.notified().await;
            }
        }
    }

    pub fn start_eviction(&self) {
        self.eviction_state.start_eviction();
    }

    fn is_evicting(&self) -> bool {
        self.eviction_state.is_evicting()
    }

    async fn wait_until_not_evicting(&self) {
        self.eviction_state.wait_until_not_evicting().await;
    }
}

/// Registry key: (topic, partition, group). `None` group is the default group.
pub(crate) type QueueKey = (Box<str>, u32, Option<Box<str>>);

pub(crate) type Registry = hashbrown::HashMap<QueueKey, Arc<QueueSlot>>;

fn slot_lookup_no_alloc<'a>(
    map: &'a Registry,
    tp: &str,
    part: u32,
    group: Option<&str>,
) -> Option<&'a Arc<QueueSlot>> {
    let group = normalize_group(group);
    let mut hasher = map.hasher().build_hasher();
    tp.hash(&mut hasher);
    part.hash(&mut hasher);
    group.hash(&mut hasher);
    let hash = hasher.finish();

    map.raw_entry()
        .from_hash(hash, |k| {
            k.0.as_ref() == tp && k.1 == part && k.2.as_deref() == group
        })
        .map(|(_, v)| v)
}

#[derive(Debug, Clone)]
pub struct Stroma {
    pub(crate) start_time: Instant,
    pub(crate) root: PathBuf,
    pub(crate) keratin_cfg_msg: KeratinConfig,
    pub(crate) keratin_cfg_event: KeratinConfig,
    pub(crate) snap_cfg: SnapshotConfig,
    pub(crate) global_store: Arc<OnceCell<Arc<GlobalStore>>>,

    pub(crate) task_group: Arc<TaskGroup>,

    // Materialized queue state
    queue_handles: Arc<ArcSwap<Registry>>,

    /// Per-partition-key lifecycle lock. Serializes the operations that OPEN or
    /// CLOSE a partition's Keratin logs - building a handle (queue_handle cold
    /// path), destroy_partition, and evict - so two never race on the same dir.
    /// Without it, a build whose slot was retired mid-flight (or two churning
    /// builds) both `create_dir_all` + open the same path and collide on the
    /// `.keratin.lock` flock ("Keratin already open"). The flock stays as a
    /// redundant safety net; this is the real in-process serialization. Keyed by
    /// the stable partition key (survives slot churn); entries are tiny and the
    /// partition set is bounded.
    lifecycle_locks: Arc<DashMap<(Box<str>, u32, Option<Box<str>>), Arc<AsyncMutex<()>>>>,

    // TODO: Consider using parking lot
    // Global DLQ topic
    pub(crate) global_dlq: Arc<RwLock<Option<GlobalDLQ>>>,

    pub(crate) msg_count: Arc<AtomicU64>,

    pub(crate) event_count: Arc<AtomicU64>,

    pub(crate) metrics: Arc<StromaMetrics>,
    pub(crate) replication_cache: Option<Arc<Mutex<RecentReplicationCache>>>,

    earliest_pending_deadline_sender: tokio::sync::watch::Sender<Option<UnixMillis>>,
    earliest_pending_deadline_receiver: tokio::sync::watch::Receiver<Option<UnixMillis>>,
    pub(crate) deadline_waker: Arc<Notify>,
    initial_recovery_complete: Arc<AtomicBool>,

    #[cfg(test)]
    lazy_recoveries_started: Arc<AtomicU64>,

    #[cfg(test)]
    recovery_event_scan_starts: Arc<Mutex<Vec<u64>>>,

    #[cfg(test)]
    snapshot_worker_ticks: Arc<Notify>,
}

impl Stroma {
    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self> {
        Self::open_with_options(root, keratin_cfg, snap_cfg, StromaOptions::default()).await
    }

    pub async fn open_with_options(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
        options: StromaOptions,
    ) -> Result<Self> {
        let start_time = Instant::now();
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("events")).map_err(io_err)?;
        fs::create_dir_all(root.join("messages")).map_err(io_err)?;
        fs::create_dir_all(root.join("snapshots")).map_err(io_err)?;
        fs::create_dir_all(root.join("tmp")).map_err(io_err)?;

        let metrics = Arc::new(StromaMetrics::new(60));
        let keratin_cfg_msg = keratin_cfg.message_log;
        let keratin_cfg_event = keratin_cfg.event_log;

        let (earliest_pending_deadline_sender, earliest_pending_deadline_receiver) =
            tokio::sync::watch::channel(None);

        let st = Self {
            start_time,
            root,
            keratin_cfg_msg,
            keratin_cfg_event,
            snap_cfg,
            global_store: Arc::new(OnceCell::new()),
            task_group: Arc::new(TaskGroup::new()),
            queue_handles: Arc::new(ArcSwap::new(Arc::new(hashbrown::HashMap::new()))),
            lifecycle_locks: Arc::new(DashMap::new()),
            global_dlq: Arc::new(RwLock::new(None)),
            msg_count: Arc::new(AtomicU64::new(0)),
            event_count: Arc::new(AtomicU64::new(0)),
            metrics: metrics.clone(),
            replication_cache: options.replication_cache.is_enabled().then(|| {
                Arc::new(Mutex::new(RecentReplicationCache::new(
                    options.replication_cache.max_bytes,
                )))
            }),
            earliest_pending_deadline_sender,
            earliest_pending_deadline_receiver,
            deadline_waker: Arc::new(Notify::new()),
            initial_recovery_complete: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            lazy_recoveries_started: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            recovery_event_scan_starts: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            snapshot_worker_ticks: Arc::new(Notify::new()),
        };

        st.load_global_dlq_setting().await?;

        // Discover persisted queues, but do not open logs or replay them yet.
        // Recovery happens lazily on first queue_handle().
        st.index_existing_queues()?;
        st.initial_recovery_complete.store(true, Ordering::Release);

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

    pub(crate) fn replication_cache_key_for(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> ReplicationCacheKey {
        ReplicationCacheKey::from_parts(topic, part, group)
    }

    fn replication_cache_key_for_handle(&self, qh: &QueueHandleInner) -> ReplicationCacheKey {
        self.replication_cache_key_for(qh.topic(), qh.partition(), qh.group())
    }

    fn record_replication_cache_mutation(&self, mutation: ReplicationCacheMutation) {
        self.metrics
            .replication_cache
            .set_retained_bytes(mutation.retained_bytes);
        self.metrics
            .replication_cache
            .record_evicted_records(mutation.evicted_records);
    }

    fn cache_owner_messages(&self, qh: &QueueHandleInner, first_offset: Offset, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }
        let Some(replication_cache) = &self.replication_cache else {
            return;
        };

        let key = self.replication_cache_key_for_handle(qh);
        let mutation = match replication_cache.lock() {
            Ok(mut cache) => cache.insert_messages(&key, first_offset, messages),
            Err(err) => {
                tracing::error!("replication cache lock poisoned while inserting messages: {err}");
                return;
            }
        };
        self.record_replication_cache_mutation(mutation);
    }

    fn cache_owner_events(&self, qh: &QueueHandleInner, first_offset: Offset, events: Vec<StromaEvent>) {
        if events.is_empty() {
            return;
        }
        let Some(replication_cache) = &self.replication_cache else {
            return;
        };

        let key = self.replication_cache_key_for_handle(qh);
        let mutation = match replication_cache.lock() {
            Ok(mut cache) => cache.insert_events(&key, first_offset, events),
            Err(err) => {
                tracing::error!("replication cache lock poisoned while inserting events: {err}");
                return;
            }
        };
        self.record_replication_cache_mutation(mutation);
    }

    pub(crate) fn read_cached_owner_messages(
        &self,
        key: &ReplicationCacheKey,
        from: Offset,
        max: usize,
    ) -> Option<ReplicationCacheRead<Message>> {
        let Some(replication_cache) = &self.replication_cache else {
            return None;
        };
        let read = match replication_cache.lock() {
            Ok(cache) => cache.read_messages(key, from, max),
            Err(err) => {
                tracing::error!("replication cache lock poisoned while reading messages: {err}");
                None
            }
        };
        self.metrics
            .replication_cache
            .record_message_read(read.is_some());
        read
    }

    pub(crate) fn read_cached_owner_events(
        &self,
        key: &ReplicationCacheKey,
        from: Offset,
        max: usize,
    ) -> Option<ReplicationCacheRead<StromaEvent>> {
        let Some(replication_cache) = &self.replication_cache else {
            return None;
        };
        let read = match replication_cache.lock() {
            Ok(cache) => cache.read_events(key, from, max),
            Err(err) => {
                tracing::error!("replication cache lock poisoned while reading events: {err}");
                None
            }
        };
        self.metrics
            .replication_cache
            .record_event_read(read.is_some());
        read
    }

    pub async fn global_store(&self) -> Result<Arc<GlobalStore>> {
        self.global_store
            .get_or_try_init(|| async {
                let dir = self.root.join("global");
                fs::create_dir_all(&dir).map_err(io_err)?;
                let store = GlobalStore::open(dir, self.keratin_cfg_event).await?;
                Ok::<_, StromaError>(Arc::new(store))
            })
            .await
            .cloned()
    }

    pub async fn global_dlq(&self) -> Result<GlobalDlqSnapshot> {
        let store = self.global_store().await?;
        let key = global_dlq_key()?;
        global_dlq_snapshot_from_value(store.get(&key).await?)
    }

    pub async fn set_global_dlq(
        &self,
        target: Option<GlobalDLQ>,
        expected_version: u64,
    ) -> Result<GlobalDlqUpdateOutcome> {
        if let Some(target) = &target {
            target.validate()?;
        }
        let store = self.global_store().await?;
        let key = global_dlq_key()?;
        let bytes = GlobalDlqRecord {
            target: target.clone(),
        }
        .encode()?;

        match store.put(key, bytes, Some(expected_version)).await? {
            PutOutcome::Stored { version } => {
                let snapshot = GlobalDlqSnapshot { version, target };
                self.apply_global_dlq_snapshot(&snapshot).await;
                Ok(GlobalDlqUpdateOutcome::Stored(snapshot))
            }
            PutOutcome::Conflict { current } => {
                let snapshot = global_dlq_snapshot_from_value(current)?;
                Ok(GlobalDlqUpdateOutcome::Conflict(snapshot))
            }
        }
    }

    async fn load_global_dlq_setting(&self) -> Result<()> {
        let snapshot = self.global_dlq().await?;
        self.apply_global_dlq_snapshot(&snapshot).await;
        Ok(())
    }

    async fn apply_global_dlq_snapshot(&self, snapshot: &GlobalDlqSnapshot) {
        *self.global_dlq.write().await = snapshot.target.clone();
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
        let group = normalize_group(group);
        let mut p = self.messages_root();
        if let Some(g) = group {
            p = p.join(Self::enc_component(g))
        }
        p.join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    fn tp_part_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let group = normalize_group(group);
        let mut p = self.events_root();
        if let Some(g) = group {
            p = p.join(Self::enc_component(g))
        }
        p.join(Self::enc_component(tp))
            .join(format!("{:010}", part))
    }

    pub(crate) fn snap_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let group = normalize_group(group);
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
        let group = normalize_group(group);
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

    fn index_existing_queues(&self) -> Result<()> {
        let partitions = self.discover_partitions()?;
        if partitions.is_empty() {
            return Ok(());
        }

        let current = self.queue_handles.load();
        let mut next = (**current).clone();

        for (group, tp, part) in partitions {
            let key = (tp.into_boxed_str(), part, group.map(Into::into));

            next.entry(key).or_insert_with(|| {
                Arc::new(QueueSlot {
                    handle: OnceCell::new(),
                    exists_on_disk: true,
                    eviction_state: Arc::new(EvictionState::new()),
                })
            });
        }

        self.queue_handles.store(Arc::new(next));

        Ok(())
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

    /// Acquire the per-partition lifecycle lock (see `lifecycle_locks`). Held
    /// around operations that open/close a partition's Keratin logs so they never
    /// race on the same dir. NOTE: do not call anything that re-acquires this lock
    /// for the same key while holding it (e.g. evict's pre-swap snapshot calls
    /// queue_handle, so it must take its snapshot BEFORE acquiring this lock).
    async fn lock_partition_lifecycle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let group = normalize_group(group);
        let key = (Box::<str>::from(tp), part, group.map(Box::<str>::from));
        let lock = self
            .lifecycle_locks
            .entry(key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        lock.lock_owned().await
    }

    /// Build a handed-out ticket for `inner` (the slot's strong incarnation),
    /// carrying the registry ref + key so it can re-resolve.
    fn ticket_for(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        inner: &Arc<QueueHandleInner>,
    ) -> QueueHandle {
        let group = normalize_group(group);
        let key = (Box::<str>::from(tp), part, group.map(Box::<str>::from));
        QueueHandle::from_inner(self.queue_handles.clone(), key, inner)
    }

    pub async fn queue_handle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueHandle> {
        let group = normalize_group(group);
        loop {
            let slot = loop {
                let outcome = {
                    let current = self.queue_handles.load();
                    if let Some(slot) = slot_lookup_no_alloc(&current, tp, part, group) {
                        Ok(slot.clone())
                    } else {
                        let new_slot = Arc::new(QueueSlot {
                            handle: OnceCell::new(),
                            exists_on_disk: false,
                            eviction_state: Arc::new(EvictionState::new()),
                        });
                        let key = (Box::<str>::from(tp), part, group.map(Box::<str>::from));
                        let mut next = (**current).clone();
                        next.insert(key, new_slot.clone());

                        tracing::debug!(
                            "Attempting to insert queue handle for ({tp}, {part}, {group:?})..."
                        );
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
                            Ok(new_slot)
                        } else {
                            // lost race; retry
                            tracing::debug!(
                                "Lost race to insert queue handle for ({tp}, {part}, {group:?}), retrying..."
                            );
                            Err(()) // retry
                        }
                    }
                };
                match outcome {
                    Ok(slot) => break slot,
                    Err(()) => tokio::task::yield_now().await,
                }
            };

            // Fast path: a live handle that is not mid-eviction.
            if let Some(inner) = slot.handle.get()
                && !slot.is_evicting()
            {
                return Ok(self.ticket_for(tp, part, group, inner));
            }

            // Slow path. Park until any eviction (or a concurrent destroy)
            // clears, then revalidate that our slot is still the one the
            // registry holds. destroy_partition swaps in a tombstone slot
            // (evicting), renames the partition dir aside, then removes the
            // tombstone. A stale slot here means a destroy moved that dir out
            // from under us, so we must re-acquire and open a fresh empty dir
            // rather than the tree being deleted.
            slot.wait_until_not_evicting().await;

            // Serialize the BUILD against any concurrent destroy/evict (and other
            // builds) of this partition, so we never open/recreate its dir while
            // another incarnation is opening or tearing it down. Acquired after the
            // eviction wait (so a destroy in progress finishes first) and held
            // through the open + recovery below.
            let _lifecycle = self.lock_partition_lifecycle(tp, part, group).await;

            // Re-validate AFTER taking the lock: a destroy may have retired our
            // slot while we waited for it. A stale slot means the dir was moved out
            // from under us, so re-acquire a fresh one rather than building here.
            if !self.slot_is_current(tp, part, group, &slot) {
                drop(_lifecycle);
                tokio::task::yield_now().await;
                continue;
            }

            let qh = slot
                .handle
                .get_or_try_init(|| async {
                    let msg_log = self.msg_log_init(tp, part, group).await?;
                    let event_log = self.event_log_init(tp, part, group).await?;

                    let bundle = QueueSharedBundle {
                        event_log: event_log.clone(),
                        msg_log,
                        task_group: self.task_group.clone(),
                        metrics: self.metrics.clone(),
                        global_dlq: self.global_dlq.clone(),
                        deadline_waker: self.deadline_waker.clone(),
                    };

                    let inner =
                        QueueHandleInner::init(tp.into(), part, group.map(|s| s.into()), bundle);

                    // The snapshot task gets a TICKET (Weak), so it never pins
                    // this incarnation once the slot drops it.
                    self.periodic_snapshot(self.ticket_for(tp, part, group, &inner));

                    if slot.exists_on_disk {
                        self.recover_one_log_with_handle(
                            &self.ticket_for(tp, part, group, &inner),
                            tp,
                            part,
                            group,
                            event_log,
                        )
                        .await?;
                    }

                    inner.mark_recovery_complete();

                    Ok::<_, StromaError>(inner)
                })
                .await?;

            return Ok(self.ticket_for(tp, part, group, qh));
        }
    }

    /// True when `slot` is still the registry's slot for `(tp, part, group)`.
    /// Used after parking on eviction to detect a slot a concurrent destroy
    /// (or evict) has swapped out.
    fn slot_is_current(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        slot: &Arc<QueueSlot>,
    ) -> bool {
        let current = self.queue_handles.load();
        matches!(
            slot_lookup_no_alloc(&current, tp, part, group),
            Some(s) if Arc::ptr_eq(s, slot)
        )
    }

    fn swap_slot(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        old: &Arc<QueueSlot>,
        new: Arc<QueueSlot>,
    ) -> Result<bool> {
        let group = normalize_group(group);
        let current = self.queue_handles.load();
        let mut next = (**current).clone();
        let key = (Box::<str>::from(topic), part, group.map(Box::<str>::from));

        let Some(current_slot) = next.get(&key) else {
            return Ok(false);
        };
        if !Arc::ptr_eq(current_slot, old) {
            return Ok(false);
        }

        next.insert(key.clone(), new);

        let prev = self
            .queue_handles
            .compare_and_swap(&current, Arc::new(next));
        Ok(Arc::ptr_eq(&prev, &current))
    }

    pub async fn evict(&self, topic: &str, part: u32, group: Option<&str>) -> Result<EvictOutcome> {
        // Lookup and check guards

        let queues = self.queue_handles.load();
        let old_slot = match slot_lookup_no_alloc(queues.as_ref(), topic, part, group) {
            Some(s) => s,
            None => return Ok(EvictOutcome::NotPresent),
        };
        let qh = match old_slot.handle.get() {
            Some(h) => h.clone(),
            None => return Ok(EvictOutcome::NotMaterialized),
        };
        if qh.inflight_len().await > 0 {
            return Ok(EvictOutcome::HasInflight);
        }

        // Snapshot before swapping in the evicting slot. Snapshot writing may
        // touch queue lookup paths, and those must not wait on the eviction
        // guard that this same task is responsible for clearing.
        if qh.dirty_snapshot() {
            Self::periodic_snapshot_step(self, &self.ticket_for(topic, part, group, &qh)).await?;
        }

        // Serialize the swap + shutdown against concurrent build/destroy of this
        // partition. Acquired AFTER the pre-swap snapshot above, which calls
        // queue_handle (re-acquires this same lock) - taking it earlier deadlocks.
        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;

        // Build replacement slot, marked evicting.
        let new_eviction_state = Arc::new(EvictionState {
            evicting: AtomicBool::new(true),
            done: Notify::new(),
        });
        let new_slot = Arc::new(QueueSlot {
            handle: OnceCell::new(),
            exists_on_disk: true,
            eviction_state: new_eviction_state.clone(),
        });

        // CAS-swap. If the registry has changed under us, bail.
        if !self.swap_slot(&topic, part, group, &old_slot, new_slot)? {
            return Ok(EvictOutcome::RaceLost);
        }
        let guard = EvictionGuard::new(new_eviction_state);

        qh.cancel_background_tasks();

        // Shut down old handle. Pending writes flush; logs close.
        qh.shutdown().await;
        qh.event_log().shutdown().await.map_err(io_err)?;
        qh.msg_log().shutdown().await.map_err(io_err)?;

        // Signal completion to any waiters.
        guard.complete();

        Ok(EvictOutcome::Evicted)
    }

    pub async fn unmaterialize(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<EvictOutcome> {
        self.evict(topic, part, group).await
    }

    pub async fn materialize(&self, topic: &str, part: u32, group: Option<&str>) -> Result<()> {
        self.queue_handle(topic, part, group).await?;
        Ok(())
    }

    /// Fully remove a partition: drop it from the in-memory registry and delete
    /// its on-disk storage (message, event, and snapshot dirs).
    ///
    /// This is stronger than [`Self::unmaterialize`] / [`Self::evict`], which
    /// only close the in-memory handle and leave the data on disk. It is meant
    /// for partitions that a repartition has retired: already deregistered from
    /// coordination, drained, and no longer routed to.
    ///
    /// Airtightness against a concurrent recreate: a destroying tombstone slot
    /// (marked evicting) is swapped into the registry first, so any in-flight or
    /// fresh materialize parks in [`Self::queue_handle`] rather than reopening
    /// the dir. The dir is then renamed aside (an atomic, O(1) sibling rename)
    /// before the tombstone is removed. A recreate that arrives afterwards
    /// creates a brand-new empty dir at the original path, while the unhurried
    /// `remove_dir_all` only ever walks the renamed-aside tree. The two never
    /// share a path, so a recreate cannot collide with the delete.
    ///
    /// Returns [`DestroyOutcome::HasInflight`] without removing anything if the
    /// partition still has leased, un-acked work.
    pub async fn destroy_partition(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<DestroyOutcome> {
        let group = normalize_group(group);

        // Serialize against concurrent build/evict of this partition so the
        // shutdown + rename below cannot overlap an in-flight open of the same dir.
        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;

        // 1. Install a destroying tombstone (evicting) so concurrent materialize
        //    parks instead of reopening the dir. Capture the slot we displaced.
        let tombstone = Arc::new(QueueSlot {
            handle: OnceCell::new(),
            exists_on_disk: true,
            eviction_state: Arc::new(EvictionState {
                evicting: AtomicBool::new(true),
                done: Notify::new(),
            }),
        });

        let prev = loop {
            let current = self.queue_handles.load();
            let existing = slot_lookup_no_alloc(&current, topic, part, group).cloned();

            // Guard: never discard inflight (leased, un-acked) work. The
            // repartition caller only destroys drained partitions; this keeps
            // the primitive safe if it is ever called on a live one.
            if let Some(slot) = &existing
                && let Some(qh) = slot.handle.get()
                && qh.inflight_len().await > 0
            {
                return Ok(DestroyOutcome::HasInflight);
            }

            let key = (Box::<str>::from(topic), part, group.map(Box::<str>::from));
            let mut next = (**current).clone();
            next.insert(key, tombstone.clone());
            let prevmap = self
                .queue_handles
                .compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&prevmap, &current) {
                break existing;
            }
            tokio::task::yield_now().await;
        };

        // 2. Shut down the displaced live handle, if any. The dir is now
        //    quiescent and the tombstone holds off any reopen.
        if let Some(slot) = &prev
            && let Some(qh) = slot.handle.get()
        {
            qh.cancel_background_tasks();
            qh.shutdown().await;
            qh.event_log().shutdown().await.map_err(io_err)?;
            qh.msg_log().shutdown().await.map_err(io_err)?;
        }

        // 3. Rename the dirs aside while the tombstone still blocks reopen, then
        //    drop the tombstone and wake any parked materializers (they
        //    revalidate, find no slot, and recreate over a fresh empty dir).
        let trashed = self.rename_partition_dirs_to_trash(topic, part, group)?;
        self.remove_queue(topic, part, group);
        tombstone.eviction_state.finish_eviction();

        // 4. Delete the renamed trees unhurried; they share no path with any
        //    live or recreated incarnation.
        for dir in trashed {
            if let Err(err) = tokio::fs::remove_dir_all(&dir).await {
                tracing::warn!("destroy_partition: failed to delete {dir:?}: {err}");
            }
        }

        Ok(DestroyOutcome::Destroyed)
    }

    /// Atomically rename a partition's message, event, and snapshot dirs to
    /// `<dir>.trash-<uuid>` siblings. Returns the trash paths that were created
    /// (dirs that did not exist are skipped). The rename targets sit in the same
    /// parent as the source, so the rename is a same-filesystem O(1) op.
    fn rename_partition_dirs_to_trash(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Vec<PathBuf>> {
        let suffix = uuid::Uuid::now_v7();
        let mut trashed = Vec::new();
        for dir in [
            self.msg_tp_part_dir(topic, part, group),
            self.tp_part_dir(topic, part, group),
            self.snap_dir(topic, part, group),
        ] {
            if !dir.exists() {
                continue;
            }
            let mut trash = dir.clone().into_os_string();
            trash.push(format!(".trash-{suffix}"));
            let trash = PathBuf::from(trash);
            fs::rename(&dir, &trash).map_err(io_err)?;
            trashed.push(trash);
        }
        Ok(trashed)
    }

    pub async fn freeze_queue_for_transition(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, group).await?;
        let qh = qh.resolve()?;
        qh.freeze_owner_and_wait_operations().await?;
        qh.msg_log().freeze();
        qh.event_log().freeze();
        Ok(())
    }

    pub fn is_materialized(&self, topic: &str, part: u32, group: Option<&str>) -> bool {
        let current = self.queue_handles.load();
        slot_lookup_no_alloc(current.as_ref(), topic, part, group)
            .is_some_and(|slot| slot.handle.get().is_some())
    }

    pub async fn has_inflight(&self, topic: &str, part: u32, group: Option<&str>) -> Result<bool> {
        let current = self.queue_handles.load();
        let Some(slot) = slot_lookup_no_alloc(current.as_ref(), topic, part, group) else {
            return Ok(false);
        };
        let Some(handle) = slot.handle.get() else {
            return Ok(false);
        };
        Ok(handle.inflight_len().await > 0)
    }

    async fn ensure_queue(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        self.queue_handle(tp, part, group).await?;

        Ok(())
    }

    fn mark_all_queue_recoveries_complete(&self) {
        let current = self.queue_handles.load();

        for cell in current.values() {
            if let Some(qh) = cell.handle.get() {
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
        let queue = queue.resolve()?;
        Ok(queue.applied_upto())
    }

    // ---------------- Event apply rules ----------------

    fn queue_handle_sync(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> std::io::Result<QueueHandle> {
        let group = normalize_group(group);
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
        let inner = cell
            .handle
            .get()
            .ok_or_else(|| io::Error::other("queue handle not initialized"))?;
        Ok(self.ticket_for(tp, part, group, inner))
    }

    fn enqueue_event_inmem(&self, ev: StromaEvent, qh: &QueueHandleInner) -> std::io::Result<()> {
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
            StromaEvent::ReleaseInflightMany { reqs } => {
                let command = QueueCommand::ReleaseInflightMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
                // Release is a broker handoff primitive. It returns currently
                // leased offsets to ready without consuming retry budget.
            }
            StromaEvent::Nack { off, requeue } => {
                let command = QueueCommand::Nack {
                    offset: off,
                    requeue,
                    not_before: None,
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

    pub(crate) async fn apply_event_inmem(&self, ev: StromaEvent, qh: &QueueHandleInner) -> Result<()> {
        tracing::debug!("Applying event: {ev:?}");
        match ev {
            StromaEvent::Enqueue { off, retries } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::Enqueue {
                    offset: off,
                    retries,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::EnqueueMany { reqs } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::EnqueueMany {
                    reqs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::EnqueueDelayed { off, not_before } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::EnqueueDelayed {
                    offset: off,
                    not_before,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::EnqueueDelayedMany { reqs } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::EnqueueDelayedMany {
                    reqs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::MarkInflight { off, deadline } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::MarkInflight {
                    offset: off,
                    deadline,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::MarkInflightMany { reqs } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::MarkInflightMany {
                    reqs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::Ack { off } => {
                // Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::Ack {
                    offset: off,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::AckMany { reqs } => {
                // Accept ACK even if not inflight:
                // - race with expiry worker
                // - duplicate ACKs
                // - late ACK after consumer retry
                // ACK is idempotent and safe.
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::AckMany {
                    reqs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::ReleaseInflightMany { reqs } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::ReleaseInflightMany {
                    reqs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::Nack { off, requeue } => {
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::Nack {
                    offset: off,
                    requeue,
                    not_before: None,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::NackMany { reqs } => {
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - duplicate NACKs
                // - late NACK after consumer retry
                // NACK is idempotent and safe.
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::NackMany {
                    reqs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::DeadLetter { reqs } => {
                // On replay we just mark pending; recovery scan will re-issue copies.
                let offsets: Vec<Offset> = reqs.iter().map(|r| r.off).collect();
                // We need state.mark_pending_dlq, OR fold via nack(_, false)+pending insert.
                // Cleanest: add an explicit MarkPendingDlq command for replay.
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::MarkPendingDlq {
                    offsets,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::DeadLetterCommit { offs } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::DeadLetterCommit {
                    offsets: offs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::Declare(meta) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::Declare {
                    meta,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
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
        let qh = self.queue_handle(tp, part, group).await?;
        let owner_operation = qh.resolve()?.begin_owner_operation().await?;

        self.append_events_durable_leased(qh, evs, durability, owner_operation)
            .await
    }

    async fn append_events_durable_leased(
        &self,
        qh: QueueHandle,
        evs: Vec<StromaEvent>,
        durability: KDurability,
        _owner_operation: OwnerOperationLease,
    ) -> Result<Offset> {
        // Resolve once for the whole batch (hot path): reuse the live incarnation
        // for every per-record apply rather than re-resolving per record.
        let qh = qh.resolve()?;
        let start = Instant::now();
        let event_log = qh.event_log();
        let mut msgs = Vec::with_capacity(evs.len());
        for ev in &evs {
            msgs.push(event_msg(ev)?);
        }
        let msgs_count = msgs.len();
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
        let cache_events = evs.clone();
        for ev in evs.into_iter() {
            self.apply_event_inmem(ev, &qh).await?;
        }
        self.cache_owner_events(&qh, ar.base_offset, cache_events);

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
        let q = q.resolve()?;
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
        let q = q.resolve()?;
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
        let q = q.resolve()?;
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
        let q = q.resolve()?;
        Ok(q.filter_not_enqueued(items).await)
    }

    fn queue_keys_snapshot(&self) -> Vec<(Box<str>, u32, Option<Box<str>>)> {
        let map = self.queue_handles.load();

        let mut keys = Vec::with_capacity(map.len());

        for (k, qh) in map.iter() {
            if qh.handle.get().is_none() {
                continue;
            }
            keys.push(k.clone());
        }

        keys
    }

    pub async fn next_expiry_hint(&self) -> Result<Option<UnixMillis>> {
        let mut min: Option<UnixMillis> = None;
        let keys = self.queue_keys_snapshot();
        for (t, p, g) in keys {
            let qh = self.queue_handle(&t, p, g.as_deref()).await?;
            let qh = qh.resolve()?;
            if qh.role() != QueueRole::Owner {
                continue;
            }
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
        let q = q.resolve()?;
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
            let qh = qh.resolve()?;
            if qh.role() != QueueRole::Owner {
                continue;
            }
            if out.len() >= max {
                break;
            }
            let want = max - out.len();
            for off in qh.collect_expired(now, want).await? {
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

        // TODO: Examine ability to do in parallel, perhaps a joinset
        for (tp, part, group, off) in expired {
            let meta = NackEventMeta {
                off,
                requeue: true,
                not_before: None,
            };

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
    /// Recovery loads snapshots, then starts event replay after the snapshot
    /// offset so events already covered by the snapshot are not read again.
    async fn periodic_snapshot_step(stroma: &Stroma, qh: &QueueHandle) -> Result<()> {
        let qh = qh.resolve()?;
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
            .write_snapshots_for_partition(tp, part, group, applied_upto)
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
        // `qh` is a TICKET (Weak): this task never pins the incarnation. It
        // resolves per tick and exits once the incarnation is gone.
        let background_tasks = {
            let Ok(h) = qh.resolve() else {
                return;
            };
            if !h.try_start_snapshot_task() {
                return;
            }
            h.background_cancellation_token()
        };

        let stroma = self.clone();

        self.task_group.spawn("periodic snapshot", async move {
            match qh.resolve() {
                Ok(h) => h.wait_recovery_complete().await,
                Err(_) => return,
            }

            tracing::info!(
                "Starting periodic snapshot service for tp={} part={} group={}",
                qh.topic(),
                qh.partition(),
                qh.group().unwrap_or("Default")
            );

            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = background_tasks.cancelled() => break,
                    _ = ticker.tick() => {}
                }

                // Self-exit if the incarnation is gone (orphaned task): we hold
                // only a Weak, so there is nothing to keep alive.
                if qh.resolve().is_err() {
                    break;
                }

                let res = Self::periodic_snapshot_step(&stroma, &qh).await;
                #[cfg(test)]
                stroma.snapshot_worker_ticks.notify_waiters();
                if let Err(err) = res {
                    tracing::error!("Error during periodic snapshot: {err}");
                }
            }
        });
    }

    async fn write_snapshots_for_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        applied_upto: Offset,
    ) -> Result<()> {
        let qh = self.queue_handle(tp, part, group).await?;
        let qh = qh.resolve()?;
        let blob = if let Ok(blob) = qh.encode_snapshot(applied_upto).await {
            blob
        } else {
            // TODO:
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

    pub(crate) fn write_queue_snapshot(
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

    // TODO: Reuse once we define opt in eager queues to load early, vs ones that are loaded on demand
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
                let qh = qh.resolve()?;
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
        let qh = self.queue_handle(tp, part, group).await?;

        self.recover_one_log_with_handle(&qh, tp, part, group, event_log)
            .await
    }

    fn recover_events_from_log(
        event_log: Arc<Keratin>,
        mut cur: u64,
        tail: u64,
        applied_upto: Arc<AtomicU64>,
    ) -> Result<(Vec<StromaEvent>, u64)> {
        let reader = event_log.reader();

        let mut events = Vec::new();
        let mut events_count = 0;

        while cur < tail {
            let batch = reader.scan_from(cur, 10_000).map_err(io_err)?;
            if batch.is_empty() {
                break;
            }

            for rec in batch {
                let offset = rec.offset;
                cur = offset + 1;
                let ev = StromaEvent::decode(&rec.payload).map_err(|err| {
                    StromaError::Decode(format!(
                        "event log decode failed at offset {}: {err}",
                        offset
                    ))
                })?;
                events.push(ev);
                applied_upto.store(offset, Ordering::Release);
                events_count += 1;
            }
        }

        Ok((events, events_count))
    }

    async fn recover_one_log_with_handle(
        &self,
        qh: &QueueHandle,
        tp: &str,
        part: u32,
        group: Option<&str>,
        event_log: Arc<Keratin>,
    ) -> Result<()> {
        let start = Instant::now();
        // The incarnation is alive for the whole build/recovery (the build closure
        // holds the strong Arc), so resolve once and reuse. `qh` (the ticket) is
        // kept for cloning into the spawned DLQ-recovery tasks (non-pinning).
        let h = qh.resolve()?;

        tracing::info!(
            "Recovering log tp: {tp}, partition: {part}, group: {}",
            group.unwrap_or("Default")
        );

        #[cfg(test)]
        {
            self.lazy_recoveries_started.fetch_add(1, Ordering::Relaxed);
        }

        let snap_load_start = Instant::now();
        // load snapshot...

        let mut cur = 0u64;

        if let Some((applied_upto, blob)) =
            self.read_queue_snapshot(&self.snap_file(tp, part, group))?
        {
            h.load_snapshot(blob).await.map_err(|err| {
                StromaError::Io(format!(
                    "snapshot load failed for tp={tp} part={part} group={group:?}: {err}"
                ))
            })?;
            h.applied_upto().store(applied_upto, Ordering::Release);
            cur = applied_upto.saturating_add(1);
        }

        let tail = event_log.next_offset();
        let applied_upto = h.applied_upto();

        self.metrics
            .recovery
            .snapshot_load_latency
            .observe(snap_load_start.elapsed());

        let replay_start = Instant::now();

        #[cfg(test)]
        {
            if let Ok(mut starts) = self.recovery_event_scan_starts.lock() {
                starts.push(cur);
            }
        }

        let (events, events_count) = tokio::task::spawn_blocking(move || {
            Self::recover_events_from_log(event_log, cur, tail, applied_upto)
        })
        .await
        .map_err(|err| StromaError::Io(err.to_string()))??;

        for ev in events {
            self.apply_event_inmem(ev, &h).await?;
        }

        let pending = h.pending_dlq().await?;
        let source_tp = tp;
        let source_part = part;
        let source_group = group;
        let target = h.get_dlq_target().await?;
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
                        retry_count: 0,
                        reason: DeadLetterReason::PendingRecovery,
                        target_tp: tp.clone().into(),
                        target_part: part,
                        target_group: grp.clone().map(Into::into),
                    };
                    let src = src.clone();
                    tokio::spawn(async move {
                        let fut: std::pin::Pin<
                            Box<dyn std::future::Future<Output = Result<()>> + Send>,
                        > = Box::pin(stroma.dlq_copy_then_commit(src.clone(), qh2, meta, None));
                        fut.await.unwrap_or_else(|err| {
                            let (source_tp, source_part, source_group) = src;
                            tracing::error!(
                                "Error in dlq copy task for tp={} part={} group={:?} off={}: {err}",
                                source_tp,
                                source_part,
                                source_group,
                                off
                            );
                        });
                    });
                }
                None => {
                    // Policy is now Discard -> ack locally.
                    // self.commit_dlq_event(&qh, vec![off]).await;
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

    fn remove_queue(&self, tp: &str, part: u32, group: Option<&str>) -> Option<Arc<QueueSlot>> {
        let group = normalize_group(group);
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
        use futures::stream::{FuturesUnordered, StreamExt};

        // Step 1: atomically take ownership of all queues
        let old = self.queue_handles.swap(Arc::new(hashbrown::HashMap::new()));

        // Step 2: stop per-queue background work before draining queue actors.
        // Snapshot tasks can otherwise enqueue work while shutdown is trying to
        // drain the same queue, which is especially visible in slow CI runs.
        for (_key, slot) in old.iter() {
            if let Some(q) = slot.handle.get() {
                q.cancel_background_tasks();
            }
        }

        // Step 3: shutdown everything from the old snapshot
        let mut futs = FuturesUnordered::new();
        for (_key, slot) in old.iter() {
            if let Some(q) = slot.handle.get() {
                let q = q.clone();
                futs.push(async move {
                    q.shutdown().await;
                    q.event_log().shutdown().await.map_err(io_err)?;
                    q.msg_log().shutdown().await.map_err(io_err)?;
                    Ok::<_, StromaError>(())
                });
            }
        }

        let mut first_error = None;
        while let Some(result) = futs.next().await {
            if let Err(err) = result
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }

        self.task_group.shutdown().await;
        if let Some(global_store) = self.global_store.get()
            && let Err(err) = global_store.shutdown().await
            && first_error.is_none()
        {
            first_error = Some(err);
        }
        if let Some(err) = first_error {
            return Err(err);
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

    pub async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot> {
        let map = self.queue_handles.load();

        use futures::stream::{FuturesUnordered, StreamExt};

        let mut futs = FuturesUnordered::new();
        let mut queues = Vec::with_capacity(map.len());
        let mut materialized_queue_count = 0;
        for ((tp, part, group), slot) in map.iter() {
            let materialized = slot.handle.get().cloned();
            let evicting = slot.is_evicting();
            let exists_on_disk = slot.exists_on_disk;
            let tp = tp.clone();
            let group = group.clone();
            if let Some(qh) = materialized {
                materialized_queue_count += 1;
                futs.push(async move {
                    let mut info = qh.full_debug_info().await;
                    info.materialized = true;
                    info.exists_on_disk = exists_on_disk;
                    info.evicting = evicting;
                    Ok::<_, StromaError>(info)
                });
                continue;
            }

            queues.push(QueueDebugInfo {
                topic: tp.to_string(),
                partition: *part,
                group: group.map(|group| group.to_string()),
                materialized: false,
                exists_on_disk,
                evicting,
                applied_upto: 0,
                last_snapshot_timestamp: 0,
                last_snapshot_event_offset: 0,
                dirty_since_snapshot: false,
                creating_snapshot: false,
                role: QueueRole::Owner,
                role_generation: 0,
                state: QueueInternalDebugInfo::default(),
            });
        }

        drop(map);

        while let Some(result) = futs.next().await {
            queues.push(result?);
        }

        queues.sort_by(|a, b| {
            a.group
                .cmp(&b.group)
                .then_with(|| a.topic.cmp(&b.topic))
                .then_with(|| a.partition.cmp(&b.partition))
        });

        Ok(StromaDebugSnapshot {
            queue_count: queues.len(),
            materialized_queue_count,
            queues,
            cmd_queue_depths: self.metrics.cmd_queue_depths_snapshot(),
            snapshot_metrics: self.metrics.snapshot.snapshot(),
            recovery_metrics: self.metrics.recovery.snapshot(),
            log_metrics: self.metrics.log_snapshot(),
            replication_cache_metrics: self.metrics.replication_cache.snapshot(),
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
        writeln!(out, "Indexed queues: {}", snap.queue_count).unwrap();
        writeln!(
            out,
            "Materialized queues: {}",
            snap.materialized_queue_count
        )
        .unwrap();
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
                "  {}/{}/{}: loaded={} ready={} inflight={} settled={} dirty={}",
                q.topic,
                q.partition,
                g,
                q.materialized,
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

        let qh = self.queue_handle(tp, part, group).await?;
        let (owner_operation, msg_log) = {
            let h = qh.resolve()?;
            (h.begin_owner_operation().await?, h.msg_log())
        };

        // Build msg_log batch and extract per client completions.
        // Per message header encode failures fail just that one completion.
        let mut messages = Vec::with_capacity(items.len());
        let mut cache_messages = Vec::with_capacity(items.len());
        let mut completion_items = Vec::with_capacity(items.len());
        for item in items {
            let PublishItem {
                headers,
                payload,
                completion,
                not_before,
            } = item;
            if let Err(err) = validate_user_message_headers(&headers) {
                completion.complete(Err(IoError::new(err.to_string())));
                continue;
            }
            let header_bytes = match headers.encode() {
                Ok(b) => b,
                Err(err) => {
                    completion.complete(Err(IoError::new(err.to_string())));
                    continue;
                }
            };
            let message = Message {
                flags: 0,
                headers: header_bytes,
                payload,
            };
            cache_messages.push(message.clone());
            messages.push(message);
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
        let msg_completion = MsgBatchCompletion::new(
            stroma,
            completion_items,
            cache_messages,
            self.keratin_cfg_msg.default_durability,
            qh,
            owner_operation,
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
        event_completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        validate_user_message_headers(headers)?;
        self.append_message_unchecked(tp, part, group, headers, payload, event_completion)
            .await
    }

    async fn append_message_unchecked(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        event_completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let (msg_completion, msg_rx) = KeratinAppendCompletion::pair();
        let qh = self.queue_handle(tp, part, group).await?;
        let (owner_operation, msg_log) = {
            let h = qh.resolve()?;
            (h.begin_owner_operation().await?, h.msg_log())
        };
        let message = Message {
            flags: 0,
            headers: headers.encode()?,
            payload,
        };
        let cache_message = message.clone();
        msg_log
            .append_enqueue(message, None, msg_completion)
            .map_err(io_err)?;
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

            let durability = stroma.keratin_cfg_msg.default_durability;

            let event_res = stroma
                .append_events_durable_leased(qh.clone(), vec![ev], durability, owner_operation)
                .await;

            match event_res {
                Ok(_event_offset) => {
                    if let Ok(qh) = qh.resolve() {
                        stroma.cache_owner_messages(&qh, msg_offset, vec![cache_message]);
                    }
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
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let ev = StromaEvent::Ack { off: offset };

        let qh = self.queue_handle(tp, part, group).await?;
        let (owner_operation, event_log) = {
            let h = qh.resolve()?;
            (h.begin_owner_operation().await?, h.event_log())
        };
        let event_msg = event_msg(&ev)?;
        let outter_completion =
            ApplyThenComplete::new(self.clone(), ev, qh, owner_operation, completion);
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
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let ev = StromaEvent::AckMany { reqs };

        let qh = self.queue_handle(tp, part, group).await?;
        let (owner_operation, event_log) = {
            let h = qh.resolve()?;
            (h.begin_owner_operation().await?, h.event_log())
        };
        let event_msg = event_msg(&ev)?;
        let outter_completion =
            ApplyThenComplete::new(self.clone(), ev, qh, owner_operation, completion);
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
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        self.nack_enqueue_many(
            tp,
            part,
            group,
            vec![NackEventMeta {
                off: offset,
                requeue,
                not_before: None,
            }],
            completion,
        )
        .await
    }

    pub async fn release_inflight_many(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        reqs: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let qh = self.queue_handle(tp, part, group).await?;
        let qh = qh.resolve()?;
        let owner_operation = qh.begin_owner_operation().await?;
        let event_log = qh.event_log();

        let event = StromaEvent::ReleaseInflightMany { reqs: reqs.clone() };
        let m = event_msg(&event)?;
        let ar = event_log
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
            .map_err(io_err)?;
        qh.applied_upto()
            .fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        qh.set_dirty_snapshot(true);

        qh.release_inflight_many(reqs)
            .await
            .map_err(|err| StromaError::Io(err.to_string()))?;

        completion.complete(Ok(ar));
        drop(owner_operation);
        Ok(())
    }

    pub async fn nack_enqueue_many(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        reqs: Vec<NackEventMeta>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let qh = self.queue_handle(tp, part, group).await?;
        let h = qh.resolve()?;
        let owner_operation = h.begin_owner_operation().await?;
        let event_log = h.event_log();

        // Phase 1: durable Nack write
        let nack_event = StromaEvent::NackMany { reqs: reqs.clone() };
        let m = event_msg(&nack_event)?;
        let ar = event_log
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
            .map_err(io_err)?;
        h.applied_upto()
            .fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        h.set_dirty_snapshot(true);

        // Apply -> get outcomes
        let outcomes = {
            let (tx, rx) = tokio::sync::oneshot::channel();
            h.command_enqueue(QueueCommand::NackMany {
                reqs,
                response: Some(tx),
            })
            .await
            .map_err(io_err)?;
            rx.await.map_err(|_| StromaError::QueueActorGone)?
        };
        let dl_requests: Vec<(Offset, u32, DeadLetterReason)> = outcomes
            .iter()
            .filter_map(|(o, oc)| match oc {
                NackOutcome::DeadLetterRequested {
                    retry_count,
                    reason,
                } => Some((*o, *retry_count, *reason)),
                _ => None,
            })
            .collect();

        if dl_requests.is_empty() {
            completion.complete(Ok(ar));
            return Ok(());
        }

        // Phase 2: resolve policy, decide per-offset
        let (to_dlq, to_discard) = self.resolve_dlq_targets(&h, &dl_requests).await?;

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
            h.discard_pending_dlq(to_discard).await?;
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
                let src = (
                    tp.to_string(),
                    part,
                    normalize_group(group).map(String::from),
                );
                let qh2 = qh.clone();
                let owner_operation = owner_operation.clone_for_continuation();
                tokio::spawn(async move {
                    stroma
                        .dlq_copy_then_commit(src.clone(), qh2, meta.clone(), Some(owner_operation))
                        .await
                        .unwrap_or_else(|err| {
                            let (source_tp, source_part, source_group) = src;
                            tracing::error!(
                                "Error in dlq copy task for tp={} part={} group={:?} off={}: {err}",
                                source_tp,
                                source_part,
                                source_group,
                                meta.off
                            );
                        });
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
        qh: &QueueHandleInner,
        requests: &[(Offset, u32, DeadLetterReason)],
    ) -> Result<(Vec<DeadLetterMeta>, Vec<Offset>)> {
        let resolved = match qh.get_dlq_target().await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(
                    "Failed to get DLQ target for {}/{}/{}: {e}",
                    qh.topic(),
                    qh.partition(),
                    qh.group().unwrap_or("Default")
                );
                return Err(StromaError::NotFound(format!(
                    "DLQ target not found for {}/{}/{}",
                    qh.topic(),
                    qh.partition(),
                    qh.group().unwrap_or("Default")
                )));
            }
        };
        match resolved {
            Some((tp, part, grp)) => {
                let metas = requests
                    .iter()
                    .map(|(off, retry_count, reason)| DeadLetterMeta {
                        off: *off,
                        retry_count: *retry_count,
                        reason: *reason,
                        target_tp: tp.clone().into(),
                        target_part: part,
                        target_group: grp.clone().map(Into::into),
                    })
                    .collect();
                Ok((metas, Vec::new()))
            }
            None => Ok((
                Vec::new(),
                requests.iter().map(|(off, _, _)| *off).collect(),
            )),
        }
    }

    fn dlq_copy_then_commit(
        &self,
        src: (String, u32, Option<String>),
        src_qh: QueueHandle,
        meta: DeadLetterMeta,
        mut owner_operation: Option<OwnerOperationLease>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            const MAX_ATTEMPTS: u32 = 5;
            let (src_tp, src_part, src_group) = src;
            // Resolve once for this copy operation (the owner lease / teardown
            // contract keeps the source alive for its duration).
            let src_qh = src_qh.resolve()?;

            // Fetch source message.
            let msg = match self.fetch_message_by_offset(&src_qh, meta.off).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    tracing::error!(
                        "DLQ copy: source message {} missing in {}/{}/{:?} — discarding",
                        meta.off,
                        src_tp,
                        src_part,
                        src_group
                    );
                    self.commit_dlq_event_with_optional_lease(
                        &src_qh,
                        vec![meta.off],
                        owner_operation.take(),
                    )
                    .await?;
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!("DLQ copy: fetch failed: {e}");
                    self.commit_dlq_event_with_optional_lease(
                        &src_qh,
                        vec![meta.off],
                        owner_operation.take(),
                    )
                    .await?; // give up, ack-locally
                    return Ok(());
                }
            };

            let mut headers =
                MessageHeaders::decode(&msg.headers).unwrap_or_else(|_| MessageHeaders {
                    published: 0,
                    publish_received: 0,
                    content_type: None,
                    extra: HashMap::new(),
                });
            headers
                .extra
                .insert("stroma.dlq.source_topic".to_string(), src_tp.clone());
            if let Some(group) = &src_group {
                headers
                    .extra
                    .insert("stroma.dlq.source_group".to_string(), group.clone());
            }
            headers
                .extra
                .insert("stroma.dlq.source_offset".to_string(), meta.off.to_string());
            headers.extra.insert(
                "stroma.dlq.retry_count".to_string(),
                meta.retry_count.to_string(),
            );
            headers.extra.insert(
                "stroma.dlq.reason".to_string(),
                meta.reason.as_header().to_string(),
            );
            headers.extra.insert(
                "stroma.dlq.dead_lettered_at_ms".to_string(),
                unix_millis().to_string(),
            );

            // Preserve user headers and add Stroma-owned DLQ metadata only on
            // this uncommon path. Regular messages do not pay for these fields.
            let headers = headers;

            // Append to target with bounded retries.
            let mut attempt = 0u32;
            let target_group = meta.target_group.as_deref();
            loop {
                let (cmp, rx) = KeratinAppendCompletion::pair();
                let res = self
                    .append_message_unchecked(
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

            self.commit_dlq_event_with_optional_lease(
                &src_qh,
                vec![meta.off],
                owner_operation.take(),
            )
            .await?;

            Ok(())
        })
    }

    async fn commit_dlq_event(&self, qh: &QueueHandleInner, offs: Vec<Offset>) -> Result<()> {
        let owner_operation = qh.begin_owner_operation().await?;
        self.commit_dlq_event_leased(qh, offs, owner_operation)
            .await
    }

    async fn commit_dlq_event_with_optional_lease(
        &self,
        qh: &QueueHandleInner,
        offs: Vec<Offset>,
        owner_operation: Option<OwnerOperationLease>,
    ) -> Result<()> {
        match owner_operation {
            Some(owner_operation) => {
                self.commit_dlq_event_leased(qh, offs, owner_operation)
                    .await
            }
            None => self.commit_dlq_event(qh, offs).await,
        }
    }

    async fn commit_dlq_event_leased(
        &self,
        qh: &QueueHandleInner,
        offs: Vec<Offset>,
        _owner_operation: OwnerOperationLease,
    ) -> Result<()> {
        let ev = StromaEvent::DeadLetterCommit { offs: offs.clone() };
        let Ok(m) = event_msg(&ev) else {
            return Err(StromaError::Encode(
                "Failed to encode DeadLetterCommit event".into(),
            ));
        };

        if let Err(e) = qh
            .event_log()
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
        {
            tracing::error!("DeadLetterCommit append failed: {e}");
            return Err(StromaError::Encode(
                "Failed to encode DeadLetterCommit event".into(),
            ));
        }
        self.apply_event_inmem(StromaEvent::DeadLetterCommit { offs }, qh)
            .await?;
        Ok(())
    }

    pub async fn fetch_message_by_offset(
        &self,
        qh: &QueueHandleInner,
        off: Offset,
    ) -> Result<Option<Message>> {
        let log = qh.msg_log();
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
        upper: Offset,
    ) -> Result<Vec<(Offset, MessageHeaders, Vec<u8>, u32)>> {
        let qs = self.queue_handle(tp, part, group).await?;
        let qs = qs.resolve()?;

        // Offsets are now already marked inflight inside queue. `upper` is the
        // exclusive deliverable ceiling (replica-durable committed watermark);
        // u64::MAX disables it for local-durable queues.
        let offs = qs.poll_ready_and_mark(max, lease_deadline, upper).await?;

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
                    let h = qh.resolve()?;
                    let batch: Vec<(u64, Vec<u8>, MessageHeaders)> =
                        stroma.scan_messages_from(&h, start, len)?;

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
        qh: &QueueHandleInner,
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

    pub async fn inspect_messages(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        limit: usize,
        mode: InspectMode,
        include_payload: bool,
        payload_limit_bytes: usize,
    ) -> Result<MessageInspectionPage> {
        let qh = self.queue_handle(tp, part, group).await?;
        let qh = qh.resolve()?;
        let snapshot: QueueInspectionSnapshot = qh.inspect_offsets(from, limit, mode).await;
        if snapshot.items.is_empty() {
            return Ok(MessageInspectionPage {
                next_offset_hint: snapshot.next_offset_hint,
                items: Vec::new(),
            });
        }

        let mut records = HashMap::with_capacity(snapshot.items.len());
        for (start, len) in contiguous_spans(snapshot.items.iter().map(|item| item.offset)) {
            for (offset, payload, headers) in self.scan_messages_from(&qh, start, len)? {
                records.insert(offset, (payload, headers));
            }
        }

        let items = snapshot
            .items
            .into_iter()
            .filter_map(|state| {
                let Some((payload, headers)) = records.remove(&state.offset) else {
                    return None;
                };

                let payload_len = payload.len();
                let payload_truncated = include_payload && payload_len > payload_limit_bytes;
                let payload = if include_payload {
                    Some(payload.into_iter().take(payload_limit_bytes).collect())
                } else {
                    None
                };

                Some(MessageInspectionItem {
                    state,
                    headers: Some(headers),
                    payload_len: Some(payload_len),
                    payload,
                    payload_truncated,
                    missing_payload: false,
                })
            })
            .collect();

        Ok(MessageInspectionPage {
            next_offset_hint: snapshot.next_offset_hint,
            items,
        })
    }

    pub async fn current_next_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let msg_log = self.queue_handle(tp, part, group).await?.resolve()?.msg_log();
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
        let msg_log = self.queue_handle(tp, part, group).await?.resolve()?.msg_log();
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
        let qh = qh.resolve()?;
        let settled_until = qh.settled_until().await;
        let min = settled_until.min(qh.lowest_not_acked_offset().await);

        Ok(min)
    }

    pub fn list_queues(&self) -> Vec<(Box<str>, u32, Option<Box<str>>)> {
        let map = self.queue_handles.load();
        map.keys()
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
        let q = q.resolve()?;
        Ok(q.is_acked(off).await)
    }

    pub async fn count_inflight(&self, tp: &str, part: u32, group: Option<&str>) -> Result<usize> {
        let q = self.queue_handle(tp, part, group).await?;
        let q = q.resolve()?;
        Ok(q.inflight_len().await)
    }

    pub fn list_topics(&self) -> Vec<Box<str>> {
        // TODO: Should return groups too
        let map = self.queue_handles.load();
        map.keys()
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
            let qh = qh.resolve()?;
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
            let qh = qh.resolve()?;
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
        self.write_snapshots_for_partition(tp, part, group, upto).await
    }

    pub async fn truncate_partition_log(
        &self,
        qh: QueueHandle,
        before_event: Offset,
    ) -> Result<u64> {
        let qh = qh.resolve()?;
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
        let q = q.resolve()?;
        Ok(format!("{:#?}", q.canonical().await))
    }

    pub async fn validate(&self) -> Result<()> {
        let keys = self.queue_keys_snapshot();
        for (k_tp, k_part, k_group) in keys {
            let qh = self.queue_handle(&k_tp, k_part, k_group.as_deref()).await?;
            let qh = qh.resolve()?;
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

pub(crate) fn replicated_append_outcome_allows_state_apply(outcome: &ReplicatedAppendOutcome) -> bool {
    matches!(
        outcome,
        ReplicatedAppendOutcome::Applied(_)
            | ReplicatedAppendOutcome::AppliedSuffix { .. }
            | ReplicatedAppendOutcome::AlreadyPresent { .. }
    )
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

fn contiguous_spans(offsets: impl IntoIterator<Item = Offset>) -> Vec<(Offset, usize)> {
    let mut spans = Vec::new();
    let mut iter = offsets.into_iter();
    let Some(mut start) = iter.next() else {
        return spans;
    };
    let mut last = start;
    let mut len = 1usize;

    for offset in iter {
        if offset == last.saturating_add(1) {
            last = offset;
            len += 1;
        } else {
            spans.push((start, len));
            start = offset;
            last = offset;
            len = 1;
        }
    }

    spans.push((start, len));
    spans
}

fn assert_send<T: Send>(_: T) {}

#[allow(dead_code)]
fn queue_handle_future_is_send(stroma: Stroma) {
    assert_send(stroma.queue_handle("topic", 0, None));
}

#[allow(dead_code)]
fn recover_future_is_send(stroma: Stroma, qh: QueueHandle, event_log: Arc<Keratin>) {
    assert_send(stroma.recover_one_log_with_handle(&qh, "topic", 0, None, event_log));
}

#[allow(dead_code)]
fn dlq_then_commit_future_is_send(stroma: Stroma, qh: QueueHandle, meta: DeadLetterMeta) {
    assert_send(stroma.dlq_copy_then_commit(("topic".to_string(), 0, None), qh, meta, None));
}

#[allow(dead_code)]
fn assert_append_message_send(stroma: Stroma) {
    fn assert_send<T: Send>(_: T) {}
    assert_send(async move {
        let headers = MessageHeaders {
            published: 0,
            publish_received: 0,
            content_type: None,
            extra: Default::default(),
        };
        let (cmp, _rx) = KeratinAppendCompletion::pair();
        let _ = stroma
            .append_message("t", 0, None, &headers, vec![], cmp)
            .await;
    });
}

#[cfg(test)]
impl Stroma {
    fn indexed_queue_count(&self) -> usize {
        self.queue_handles.load().len()
    }

    fn materialized_queue_count(&self) -> usize {
        self.queue_handles
            .load()
            .values()
            .filter(|slot| slot.handle.get().is_some())
            .count()
    }

    fn is_queue_materialized(&self, tp: &str, part: u32, group: Option<&str>) -> bool {
        let group = normalize_group(group);
        let key = (Box::<str>::from(tp), part, group.map(Box::<str>::from));

        self.queue_handles
            .load()
            .get(&key)
            .and_then(|slot| slot.handle.get())
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use keratin_log::{KeratinReplicaExt, test_dir};

    use super::*;
    use crate::state::QueueInternalState;

    async fn test_step<T>(
        label: impl std::fmt::Display,
        fut: impl std::future::Future<Output = T>,
    ) -> T {
        let label = label.to_string();
        tokio::time::timeout(Duration::from_secs(15), fut)
            .await
            .unwrap_or_else(|_| panic!("stroma test step timed out: {label}"))
    }

    async fn shutdown_stroma(label: impl std::fmt::Display, stroma: &Stroma) {
        test_step(format!("{label}/shutdown"), stroma.shutdown())
            .await
            .expect("stroma shutdown failed");
    }

    fn test_keratin_config() -> StromaKeratinConfig {
        StromaKeratinConfig::from_message_log(KeratinConfig::default())
    }

    #[tokio::test]
    async fn global_dlq_setting_is_persisted_and_loaded() {
        let dir = test_dir!("global_dlq_setting");
        let target = GlobalDLQ::new("_dlq.orders", 0, Some("failed"))
            .await
            .unwrap();

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        assert_eq!(
            stroma.global_dlq().await.unwrap(),
            GlobalDlqSnapshot {
                version: 0,
                target: None,
            }
        );

        assert_eq!(
            stroma
                .set_global_dlq(Some(target.clone()), 0)
                .await
                .unwrap(),
            GlobalDlqUpdateOutcome::Stored(GlobalDlqSnapshot {
                version: 1,
                target: Some(target.clone()),
            })
        );
        assert_eq!(*stroma.global_dlq.read().await, Some(target.clone()));
        shutdown_stroma("global_dlq_setting_is_persisted_and_loaded/write", &stroma).await;
        drop(stroma);

        let recovered = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.global_dlq().await.unwrap(),
            GlobalDlqSnapshot {
                version: 1,
                target: Some(target.clone()),
            }
        );
        assert_eq!(*recovered.global_dlq.read().await, Some(target));
        shutdown_stroma(
            "global_dlq_setting_is_persisted_and_loaded/read",
            &recovered,
        )
        .await;
    }

    #[tokio::test]
    async fn global_dlq_setting_uses_expected_version() {
        let dir = test_dir!("global_dlq_setting_cas");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let first = GlobalDLQ::new("_dlq.one", 0, None).await.unwrap();
        let second = GlobalDLQ::new("_dlq.two", 0, None).await.unwrap();

        stroma.set_global_dlq(Some(first.clone()), 0).await.unwrap();
        assert_eq!(
            stroma.set_global_dlq(Some(second), 0).await.unwrap(),
            GlobalDlqUpdateOutcome::Conflict(GlobalDlqSnapshot {
                version: 1,
                target: Some(first.clone()),
            })
        );
        assert_eq!(
            stroma.set_global_dlq(None, 1).await.unwrap(),
            GlobalDlqUpdateOutcome::Stored(GlobalDlqSnapshot {
                version: 2,
                target: None,
            })
        );
        assert_eq!(*stroma.global_dlq.read().await, None);

        shutdown_stroma("global_dlq_setting_uses_expected_version", &stroma).await;
    }

    #[tokio::test]
    async fn global_dlq_setting_validates_target() {
        let dir = test_dir!("global_dlq_setting_validation");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let err = stroma
            .set_global_dlq(
                Some(GlobalDLQ {
                    tp: "BadTopic".into(),
                    part: 0,
                    group: None,
                }),
                0,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StromaError::InvalidArgument(_)));

        shutdown_stroma("global_dlq_setting_validates_target", &stroma).await;
    }

    #[tokio::test]
    async fn append_message_rejects_user_stroma_headers() {
        let dir = test_dir!("reserved_header_validation");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let mut headers = MessageHeaders {
            published: 0,
            publish_received: 0,
            content_type: None,
            extra: HashMap::new(),
        };
        headers
            .extra
            .insert("stroma.dlq.source_topic".to_string(), "source".to_string());
        let (cmp, _rx) = KeratinAppendCompletion::pair();

        let err = stroma
            .append_message("topic", 0, None, &headers, b"x".to_vec(), cmp)
            .await
            .unwrap_err();

        assert!(matches!(err, StromaError::InvalidArgument(_)));
        shutdown_stroma("append_message_rejects_user_stroma_headers", &stroma).await;
    }

    /// Microbench for the ticket/re-resolve overhead. Run with:
    ///   cargo test -p stroma-core --lib --release ticket_resolve_overhead_bench -- --ignored --nocapture
    /// Measures the per-op cost added by the ticket design: `resolve()` (one
    /// `Weak::upgrade`) on the hot path, vs. the full `queue_handle()` lookup.
    #[tokio::test]
    #[ignore]
    async fn ticket_resolve_overhead_bench() {
        let dir = test_dir!("ticket_resolve_overhead_bench");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        let ticket = stroma.queue_handle("topic", 0, None).await.unwrap();

        const N: u32 = 20_000_000;

        // resolve() in isolation: the per-batch cost added to hot paths.
        let start = std::time::Instant::now();
        let mut sink = 0u64;
        for _ in 0..N {
            let h = ticket.resolve().unwrap();
            sink = sink.wrapping_add(h.partition() as u64);
            std::hint::black_box(&sink);
        }
        let resolve_ns = start.elapsed().as_nanos() as f64 / N as f64;

        // Full queue_handle() lookup (registry load + key alloc + ticket build).
        let start = std::time::Instant::now();
        for _ in 0..(N / 20) {
            let t = stroma.queue_handle("topic", 0, None).await.unwrap();
            std::hint::black_box(&t);
        }
        let qh_ns = start.elapsed().as_nanos() as f64 / (N / 20) as f64;

        println!("resolve(): {resolve_ns:.1} ns/op    queue_handle(): {qh_ns:.1} ns/op");
        shutdown_stroma("ticket_resolve_overhead_bench", &stroma).await;
    }

    #[tokio::test]
    async fn inspect_messages_skips_offsets_without_log_records() {
        let dir = test_dir!("inspect_messages_missing_log_records");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        qh.enqueue(0, 0).await.unwrap();

        let page = stroma
            .inspect_messages("topic", 0, None, 0, 10, InspectMode::ActiveOnly, true, 1024)
            .await
            .unwrap();
        assert!(page.items.is_empty());

        let page = stroma
            .inspect_messages(
                "topic",
                0,
                None,
                0,
                10,
                InspectMode::IncludeSettled,
                true,
                1024,
            )
            .await
            .unwrap();
        assert!(page.items.is_empty());

        shutdown_stroma(
            "inspect_messages_skips_offsets_without_log_records",
            &stroma,
        )
        .await;
    }

    #[tokio::test]
    async fn owner_replication_read_returns_message_and_event_records() {
        let dir = test_dir!("owner_replication_read_records");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let offset = publish_one(&stroma, "topic", 0, None).await;
        assert_eq!(offset, 0);

        let messages = stroma
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(messages) = messages else {
            panic!("expected message batch");
        };
        assert_eq!(messages.epoch, 0);
        assert_eq!(messages.requested_offset, 0);
        assert_eq!(messages.next_offset, 1);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 0);
        assert_eq!(messages.records[0].1.payload, b"x");

        let events = stroma
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(events) = events else {
            panic!("expected event batch");
        };
        assert_eq!(events.epoch, 0);
        assert_eq!(events.requested_offset, 0);
        assert_eq!(events.next_offset, 1);
        assert_eq!(
            events.records,
            vec![(0, StromaEvent::Enqueue { off: 0, retries: 0 })]
        );

        shutdown_stroma("owner_replication_read_records", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_reads_use_recent_cache_and_fall_back_at_tail() {
        let dir = test_dir!("owner_replication_cache_hits");
        let stroma = Stroma::open_with_options(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
            StromaOptions {
                replication_cache: ReplicationCacheConfig::enabled(64 * 1024),
            },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;

        let messages = stroma
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(messages) = messages else {
            panic!("expected message batch");
        };
        assert_eq!(messages.records.len(), 1);

        let events = stroma
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(events) = events else {
            panic!("expected event batch");
        };
        assert_eq!(events.records.len(), 1);

        let cache_metrics = stroma.metrics.replication_cache.snapshot();
        assert_eq!(cache_metrics.message_hits, 1);
        assert_eq!(cache_metrics.message_misses, 0);
        assert_eq!(cache_metrics.event_hits, 1);
        assert_eq!(cache_metrics.event_misses, 0);
        assert!(cache_metrics.retained_bytes > 0);

        let tail = stroma
            .read_owner_message_records("topic", 0, None, 1, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(tail) = tail else {
            panic!("expected message batch");
        };
        assert!(tail.records.is_empty());
        assert_eq!(tail.next_offset, 1);

        let cache_metrics = stroma.metrics.replication_cache.snapshot();
        assert_eq!(cache_metrics.message_hits, 1);
        assert_eq!(cache_metrics.message_misses, 1);

        shutdown_stroma("owner_replication_cache_hits", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_cache_is_disabled_by_default() {
        let dir = test_dir!("owner_replication_cache_disabled");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;

        let messages = stroma
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(messages) = messages else {
            panic!("expected message batch");
        };
        assert_eq!(messages.records.len(), 1);

        let events = stroma
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(events) = events else {
            panic!("expected event batch");
        };
        assert_eq!(events.records.len(), 1);

        let cache_metrics = stroma.metrics.replication_cache.snapshot();
        assert_eq!(cache_metrics.message_hits, 0);
        assert_eq!(cache_metrics.message_misses, 0);
        assert_eq!(cache_metrics.event_hits, 0);
        assert_eq!(cache_metrics.event_misses, 0);
        assert_eq!(cache_metrics.retained_bytes, 0);

        shutdown_stroma("owner_replication_cache_disabled", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_read_next_offset_tracks_returned_batch() {
        let dir = test_dir!("owner_replication_read_bounded_next_offset");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        publish_one(&stroma, "topic", 0, None).await;

        let messages = stroma
            .read_owner_message_records("topic", 0, None, 0, 1)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(messages) = messages else {
            panic!("expected message batch");
        };
        assert_eq!(messages.requested_offset, 0);
        assert_eq!(messages.next_offset, 1);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 0);

        let messages = stroma
            .read_owner_message_records("topic", 0, None, messages.next_offset, 1)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(messages) = messages else {
            panic!("expected message batch");
        };
        assert_eq!(messages.requested_offset, 1);
        assert_eq!(messages.next_offset, 2);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 1);

        let events = stroma
            .read_owner_event_records("topic", 0, None, 0, 1)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(events) = events else {
            panic!("expected event batch");
        };
        assert_eq!(events.requested_offset, 0);
        assert_eq!(events.next_offset, 1);
        assert_eq!(events.records.len(), 1);
        assert_eq!(events.records[0].0, 0);

        let events = stroma
            .read_owner_event_records("topic", 0, None, events.next_offset, 1)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(events) = events else {
            panic!("expected event batch");
        };
        assert_eq!(events.requested_offset, 1);
        assert_eq!(events.next_offset, 2);
        assert_eq!(events.records.len(), 1);
        assert_eq!(events.records[0].0, 1);

        shutdown_stroma("owner_replication_read_bounded_next_offset", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_read_rejects_follower_queue() {
        let dir = test_dir!("owner_replication_read_rejects_follower");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        stroma
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();

        let err = stroma
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StromaError::WrongQueueRole {
                expected: QueueRole::Owner,
                actual: QueueRole::Follower
            }
        ));

        let err = stroma
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StromaError::WrongQueueRole {
                expected: QueueRole::Owner,
                actual: QueueRole::Follower
            }
        ));

        shutdown_stroma("owner_replication_read_rejects_follower", &stroma).await;
    }

    #[tokio::test]
    async fn stopped_follower_rejects_replicated_ingest() {
        let dir = test_dir!("stopped_follower_rejects_replicated_ingest");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        stroma
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();
        stroma
            .stop_queue_follower_for_transition("topic", 0, None)
            .await
            .unwrap();

        let err = stroma
            .apply_replicated_queue_batch("topic", 0, None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StromaError::WrongQueueRole {
                expected: QueueRole::Follower,
                actual: QueueRole::Frozen
            }
        ));

        shutdown_stroma("stopped_follower_rejects_replicated_ingest", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_gap_after_head_advance_requires_checkpoint() {
        let dir = test_dir!("owner_replication_gap_after_head_advance");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        qh.msg_log().become_follower();
        qh.msg_log()
            .destructive_reset_to_checkpoint(7)
            .await
            .unwrap();
        qh.msg_log().become_owner();

        let read = owner_replication_gap::<Message>("message", &qh.msg_log(), 5, 5, 7).unwrap();
        let OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        } = read
        else {
            panic!("expected checkpoint requirement");
        };
        assert_eq!(epoch, 0);
        assert_eq!(requested_offset, 5);
        assert_eq!(head_offset, 7);
        assert_eq!(next_offset, 7);

        shutdown_stroma("owner_replication_gap_after_head_advance", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_initial_gap_requires_checkpoint() {
        let dir = test_dir!("owner_replication_initial_gap");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        for _ in 0..10 {
            publish_one(&stroma, "topic", 0, None).await;
        }
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();

        let read = owner_replication_gap::<Message>("message", &qh.msg_log(), 0, 0, 7).unwrap();
        let OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        } = read
        else {
            panic!("expected checkpoint requirement");
        };
        assert_eq!(epoch, 0);
        assert_eq!(requested_offset, 0);
        assert_eq!(head_offset, 7);
        assert_eq!(next_offset, 10);

        shutdown_stroma("owner_replication_initial_gap", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_gap_without_head_advance_is_corruption() {
        let dir = test_dir!("owner_replication_gap_without_head_advance");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let err = owner_replication_gap::<Message>("message", &qh.msg_log(), 0, 3, 7).unwrap_err();
        assert!(matches!(err, StromaError::Corruption(_)));

        shutdown_stroma("owner_replication_gap_without_head_advance", &stroma).await;
    }

    #[tokio::test]
    async fn owner_replication_read_reports_checkpoint_required_after_truncation() {
        let dir = test_dir!("owner_replication_read_checkpoint_required");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        qh.msg_log().become_follower();
        qh.event_log().become_follower();
        qh.msg_log()
            .destructive_reset_to_checkpoint(1)
            .await
            .unwrap();
        qh.event_log()
            .destructive_reset_to_checkpoint(1)
            .await
            .unwrap();
        qh.msg_log().become_owner();
        qh.event_log().become_owner();

        let messages = stroma
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        } = messages
        else {
            panic!("expected message checkpoint requirement");
        };
        assert_eq!(epoch, 0);
        assert_eq!(requested_offset, 0);
        assert_eq!(head_offset, 1);
        assert_eq!(next_offset, 1);

        let events = stroma
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        assert_eq!(
            events,
            OwnerReplicationRead::CheckpointRequired {
                epoch: 0,
                requested_offset: 0,
                head_offset: 1,
                next_offset: 1,
            }
        );

        shutdown_stroma("owner_replication_read_checkpoint_required", &stroma).await;
    }

    #[tokio::test]
    async fn follower_state_checkpoint_install_rejects_owner_role() {
        let dir = test_dir!("follower_state_checkpoint_rejects_owner");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let snapshot = qh.encode_snapshot(0).await.unwrap();

        let err = stroma
            .install_follower_state_checkpoint(
                "topic",
                0,
                None,
                FollowerStateCheckpointInstall {
                    message_next_offset: 0,
                    event_next_offset: 1,
                    applied_event_offset: 0,
                    state_snapshot: snapshot,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StromaError::WrongQueueRole {
                expected: QueueRole::Follower,
                actual: QueueRole::Owner
            }
        ));

        shutdown_stroma("follower_state_checkpoint_rejects_owner", &stroma).await;
    }

    #[tokio::test]
    async fn follower_state_checkpoint_install_requires_matching_snapshot_offset() {
        let dir = test_dir!("follower_state_checkpoint_validates_offset");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let snapshot = qh.encode_snapshot(0).await.unwrap();
        stroma
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();

        let err = stroma
            .install_follower_state_checkpoint(
                "topic",
                0,
                None,
                FollowerStateCheckpointInstall {
                    message_next_offset: 1,
                    event_next_offset: 2,
                    applied_event_offset: 1,
                    state_snapshot: snapshot,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StromaError::InvalidArgument(_)));

        shutdown_stroma("follower_state_checkpoint_validates_offset", &stroma).await;
    }

    #[tokio::test]
    async fn follower_state_checkpoint_install_rejects_skipped_referenced_messages() {
        let dir = test_dir!("follower_state_checkpoint_validates_messages");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let snapshot = qh.encode_snapshot(1).await.unwrap();
        stroma
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();

        let err = stroma
            .install_follower_state_checkpoint(
                "topic",
                0,
                None,
                FollowerStateCheckpointInstall {
                    message_next_offset: 2,
                    event_next_offset: 2,
                    applied_event_offset: 1,
                    state_snapshot: snapshot,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StromaError::InvalidArgument(_)));

        shutdown_stroma("follower_state_checkpoint_validates_messages", &stroma).await;
    }

    #[tokio::test]
    async fn follower_state_checkpoint_install_resets_logs_but_messages_still_need_replication() {
        let owner_dir = test_dir!("state_checkpoint_owner");
        let owner = Stroma::open(
            &owner_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let follower_dir = test_dir!("state_checkpoint_follower");
        let follower = Stroma::open(
            &follower_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&owner, "topic", 0, None).await;
        publish_one(&owner, "topic", 0, None).await;
        let owner_qh = owner.queue_handle("topic", 0, None).await.unwrap();
        let owner_qh = owner_qh.resolve().unwrap();
        let state_snapshot = owner_qh.encode_snapshot(1).await.unwrap();

        follower
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();
        let outcome = follower
            .install_follower_state_checkpoint(
                "topic",
                0,
                None,
                FollowerStateCheckpointInstall {
                    message_next_offset: 0,
                    event_next_offset: 2,
                    applied_event_offset: 1,
                    state_snapshot,
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.message_next_offset, 0);
        assert_eq!(outcome.event_next_offset, 2);
        assert_eq!(outcome.applied_event_offset, 1);
        assert_eq!(outcome.snapshot_meta.last_snapshot_event_offset, 1);

        let not_ready = follower
            .promote_queue_follower_if_caught_up("topic", 0, None, 2, 2)
            .await
            .unwrap();
        assert_eq!(
            not_ready,
            QueuePromotionOutcome::MessageLogBehind {
                local_next_offset: 0,
                expected_next_offset: 2,
            }
        );

        let message_read = owner
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(message_read) = message_read else {
            panic!("expected owner message batch");
        };
        follower
            .apply_replicated_queue_batch(
                "topic",
                0,
                None,
                Some(ReplicatedMessageBatch {
                    epoch: message_read.epoch,
                    first_offset: message_read.records[0].0,
                    records: message_read
                        .records
                        .into_iter()
                        .map(|(_, message)| message)
                        .collect(),
                    durability: None,
                }),
                None,
            )
            .await
            .unwrap();

        let promoted = follower
            .promote_queue_follower_if_caught_up("topic", 0, None, 2, 2)
            .await
            .unwrap();
        assert_eq!(
            promoted,
            QueuePromotionOutcome::Promoted {
                message_next_offset: 2,
                event_next_offset: 2,
                applied_event_offset: Some(1),
            }
        );

        let delivered = follower
            .poll_ready("topic", 0, None, 10, unix_millis() + 30_000, u64::MAX)
            .await
            .unwrap();
        assert_eq!(delivered.len(), 2);

        shutdown_stroma("state_checkpoint_owner", &owner).await;
        shutdown_stroma("state_checkpoint_follower", &follower).await;
    }

    #[tokio::test]
    async fn owner_state_checkpoint_export_rejects_follower_role() {
        let dir = test_dir!("owner_state_checkpoint_rejects_follower");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&stroma, "topic", 0, None).await;
        stroma
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();

        let err = stroma
            .export_owner_state_checkpoint("topic", 0, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StromaError::WrongQueueRole {
                expected: QueueRole::Owner,
                actual: QueueRole::Follower
            }
        ));

        shutdown_stroma("owner_state_checkpoint_rejects_follower", &stroma).await;
    }

    #[tokio::test]
    async fn ack_snapshot_offset_tracks_last_applied_event_not_next_offset() {
        let dir = test_dir!("ack_snapshot_offset_tracks_last_event");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let message_offset = publish_one(&stroma, "topic", 0, None).await;
        let (cmp, rx) = KeratinAppendCompletion::pair();
        stroma
            .ack_enqueue("topic", 0, None, message_offset, cmp)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let event_next_offset = qh.event_log().next_offset();
        assert_eq!(event_next_offset, 2);

        let applied_event_offset = qh.applied_upto().load(Ordering::Acquire);
        assert_eq!(applied_event_offset, event_next_offset - 1);

        let snapshot = qh
            .force_encode_snapshot(applied_event_offset)
            .await
            .unwrap();
        let mut state = QueueInternalState::new("topic".to_string(), 0);
        let snapshot_meta = state.load_snapshot(&snapshot).unwrap();
        assert_eq!(snapshot_meta.last_snapshot_event_offset, 1);

        shutdown_stroma("ack_snapshot_offset_tracks_last_event", &stroma).await;
    }

    #[tokio::test]
    async fn checkpoint_pause_waits_without_changing_owner_role() {
        let dir = test_dir!("checkpoint_pause_waits_without_role_change");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let h = qh.resolve().unwrap();
        let generation = h.role_generation();

        let pause = h.pause_owner_operations_and_wait().await.unwrap();
        assert_eq!(h.role(), QueueRole::Owner);
        assert_eq!(h.role_generation(), generation);

        let qh_for_owner_op = qh.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let _ = started_tx.send(());
            qh_for_owner_op
                .resolve()
                .unwrap()
                .begin_owner_operation()
                .await
                .map(|_lease| ())
        });

        started_rx.await.unwrap();
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert!(
            !join.is_finished(),
            "new owner operations should wait while checkpoint export is paused"
        );

        drop(pause);
        join.await.unwrap().unwrap();

        shutdown_stroma("checkpoint_pause_waits_without_role_change", &stroma).await;
    }

    #[tokio::test]
    async fn owner_state_checkpoint_export_installs_on_follower_then_messages_catch_up() {
        let owner_dir = test_dir!("owner_state_checkpoint_export_owner");
        let owner = Stroma::open(
            &owner_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let follower_dir = test_dir!("owner_state_checkpoint_export_follower");
        let follower = Stroma::open(
            &follower_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&owner, "topic", 0, None).await;
        publish_one(&owner, "topic", 0, None).await;
        let owner_qh = owner.queue_handle("topic", 0, None).await.unwrap();
        let owner_qh = owner_qh.resolve().unwrap();
        let owner_role_generation = owner_qh.role_generation();

        let checkpoint = owner
            .export_owner_state_checkpoint("topic", 0, None)
            .await
            .unwrap();
        assert_eq!(checkpoint.message_checkpoint_offset, 0);
        assert_eq!(checkpoint.message_next_offset, 2);
        assert_eq!(checkpoint.event_next_offset, 2);
        assert_eq!(checkpoint.applied_event_offset, 1);
        assert_eq!(owner_qh.role(), QueueRole::Owner);
        assert_eq!(owner_qh.role_generation(), owner_role_generation);

        let repeated_checkpoint = owner
            .export_owner_state_checkpoint("topic", 0, None)
            .await
            .unwrap();
        assert_eq!(repeated_checkpoint.message_checkpoint_offset, 0);
        assert_eq!(repeated_checkpoint.message_next_offset, 2);
        assert_eq!(repeated_checkpoint.event_next_offset, 2);
        assert_eq!(repeated_checkpoint.applied_event_offset, 1);
        assert_eq!(owner_qh.role(), QueueRole::Owner);
        assert_eq!(owner_qh.role_generation(), owner_role_generation);

        let owner_read_after_export = owner
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        assert!(matches!(
            owner_read_after_export,
            OwnerReplicationRead::Batch(_)
        ));

        follower
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();
        follower
            .install_follower_state_checkpoint(
                "topic",
                0,
                None,
                FollowerStateCheckpointInstall {
                    message_next_offset: checkpoint.message_checkpoint_offset,
                    event_next_offset: checkpoint.event_next_offset,
                    applied_event_offset: checkpoint.applied_event_offset,
                    state_snapshot: checkpoint.state_snapshot,
                },
            )
            .await
            .unwrap();

        let not_ready = follower
            .promote_queue_follower_if_caught_up(
                "topic",
                0,
                None,
                checkpoint.message_next_offset,
                checkpoint.event_next_offset,
            )
            .await
            .unwrap();
        assert_eq!(
            not_ready,
            QueuePromotionOutcome::MessageLogBehind {
                local_next_offset: checkpoint.message_checkpoint_offset,
                expected_next_offset: checkpoint.message_next_offset,
            }
        );

        let message_read = owner
            .read_owner_message_records("topic", 0, None, checkpoint.message_checkpoint_offset, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(message_read) = message_read else {
            panic!("expected owner message batch");
        };
        follower
            .apply_replicated_queue_batch(
                "topic",
                0,
                None,
                Some(ReplicatedMessageBatch {
                    epoch: message_read.epoch,
                    first_offset: message_read.records[0].0,
                    records: message_read
                        .records
                        .into_iter()
                        .map(|(_, message)| message)
                        .collect(),
                    durability: None,
                }),
                None,
            )
            .await
            .unwrap();

        let promoted = follower
            .promote_queue_follower_if_caught_up(
                "topic",
                0,
                None,
                checkpoint.message_next_offset,
                checkpoint.event_next_offset,
            )
            .await
            .unwrap();
        assert_eq!(
            promoted,
            QueuePromotionOutcome::Promoted {
                message_next_offset: 2,
                event_next_offset: 2,
                applied_event_offset: Some(1),
            }
        );

        shutdown_stroma("owner_state_checkpoint_export_owner", &owner).await;
        shutdown_stroma("owner_state_checkpoint_export_follower", &follower).await;
    }

    #[tokio::test]
    async fn follower_can_catch_up_from_owner_read_batches_and_promote() {
        let owner_dir = test_dir!("owner_replication_pull_owner");
        let owner = Stroma::open(
            &owner_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let follower_dir = test_dir!("owner_replication_pull_follower");
        let follower = Stroma::open(
            &follower_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        publish_one(&owner, "topic", 0, None).await;
        publish_one(&owner, "topic", 0, None).await;
        follower
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();

        let message_read = owner
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let event_read = owner
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap();

        let OwnerReplicationRead::Batch(message_read) = message_read else {
            panic!("expected owner message batch");
        };
        let OwnerReplicationRead::Batch(event_read) = event_read else {
            panic!("expected owner event batch");
        };

        let messages = Some(ReplicatedMessageBatch {
            epoch: message_read.epoch,
            first_offset: message_read.records[0].0,
            records: message_read
                .records
                .into_iter()
                .map(|(_, message)| message)
                .collect(),
            durability: None,
        });
        let events = Some(ReplicatedEventBatch {
            epoch: event_read.epoch,
            first_offset: event_read.records[0].0,
            events: event_read
                .records
                .into_iter()
                .map(|(_, event)| event)
                .collect(),
            durability: None,
        });

        follower
            .apply_replicated_queue_batch("topic", 0, None, messages, events)
            .await
            .unwrap();

        let outcome = follower
            .promote_queue_follower_if_caught_up("topic", 0, None, 2, 2)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            QueuePromotionOutcome::Promoted {
                message_next_offset: 2,
                event_next_offset: 2,
                applied_event_offset: Some(1),
            }
        );

        let delivered = follower
            .poll_ready("topic", 0, None, 10, unix_millis() + 30_000, u64::MAX)
            .await
            .unwrap();
        assert_eq!(delivered.len(), 2);
        assert_eq!(delivered[0].0, 0);
        assert_eq!(delivered[1].0, 1);

        shutdown_stroma("owner_replication_pull_owner", &owner).await;
        shutdown_stroma("owner_replication_pull_follower", &follower).await;
    }

    #[tokio::test]
    async fn follower_promotion_refuses_partial_replication() {
        let owner_dir = test_dir!("partial_replication_owner");
        let owner = Stroma::open(
            &owner_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        publish_one(&owner, "topic", 0, None).await;
        publish_one(&owner, "topic", 0, None).await;

        let message_read = owner
            .read_owner_message_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let event_read = owner
            .read_owner_event_records("topic", 0, None, 0, 10)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(message_read) = message_read else {
            panic!("expected owner message batch");
        };
        let OwnerReplicationRead::Batch(event_read) = event_read else {
            panic!("expected owner event batch");
        };

        let messages_only_dir = test_dir!("partial_replication_messages_only");
        let messages_only = Stroma::open(
            &messages_only_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        messages_only
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();
        messages_only
            .apply_replicated_queue_batch(
                "topic",
                0,
                None,
                Some(ReplicatedMessageBatch {
                    epoch: message_read.epoch,
                    first_offset: message_read.records[0].0,
                    records: message_read
                        .records
                        .clone()
                        .into_iter()
                        .map(|(_, message)| message)
                        .collect(),
                    durability: None,
                }),
                None,
            )
            .await
            .unwrap();
        let outcome = messages_only
            .promote_queue_follower_if_caught_up("topic", 0, None, 2, 2)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            QueuePromotionOutcome::EventLogBehind {
                local_next_offset: 0,
                expected_next_offset: 2,
            }
        );

        let events_only_dir = test_dir!("partial_replication_events_only");
        let events_only = Stroma::open(
            &events_only_dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        events_only
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();
        events_only
            .apply_replicated_queue_batch(
                "topic",
                0,
                None,
                None,
                Some(ReplicatedEventBatch {
                    epoch: event_read.epoch,
                    first_offset: event_read.records[0].0,
                    events: event_read
                        .records
                        .clone()
                        .into_iter()
                        .map(|(_, event)| event)
                        .collect(),
                    durability: None,
                }),
            )
            .await
            .unwrap();
        let outcome = events_only
            .promote_queue_follower_if_caught_up("topic", 0, None, 2, 2)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            QueuePromotionOutcome::MessageLogBehind {
                local_next_offset: 0,
                expected_next_offset: 2,
            }
        );

        shutdown_stroma("partial_replication_owner", &owner).await;
        shutdown_stroma("partial_replication_messages_only", &messages_only).await;
        shutdown_stroma("partial_replication_events_only", &events_only).await;
    }

    #[tokio::test]
    async fn default_group_aliases_ungrouped_queue() {
        let dir = test_dir!("default_group_aliases_ungrouped_queue");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let offset = publish_one(&stroma, "topic", 0, Some("default")).await;
        assert_eq!(offset, 0);
        assert_eq!(stroma.indexed_queue_count(), 1);
        assert!(stroma.is_queue_materialized("topic", 0, None));
        assert!(stroma.is_queue_materialized("topic", 0, Some("default")));

        let page = stroma
            .inspect_messages("topic", 0, None, 0, 10, InspectMode::ActiveOnly, true, 1024)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].state.offset, 0);

        let dlq = GlobalDLQ::new("_dlq.topic", 0, Some("default"))
            .await
            .unwrap();
        assert_eq!(dlq.group, None);

        shutdown_stroma("default_group_aliases_ungrouped_queue", &stroma).await;
    }

    #[tokio::test]
    async fn queue_handle_starts_snapshot_task_once() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();
        let h = qh.resolve().unwrap();

        assert!(h.snapshot_task_started());
        assert!(h.recovery_complete());

        stroma.periodic_snapshot(qh.clone());

        assert!(h.snapshot_task_started());

        shutdown_stroma("queue_handle_starts_snapshot_task_once", &stroma).await;
    }

    #[tokio::test]
    async fn new_queue_after_empty_recovery_is_marked_recovered() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        assert!(stroma.initial_recovery_complete.load(Ordering::Acquire));

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();

        assert!(qh.recovery_complete());
        assert!(qh.snapshot_task_started());

        shutdown_stroma(
            "new_queue_after_empty_recovery_is_marked_recovered",
            &stroma,
        )
        .await;
    }

    #[tokio::test]
    async fn mark_all_queue_recoveries_completes_existing_waiters() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        qh.resolve().unwrap().recovery_complete.store(false, Ordering::Release);

        let qh_waiter = qh.clone();
        let waiter = tokio::spawn(async move {
            qh_waiter.resolve().unwrap().wait_recovery_complete().await;
        });

        stroma.mark_all_queue_recoveries_complete();

        waiter.await.unwrap();

        assert!(qh.resolve().unwrap().recovery_complete());

        shutdown_stroma(
            "mark_all_queue_recoveries_completes_existing_waiters",
            &stroma,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn newly_created_queue_writes_periodic_snapshot() {
        let dir = test_dir!("test_data");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let notified = stroma.snapshot_worker_ticks.notified();

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();

        qh.enqueue(0, 0).await.unwrap();

        tokio::time::advance(Duration::from_secs(21)).await;
        notified.await;

        assert!(stroma.snap_file("new-topic", 0, None).exists());

        stroma.shutdown().await.expect("stroma shutdown failed");
    }

    #[tokio::test]
    async fn open_indexes_existing_queues_without_materializing_them() {
        let dir = test_dir!("lazy_recovery_indexes_only");

        {
            let stroma = Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap();

            let qh = stroma.queue_handle("topic-a", 0, None).await.unwrap();
            let qh = qh.resolve().unwrap();
            qh.enqueue(0, 0).await.unwrap();

            shutdown_stroma(
                "open_indexes_existing_queues_without_materializing_them/setup",
                &stroma,
            )
            .await;
        }

        let stroma = test_step(
            "open_indexes_existing_queues_without_materializing_them/reopen",
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            ),
        )
        .await
        .unwrap();

        assert_eq!(stroma.indexed_queue_count(), 1);
        assert_eq!(stroma.materialized_queue_count(), 0);
        assert!(!stroma.is_queue_materialized("topic-a", 0, None));

        shutdown_stroma(
            "open_indexes_existing_queues_without_materializing_them/reopened",
            &stroma,
        )
        .await;
    }

    async fn publish_one(stroma: &Stroma, tp: &str, part: u32, group: Option<&str>) -> Offset {
        let (cmp, rx) = KeratinAppendCompletion::pair();
        let headers = MessageHeaders {
            published: 0,
            publish_received: 0,
            content_type: None,
            extra: Default::default(),
        };
        test_step(
            format!("publish_one/{tp}/{part}/{group:?}/append_message"),
            stroma.append_message(tp, part, group, &headers, b"x".to_vec(), cmp),
        )
        .await
        .unwrap();
        test_step(format!("publish_one/{tp}/{part}/{group:?}/completion"), rx)
            .await
            .unwrap()
            .unwrap()
            .base_offset
    }

    #[tokio::test]
    async fn recovery_replays_only_events_after_snapshot_offset() {
        let dir = test_dir!("snapshot_recovery_replay_start");

        {
            let stroma = Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap();

            let first = publish_one(&stroma, "topic-a", 0, None).await;
            stroma
                .nack_one("topic-a", 0, None, first, true)
                .await
                .unwrap();

            stroma.snapshot_partition("topic-a", 0, None).await.unwrap();

            let qh = stroma.queue_handle("topic-a", 0, None).await.unwrap();
            let qh = qh.resolve().unwrap();
            assert_eq!(qh.applied_upto().load(Ordering::Acquire), 1);

            let second = publish_one(&stroma, "topic-a", 0, None).await;
            assert_eq!(second, 1);
            let third = publish_one(&stroma, "topic-a", 0, None).await;
            assert_eq!(third, 2);

            shutdown_stroma("snapshot_recovery_replay_start/write", &stroma).await;
        }

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let qh = stroma.queue_handle("topic-a", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        assert!(qh.recovery_complete());
        assert_eq!(qh.applied_upto().load(Ordering::Acquire), 3);
        let scan_starts = stroma.recovery_event_scan_starts.lock().unwrap().clone();
        assert_eq!(scan_starts, vec![2]);

        let page = stroma
            .inspect_messages("topic-a", 0, None, 0, 10, InspectMode::ActiveOnly, false, 0)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 3);

        let first = page
            .items
            .iter()
            .find(|item| item.state.offset == 0)
            .expect("offset 0 should be present after recovery");
        let second = page
            .items
            .iter()
            .find(|item| item.state.offset == 1)
            .expect("offset 1 should be present after recovery");
        let third = page
            .items
            .iter()
            .find(|item| item.state.offset == 2)
            .expect("offset 2 should be present after recovery");

        assert_eq!(first.state.retry_count, 1);
        assert_eq!(second.state.retry_count, 0);
        assert_eq!(third.state.retry_count, 0);

        shutdown_stroma("snapshot_recovery_replay_start/read", &stroma).await;
    }

    #[tokio::test]
    async fn first_queue_handle_recovers_persisted_queue() {
        let dir = test_dir!("lazy_recovery_first_access");

        let mut expected = Vec::new();
        for i in 0..5 {
            let stroma = test_step(
                format!("first_queue_handle_recovers_persisted_queue/{i}/open-write"),
                Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()),
            )
            .await
            .unwrap();
            let _ = test_step(
                format!("first_queue_handle_recovers_persisted_queue/{i}/queue_handle-write"),
                stroma.queue_handle("topic-a", 0, None),
            )
            .await
            .unwrap();
            let offset = publish_one(&stroma, "topic-a", 0, None).await;
            expected.push(offset);
            shutdown_stroma(
                format!("first_queue_handle_recovers_persisted_queue/{i}/write"),
                &stroma,
            )
            .await;

            let stroma = test_step(
                format!("first_queue_handle_recovers_persisted_queue/{i}/open-read"),
                Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()),
            )
            .await
            .unwrap();
            assert_eq!(stroma.materialized_queue_count(), 0);
            let qh = test_step(
                format!("first_queue_handle_recovers_persisted_queue/{i}/queue_handle-read"),
                stroma.queue_handle("topic-a", 0, None),
            )
            .await
            .unwrap();
            let qh = qh.resolve().unwrap();
            assert!(qh.recovery_complete());
            for &off in &expected {
                assert!(
                    test_step(
                        format!("first_queue_handle_recovers_persisted_queue/{i}/is_ready/{off}"),
                        qh.is_ready(off),
                    )
                    .await,
                    "offset {off} not ready after reopen"
                );
            }
            shutdown_stroma(
                format!("first_queue_handle_recovers_persisted_queue/{i}/read"),
                &stroma,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn new_queue_after_lazy_startup_is_recovered_immediately() {
        let dir = test_dir!("lazy_recovery_new_queue");

        let stroma = test_step(
            "new_queue_after_lazy_startup_is_recovered_immediately/open",
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            ),
        )
        .await
        .unwrap();

        assert_eq!(stroma.indexed_queue_count(), 0);
        assert_eq!(stroma.materialized_queue_count(), 0);

        let qh = stroma.queue_handle("new-topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();

        assert!(qh.recovery_complete());
        assert!(qh.snapshot_task_started());
        assert_eq!(stroma.materialized_queue_count(), 1);

        shutdown_stroma(
            "new_queue_after_lazy_startup_is_recovered_immediately",
            &stroma,
        )
        .await;
    }

    #[tokio::test]
    async fn concurrent_first_access_recovers_only_once() {
        let dir = test_dir!("lazy_recovery_concurrent_once");

        {
            let stroma = Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap();

            let qh = stroma.queue_handle("topic-a", 0, None).await.unwrap();
            let qh = qh.resolve().unwrap();
            qh.enqueue(0, 0).await.unwrap();

            shutdown_stroma("concurrent_first_access_recovers_only_once/setup", &stroma).await;
        }

        let stroma = Arc::new(
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap(),
        );

        let mut joins = Vec::new();

        for _ in 0..32 {
            let stroma = stroma.clone();
            joins.push(tokio::spawn(async move {
                stroma.queue_handle("topic-a", 0, None).await.unwrap()
            }));
        }

        for join in joins {
            let qh = join.await.unwrap();
            let qh = qh.resolve().unwrap();
            assert!(qh.recovery_complete());
        }

        assert_eq!(stroma.lazy_recoveries_started.load(Ordering::Relaxed), 1);

        shutdown_stroma("concurrent_first_access_recovers_only_once/main", &stroma).await;
    }

    #[tokio::test]
    async fn unmaterialize_drops_idle_handle_but_keeps_queue_indexed() {
        let dir = test_dir!("evict_idle_materialized_queue");

        let stroma = test_step(
            "unmaterialize_drops_idle_handle_but_keeps_queue_indexed/open",
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            ),
        )
        .await
        .unwrap();

        test_step(
            "unmaterialize_drops_idle_handle_but_keeps_queue_indexed/materialize",
            stroma.materialize("topic-a", 0, None),
        )
        .await
        .unwrap();
        assert_eq!(stroma.indexed_queue_count(), 1);
        assert_eq!(stroma.materialized_queue_count(), 1);

        let outcome = test_step(
            "unmaterialize_drops_idle_handle_but_keeps_queue_indexed/unmaterialize",
            stroma.unmaterialize("topic-a", 0, None),
        )
        .await
        .unwrap();

        assert_eq!(outcome, EvictOutcome::Evicted);
        assert_eq!(stroma.indexed_queue_count(), 1);
        assert_eq!(stroma.materialized_queue_count(), 0);
        assert!(!stroma.is_materialized("topic-a", 0, None));
        let debug = stroma.debug_snapshot().await.unwrap();
        assert_eq!(debug.queue_count, 1);
        assert_eq!(debug.materialized_queue_count, 0);
        assert_eq!(debug.queues[0].topic, "topic-a");
        assert!(!debug.queues[0].materialized);
        assert!(debug.queues[0].exists_on_disk);
        assert_eq!(stroma.materialized_queue_count(), 0);

        shutdown_stroma(
            "unmaterialize_drops_idle_handle_but_keeps_queue_indexed",
            &stroma,
        )
        .await;
    }

    #[tokio::test]
    async fn destroy_partition_removes_index_entry_and_on_disk_storage() {
        let dir = test_dir!("destroy_partition_frees_storage");

        let stroma = test_step(
            "destroy_partition/open",
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            ),
        )
        .await
        .unwrap();

        test_step("destroy_partition/materialize", stroma.materialize("orders", 3, None))
            .await
            .unwrap();
        assert_eq!(stroma.indexed_queue_count(), 1);

        let msg_dir = stroma.msg_tp_part_dir("orders", 3, None);
        let event_dir = stroma.tp_part_dir("orders", 3, None);
        assert!(msg_dir.exists(), "message dir should exist after materialize");
        assert!(event_dir.exists(), "event dir should exist after materialize");

        // Drop a marker into the on-disk tree so we can prove the storage is
        // actually deleted (not just unindexed) and a recreate starts fresh.
        let marker = msg_dir.join("marker.proof");
        fs::write(&marker, b"present").unwrap();

        let outcome = test_step(
            "destroy_partition/destroy",
            stroma.destroy_partition("orders", 3, None),
        )
        .await
        .unwrap();
        assert_eq!(outcome, DestroyOutcome::Destroyed);
        assert_eq!(stroma.indexed_queue_count(), 0);
        assert!(!msg_dir.exists(), "message dir should be deleted after destroy");
        assert!(!event_dir.exists(), "event dir should be deleted after destroy");

        // A recreate (later grow reusing the index) starts from empty storage.
        test_step(
            "destroy_partition/rematerialize",
            stroma.materialize("orders", 3, None),
        )
        .await
        .unwrap();
        assert_eq!(stroma.indexed_queue_count(), 1);
        assert!(msg_dir.exists(), "message dir should be recreated on materialize");
        assert!(
            !marker.exists(),
            "recreated partition must not see the destroyed partition's files"
        );

        shutdown_stroma("destroy_partition_removes_index_entry_and_on_disk_storage", &stroma).await;
    }

    // Adversarial: hammer destroy and materialize against the same partition
    // concurrently, many times. Proves the low-level invariant the cluster
    // layer relies on: a materialize never opens a dir mid-deletion and a
    // destroy never leaves a half-built incarnation. After each round a clean
    // re-materialize must see a FRESH dir (never the destroyed round's marker),
    // and no `.trash-` tree may leak.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn destroy_and_materialize_race_never_corrupts_or_leaks() {
        let dir = test_dir!("destroy_materialize_race");

        let stroma = Arc::new(
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap(),
        );

        let msg_dir = stroma.msg_tp_part_dir("orders", 5, None);
        let parent = msg_dir.parent().unwrap().to_path_buf();

        for round in 0..200u32 {
            // Establish an incarnation with a round-stamped marker on disk.
            stroma.materialize("orders", 5, None).await.unwrap();
            let marker = msg_dir.join(format!("marker-{round}.proof"));
            // The dir may be mid-recreate from the previous round's racing
            // materialize; retry the marker write briefly until it lands.
            loop {
                if fs::create_dir_all(&msg_dir).is_ok() && fs::write(&marker, b"x").is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }

            // Race a destroy against a fresh materialize of the same partition.
            let s1 = stroma.clone();
            let s2 = stroma.clone();
            let destroyer =
                tokio::spawn(async move { s1.destroy_partition("orders", 5, None).await });
            let materializer = tokio::spawn(async move { s2.materialize("orders", 5, None).await });

            destroyer.await.unwrap().unwrap();
            materializer.await.unwrap().unwrap();

            // Drain to a known clean state, then prove the recreate is fresh.
            stroma.destroy_partition("orders", 5, None).await.unwrap();
            stroma.materialize("orders", 5, None).await.unwrap();

            // The freshly materialized incarnation must not carry this round's
            // marker (its storage was actually freed, not merely unindexed).
            assert!(
                !marker.exists(),
                "round {round}: recreated partition still sees the destroyed marker"
            );

            // No `.trash-` siblings may linger: destroy deletes them before it
            // returns.
            if parent.exists() {
                for entry in fs::read_dir(&parent).unwrap() {
                    let name = entry.unwrap().file_name();
                    let name = name.to_string_lossy();
                    assert!(
                        !name.contains(".trash-"),
                        "round {round}: leaked trash dir {name}"
                    );
                }
            }

            // Exactly one (or zero) registry entry for the key: never duplicated.
            assert!(stroma.indexed_queue_count() <= 1, "round {round}: duplicate slot");
        }

        shutdown_stroma("destroy_and_materialize_race_never_corrupts_or_leaks", &stroma).await;
    }

    // One trip-wire, two scales (mouse / bear): a storm of concurrent destroyers,
    // materializers, and queue_handle "reader" victims on the same partition. The
    // readers must NEVER spuriously fail - queue_handle lazily (re)creates over a
    // fresh dir, so a destroy that retires the dir mid-build must be ridden out,
    // not surfaced as an error - and the engine must stay consistent and never
    // wedge, no matter how intense the storm.
    async fn destroy_materialize_storm(stroma: Arc<Stroma>, rounds: usize, tasks: usize) {
        for round in 0..rounds {
            let mut joins = Vec::new();
            for i in 0..tasks {
                let s = stroma.clone();
                joins.push(tokio::spawn(async move {
                    match i % 3 {
                        0 => {
                            let _ = s.destroy_partition("orders", 0, None).await;
                        }
                        1 => {
                            let _ = s.materialize("orders", 0, None).await;
                        }
                        _ => {
                            let qh = s.queue_handle("orders", 0, None).await.unwrap_or_else(|e| {
                                panic!(
                                    "round {round}: queue_handle raced a destroy and failed: {e:?}"
                                )
                            });
                            // The ticket may be retired by a concurrent destroy
                            // immediately after we got it; that race is allowed.
                            // If it still resolves, it must be fully recovered.
                            if let Ok(h) = qh.resolve() {
                                assert!(
                                    h.recovery_complete(),
                                    "round {round}: handle not recovered"
                                );
                            }
                        }
                    }
                }));
            }
            for join in joins {
                join.await.unwrap();
            }
            assert!(
                stroma.indexed_queue_count() <= 1,
                "round {round}: duplicate slot"
            );
        }

        // The engine is still usable after the storm.
        let qh = stroma.queue_handle("orders", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        assert!(qh.recovery_complete());
    }

    // mouse: small + fast, the everyday trip-wire.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_destroyers_and_materializers_stay_consistent() {
        let dir = test_dir!("destroy_materialize_storm_mouse");
        let stroma = Arc::new(
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap(),
        );
        destroy_materialize_storm(stroma.clone(), 8, 24).await;
        shutdown_stroma("destroy_materialize_storm_mouse", &stroma).await;
    }

    // bear: heavy, exposes deadlocks/leaks under pathological concurrency.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_destroyers_and_materializers_stay_consistent_bear() {
        let dir = test_dir!("destroy_materialize_storm_bear");
        let stroma = Arc::new(
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap(),
        );
        destroy_materialize_storm(stroma.clone(), 40, 96).await;
        shutdown_stroma("destroy_materialize_storm_bear", &stroma).await;
    }

    #[tokio::test]
    async fn destroy_partition_absent_is_a_noop() {
        let dir = test_dir!("destroy_partition_absent");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let outcome = stroma
            .destroy_partition("never-existed", 7, Some("g"))
            .await
            .unwrap();
        assert_eq!(outcome, DestroyOutcome::Destroyed);
        assert_eq!(stroma.indexed_queue_count(), 0);

        shutdown_stroma("destroy_partition_absent_is_a_noop", &stroma).await;
    }

    #[tokio::test]
    async fn displaced_queue_handle_rejects_commands_after_unmaterialize() {
        let dir = test_dir!("evicted_handle_rejects_commands");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let ticket = stroma.queue_handle("topic-a", 0, None).await.unwrap();
        // A resolved handle held across the teardown keeps the displaced
        // incarnation alive (and its control task shut down by evict).
        let qh = ticket.resolve().unwrap();
        assert_eq!(
            stroma.unmaterialize("topic-a", 0, None).await.unwrap(),
            EvictOutcome::Evicted
        );

        let async_err = qh
            .command_enqueue(QueueCommand::Enqueue {
                offset: 1,
                retries: 0,
                response: None,
            })
            .await
            .expect_err("displaced async queue handle should reject commands");
        assert_eq!(async_err.kind(), std::io::ErrorKind::BrokenPipe);

        // The TICKET is movable cross-thread (it is `'static`); the `Resolved`
        // guard above is not (it cannot escape this scope - by construction). The
        // thread re-resolves the still-held displaced incarnation and sees its
        // shut-down control task reject the blocking command.
        let ticket_for_thread = ticket.clone();
        let blocking_err = std::thread::spawn(move || {
            ticket_for_thread
                .resolve()
                .expect("displaced incarnation still resolvable while a handle is held")
                .blocking_command_enqueue(QueueCommand::Enqueue {
                    offset: 2,
                    retries: 0,
                    response: None,
                })
                .expect_err("displaced blocking queue handle should reject commands")
        })
        .join()
        .expect("blocking enqueue thread panicked");
        assert_eq!(blocking_err.kind(), std::io::ErrorKind::BrokenPipe);

        // Once every resolved handle is dropped, the ticket no longer resolves:
        // the partition was unmaterialized.
        drop(qh);
        assert!(ticket.resolve().is_err());

        shutdown_stroma("displaced_queue_handle_rejects_commands", &stroma).await;
    }

    #[tokio::test]
    async fn materialize_after_unmaterialize_recovers_messages() {
        let dir = test_dir!("evict_then_recover_queue");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let off = publish_one(&stroma, "topic-a", 0, None).await;

        assert_eq!(
            test_step(
                "materialize_after_unmaterialize_recovers_messages/unmaterialize",
                stroma.unmaterialize("topic-a", 0, None),
            )
            .await
            .unwrap(),
            EvictOutcome::Evicted
        );
        assert_eq!(stroma.materialized_queue_count(), 0);

        test_step(
            "materialize_after_unmaterialize_recovers_messages/materialize",
            stroma.materialize("topic-a", 0, None),
        )
        .await
        .unwrap();
        assert_eq!(stroma.materialized_queue_count(), 1);

        let qh = test_step(
            "materialize_after_unmaterialize_recovers_messages/queue_handle",
            stroma.queue_handle("topic-a", 0, None),
        )
        .await
        .unwrap();
        let qh = qh.resolve().unwrap();
        assert!(
            test_step(
                format!("materialize_after_unmaterialize_recovers_messages/is_ready/{off}"),
                qh.is_ready(off),
            )
            .await
        );

        shutdown_stroma("materialize_after_unmaterialize_recovers_messages", &stroma).await;
    }

    #[tokio::test]
    async fn materialize_and_unmaterialize_race_without_double_open() {
        let dir = test_dir!("materialize_unmaterialize_race");

        let stroma = Arc::new(
            Stroma::open(
                &dir.root,
                test_keratin_config(),
                SnapshotConfig { every_events: 1 },
            )
            .await
            .unwrap(),
        );
        publish_one(&stroma, "topic-a", 0, None).await;

        for i in 0..64 {
            let barrier = Arc::new(tokio::sync::Barrier::new(3));

            let materialize_barrier = barrier.clone();
            let materialize_stroma = stroma.clone();
            let materialize = tokio::spawn(async move {
                materialize_barrier.wait().await;
                materialize_stroma.materialize("topic-a", 0, None).await
            });

            let evict_barrier = barrier.clone();
            let evict_stroma = stroma.clone();
            let evict = tokio::spawn(async move {
                evict_barrier.wait().await;
                evict_stroma.unmaterialize("topic-a", 0, None).await
            });

            barrier.wait().await;

            materialize
                .await
                .unwrap()
                .unwrap_or_else(|err| panic!("materialize failed on iteration {i}: {err:?}"));

            match evict.await.unwrap() {
                Ok(
                    EvictOutcome::Evicted | EvictOutcome::NotMaterialized | EvictOutcome::RaceLost,
                ) => {}
                Ok(other) => panic!("unexpected evict outcome on iteration {i}: {other:?}"),
                Err(err) => panic!("unmaterialize failed on iteration {i}: {err:?}"),
            }
        }

        test_step(
            "materialize_and_unmaterialize_race_without_double_open/final-materialize",
            stroma.materialize("topic-a", 0, None),
        )
        .await
        .unwrap();

        shutdown_stroma(
            "materialize_and_unmaterialize_race_without_double_open",
            &stroma,
        )
        .await;
    }

    #[tokio::test]
    async fn unmaterialize_refuses_queue_with_inflight_messages() {
        let dir = test_dir!("evict_refuses_inflight_queue");

        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();

        let off = publish_one(&stroma, "topic-a", 0, None).await;
        stroma
            .mark_inflight_one("topic-a", 0, None, off, 1000)
            .await
            .unwrap();

        assert!(stroma.has_inflight("topic-a", 0, None).await.unwrap());
        assert_eq!(
            stroma.unmaterialize("topic-a", 0, None).await.unwrap(),
            EvictOutcome::HasInflight
        );
        assert!(stroma.is_materialized("topic-a", 0, None));

        shutdown_stroma(
            "unmaterialize_refuses_queue_with_inflight_messages",
            &stroma,
        )
        .await;
    }
}
