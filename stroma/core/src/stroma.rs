use std::{
    fs,
    hash::{BuildHasher, Hash, Hasher},
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use keratin_log::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KDurability, Keratin,
    KeratinAppendCompletion, KeratinConfig, KeratinReplicaExt, KeratinRole, Message,
    ReplicatedAppendOutcome, SegmentInfo, util::unix_millis,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OnceCell, RwLock, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    DeclareMeta, RecoveryMismatchPolicy, Result, StromaError,
    engine::PartitionKind,
    event::{
        AckEventMeta, DeadLetterMeta, DeadLetterReason, EnqueueDelayedEventMeta, EnqueueEventMeta,
        NackEventMeta, StromaEvent,
    },
    global::{GlobalKey, GlobalStore, GlobalValue, PutOutcome},
    group,
    metrics::StromaMetrics,
    state::{
        CustomDLQ, InspectMode, NackOutcome, Offset, OwnerOperationLease, QueueCommand,
        QueueDebugInfo, QueueHandle, QueueHandleInner, QueueInspectionSnapshot,
        QueueInspectionState, QueueInternalDebugInfo, QueueRole, QueueSharedBundle,
        QueueStatusReport, StromaDebugSnapshot, UnixMillis,
    },
    stream_state::{RetentionConfig, StreamCommand},
    topic,
};

// Replication data types + owner-read helpers now live in `replication.rs`;
// re-export so existing `stroma_core::` and `crate::stroma::` paths keep resolving
// (clustering-module separation).
pub use crate::replication::*;
#[path = "recovery_disk.rs"]
mod recovery_disk;
pub use recovery_disk::RecoveryDiskUsage;
#[path = "storage_history.rs"]
mod storage_history;
pub use storage_history::{PreparedStorageHistory, StorageHistoryBinding};
#[path = "recovery_history.rs"]
mod recovery_history;
#[path = "recovery_seal.rs"]
mod recovery_seal;
pub use recovery_history::{RetainedHistoryIdentity, RetainedLog, RetainedLogs};
#[path = "recovery_read.rs"]
mod recovery_read;
pub use recovery_read::{
    RecoveryReadPage, RecoveryReadRequest, RecoveryReadSource, RecoveryRecord,
    RecoverySequentialRead,
};
#[path = "recovery_install.rs"]
mod recovery_install;
#[path = "recovery_stage.rs"]
mod recovery_stage;
pub use recovery_install::PreparedQueueRecovery;
pub use recovery_stage::{
    QueueRecoveryStage, QueueRecoveryStageReceipt, QueueRecoveryStageSpec, RecoveryStageLimits,
};
#[path = "agreed_checkpoint.rs"]
mod agreed_checkpoint;
#[path = "stroma/checkpoint_capture.rs"]
mod checkpoint_capture;
#[path = "stroma/combined.rs"]
mod combined;
#[path = "stroma/layout.rs"]
pub(crate) mod layout;
#[path = "checkpoint_install.rs"]
mod checkpoint_install;
#[path = "replication_retention.rs"]
mod replication_retention;
pub use agreed_checkpoint::{
    QueueCheckpointActivity, QueueCheckpointBase, QueueCheckpointBuildLimits,
    QueueCheckpointCapsule, QueueCheckpointContents, QueueCheckpointPin, QueueCheckpointTarget,
};
pub use recovery_seal::{RecoverySealRequest, SealedReplicaFrontiers};
pub use replication_retention::QueueReplicationRetention;

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

/// Exclusive end of a completed queue publication and the log epoch it was
/// written under. Message ids are offsets in the same log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuePublishCommit {
    pub next: Offset,
    pub epoch: u64,
}

/// Called once, before individual successful completions, after log durability
/// and queue application complete. The callback must do bounded, nonblocking work.
pub type QueuePublishObserver = Box<dyn FnOnce(QueuePublishCommit) + Send>;

pub struct PublishItem {
    pub headers: MessageHeaders,
    pub payload: Vec<u8>,
    pub not_before: Option<UnixMillis>,
    /// Absolute drop deadline (message TTL), already resolved by the caller.
    /// `None` = never expires.
    pub expire_at: Option<UnixMillis>,
    pub completion: Box<dyn AppendCompletion<IoError> + Send>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueuePromotionOutcome {
    Promoted {
        next: Offset,
    },
    LogBehind {
        local_next: Offset,
        expected_next: Offset,
    },
    LogAhead {
        local_next: Offset,
        expected_next: Offset,
    },
    /// Records exist that the state machine has not applied. `applied_next`
    /// is exclusive, 0 when nothing is applied.
    NotApplied {
        applied_next: Offset,
        log_next: Offset,
    },
    /// The state references payloads at or above the local log tail.
    PayloadsMissing {
        log_next: Offset,
        required_next: Offset,
    },
}

/// Promotion of a stream follower at its local tails. Streams keep a record
/// log and a cursor-commit event log, and every cursor event must be applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamPromotionOutcome {
    Promoted {
        message_next_offset: Offset,
        event_next_offset: Offset,
        applied_event_offset: Option<Offset>,
    },
    EventsNotApplied {
        applied_event_offset: Option<Offset>,
        event_next_offset: Offset,
    },
}

/// Single queue log position when an owner stepped down. `applied_next` is
/// exclusive, 0 when nothing is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDemotionOutcome {
    pub next: Offset,
    pub applied_next: Offset,
}

struct ItemMeta {
    not_before: Option<UnixMillis>,
    expire_at: Option<UnixMillis>,
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

/// Default cadence for the background stream-retention sweep.
pub const DEFAULT_STREAM_RETENTION_SWEEP_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StromaOptions {
    /// How often the background worker sweeps already-materialized stream
    /// partitions to enforce their retention policy. `None` (or zero) disables the
    /// sweep, for example when an external scheduler drives retention instead. The
    /// sweep never materializes an idle partition.
    pub stream_retention_sweep_interval_ms: Option<u64>,
}

impl Default for StromaOptions {
    fn default() -> Self {
        Self {
            stream_retention_sweep_interval_ms: Some(DEFAULT_STREAM_RETENTION_SWEEP_INTERVAL_MS),
        }
    }
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
    ordered_scope: Option<crate::ordered_apply::Scope>,
    runtime: Option<tokio::runtime::Handle>,
}

impl AppendCompletion<IoError> for ApplyThenComplete {
    fn complete(self: Box<Self>, res: std::result::Result<AppendResult, IoError>) {
        match res {
            Ok(ar) => {
                if self.ordered_scope.is_some() {
                    let runtime = self.runtime.clone().expect("ordered callback runtime");
                    runtime.spawn(async move {
                        let ApplyThenComplete { stroma, ev, qh, _owner_operation, inner, ordered_scope, .. } = *self;
                        let result = async {
                            let h = qh.resolve()?;
                            let turn = crate::ordered_apply::enter(&ordered_scope, ar.base_offset, ar.count as u64).await?;
                            stroma.apply_record_inmem(ev, ar.base_offset, &h).await?;
                            h.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Release);
                            crate::ordered_apply::finish(turn, ordered_scope)?;
                            Ok::<_, StromaError>(ar)
                        }.await;
                        inner.complete(result.map_err(|err| IoError::new(err.to_string())));
                        drop(_owner_operation);
                    });
                    return;
                }
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
        let ordered_scope = qh.resolve().ok().and_then(|h| h.ordered_apply_scope());
        let runtime = ordered_scope.as_ref().map(|_| tokio::runtime::Handle::current());
        Box::new(Self {
            stroma,
            ev,
            qh,
            _owner_operation: owner_operation,
            inner,
            ordered_scope,
            runtime,
        })
    }
}

struct CompletionItem {
    meta: ItemMeta,
    completion: Box<dyn AppendCompletion<IoError> + Send>,
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

/// Total size of the files under `path`, recursively. Unreadable entries
/// count as zero: this feeds an observability estimate, not accounting.
fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            total += dir_size_bytes(&entry.path());
        } else if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    total
}

/// One queue's share of the on-disk footprint (partitions aggregated under
/// topic + group), split by log. Produced by `estimate_disk_used_breakdown`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskUsedBreakdownEntry {
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub message_bytes: u64,
    pub event_bytes: u64,
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

/// Result of a pipelined durable stream append
/// ([`Stroma::append_stream_records_enqueue`]). Neither the offset nor durability
/// is awaited by the caller: `offset` resolves at stage (the base offset of the
/// batch) and `durable` resolves when the batch is fsynced (or with an error).
/// Returning both as receivers lets the caller enqueue the next append without
/// waiting for keratin to report back, so keratin coalesces fsyncs across the
/// queued appends instead of paying one fsync per record.
#[derive(Debug)]
pub struct EnqueuedStreamAppend {
    pub offset: tokio::sync::oneshot::Receiver<Offset>,
    pub durable: tokio::sync::oneshot::Receiver<std::result::Result<(), StromaError>>,
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

    pub fn finish_eviction(&self) {
        self.evicting.store(false, Ordering::Release);
        self.done.notify_waiters();
    }

    pub fn is_evicting(&self) -> bool {
        self.evicting.load(Ordering::Acquire)
    }
}

impl QueueSlot {
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

/// Why a partition is quarantined, and what a repair would do. Surfaced to
/// operators (admin/health) so the issue is clear and actionable.
#[derive(Debug, Clone, Serialize)]
pub struct QuarantineInfo {
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    /// Human-readable description of the problem (covers both a dangling
    /// event->message reference and a corrupt event record).
    pub reason: String,
    /// Repair truncates the event log to this offset (drops the bad suffix).
    pub truncate_event_offset: u64,
}

/// What recovery found wrong with the event log at `event_offset`.
#[derive(Debug, Clone)]
enum RecoveryMismatchKind {
    /// An event record failed decoding or reference validation. Skipping it
    /// would drop a state transition. Explicit local repair truncates the suffix
    /// only when the storage history is unbound.
    CorruptRecord { detail: String },
}

/// A bad event found during the event-log scan: the first offending record.
#[derive(Debug, Clone)]
struct RecoveryMismatchFound {
    /// Event-log offset of the first offending record (the truncation point).
    event_offset: u64,
    kind: RecoveryMismatchKind,
}

/// Result of scanning the event log during recovery: the valid prefix of events
/// (up to but excluding the first dangling reference, if any) plus the mismatch.
struct RecoveryScanOutcome {
    events: Vec<StromaEvent>,
    events_count: u64,
    mismatch: Option<RecoveryMismatchFound>,
}

#[derive(Debug, Clone)]
pub struct Stroma {
    pub(crate) start_time: Instant,
    pub(crate) root: PathBuf,
    pub(crate) keratin_cfg_msg: KeratinConfig,
    pub(crate) keratin_cfg_event: KeratinConfig,
    pub(crate) log_runtime: Option<Arc<keratin_log::LogRuntimeSettings>>,
    // FIXME: snapshot cadence. snap_cfg.every_events is not wired yet - the gate
    // in periodic_snapshot_step is commented out, so snapshots currently fire on
    // every periodic tick when the partition is dirty. The plan is to make
    // every_events an ADDITIONAL cadence knob alongside the time/dirty trigger (an
    // events-since-last-snapshot threshold), not a replacement. The
    // last_snapshot_event_offset tracking needed to honor it is already maintained,
    // so wiring is low-risk.
    #[allow(dead_code)]
    pub(crate) snap_cfg: SnapshotConfig,
    pub(crate) global_store: Arc<OnceCell<Arc<GlobalStore>>>,

    pub(crate) task_group: Arc<TaskGroup>,

    // Materialized queue state
    queue_handles: Arc<ArcSwap<Registry>>,

    /// Bounds concurrent full-history scans across this storage instance.
    recovery_read_slots: Arc<Semaphore>,
    recovery_stream_slots: Arc<Semaphore>,
    recovery_stage_slots: Arc<Semaphore>,
    // Current generation of each partition, loaded from `partitions/` at open.
    generations: Arc<DashMap<(Box<str>, u32, Option<Box<str>>), layout::Generation>>,
    // Set on a recovery view. It resolves only this one partition, to this
    // generation, and never creates or resolves any other partition.
    view_generation: Option<Arc<((Box<str>, u32, Option<Box<str>>), layout::Generation)>>,
    // A new storage instance cannot inherit writer admission from an old process.
    storage_session: [u8; 16],
    reseed_views: Arc<DashMap<[u8; 32], Stroma>>,
    retired_view: Arc<AtomicBool>,
    admitted_histories: Arc<DashMap<(Box<str>, u32, Option<Box<str>>), StorageHistoryBinding>>,
    replication_retention: Arc<std::sync::RwLock<Option<Arc<dyn QueueReplicationRetention>>>>,

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

    pub(crate) metrics: Arc<StromaMetrics>,

    pub(crate) deadline_waker: Arc<Notify>,
    initial_recovery_complete: Arc<AtomicBool>,

    /// Partitions parked because recovery found a dangling event->message
    /// reference (the event log references a message the message log has not
    /// durably accepted). Keyed by the stable partition key. `queue_handle`
    /// checks this first, so a quarantined partition errors loudly instead of
    /// serving corrupt state; `repair_partition` clears it. Default policy keeps
    /// the rest of the broker running (blast radius = the one queue).
    quarantined: Arc<DashMap<(Box<str>, u32, Option<Box<str>>), QuarantineInfo>>,

    /// `RecoveryMismatchPolicy` (Quarantine default / Refuse / Ignore), settable
    /// at startup. Shared so a clone observes a runtime change.
    recovery_mismatch_policy: Arc<AtomicU8>,

    #[cfg(test)]
    lazy_recoveries_started: Arc<AtomicU64>,

    #[cfg(test)]
    recovery_event_scan_starts: Arc<std::sync::Mutex<Vec<u64>>>,

    #[cfg(test)]
    snapshot_worker_ticks: Arc<Notify>,

    #[cfg(test)]
    checkpoint_fault: Arc<std::sync::Mutex<Option<&'static str>>>,

    #[cfg(test)]
    publish_apply_pause: Arc<std::sync::Mutex<Option<Arc<PublishApplyPause>>>>,
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
        Self::open_internal(root, keratin_cfg, snap_cfg, options, None).await
    }

    pub async fn open_with_runtime(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
        runtime: Arc<keratin_log::LogRuntimeSettings>,
    ) -> Result<Self> {
        Self::open_internal(
            root,
            keratin_cfg,
            snap_cfg,
            StromaOptions::default(),
            Some(runtime),
        )
        .await
    }

    async fn open_internal(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
        options: StromaOptions,
        log_runtime: Option<Arc<keratin_log::LogRuntimeSettings>>,
    ) -> Result<Self> {
        let start_time = Instant::now();
        let root = root.as_ref().to_path_buf();
        layout::prepare_root(&root)?;

        let metrics = Arc::new(StromaMetrics::new(60));
        let keratin_cfg_msg = keratin_cfg.message_log;
        let keratin_cfg_event = keratin_cfg.event_log;

        let st = Self {
            start_time,
            root,
            keratin_cfg_msg,
            keratin_cfg_event,
            log_runtime,
            snap_cfg,
            global_store: Arc::new(OnceCell::new()),
            task_group: Arc::new(TaskGroup::new()),
            queue_handles: Arc::new(ArcSwap::new(Arc::new(hashbrown::HashMap::new()))),
            lifecycle_locks: Arc::new(DashMap::new()),
            recovery_read_slots: Arc::new(Semaphore::new(1)),
            recovery_stream_slots: Arc::new(Semaphore::new(2)),
            recovery_stage_slots: Arc::new(Semaphore::new(1)),
            generations: Arc::new(DashMap::new()),
            view_generation: None,
            storage_session: *uuid::Uuid::now_v7().as_bytes(),
            reseed_views: Arc::new(DashMap::new()),
            retired_view: Arc::new(AtomicBool::new(false)),
            admitted_histories: Arc::new(DashMap::new()),
            replication_retention: Arc::new(std::sync::RwLock::new(None)),
            global_dlq: Arc::new(RwLock::new(None)),
            metrics: metrics.clone(),
            deadline_waker: Arc::new(Notify::new()),
            initial_recovery_complete: Arc::new(AtomicBool::new(false)),
            quarantined: Arc::new(DashMap::new()),
            recovery_mismatch_policy: Arc::new(AtomicU8::new(
                RecoveryMismatchPolicy::default().as_u8(),
            )),
            #[cfg(test)]
            lazy_recoveries_started: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            recovery_event_scan_starts: Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(test)]
            snapshot_worker_ticks: Arc::new(Notify::new()),
            #[cfg(test)]
            checkpoint_fault: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            publish_apply_pause: Arc::new(std::sync::Mutex::new(None)),
        };

        st.load_generations()?;
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

        // Background stream-retention sweep. Periodically enforces retention on
        // already-materialized stream partitions (it never materializes an idle
        // one). Disabled when the interval is None or zero.
        if let Some(interval_ms) = options
            .stream_retention_sweep_interval_ms
            .filter(|&ms| ms > 0)
        {
            let st_retention = st.clone();
            st.task_group.spawn("stream retention", async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                    let _ = st_retention.enforce_all_stream_retention().await;
                }
            });
        }

        Ok(st)
    }

    // ---------------- Paths / naming ----------------

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn metrics(&self) -> Arc<StromaMetrics> {
        self.metrics.clone()
    }

    pub async fn global_store(&self) -> Result<Arc<GlobalStore>> {
        self.global_store
            .get_or_try_init(|| async {
                let dir = self.root.join(layout::GLOBAL);
                layout::create_dir_within(&self.root, &dir)?;
                let store = GlobalStore::open_with_runtime(
                    dir,
                    self.keratin_cfg_event,
                    self.log_runtime.clone(),
                )
                .await?;
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

    fn partition_key(tp: &str, part: u32, group: Option<&str>) -> (Box<str>, u32, Option<Box<str>>) {
        (tp.into(), part, normalize_group(group).map(Into::into))
    }

    /// Directory holding every generation and the bookkeeping of one partition.
    pub(super) fn partition_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        layout::partition_dir(&self.root, tp, part, group)
    }

    /// The generation this storage resolves for a partition. A recovery view
    /// resolves only its own partition. Otherwise the cache is filled at open
    /// and by every switch this storage makes. A miss reads `current` from
    /// disk, for a partition another storage instance on the same root may
    /// have created, without caching it, so a racing destroy cannot leave a
    /// stale entry behind.
    pub(super) fn partition_generation(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Option<layout::Generation> {
        let key = Self::partition_key(tp, part, group);
        if let Some(view) = &self.view_generation {
            return (view.0 == key).then_some(view.1);
        }
        if let Some(generation) = self.generations.get(&key) {
            return Some(*generation);
        }
        match layout::read_current(&self.partition_dir(tp, part, group)) {
            Ok(generation) => generation,
            Err(err) => {
                tracing::warn!("cannot read current generation of {tp}/{part}: {err}");
                None
            }
        }
    }

    /// Root of the partition's current generation. A partition without one
    /// resolves to a reserved name that nothing creates.
    pub(super) fn partition_root(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        let dir = self.partition_dir(tp, part, group);
        match self.partition_generation(tp, part, group) {
            Some(generation) => dir.join(generation.name()),
            None => dir.join(layout::NO_GENERATION),
        }
    }

    /// Stream message log. Queues keep payloads in their event log and never
    /// create this directory.
    fn msg_tp_part_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.partition_root(tp, part, group).join(layout::MESSAGES)
    }

    fn tp_part_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.partition_root(tp, part, group).join(layout::LOG)
    }

    pub(crate) fn snap_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.partition_root(tp, part, group).join(layout::STATE)
    }

    fn snap_file(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.snap_dir(tp, part, group).join("snapshot")
    }

    /// Per-partition recovery bookkeeping, shared by all generations.
    pub(super) fn recovery_dir(&self, tp: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.partition_dir(tp, part, group).join(layout::RECOVERY)
    }

    /// Create `dir`, a directory inside the partition's resolved generation.
    /// Refused while the partition has no generation, so a write can never
    /// land outside a generation that `current` or a recovery view names.
    pub(super) fn create_generation_dir(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        dir: &Path,
    ) -> Result<()> {
        let root = self.partition_root(tp, part, group);
        if self.partition_generation(tp, part, group).is_none() || !dir.starts_with(&root) {
            return Err(StromaError::InvalidArgument(format!(
                "partition {tp}/{part} has no storage generation to write into"
            )));
        }
        layout::create_dir_within(&self.root, dir)
    }

    /// Make sure the partition exists with `kind`, so files can be written
    /// into its current generation. A new partition gets its kind, an empty
    /// initial generation and `current`, in that order, so a crash leaves
    /// either a complete partition or a directory without `current`, which
    /// is not served. A recovery view only creates its own generation and
    /// never switches `current`. Callers hold the partition lifecycle lock.
    pub(super) fn ensure_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
    ) -> Result<()> {
        let key = Self::partition_key(tp, part, group);
        let dir = self.partition_dir(tp, part, group);
        if let Some(view) = &self.view_generation {
            if view.0 != key {
                return Err(StromaError::InvalidArgument(
                    "a recovery view cannot create another partition".into(),
                ));
            }
            self.ensure_partition_kind(&dir, kind)?;
            return layout::create_dir_within(&self.root, &dir.join(view.1.name()));
        }
        if self.partition_generation(tp, part, group).is_some() {
            return match layout::read_kind(&dir)? {
                Some(actual) if actual == kind => Ok(()),
                Some(actual) => Err(StromaError::WrongPartitionKind {
                    expected: kind,
                    actual,
                }),
                None => Err(StromaError::Corruption(format!(
                    "partition {tp}/{part} has a current generation but no kind"
                ))),
            };
        }
        // An interrupted first recovery installation must finish or fail. It
        // never turns into a fresh empty partition.
        if self
            .recovery_dir(tp, part, group)
            .join("started")
            .try_exists()
            .map_err(io_err)?
        {
            return Err(StromaError::Corruption(
                "installed recovery lost its current generation".into(),
            ));
        }
        self.ensure_partition_kind(&dir, kind)?;
        let generation = layout::Generation::new_initial();
        layout::create_dir_within(&self.root, &dir.join(generation.name()))?;
        layout::write_current(&dir, &generation)?;
        self.generations.insert(key, generation);
        Ok(())
    }

    fn ensure_partition_kind(&self, dir: &Path, kind: PartitionKind) -> Result<()> {
        layout::create_dir_within(&self.root, dir)?;
        match layout::read_kind(dir)? {
            Some(actual) if actual == kind => Ok(()),
            Some(actual) => Err(StromaError::WrongPartitionKind {
                expected: kind,
                actual,
            }),
            None => layout::write_kind(dir, kind),
        }
    }

    /// Load the current generation of every partition at open.
    fn load_generations(&self) -> Result<()> {
        for found in layout::scan_partitions(&self.root)? {
            if let Some(generation) = found.current {
                self.generations.insert(
                    Self::partition_key(&found.topic, found.part, found.group.as_deref()),
                    generation,
                );
            }
        }
        Ok(())
    }

    /// Read a partition's engine kind, defaulting to `Queue` when the
    /// partition does not exist yet. Called on the materialization path
    /// before spawning the control actor, so a stream partition spawns the
    /// stream engine rather than the work queue.
    fn read_partition_kind(&self, tp: &str, part: u32, group: Option<&str>) -> PartitionKind {
        self.partition_kind_marker(tp, part, group)
            .unwrap_or(PartitionKind::Queue)
    }

    /// Read the kind WITHOUT defaulting: `None` means the partition was never
    /// declared, distinct from one that says `Queue`. Used by the declare
    /// paths to reject re-declaring a topic as the other channel kind.
    fn partition_kind_marker(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Option<PartitionKind> {
        match layout::read_kind(&self.partition_dir(tp, part, group)) {
            Ok(kind) => kind,
            Err(err) => {
                tracing::warn!("cannot read kind of {tp}/{part}: {err}");
                None
            }
        }
    }

    /// Read a partition's durable engine kind (Queue when absent). Exposed
    /// so a caller can route a request to the stream or queue path before the
    /// partition is materialized in memory (e.g. a publish that arrives at a fresh
    /// owner after failover, before the stream channel is opened).
    pub fn partition_kind(&self, tp: &str, part: u32, group: Option<&str>) -> PartitionKind {
        self.read_partition_kind(tp, part, group)
    }

    /// Create the partition as `kind`, or confirm an existing partition has
    /// that kind. The kind never changes once written.
    fn write_partition_kind(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
    ) -> Result<()> {
        self.ensure_partition(tp, part, group, kind)
    }

    // A unique name per write: partitions and recovery views never share a
    // temporary snapshot. Leftovers are cleared with `tmp/` at open.
    fn snap_tmp_file(&self) -> PathBuf {
        self.root
            .join(layout::TMP)
            .join(format!("snapshot-{}.tmp", uuid::Uuid::now_v7()))
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

    /// Combined queues inherit the payload writer's capacities and cadence.
    /// Keep the small event configuration for metadata/global-store logs.
    fn queue_event_log_config(&self) -> KeratinConfig {
        KeratinConfig {
            default_durability: if self.keratin_cfg_msg.default_durability == KDurability::AfterFsync
                || self.keratin_cfg_event.default_durability == KDurability::AfterFsync {
                KDurability::AfterFsync
            } else { KDurability::AfterWrite },
            ..self.keratin_cfg_msg
        }
    }

    fn partition_event_log_config(&self, kind: PartitionKind) -> KeratinConfig {
        match kind {
            PartitionKind::Queue => self.queue_event_log_config(),
            PartitionKind::Stream => self.keratin_cfg_event,
        }
    }

    async fn event_log_init(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Arc<Keratin>> {
        self.ensure_partition(tp, part, group, self.read_partition_kind(tp, part, group))?;
        let dir = self.tp_part_dir(tp, part, group);
        self.create_generation_dir(tp, part, group, &dir)?;

        tracing::info!("Initializing event log: (`{tp}` `{part}` `{group:?}`)");
        let k = self
            .open_keratin(
                dir,
                self.partition_event_log_config(self.read_partition_kind(tp, part, group)),
                self.storage_history_binding(tp, part, group)?.is_some(),
            )
            .await
            .map_err(io_err)?;
        tracing::info!("Initialized event log: (`{tp}` `{part}` `{group:?}`)");

        Ok(Arc::new(k))
    }

    async fn msg_log_init(&self, tp: &str, part: u32, group: Option<&str>) -> Result<Arc<Keratin>> {
        self.ensure_partition(tp, part, group, PartitionKind::Stream)?;
        let dir = self.msg_tp_part_dir(tp, part, group);
        self.create_generation_dir(tp, part, group, &dir)?;

        tracing::info!("Initializing message log: (`{tp}` `{part}` `{group:?}`)");
        let k = self
            .open_keratin(
                dir,
                self.keratin_cfg_msg,
                self.storage_history_binding(tp, part, group)?.is_some(),
            )
            .await
            .map_err(io_err)?;
        tracing::info!("Initialized message log: (`{tp}` `{part}` `{group:?}`)");

        Ok(Arc::new(k))
    }

    pub(crate) async fn open_keratin(
        &self,
        root: impl AsRef<Path>,
        cfg: KeratinConfig,
        preserve_history: bool,
    ) -> std::io::Result<Keratin> {
        match &self.log_runtime {
            Some(runtime) => {
                Keratin::open_with_runtime(root, cfg, preserve_history, runtime.clone()).await
            }
            None if preserve_history => Keratin::open_preserving_history(root, cfg).await,
            None => Keratin::open(root, cfg).await,
        }
    }

    pub fn deadline_waker(&self) -> Arc<Notify> {
        self.deadline_waker.clone()
    }

    /// Set the policy for recovery dangling event->message mismatches. Startup
    /// config; shared across clones.
    pub fn set_recovery_mismatch_policy(&self, policy: RecoveryMismatchPolicy) {
        self.recovery_mismatch_policy
            .store(policy.as_u8(), Ordering::Release);
    }

    pub fn recovery_mismatch_policy(&self) -> RecoveryMismatchPolicy {
        RecoveryMismatchPolicy::from_u8(self.recovery_mismatch_policy.load(Ordering::Acquire))
    }

    /// Snapshot of all currently-quarantined partitions (for health/admin).
    pub fn quarantined_partitions(&self) -> Vec<QuarantineInfo> {
        self.quarantined.iter().map(|e| e.value().clone()).collect()
    }

    pub fn is_quarantined(&self, tp: &str, part: u32, group: Option<&str>) -> bool {
        let group = normalize_group(group);
        self.quarantined
            .contains_key(&(Box::<str>::from(tp), part, group.map(Box::<str>::from)))
    }

    /// Repair a quarantined partition: truncate the event log's unresolvable
    /// suffix (truncate-to-valid), then clear the quarantine so the next access
    /// re-recovers the valid prefix. Operator-triggered (admin) - this drops the
    /// dangling events (possible data loss), but it is an explicit decision, not
    /// auto-heal. Errors if the partition is not quarantined.
    pub async fn repair_partition(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        let group = normalize_group(group);
        let key = (Box::<str>::from(tp), part, group.map(Box::<str>::from));
        let info = match self.quarantined.get(&key) {
            Some(i) => i.value().clone(),
            None => {
                return Err(StromaError::NotFound(format!(
                    "partition {tp}/{part}/{group:?} is not quarantined"
                )));
            }
        };

        // Serialize against concurrent open/close of this partition, then open
        // the event log just to truncate its suffix and close it again. The next
        // queue_handle re-opens and recovers the now-valid log.
        let _lifecycle = self.lock_partition_lifecycle(tp, part, group).await;
        self.ensure_partition_not_sealed(tp, part, group)?;
        self.ensure_checkpoint_not_pending(tp, part, group)?;
        self.require_unbound_storage_history(tp, part, group)?;
        let event_log = self.event_log_init(tp, part, group).await?;
        let prev_role = event_log.role();
        event_log.become_follower();
        let reset = event_log
            .repair_suffix_at_epoch(info.truncate_event_offset, event_log.current_epoch())
            .await
            .map_err(io_err);
        match prev_role {
            KeratinRole::Owner => event_log.become_owner(),
            _ => event_log.become_follower(),
        }
        reset?;
        event_log.shutdown().await.map_err(io_err)?;

        if self.quarantined.remove(&key).is_some() {
            self.metrics
                .recovery
                .quarantined
                .fetch_sub(1, Ordering::Relaxed);
        }
        tracing::warn!(
            "repaired quarantined partition {tp}/{part}/{group:?}: truncated event log to {} \
             (dropped the dangling suffix); it will re-recover on next access",
            info.truncate_event_offset
        );
        Ok(())
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

        // A quarantined partition (recovery found a dangling event->message
        // reference) does not serve: fail loud instead of materializing corrupt
        // state. An operator clears this via `repair_partition`.
        if let Some(info) =
            self.quarantined
                .get(&(Box::<str>::from(tp), part, group.map(Box::<str>::from)))
        {
            return Err(StromaError::QueueQuarantined {
                topic: tp.to_string(),
                partition: part,
                group: group.map(|g| g.to_string()),
                reason: info.reason.clone(),
            });
        }

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
                if inner.recovery_gate.checkpoint_pending.load(Ordering::Acquire) {
                    self.resume_live_checkpoint(tp, part, group, inner).await?;
                    continue;
                }
                inner.ensure_not_recovery_sealed()?;
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

            // Check before checkpoint recovery or either log can mutate disk.
            if slot.handle.get().is_none() {
                self.ensure_storage_history_admitted(tp, part, group)?;
            }
            let _lifecycle = if slot.handle.get().is_none() {
                self.resume_cold_checkpoint(tp, part, group, _lifecycle).await?
            } else {
                _lifecycle
            };

            let qh = slot
                .handle
                .get_or_try_init(|| async {
                    self.ensure_partition_not_sealed(tp, part, group)?;
                    let kind = self.read_partition_kind(tp, part, group);
                    let msg_log = if kind == PartitionKind::Queue {
                        None
                    } else {
                        Some(self.msg_log_init(tp, part, group).await?)
                    };
                    let event_log = self.event_log_init(tp, part, group).await?;

                    let bundle = QueueSharedBundle {
                        event_log: event_log.clone(),
                        msg_log,
                        task_group: self.task_group.clone(),
                        metrics: self.metrics.clone(),
                        global_dlq: self.global_dlq.clone(),
                        deadline_waker: self.deadline_waker.clone(),
                    };

                    let inner = QueueHandleInner::init(
                        tp.into(),
                        part,
                        group.map(|s| s.into()),
                        bundle,
                        kind,
                    );

                    if self
                        .snap_dir(tp, part, group)
                        .join("storage.learner")
                        .try_exists()
                        .map_err(io_err)?
                    {
                        inner.become_follower();
                        for log in inner.physical_logs() { log.become_follower(); }
                    }

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

                    // Spawn the snapshot task only AFTER recovery completes. The
                    // task gets a TICKET (Weak), so it never pins this incarnation
                    // once the slot drops it. Spawning post-recovery also means a
                    // failed build (the `?` above) never leaves an orphan task
                    // parked in wait_recovery_complete on a notify that will never
                    // fire (which would hold the incarnation alive forever).
                    self.periodic_snapshot(self.ticket_for(tp, part, group, &inner));

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
        // Streams have no leased-delivery set, so the inflight guard only applies
        // to work queues. A stream is always safe on this axis.
        let inflight = match qh.as_work_queue() {
            Some(wq) => wq.inflight_len().await,
            None => 0,
        };
        if inflight > 0 {
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

        // Drain in-flight owner operations before tearing the logs down, so a
        // durable-but-not-yet-applied append cannot be cut off mid-flight. New
        // owner ops are frozen (they error rather than block), so this cannot
        // hang. This makes the teardown contract self-enforced rather than
        // relying on the caller having quiesced the partition first.
        qh.quiesce_for_teardown().await;

        qh.cancel_background_tasks();

        qh.recovery_gate.retire_snapshots().await?;

        // Shut down old handle. Pending writes flush and the logs close. The
        // stream actor exits when the slot drops its sender, so it has no
        // explicit shutdown command.
        if let Some(wq) = qh.as_work_queue() {
            wq.shutdown().await;
        }
        qh.event_log().shutdown().await.map_err(io_err)?;
        if let Some(log) = qh.optional_message_log() { log.shutdown().await.map_err(io_err)?; }

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

        self.ensure_partition_not_sealed(topic, part, group)?;
        self.ensure_checkpoint_not_pending(topic, part, group)?;

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

            if let Some(handle) = existing.as_ref().and_then(|slot| slot.handle.get()) {
                handle.ensure_not_recovery_sealed()?;
            }

            // Guard: never discard inflight (leased, un-acked) work. The
            // repartition caller only destroys drained partitions; this keeps
            // the primitive safe if it is ever called on a live one.
            if let Some(slot) = &existing
                && let Some(qh) = slot.handle.get()
                && let Some(wq) = qh.as_work_queue()
                && wq.inflight_len().await > 0
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
            // Drain in-flight owner operations before teardown (frozen -> new
            // ops error, no hang), so an append in progress is not cut off.
            qh.quiesce_for_teardown().await;
            qh.cancel_background_tasks();
            qh.recovery_gate.retire_snapshots().await?;
            if let Some(wq) = qh.as_work_queue() {
                wq.shutdown().await;
            }
            qh.event_log().shutdown().await.map_err(io_err)?;
            if let Some(log) = qh.optional_message_log() { log.shutdown().await.map_err(io_err)?; }
        }

        // 3. Move the partition into trash while the tombstone still blocks
        //    reopen, then drop the tombstone and wake any parked materializers
        //    (they revalidate, find no slot, and recreate a fresh partition).
        let trashed = self.move_partition_to_trash(topic, part, group)?;
        self.remove_queue(topic, part, group);
        tombstone.eviction_state.finish_eviction();

        // 4. Delete the moved tree unhurried. It shares no path with any live
        //    or recreated incarnation, and open sweeps it after a crash.
        if let Some(dir) = trashed
            && let Err(err) = tokio::fs::remove_dir_all(&dir).await
        {
            tracing::warn!("destroy_partition: failed to delete {dir:?}: {err}");
        }

        Ok(DestroyOutcome::Destroyed)
    }

    /// Move the whole partition directory, every generation and its recovery
    /// bookkeeping, into `trash/<uuid>` with one rename on the same
    /// filesystem. Returns the trash path, or `None` when nothing existed.
    fn move_partition_to_trash(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Option<PathBuf>> {
        let dir = self.partition_dir(topic, part, group);
        let key = Self::partition_key(topic, part, group);
        if !dir.try_exists().map_err(io_err)? {
            self.generations.remove(&key);
            return Ok(None);
        }
        let trash_dir = self.root.join(layout::TRASH);
        let trash = trash_dir.join(uuid::Uuid::now_v7().to_string());
        fs::rename(&dir, &trash).map_err(io_err)?;
        self.generations.remove(&key);
        keratin_log::util::fsync_dir(dir.parent().unwrap()).map_err(io_err)?;
        keratin_log::util::fsync_dir(&trash_dir).map_err(io_err)?;
        Ok(Some(trash))
    }

    /// Freeze an existing follower after its replication worker has stopped.
    /// An absent or cold partition stays unmaterialized; cleanup can arrive
    /// after the repartition reclaimer has already deleted its storage.
    pub async fn stop_queue_follower_for_transition(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        // Cleanup must not reopen storage after repartition retirement. Serialize
        // lookup and freezing with destroy/evict so an absent handle stays cold.
        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;
        let qh = {
            let current = self.queue_handles.load();
            slot_lookup_no_alloc(current.as_ref(), topic, part, group)
                .and_then(|slot| slot.handle.get().cloned())
        };
        let Some(qh) = qh else {
            return Ok(());
        };
        let role = qh.role();
        if role != QueueRole::Follower {
            return Err(StromaError::WrongQueueRole {
                expected: QueueRole::Follower,
                actual: role,
            });
        }

        qh.freeze();
        for log in qh.physical_logs() { log.freeze(); }
        Ok(())
    }

    /// Drain and freeze an existing owner without reopening a cold or deleted
    /// partition. Serialized with storage destruction and eviction.
    pub async fn freeze_queue_for_transition(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        // Cleanup must not reopen storage after repartition retirement. Serialize
        // lookup and freezing with destroy/evict so an absent handle stays cold.
        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;
        let qh = {
            let current = self.queue_handles.load();
            slot_lookup_no_alloc(current.as_ref(), topic, part, group)
                .and_then(|slot| slot.handle.get().cloned())
        };
        let Some(qh) = qh else {
            return Ok(());
        };
        qh.freeze_owner_and_wait_operations().await?;
        for log in qh.physical_logs() { log.freeze(); }
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
        // A stream has no leased deliveries, so it is never "inflight".
        Ok(match handle.as_work_queue() {
            Some(wq) => wq.inflight_len().await > 0,
            None => false,
        })
    }

    async fn ensure_queue(&self, tp: &str, part: u32, group: Option<&str>) -> Result<()> {
        self.queue_handle(tp, part, group).await?;

        Ok(())
    }

    // Companion to recover_all (eager startup recovery, not yet wired): once the
    // whole disk is recovered, flips every materialized handle to recovery-done.
    // Has a unit test, so it is compiled in but not yet called on any prod path.
    #[allow(dead_code)]
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

    /// Which partition kind an event belongs to. Cursor commits and stream
    /// truncation are the stream-side events, snapshots apply to either kind
    /// (None), everything else drives queue state.
    fn event_partition_kind(ev: &StromaEvent) -> Option<PartitionKind> {
        match ev {
            StromaEvent::CursorCommit { .. }
            | StromaEvent::CursorCommitBatch { .. }
            | StromaEvent::StreamTruncate { .. } => Some(PartitionKind::Stream),
            StromaEvent::Snapshot { .. } => None,
            _ => Some(PartitionKind::Queue),
        }
    }

    /// True when `ev` cannot apply to this partition's kind. Such an event is
    /// durable log poison from a mis-routed append (guarded against at append
    /// time now, but older logs can carry them): it was never applied and its
    /// producer was never confirmed, so skipping it is safe - and refusing to
    /// open the partition over it would brick recovery forever.
    fn skip_wrong_kind_event(ev: &StromaEvent, qh: &QueueHandleInner) -> bool {
        let Some(event_kind) = Self::event_partition_kind(ev) else {
            return false;
        };
        if event_kind == qh.kind() {
            return false;
        }
        tracing::error!(
            "skipping a {event_kind:?}-kind event in a {:?} partition's log \
             (poison from a mis-routed append, never confirmed to its producer)",
            qh.kind()
        );
        true
    }

    fn enqueue_event_inmem(&self, ev: StromaEvent, qh: &QueueHandleInner) -> std::io::Result<()> {
        tracing::debug!("Applying event: {ev:?}");
        if Self::skip_wrong_kind_event(&ev, qh) {
            return Ok(());
        }
        match ev {
            StromaEvent::PayloadEnqueue { .. } => return Err(io::Error::new(io::ErrorKind::InvalidData, "payload enqueue needs record offset")),
            StromaEvent::Enqueue {
                off,
                retries,
                expire_at,
            } => {
                let command = QueueCommand::Enqueue {
                    offset: off,
                    retries,
                    expire_at,
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
            StromaEvent::CancelEnqueueMany { offs } => {
                let command = QueueCommand::CancelEnqueueMany {
                    offs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
            }
            StromaEvent::ActivateDelayed { now, max } => {
                qh.blocking_command_enqueue(QueueCommand::ActivateDelayed { now, max, response: None })?;
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
                // - late NACK after consumer retry
                // A requeue NACK can increment retries again on a ready offset.
                // Consumer-tag validation prevents duplicate client settlement;
                // replay must apply each durable event only once.
            }
            StromaEvent::NackMany { reqs } => {
                let command = QueueCommand::NackMany {
                    reqs,
                    response: None,
                };
                qh.blocking_command_enqueue(command)?;
                // Accept NACK even if not inflight:
                // - race with expiry worker
                // - late NACK after consumer retry
                // A requeue NACK can increment retries again on a ready offset.
                // Consumer-tag validation prevents duplicate client settlement;
                // replay must apply each durable event only once.
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
            StromaEvent::CursorCommit { name, offset } => {
                qh.blocking_stream_command_enqueue(StreamCommand::CommitCursor {
                    name: name.into(),
                    offset,
                    response: None,
                })?;
            }
            StromaEvent::CursorCommitBatch { commits } => {
                qh.blocking_stream_command_enqueue(StreamCommand::CommitCursors {
                    commits: commits
                        .into_iter()
                        .map(|(name, offset)| (name.into(), offset))
                        .collect(),
                    response: None,
                })?;
            }
            StromaEvent::StreamTruncate { before } => {
                // Logical head advance only on the blocking path. Physical reclaim
                // happens on the async apply path that emits and replicates this
                // event; this path is for recovery replay, where the message log is
                // already physically truncated.
                qh.blocking_stream_command_enqueue(StreamCommand::ApplyTruncation {
                    new_head: before,
                    response: None,
                })?;
            }
        }
        Ok(())
    }

    pub(crate) fn payload_log_for(&self, qh: &QueueHandleInner) -> Arc<Keratin> {
        qh.payload_log()
    }

    pub(crate) async fn apply_record_inmem(&self, ev: StromaEvent, off: u64, qh: &QueueHandleInner) -> Result<()> {
        if qh.has_combined_log() { return self.apply_combined_records(vec![ev], off, qh).await; }
        let _ = off;
        self.apply_event_inmem(ev, qh).await
    }

    pub(crate) async fn apply_event_inmem(
        &self,
        ev: StromaEvent,
        qh: &QueueHandleInner,
    ) -> Result<()> {
        tracing::debug!("Applying event: {ev:?}");
        if Self::skip_wrong_kind_event(&ev, qh) {
            return Ok(());
        }
        match ev {
            StromaEvent::PayloadEnqueue { .. } => return Err(StromaError::InvalidArgument("payload enqueue needs record offset".into())),
            StromaEvent::Enqueue {
                off,
                retries,
                expire_at,
            } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::Enqueue {
                    offset: off,
                    retries,
                    expire_at,
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
            StromaEvent::CancelEnqueueMany { offs } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::CancelEnqueueMany {
                    offs,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::ActivateDelayed { now, max } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.command_enqueue(QueueCommand::ActivateDelayed { now, max, response: Some(tx) })
                    .await.map_err(io_err)?;
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
                // - late NACK after consumer retry
                // A requeue NACK can increment retries again on a ready offset.
                // Consumer-tag validation prevents duplicate client settlement;
                // replay must apply each durable event only once.
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
                // - late NACK after consumer retry
                // A requeue NACK can increment retries again on a ready offset.
                // Consumer-tag validation prevents duplicate client settlement;
                // replay must apply each durable event only once.
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
            StromaEvent::CursorCommit { name, offset } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.stream_command_enqueue(StreamCommand::CommitCursor {
                    name: name.into(),
                    offset,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::CursorCommitBatch { commits } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.stream_command_enqueue(StreamCommand::CommitCursors {
                    commits: commits
                        .into_iter()
                        .map(|(name, offset)| (name.into(), offset))
                        .collect(),
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
            StromaEvent::StreamTruncate { before } => {
                // Best-effort physical reclaim (segment-granular, idempotent). The
                // logical head advance below is what gates reads and keeps the owner
                // and followers converged on the same head.
                let _ = qh.msg_log()?.truncate_before(before).await;
                let (tx, rx) = tokio::sync::oneshot::channel();
                qh.stream_command_enqueue(StreamCommand::ApplyTruncation {
                    new_head: before,
                    response: Some(tx),
                })
                .await
                .map_err(io_err)?;
                rx.await.map_err(|_| StromaError::QueueActorGone)?;
            }
        }
        Ok(())
    }

    /// Owner state operations can reference only already-assigned payload
    /// positions. The tail is sampled while an owner-operation lease prevents
    /// destructive transitions. Concurrent appends can only advance this bound.
    fn owner_event_msg(&self, qh: &QueueHandleInner, event: &StromaEvent) -> Result<Message> {
        if qh.kind() == PartitionKind::Queue
            && event.max_combined_referenced_offset().is_some_and(|off| off >= qh.event_log().next_offset())
        {
            return Err(StromaError::InvalidArgument("combined owner event has a forward payload reference".into()));
        }
        event_msg(event)
    }

    /// Direct actor operations still need to settle their non-payload log slot
    /// before publishing the ordered application boundary.
    async fn settle_metadata_record(&self, qh: &QueueHandleInner, offset: Offset) -> Result<()> {
        if qh.kind() == PartitionKind::Queue {
            self.apply_event_inmem(StromaEvent::Ack { off: offset }, qh).await?;
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
            msgs.push(self.owner_event_msg(&qh, ev)?);
        }
        let msgs_count = msgs.len();
        let bytes_count: usize = msgs.iter().map(|m| m.bytes_len()).sum();
        let ordered_scope = qh.ordered_apply_scope();

        // Durable append first.
        let ar = event_log
            .append_batch(msgs, Some(durability))
            .await
            .map_err(io_err)?;

        self.metrics
            .event_log_appends
            .observe(msgs_count, bytes_count);
        if ordered_scope.is_none() {
            qh.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        }
        qh.set_dirty_snapshot(true);

        // Apply in memory after durable accept.
        let turn = crate::ordered_apply::enter(&ordered_scope, ar.base_offset, ar.count as u64).await?;
        for (i, ev) in evs.into_iter().enumerate() {
            self.apply_record_inmem(ev, ar.base_offset + i as u64, &qh).await?;
        }
        if ordered_scope.is_some() {
            qh.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Release);
        }
        crate::ordered_apply::finish(turn, ordered_scope)?;

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

    pub async fn lowest_unsettled_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset> {
        let q = self.queue_handle(tp, part, group).await?;
        let q = q.resolve()?;
        Ok(q.work_queue()?.lowest_unsettled_offset().await)
    }

    pub async fn is_inflight_or_settled(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<bool> {
        let q = self.queue_handle(tp, part, group).await?;
        let q = q.resolve()?;
        Ok(q.work_queue()?.is_inflight_or_settled(off).await)
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
        Ok(q.work_queue()?.is_ready(off).await)
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
            // Per queue so one unreachable partition does not drop the hint for all.
            let hint = async {
                let qh = self.queue_handle(&t, p, g.as_deref()).await?;
                let qh = qh.resolve()?;
                // Only owner work queues have lease-expiry. The projection skips
                // streams (which run no work-queue actor) by construction.
                let Some(wq) = qh.as_work_queue() else {
                    return Ok::<Option<UnixMillis>, StromaError>(None);
                };
                if wq.role() != QueueRole::Owner {
                    return Ok::<Option<UnixMillis>, StromaError>(None);
                }
                Ok(wq.next_expiry_hint().await)
            }
            .await;
            match hint {
                Ok(Some(hint)) => {
                    min = Some(match min {
                        Some(m) => m.min(hint),
                        None => hint,
                    });
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        topic = %t, partition = p, group = ?g, %err,
                        "expiry: skipping queue, next_expiry_hint failed"
                    );
                }
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
        Ok(q.work_queue()?.next_deliverable(from, upper).await)
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
            if out.len() >= max {
                break;
            }
            let want = max - out.len();
            // Collect per queue so one unreachable partition does not abort expiry
            // for the rest. A non-owner contributes nothing.
            let collected = async {
                let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
                let qh = qh.resolve()?;
                // Only owner work queues have lease-expiry. Streams have their own
                // retention worker and run no work-queue actor, so the projection
                // skips them by construction.
                let Some(wq) = qh.as_work_queue() else {
                    return Ok::<Vec<Offset>, StromaError>(Vec::new());
                };
                if wq.role() != QueueRole::Owner {
                    return Ok::<Vec<Offset>, StromaError>(Vec::new());
                }
                wq.collect_expired(now, want)
                    .await
                    .map_err(StromaError::from)
            }
            .await;
            let offsets = match collected {
                Ok(offsets) => offsets,
                Err(err) => {
                    tracing::warn!(
                        topic = %tp, partition = part, group = ?group, %err,
                        "expiry: skipping queue, collect_expired failed"
                    );
                    continue;
                }
            };
            for off in offsets {
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

    /// Drop ready messages whose TTL has passed across all owned queues. Routes
    /// through the terminal-nack/DLQ pipeline with `DeadLetterReason::Expired`, so
    /// it honors the queue's dlq_policy: discard when no DLQ is configured (the
    /// usual TTL case), dead-letter when one is. Inflight work is never dropped.
    /// Returns the set of dropped (tp, part, group, offset).
    pub async fn drop_ttl_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>> {
        let mut dropped = HashSet::new();
        let mut reqs_per_queue =
            HashMap::<(String, u32, Option<String>), Vec<NackEventMeta>>::new();

        let keys = self.queue_keys_snapshot();
        for (tp, part, group) in keys {
            if dropped.len() >= max {
                break;
            }
            let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
            let qh = qh.resolve()?;
            // Message TTL applies only to owner work queues. Streams expire by
            // their own retention worker, so the projection skips them.
            let Some(wq) = qh.as_work_queue() else {
                continue;
            };
            if wq.role() != QueueRole::Owner {
                continue;
            }
            let want = max - dropped.len();
            let expired = wq.collect_ttl_expired(now, want).await?;
            if expired.is_empty() {
                continue;
            }
            let entry = reqs_per_queue
                .entry((tp.to_string(), part, group.clone().map(|s| s.to_string())))
                .or_default();
            for off in expired {
                // requeue=false routes through the DLQ/discard pipeline.
                entry.push(NackEventMeta {
                    off,
                    requeue: false,
                    not_before: None,
                });
                dropped.insert((
                    tp.to_string(),
                    part,
                    group.clone().map(|s| s.to_string()),
                    off,
                ));
            }
        }

        let mut awaiters = Vec::new();
        for ((tp, part, group), reqs) in reqs_per_queue {
            let (completion, rx) = KeratinAppendCompletion::pair();
            self.nack_enqueue_many_with_reason(
                &tp,
                part,
                group.as_deref(),
                reqs,
                Some(DeadLetterReason::Expired),
                completion,
            )
            .await?;
            awaiters.push(rx);
        }

        for awaiter in awaiters {
            awaiter
                .await
                .map_err(|_err| StromaError::Io("Broken pipe".into()))?
                .map_err(|err| StromaError::Io(err.to_string()))?;
        }

        Ok(dropped)
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
        if qh.resolve()?.has_ordered_apply() {
            return stroma.write_exact_queue_checkpoint(qh.clone(), true).await;
        }
        let qh = qh.resolve()?;
        let _apply = qh.follower_apply_state().await;
        if qh.ensure_not_recovery_sealed().is_err() {
            return Ok(());
        }
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
        let msg_log = qh.msg_log()?;
        // The message-log truncation floor is the lowest un-acked offset for a
        // work queue. A stream has no acks and prunes its message log through its
        // own retention worker, so it contributes no floor here (0 truncates
        // nothing).
        let safe_msg_truncate = match qh.as_work_queue() {
            Some(wq) => wq.lowest_not_settled_offset().await,
            None => 0,
        };
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
                    // A partition being torn down mid-tick (destroy renames its
                    // dirs aside, shutdown closes its logs) surfaces here as a
                    // file-not-found once, then the next tick's resolve ends
                    // this task. That race is expected - say so instead of
                    // alarming, and name the partition either way.
                    match qh.resolve() {
                        Ok(h) => tracing::error!(
                            "Error during periodic snapshot of {}/{} (group {:?}): {err}",
                            h.topic(),
                            h.partition(),
                            h.group(),
                        ),
                        Err(_) => tracing::info!(
                            "periodic snapshot lost its partition mid-tick \
                             (evicted or destroyed - the task exits): {err}"
                        ),
                    }
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
        let snapshot_generation = qh.recovery_gate.checkpoint_generation.load(Ordering::Acquire);
        let blob = if let Ok(blob) = qh.encode_snapshot(applied_upto).await {
            blob
        } else {
            // TODO:
            return Ok(());
        };

        let dir = self.snap_dir(tp, part, group);
        let (tp, group) = (tp.to_string(), group.map(|s| s.to_string()));
        let stroma = self.clone();
        let recovery_gate = qh.recovery_gate.clone();
        tokio::task::spawn_blocking(move || {
            recovery_gate.write_snapshot(snapshot_generation, &tp, part, group.as_deref(), || {
                stroma.create_generation_dir(&tp, part, group.as_deref(), &dir)?;
                stroma.write_queue_snapshot(&tp, part, group.as_deref(), applied_upto, &blob)
            })
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
        self.write_queue_snapshot_envelope(tp, part, group, 1, last_applied_event_offset, blob)
    }

    pub(super) fn write_queue_snapshot_envelope(
        &self, tp: &str, part: u32, group: Option<&str>,
        version: u16, boundary: u64, blob: &[u8],
    ) -> Result<()> {
        let tmp = self.snap_tmp_file();
        let final_path = self.snap_file(tp, part, group);
        self.create_generation_dir(tp, part, group, final_path.parent().unwrap())?;

        let out = Self::queue_snapshot_envelope(version, boundary, blob)?;

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
        // Publish the directory entry durably before any event-log compaction.
        recovery_seal::sync_directories(final_path.parent().unwrap())?;
        keratin_log::util::fsync_dir(tmp.parent().unwrap()).map_err(io_err)?;
        Ok(())
    }

    pub(super) fn queue_snapshot_envelope(version: u16, boundary: u64, blob: &[u8]) -> Result<Vec<u8>> {
        // file format (big endian):
        // magic 8: b"SSNAP\0\0\0"
        // ver u16: 1 (legacy inclusive), 2 (exact exclusive)
        // reserved u16
        // event boundary u64
        // blob_len u32
        // blob bytes
        // crc32c u32 over (ver..blob)
        const MAGIC: &[u8; 8] = b"SSNAP\0\0\0";

        let blob_len = u32::try_from(blob.len()).map_err(|_| StromaError::InvalidArgument("snapshot is too large".into()))?;
        let mut payload = Vec::with_capacity(2 + 2 + 8 + 4 + blob.len());
        payload.extend_from_slice(&version.to_be_bytes());
        payload.extend_from_slice(&0u16.to_be_bytes());
        payload.extend_from_slice(&boundary.to_be_bytes());
        payload.extend_from_slice(&blob_len.to_be_bytes());
        payload.extend_from_slice(blob);

        let crc = crc32c::crc32c(&payload);

        let mut out = Vec::with_capacity(8 + payload.len() + 4);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&payload);
        out.extend_from_slice(&crc.to_be_bytes());

        Ok(out)
    }

    fn read_queue_snapshot(&self, path: &Path) -> Result<Option<(checkpoint_capture::SnapshotBoundary, Vec<u8>)>> {
        if !path.exists() {
            return Ok(None);
        }

        const MAGIC: &[u8; 8] = b"SSNAP\0\0\0";

        let bytes = fs::read(path).map_err(io_err)?;
        if bytes.len() < 8 + 2 + 2 + 8 + 4 + 4 {
            return Err(StromaError::Decode("snapshot too small".into()));
        }
        if &bytes[0..8] != MAGIC {
            return Err(StromaError::Decode("bad snapshot magic".into()));
        }

        // crc check
        let want = u32::from_be_bytes(
            bytes[bytes.len() - 4..]
                .try_into()
                .expect("exact-length slice"),
        );
        let payload = &bytes[8..bytes.len() - 4];
        let got = crc32c::crc32c(payload);
        if got != want {
            return Err(StromaError::Decode("snapshot crc mismatch".into()));
        }

        let ver = u16::from_be_bytes(payload[0..2].try_into().expect("exact-length slice"));
        if !matches!(ver, 1 | 2) || payload[2..4] != [0, 0] {
            return Err(StromaError::Decode("snapshot version mismatch".into()));
        }

        let last_applied =
            u64::from_be_bytes(payload[4..12].try_into().expect("exact-length slice"));
        let blob_len =
            u32::from_be_bytes(payload[12..16].try_into().expect("exact-length slice")) as usize;

        if 16 + blob_len != payload.len() {
            return Err(StromaError::Decode("snapshot blob truncated".into()));
        }
        let blob = payload[16..16 + blob_len].to_vec();
        Ok(Some((checkpoint_capture::SnapshotBoundary::new(ver, last_applied), blob)))
    }

    // ---------------- Recovery ----------------

    // Eager whole-disk recovery at boot. Recovery is lazy by default (per
    // queue_handle on first use), so this is unused until an opt-in config wires
    // eager startup recovery. Kept because that is the original design and the
    // only way recovery.on_mismatch=refuse becomes a literal refuse-to-start.
    #[allow(dead_code)]
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

    // Companion to recover_all (eager startup recovery, not yet wired).
    #[allow(dead_code)]
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

    /// Drop only the unresolvable suffix. The durable repair journal preserves
    /// the valid prefix across cancellation, I/O errors and process restart.
    /// Recovery holds the lifecycle lock and has not exposed this incarnation.
    async fn truncate_event_log_tail(&self, h: &QueueHandleInner, next_offset: u64) -> Result<()> {
        let event_log = h.event_log();
        let prev_role = event_log.role();
        event_log.become_follower();
        let res = event_log
            .repair_suffix_at_epoch(next_offset, event_log.current_epoch())
            .await
            .map_err(io_err);
        match prev_role {
            KeratinRole::Owner => event_log.become_owner(),
            _ => event_log.become_follower(),
        }
        res
    }

    fn recover_stream_events_from_log(
        event_log: Arc<Keratin>,
        mut cur: u64,
        tail: u64,
        applied_upto: Arc<AtomicU64>,
    ) -> Result<RecoveryScanOutcome> {
        let reader = event_log.reader();

        // Decode the whole replay range, stopping at the first corrupt record (we
        // cannot decode past it).
        let mut scanned: Vec<(u64, StromaEvent)> = Vec::new();
        let mut corrupt: Option<RecoveryMismatchFound> = None;
        'scan: while cur < tail {
            let batch = reader.scan_from(cur, 10_000).map_err(io_err)?;
            if batch.is_empty() {
                break;
            }
            for rec in batch {
                let offset = rec.offset;
                cur = offset + 1;
                match StromaEvent::decode(&rec.payload) {
                    Ok(ev) => scanned.push((offset, ev)),
                    Err(err) => {
                        corrupt = Some(RecoveryMismatchFound {
                            event_offset: offset,
                            kind: RecoveryMismatchKind::CorruptRecord {
                                detail: err.to_string(),
                            },
                        });
                        break 'scan;
                    }
                }
            }
        }

        // Stream payloads and cursor events remain independent. Queue-specific
        // events are ignored by stream application and cannot authorize a
        // two-log queue suffix repair.
        let valid_len = scanned.len();
        let mismatch = corrupt;

        let mut events = Vec::with_capacity(valid_len);
        for (ev_off, ev) in scanned.into_iter().take(valid_len) {
            applied_upto.store(ev_off, Ordering::Release);
            events.push(ev);
        }

        Ok(RecoveryScanOutcome {
            // Exactly `valid_len` events were pushed above.
            events_count: valid_len as u64,
            events,
            mismatch,
        })
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
        let mut pending_combined_backfill = false;

        // Legacy snapshots encode an inclusive applied offset, so zero also
        // represents an empty checkpoint. If event zero is still retained,
        // rebuild from the log instead of possibly skipping the first enqueue.
        // Compacted checkpoints have a positive event head and retain their blob.
        if let Some((applied_upto, blob)) = self
            .read_queue_snapshot(&self.snap_file(tp, part, group))?
            .filter(|(applied, _)| !applied.ambiguous_zero() || event_log.head_offset() != 0)
        {
            let learner_backfill = applied_upto.event_next() > event_log.next_offset()
                && self.permits_combined_checkpoint_backfill(tp, part, group, applied_upto.event_next(), &blob, &event_log)?;
            if matches!(applied_upto, checkpoint_capture::SnapshotBoundary::ExactNext(_))
                && (applied_upto.event_next() < event_log.head_offset()
                    || (applied_upto.event_next() > event_log.next_offset() && !learner_backfill)) {
                return Err(StromaError::Corruption("exact checkpoint does not cover retained event prefix".into()));
            }
            { pending_combined_backfill = learner_backfill; }
            if learner_backfill {
                h.become_follower();
                for log in h.physical_logs() { log.become_follower(); }
                h.reset_ordered_apply_after_recovery(applied_upto.event_next());
            }
            h.load_snapshot(blob).await.map_err(|err| {
                StromaError::Io(format!(
                    "snapshot load failed for tp={tp} part={part} group={group:?}: {err}"
                ))
            })?;
            // A checkpoint can be durable before its payload backfill. Preserve
            // the state for follower catch-up, but never reopen it as an owner.
            if let Some(wq) = h.as_work_queue() {
                let required = wq.required_message_next().await.map_err(io_err)?;
                let payload_next = self.payload_log_for(&h).next_offset();
                if payload_next < required {
                    tracing::warn!(
                        tp,
                        part,
                        required,
                        message_next = h.payload_log().next_offset(),
                        "checkpoint payload backfill incomplete; recovery keeps queue as follower"
                    );
                    h.become_follower();
                    for log in h.physical_logs() { log.become_follower(); }
                }
            }
            h.applied_upto().store(applied_upto.last_applied(), Ordering::Release);
            cur = applied_upto.event_next();
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

        let combined_mode = h.has_combined_log();
        let RecoveryScanOutcome {
            events,
            events_count,
            mismatch,
        } = tokio::task::spawn_blocking(move || {
            if combined_mode { return combined::recover_events(event_log,cur,tail,applied_upto); }
            Self::recover_stream_events_from_log(event_log, cur, tail, applied_upto)
        })
        .await
        .map_err(|err| StromaError::Io(err.to_string()))??;

        if let Some(m) = mismatch {
            // Reusing offsets after local repair would fork the accepted writer
            // session. Preserve these bytes for coordinated recovery instead.
            self.require_unbound_storage_history(tp, part, group)?;
            let reason = match &m.kind {
                RecoveryMismatchKind::CorruptRecord { detail } => format!(
                    "corrupt event record at offset {}: {detail} (repair drops the event-log \
                     suffix from offset {})",
                    m.event_offset, m.event_offset
                ),
            };

            let policy = RecoveryMismatchPolicy::from_u8(
                self.recovery_mismatch_policy.load(Ordering::Acquire),
            );
            match policy {
                RecoveryMismatchPolicy::Ignore => {
                    tracing::error!(
                        "recovery: {tp}/{part}/{group:?}: {reason}; on_mismatch=Ignore -> \
                         truncating the event log to {} and continuing (POSSIBLE DATA LOSS)",
                        m.event_offset
                    );
                    self.truncate_event_log_tail(&h, m.event_offset).await?;
                    // fall through and apply only the valid prefix in `events`
                }
                RecoveryMismatchPolicy::Quarantine | RecoveryMismatchPolicy::Refuse => {
                    tracing::error!(
                        "recovery: QUARANTINING {tp}/{part}/{group:?}: {reason} (policy={policy:?})"
                    );
                    let was_present = self
                        .quarantined
                        .insert(
                            (
                                Box::<str>::from(tp),
                                part,
                                normalize_group(group).map(Box::<str>::from),
                            ),
                            QuarantineInfo {
                                topic: tp.to_string(),
                                partition: part,
                                group: group.map(|g| g.to_string()),
                                reason: reason.clone(),
                                truncate_event_offset: m.event_offset,
                            },
                        )
                        .is_some();
                    if !was_present {
                        self.metrics
                            .recovery
                            .quarantined
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.metrics
                        .recovery
                        .quarantines_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(StromaError::RecoveryMismatch {
                        topic: tp.to_string(),
                        partition: part,
                        group: group.map(|g| g.to_string()),
                        reason,
                    });
                }
            }
        }

        // A stream's tail is the message log head. Per-record appends carry no
        // event (only cursor commits do), so recovery cannot replay the tail.
        // Reconcile it from the durable message log BEFORE replaying cursor
        // commits, so a committed cursor clamps against the real retained window
        // rather than an empty one.
        if h.kind() == PartitionKind::Stream {
            let next = h.msg_log()?.next_offset();
            let (tx, rx) = tokio::sync::oneshot::channel();
            if h.stream_command_enqueue(StreamCommand::AdvanceTail {
                next_offset: next,
                response: Some(tx),
            })
            .await
            .is_ok()
            {
                let _ = rx.await;
            }
        }

        for ev in events {
            self.apply_event_inmem(ev, &h).await?;
        }

        if h.has_combined_log() {
            self.verify_combined_payload_coverage(&h, pending_combined_backfill).await?;
        }

        // Re-issue any DLQ copies that were pending at the crash. Streams have no
        // dead-letter path, so this is a work-queue-only recovery step.
        if let Some(wq) = h.as_work_queue() {
            let pending = wq.pending_dlq().await?;
            let source_tp = tp;
            let source_part = part;
            let source_group = group;
            let target = wq.get_dlq_target().await?;
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
            .fetch_add(events_count, Ordering::Relaxed);
        self.metrics
            .recovery
            .replay_duration
            .observe(replay_start.elapsed());

        self.metrics.recovery.startup_duration.observe(elapsed);

        Ok(())
    }

    /// Every partition with a current generation, walking
    /// `partitions/<group>/<topic>/<part>`.
    pub fn discover_partitions(&self) -> Result<Vec<(Option<String>, String, u32)>> {
        let mut out: Vec<_> = layout::scan_partitions(&self.root)?
            .into_iter()
            .filter(|found| found.current.is_some())
            .map(|found| (found.group, found.topic, found.part))
            .collect();
        out.sort();
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
        let mut registries = vec![self.queue_handles.swap(Arc::new(hashbrown::HashMap::new()))];
        for view in self.reseed_views.iter() {
            registries.push(view.queue_handles.swap(Arc::new(hashbrown::HashMap::new())));
        }
        self.reseed_views.clear();

        // Step 2: stop per-queue background work before draining queue actors.
        // Snapshot tasks can otherwise enqueue work while shutdown is trying to
        // drain the same queue, which is especially visible in slow CI runs.
        for (_key, slot) in registries.iter().flat_map(|registry| registry.iter()) {
            if let Some(q) = slot.handle.get() {
                q.cancel_background_tasks();
            }
        }

        // Step 3: shutdown everything from the old snapshot
        let mut futs = FuturesUnordered::new();
        for (_key, slot) in registries.iter().flat_map(|registry| registry.iter()) {
            if let Some(q) = slot.handle.get() {
                let q = q.clone();
                futs.push(async move {
                    q.recovery_gate.retire_snapshots().await?;
                    if let Some(wq) = q.as_work_queue() {
                        wq.shutdown().await;
                    }
                    // A failed writer must not prevent its sibling from draining.
                    // Preserve the first error after both shutdown attempts.
                    let events = q.event_log().shutdown().await.map_err(io_err);
                    let messages = match q.optional_message_log() {
                        Some(log) => log.shutdown().await.map_err(io_err),
                        None => Ok(()),
                    };
                    events.and(messages)
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

            let kind = self.read_partition_kind(&tp, *part, group.as_deref());
            let sealed = self.ensure_partition_not_sealed(&tp, *part, group.as_deref()).is_err()
                || self.ensure_checkpoint_not_pending(&tp, *part, group.as_deref()).is_err();
            queues.push(QueueDebugInfo {
                topic: tp.to_string(),
                partition: *part,
                group: group.map(|group| group.to_string()),
                kind,
                materialized: false,
                exists_on_disk,
                evicting,
                applied_upto: 0,
                last_snapshot_timestamp: 0,
                last_snapshot_event_offset: 0,
                dirty_since_snapshot: false,
                creating_snapshot: false,
                role: if sealed {
                    QueueRole::Frozen
                } else {
                    QueueRole::Owner
                },
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
            command_metrics: self.metrics.command_snapshot(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
        })
    }

    pub async fn debug_report(&self) -> Result<String> {
        let snap = self.debug_snapshot().await?;
        let mut out = String::new();
        use std::fmt::Write;

        writeln!(out, "=== Stroma debug report ===")
            .expect("writing to an in-memory String never fails");
        writeln!(out, "Uptime: {}s", snap.uptime_seconds)
            .expect("writing to an in-memory String never fails");
        writeln!(out, "Indexed queues: {}", snap.queue_count)
            .expect("writing to an in-memory String never fails");
        writeln!(
            out,
            "Materialized queues: {}",
            snap.materialized_queue_count
        )
        .expect("writing to an in-memory String never fails");
        writeln!(out).expect("writing to an in-memory String never fails");

        writeln!(out, "Command queue depths:").expect("writing to an in-memory String never fails");
        for (lane, depth) in &snap.cmd_queue_depths {
            writeln!(out, "  {}: {}", lane, depth)
                .expect("writing to an in-memory String never fails");
        }
        writeln!(out).expect("writing to an in-memory String never fails");

        writeln!(out, "Snapshots:").expect("writing to an in-memory String never fails");
        writeln!(out, "  attempts: {}", snap.snapshot_metrics.attempts_total)
            .expect("writing to an in-memory String never fails");
        writeln!(
            out,
            "  skipped (not dirty): {}",
            snap.snapshot_metrics.skipped_not_dirty
        )
        .expect("writing to an in-memory String never fails");
        if let Some(avg) = snap.snapshot_metrics.avg_clone_ms {
            writeln!(out, "  avg clone: {:.1}ms", avg)
                .expect("writing to an in-memory String never fails");
        }
        if let Some(avg) = snap.snapshot_metrics.avg_total_ms {
            writeln!(out, "  avg total: {:.1}ms", avg)
                .expect("writing to an in-memory String never fails");
        }
        writeln!(out).expect("writing to an in-memory String never fails");

        writeln!(out, "Queues:").expect("writing to an in-memory String never fails");
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
            .expect("writing to an in-memory String never fails");
        }

        Ok(out)
    }
}

impl Stroma {
    /// Append payload-bearing enqueue records in one durable batch. Each client
    /// receives its record offset after persistence and ordered state application.
    pub async fn append_message_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
    ) -> Result<()> {
        self.append_message_batch_observed(tp, part, group, items, None)
            .await
    }

    /// Append with an optional exact queue-dependency observer. Existing callers
    /// retain the ordinary per-message completion interface.
    pub async fn append_message_batch_observed(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
        observer: Option<QueuePublishObserver>,
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let qh = self.queue_handle(tp, part, group).await?;
        let (owner_operation, default_ttl_ms) = {
            let h = qh.resolve()?;
            // Refuse BEFORE anything is appended: a queue publish routed at a
            // stream partition (a client or broker that has not yet learned
            // the topic's kind) must fail cleanly. Appending first and letting
            // the in-memory apply reject it would leave queue-kind events
            // durably poisoning the stream's log, and every later recovery
            // would trip over them.
            if h.as_stream().is_some() {
                return Err(StromaError::InvalidArgument(format!(
                    "{tp}/{part} is a plexus stream - a queue publish cannot append to it. \
                     Publish through the stream API, or use a different topic name"
                )));
            }
            (
                h.begin_owner_operation().await?,
                h.default_message_ttl_ms(),
            )
        };
        // Resolve the per-queue default deadline once for this batch (broker
        // clock). A message's own expire_at wins; otherwise the default applies.
        let default_expire_at = default_ttl_ms.map(|ttl| unix_millis().saturating_add(ttl));

        // Build payload records and extract per-client completions.
        // Per message header encode failures fail just that one completion.
        let mut messages = Vec::with_capacity(items.len());
        let mut completion_items = Vec::with_capacity(items.len());
        for item in items {
            let PublishItem {
                headers,
                payload,
                completion,
                not_before,
                expire_at,
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
            messages.push(message);
            completion_items.push(CompletionItem {
                meta: ItemMeta {
                    not_before,
                    expire_at: expire_at.or(default_expire_at),
                },
                completion,
            });
        }

        if messages.is_empty() {
            return Ok(());
        }

        self.append_combined(qh, messages, completion_items, observer, owner_operation).await
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
        self.append_message_unchecked(tp, part, group, headers, payload, None, event_completion)
            .await
    }

    /// Publish a single message with an absolute drop deadline (message TTL).
    /// `expire_at` is `None` for no TTL.
    pub async fn append_message_with_ttl(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        expire_at: Option<UnixMillis>,
        event_completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        validate_user_message_headers(headers)?;
        self.append_message_unchecked(
            tp,
            part,
            group,
            headers,
            payload,
            expire_at,
            event_completion,
        )
        .await
    }

    async fn append_message_unchecked(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        expire_at: Option<UnixMillis>,
        event_completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let qh = self.queue_handle(tp, part, group).await?;
        let owner_operation = {
            let h = qh.resolve()?;
            // Same guard as the batch path: refuse before anything is
            // appended, or queue-kind events would durably poison the
            // stream's log for every later recovery.
            if h.as_stream().is_some() {
                return Err(StromaError::InvalidArgument(format!(
                    "{tp}/{part} is a plexus stream - a queue publish cannot append to it. \
                     Publish through the stream API, or use a different topic name"
                )));
            }
            h.begin_owner_operation().await?
        };
        let message = Message {
            flags: 0,
            headers: headers.encode()?,
            payload,
        };
        self.append_combined(qh, vec![message], vec![CompletionItem {
            meta: ItemMeta { not_before: None, expire_at },
            completion: event_completion,
        }], None, owner_operation).await
    }

    // ---------------- Stream (Plexus) engine API ----------------
    //
    // These operate on stream partitions (PartitionKind::Stream). The substrate is
    // shared with the work queue (log storage, durable append,
    // replication, recovery, snapshot IO); only the per-partition engine differs.
    // fibril owns consumer matching, fan-out, the live ring, and filtering on top
    // of these primitives. Streams (currently) have no group dimension.

    /// Create a stream partition: persist the engine-kind marker, then materialize
    /// it so the stream control actor is spawned, and optionally set retention.
    /// Call before first use; an already-materialized partition keeps the engine
    /// it was built with.
    pub async fn create_stream(
        &self,
        tp: &str,
        part: u32,
        retention: Option<RetentionConfig>,
    ) -> Result<()> {
        let lifecycle = self.lock_partition_lifecycle(tp, part, None).await;
        self.ensure_partition_not_sealed(tp, part, None)?;
        self.ensure_checkpoint_not_pending(tp, part, None)?;
        // Reject declaring a queue partition as a stream (it would materialize a
        // stream engine over queue data). The reverse guard lives in `declare`.
        if self.partition_kind_marker(tp, part, None) == Some(PartitionKind::Queue) {
            return Err(StromaError::InvalidArgument(format!(
                "{tp}/{part} is already declared as a queue, cannot declare it as a plexus stream - use a different topic name, or destroy the queue first if it is no longer needed"
            )));
        }
        self.write_partition_kind(tp, part, None, PartitionKind::Stream)?;
        drop(lifecycle);
        let qh = self.queue_handle(tp, part, None).await?;
        if let Some(retention) = retention {
            qh.resolve()?.stream()?.set_retention(retention).await?;
        }
        Ok(())
    }

    /// Append a record to a stream and return its offset. The record goes to the
    /// message log (durable and replicated by the substrate) and the stream tail
    /// advances past it. Unlike a queue publish this emits no per-record event:
    /// streams have no ready/inflight/ack state, so the message log is the record,
    /// and consumers read it by offset.
    pub async fn append_stream_record(
        &self,
        tp: &str,
        part: u32,
        headers: &MessageHeaders,
        payload: Vec<u8>,
    ) -> Result<Offset> {
        let qh = self.queue_handle(tp, part, None).await?;
        let (owner_operation, msg_log) = {
            let h = qh.resolve()?;
            (h.begin_owner_operation().await?, h.msg_log()?)
        };
        let (msg_completion, msg_rx) = KeratinAppendCompletion::pair();
        let message = Message {
            flags: 0,
            headers: headers.encode()?,
            payload,
        };
        msg_log
            .append_enqueue(message, None, msg_completion)
            .map_err(io_err)?;
        let ar = match msg_rx.await {
            Ok(Ok(ar)) => ar,
            Ok(Err(err)) => return Err(io_err(err)),
            Err(_) => {
                return Err(StromaError::Io(
                    "stream record completion channel closed".into(),
                ));
            }
        };
        // The owner operation only needs to span the durable append.
        drop(owner_operation);

        let msg_offset = ar.base_offset;
        let next = msg_offset + ar.count as u64;

        qh.resolve()?.stream()?.advance_tail(next).await?;

        Ok(msg_offset)
    }

    /// Pipelined durable stream append: enqueue the batch as ONE keratin append and
    /// return immediately, WITHOUT waiting for the offset to be staged or for the
    /// flush. The tail advance and replication caching run in a background task once
    /// the offset is assigned. This mirrors the queue publish path (hand keratin a
    /// completion, return) so a caller can enqueue the next batch right away and let
    /// keratin coalesce fsyncs across the queued appends, instead of paying one
    /// fsync per record by waiting for keratin to report back between appends.
    pub async fn append_stream_records_enqueue(
        &self,
        tp: &str,
        part: u32,
        records: Vec<(MessageHeaders, Vec<u8>)>,
        durability: Option<KDurability>,
    ) -> Result<EnqueuedStreamAppend> {
        if records.is_empty() {
            return Err(StromaError::InvalidArgument(
                "append_stream_records_enqueue requires at least one record".into(),
            ));
        }
        let qh = self.queue_handle(tp, part, None).await?;
        let (owner_operation, msg_log) = {
            let h = qh.resolve()?;
            (h.begin_owner_operation().await?, h.msg_log()?)
        };
        let mut messages = Vec::with_capacity(records.len());
        for (headers, payload) in records {
            messages.push(Message {
                flags: 0,
                headers: headers.encode()?,
                payload,
            });
        }
        let count = messages.len() as u64;
        let (msg_completion, msg_rx) = KeratinAppendCompletion::pair();
        let (staged_tx, staged_rx) = tokio::sync::oneshot::channel();
        msg_log
            .append_batch_enqueue_staged(messages, durability, msg_completion, staged_tx)
            .map_err(io_err)?;

        let (offset_tx, offset_rx) = tokio::sync::oneshot::channel();
        let (durable_tx, durable_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            // Hold the owner operation across the whole append so the partition is
            // not evicted mid-flush.
            let _owner_operation = owner_operation;
            let base = match staged_rx.await {
                Ok(offset) => offset,
                Err(_) => {
                    let _ = durable_tx.send(Err(StromaError::Io(
                        "stream staged-offset channel closed".into(),
                    )));
                    return;
                }
            };
            // Advance the tail (fire and forget, max-semantics). Order across
            // concurrent appends does not matter: the tail only ever moves forward.
            if let Ok(h) = qh.resolve()
                && let Some(s) = h.as_stream()
            {
                let _ = s.advance_tail_unconfirmed(base + count).await;
            }
            let _ = offset_tx.send(base);

            let result = match msg_rx.await {
                Ok(Ok(_ar)) => Ok(()),
                Ok(Err(err)) => Err(io_err(err)),
                Err(_) => Err(StromaError::Io(
                    "stream record completion channel closed".into(),
                )),
            };
            let _ = durable_tx.send(result);
        });

        Ok(EnqueuedStreamAppend {
            offset: offset_rx,
            durable: durable_rx,
        })
    }

    /// Flush and fsync a stream partition's message log, advancing the durable
    /// watermark. The fsync runs on keratin's fsync worker stage, so this does not
    /// block staging. Pairs with the ephemeral tier, which appends with AfterWrite
    /// (no per-record fsync): the broker calls this on an interval so dirty pages
    /// drain steadily instead of piling up until the kernel throttles the writer
    /// mid-write (which spikes ephemeral delivery tails).
    pub async fn sync_stream(&self, tp: &str, part: u32) -> Result<()> {
        let qh = self.queue_handle(tp, part, None).await?;
        let msg_log = qh.resolve()?.msg_log()?;
        msg_log.sync().await.map_err(io_err)?;
        Ok(())
    }

    /// Commit a durable named cursor for a stream. The commit is logged as a
    /// CursorCommit event (durable and replicated) and applied to the stream
    /// engine, so it survives a crash or redeploy and a follower promoted after
    /// owner loss resumes from it.
    pub async fn commit_stream_cursor(
        &self,
        tp: &str,
        part: u32,
        name: &str,
        offset: Offset,
    ) -> Result<()> {
        let ev = StromaEvent::CursorCommit {
            name: name.into(),
            offset,
        };
        let durability = self.keratin_cfg_event.default_durability;
        self.append_events_durable(tp, part, None, vec![ev], durability)
            .await?;
        Ok(())
    }

    /// Commit a coalesced batch of stream cursors in ONE durable record and ONE
    /// in-memory apply (a single `CursorCommitBatch` event + one actor command),
    /// instead of one of each per cursor. Used by the broker's cursor-commit
    /// microbatcher to amortize high-fan-out auto-ack. Empty input is a no-op.
    pub async fn commit_stream_cursors(
        &self,
        tp: &str,
        part: u32,
        commits: Vec<(String, Offset)>,
    ) -> Result<()> {
        if commits.is_empty() {
            return Ok(());
        }
        let ev = StromaEvent::CursorCommitBatch {
            commits: commits
                .into_iter()
                .map(|(name, offset)| (name.into_boxed_str(), offset))
                .collect(),
        };
        let durability = self.keratin_cfg_event.default_durability;
        self.append_events_durable(tp, part, None, vec![ev], durability)
            .await?;
        Ok(())
    }

    /// Read a stream's durable cursor position, or `None` if the name has no
    /// committed position.
    pub async fn stream_cursor(&self, tp: &str, part: u32, name: &str) -> Result<Option<Offset>> {
        let qh = self.queue_handle(tp, part, None).await?;
        Ok(qh.resolve()?.stream()?.cursor(name).await?)
    }

    /// List a stream's durable cursors as (name, offset), name-sorted.
    pub async fn stream_cursors(&self, tp: &str, part: u32) -> Result<Vec<(String, Offset)>> {
        let qh = self.queue_handle(tp, part, None).await?;
        Ok(qh.resolve()?.stream()?.cursors().await?)
    }

    /// Read a stream's current head/tail watermarks (lowest retained offset and
    /// one past the last appended offset).
    pub async fn stream_head_tail(&self, tp: &str, part: u32) -> Result<(Offset, Offset)> {
        let qh = self.queue_handle(tp, part, None).await?;
        Ok(qh.resolve()?.stream()?.head_tail().await?)
    }

    /// Read up to `max` stream records starting at `from`, for subscribe backfill.
    /// Returns `(offset, payload, headers)` per record, oldest first.
    pub async fn read_stream_records(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>, MessageHeaders)>> {
        let qh = self.queue_handle(tp, part, None).await?;
        let qh = qh.resolve()?;
        self.scan_messages_from(&qh, from, max)
    }

    /// Resolve the first offset whose record timestamp is at or after `ts_ms`, for
    /// by-time subscribe starts. Binary search over the retained range probing
    /// record timestamps. Returns the tail when every retained record is older
    /// than `ts_ms` (so a by-time start past the end becomes a live tail).
    pub async fn stream_offset_at_or_after_time(
        &self,
        tp: &str,
        part: u32,
        ts_ms: u64,
    ) -> Result<Offset> {
        let qh = self.queue_handle(tp, part, None).await?;
        let h = qh.resolve()?;
        let msg_log = h.msg_log()?;
        let reader = msg_log.reader();
        let mut lo = msg_log.head_offset();
        let mut hi = msg_log.next_offset();
        // Invariant: the answer is in [lo, hi]. A probe that is missing (a gap from
        // retention) or older than the target moves the lower bound up.
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match reader.fetch(mid).map_err(io_err)? {
                Some(record) if record.timestamp_ms >= ts_ms => hi = mid,
                _ => lo = mid + 1,
            }
        }
        Ok(lo)
    }

    /// Enforce a stream's retention policy once. Computes the segment-granular
    /// truncation boundary from the policy and the message log's segment list,
    /// then (if it advances the head) durably logs and applies a StreamTruncate so
    /// the head advance and physical reclaim replicate to followers. Returns the
    /// new head if anything was dropped. A no-op for queues, unconfigured
    /// retention, or when nothing is old/large/numerous enough to drop.
    pub async fn enforce_stream_retention(&self, tp: &str, part: u32) -> Result<Option<Offset>> {
        let qh = self.queue_handle(tp, part, None).await?;
        let (config, head, tail, infos) = {
            let h = qh.resolve()?;
            let Some(s) = h.as_stream() else {
                return Ok(None);
            };
            let (config, head, tail) = s.retention_state().await?;
            let infos = h.msg_log()?.segment_infos().map_err(io_err)?;
            (config, head, tail, infos)
        };

        let before = match compute_stream_truncate_before(&infos, config, tail, unix_millis()) {
            Some(before) if before > head => before,
            _ => return Ok(None),
        };

        let durability = self.keratin_cfg_event.default_durability;
        self.append_events_durable(
            tp,
            part,
            None,
            vec![StromaEvent::StreamTruncate { before }],
            durability,
        )
        .await?;
        Ok(Some(before))
    }

    /// Enforce retention across every materialized stream partition. The caller
    /// (the broker maintenance tick) drives this periodically.
    pub async fn enforce_all_stream_retention(
        &self,
    ) -> Vec<(Box<str>, u32, Result<Option<Offset>>)> {
        let mut out = Vec::new();
        for (tp, part, group) in self.list_queues() {
            if group.is_some() {
                continue;
            }
            // Only sweep partitions that are already materialized, so retention
            // never forces an idle partition back into memory. enforce_stream_
            // retention self-guards on kind, so a materialized queue is a cheap
            // no-op.
            if !self.is_materialized(&tp, part, None) {
                continue;
            }
            let res = self.enforce_stream_retention(&tp, part).await;
            if !matches!(res, Ok(None)) {
                out.push((tp, part, res));
            }
        }
        out
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
        let (owner_operation, event_log, event_msg) = {
            let h = qh.resolve()?;
            let lease = h.begin_owner_operation().await?;
            let message = self.owner_event_msg(&h, &ev)?;
            (lease, h.event_log(), message)
        };
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
        let (owner_operation, event_log, event_msg) = {
            let h = qh.resolve()?;
            let lease = h.begin_owner_operation().await?;
            let message = self.owner_event_msg(&h, &ev)?;
            (lease, h.event_log(), message)
        };
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
        let m = self.owner_event_msg(&qh, &event)?;
        let ordered_scope = qh.ordered_apply_scope();
        let ar = event_log
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
            .map_err(io_err)?;
        if ordered_scope.is_none() {
            qh.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        }
        qh.set_dirty_snapshot(true);

        let turn = crate::ordered_apply::enter(&ordered_scope, ar.base_offset, ar.count as u64).await?;
        qh.work_queue()?
            .release_inflight_many(reqs)
            .await
            .map_err(|err| StromaError::Io(err.to_string()))?;
        self.settle_metadata_record(&qh, ar.base_offset).await?;

        if ordered_scope.is_some() {
            qh.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Release);
        }
        crate::ordered_apply::finish(turn, ordered_scope)?;
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
        self.nack_enqueue_many_with_reason(tp, part, group, reqs, None, completion)
            .await
    }

    /// Like `nack_enqueue_many`, but `reason_override` replaces the dead-letter
    /// reason the in-memory transition computed. Used by the TTL drop path to
    /// label drops `Expired` while reusing the same terminal-nack/DLQ pipeline
    /// (discard when no DLQ is configured, dead-letter when one is).
    pub async fn nack_enqueue_many_with_reason(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        reqs: Vec<NackEventMeta>,
        reason_override: Option<DeadLetterReason>,
        completion: Box<dyn AppendCompletion<IoError> + Send>,
    ) -> Result<()> {
        let qh = self.queue_handle(tp, part, group).await?;
        let h = qh.resolve()?;
        let owner_operation = h.begin_owner_operation().await?;
        let event_log = h.event_log();

        // Phase 1: durable Nack write
        let nack_event = StromaEvent::NackMany { reqs: reqs.clone() };
        let m = self.owner_event_msg(&h, &nack_event)?;
        let ordered_scope = h.ordered_apply_scope();
        let ar = event_log
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
            .map_err(io_err)?;
        if ordered_scope.is_none() {
            h.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Relaxed);
        }
        h.set_dirty_snapshot(true);

        // Apply -> get outcomes
        let turn = crate::ordered_apply::enter(&ordered_scope, ar.base_offset, ar.count as u64).await?;
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
        self.settle_metadata_record(&h, ar.base_offset).await?;
        if ordered_scope.is_some() {
            h.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Release);
        }
        crate::ordered_apply::finish(turn, ordered_scope)?;
        let dl_requests: Vec<(Offset, u32, DeadLetterReason)> = outcomes
            .iter()
            .filter_map(|(o, oc)| match oc {
                NackOutcome::DeadLetterRequested {
                    retry_count,
                    reason,
                } => Some((*o, *retry_count, reason_override.unwrap_or(*reason))),
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
            let m = self.owner_event_msg(&h, &ev)?;
            let ordered_scope = h.ordered_apply_scope();
            let discard_ar = event_log
                .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
                .await
                .map_err(io_err)?;
            let turn = crate::ordered_apply::enter(&ordered_scope, discard_ar.base_offset, discard_ar.count as u64).await?;
            h.work_queue()?.discard_pending_dlq(to_discard).await?;
            self.settle_metadata_record(&h, discard_ar.base_offset).await?;
            if ordered_scope.is_some() {
                h.applied_upto().fetch_max(discard_ar.base_offset + discard_ar.count as u64 - 1, Ordering::Release);
            }
            crate::ordered_apply::finish(turn, ordered_scope)?;
        }

        // DLQ-bound: emit DeadLetter event with resolved targets.
        if !to_dlq.is_empty() {
            let ev = StromaEvent::DeadLetter {
                reqs: to_dlq.clone(),
            };
            let event_msg = self.owner_event_msg(&h, &ev)?;
            let ordered_scope = h.ordered_apply_scope();
            let dlq_ar = event_log
                .append_batch(
                    vec![event_msg],
                    Some(self.keratin_cfg_event.default_durability),
                )
                .await
                .map_err(io_err)?;
            // (Apply already done at phase-1 nack, which moved them to pending_dlq.
            //  No second apply needed, DeadLetter event is for replay durability only.)
            let turn = crate::ordered_apply::enter(&ordered_scope, dlq_ar.base_offset, dlq_ar.count as u64).await?;
            if ordered_scope.is_some() {
                // Apply the recorded marker as replay does, even though Nack
                // normally already put the offset in pending-DLQ state.
                self.apply_record_inmem(ev, dlq_ar.base_offset, &h).await?;
                h.applied_upto().fetch_max(dlq_ar.base_offset + dlq_ar.count as u64 - 1, Ordering::Release);
            }
            crate::ordered_apply::finish(turn, ordered_scope)?;

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
        let lifecycle = self.lock_partition_lifecycle(tp, part, group).await;
        self.ensure_partition_not_sealed(tp, part, group)?;
        self.ensure_checkpoint_not_pending(tp, part, group)?;
        // A topic/partition is one channel kind for life. Reject re-declaring a
        // stream partition as a queue (it would materialize a queue engine over
        // stream data). Stamp the Queue marker so the reverse case (declaring this
        // as a stream later) is just as detectable.
        if self.partition_kind_marker(tp, part, group) == Some(PartitionKind::Stream) {
            return Err(StromaError::InvalidArgument(format!(
                "{tp}/{part} is already declared as a plexus stream, cannot declare it as a queue - use a different topic name, or destroy the stream first if it is no longer needed"
            )));
        }
        self.write_partition_kind(tp, part, group, PartitionKind::Queue)?;

        drop(lifecycle);
        self.ensure_queue(tp, part, group).await?;

        let handle = self.queue_handle(tp, part, group).await?;
        let event_log = handle.resolve()?.event_log();
        event_log.record_control_event(
            keratin_log::LogControlKind::DeclareRequested,
            event_log.next_offset(),
        );
        let _upto = self
            .append_events_durable(
                tp,
                part,
                group,
                vec![StromaEvent::Declare(meta)],
                KDurability::AfterFsync,
            )
            .await?;

        event_log.record_control_event(
            keratin_log::LogControlKind::DeclareCommitted,
            event_log.next_offset(),
        );
        Ok(())
    }

    async fn resolve_dlq_targets(
        &self,
        qh: &QueueHandleInner,
        requests: &[(Offset, u32, DeadLetterReason)],
    ) -> Result<(Vec<DeadLetterMeta>, Vec<Offset>)> {
        let resolved = match qh.work_queue()?.get_dlq_target().await {
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
                        None,
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
        let m = self.owner_event_msg(qh, &ev)?;

        let ordered_scope = qh.ordered_apply_scope();
        let ar = qh
            .event_log()
            .append_batch(vec![m], Some(self.keratin_cfg_event.default_durability))
            .await
            .map_err(io_err)?;
        let turn = crate::ordered_apply::enter(&ordered_scope, ar.base_offset, ar.count as u64).await?;
        self.apply_record_inmem(StromaEvent::DeadLetterCommit { offs }, ar.base_offset, qh)
            .await?;
        if ordered_scope.is_some() {
            qh.applied_upto().fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Release);
        }
        crate::ordered_apply::finish(turn, ordered_scope)?;
        Ok(())
    }

    pub async fn fetch_message_by_offset(
        &self,
        qh: &QueueHandleInner,
        off: Offset,
    ) -> Result<Option<Message>> {
        if qh.has_combined_log() {
            return combined::fetch_payload(qh, off);
        }
        let log = qh.msg_log()?;
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
        let offs = qs
            .work_queue()?
            .poll_ready_and_mark(max, lease_deadline, upper)
            .await?;

        if offs.is_empty() {
            return Ok(Vec::new());
        }

        let qh = self.queue_handle(tp, part, group).await?;
        let closure = move || {
            let h = qh.resolve()?;
            combined::read_selected(&h, &offs)
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
        if qh.has_combined_log() { return combined::scan_payloads(qh, from, max); }
        let log = qh.msg_log()?;
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
        let snapshot: QueueInspectionSnapshot =
            qh.work_queue()?.inspect_offsets(from, limit, mode).await;
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

    pub async fn current_next_offset(&self,tp:&str,part:u32,group:Option<&str>) -> Result<Offset> {
        let q = self.queue_handle(tp,part,group).await?;
        let h = q.resolve()?;
        Ok(self.payload_log_for(&h).next_offset())
    }

    /// Optional (used by cleanup_topic): truncate message log.
    /// Drop queue log records below `before`, never past the payload floor,
    /// the applied snapshot boundary, protected followers or checkpoint pins.
    /// Refuses stream partitions.
    pub async fn truncate_queue_before(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        before: Offset,
    ) -> Result<u64> {
        let ticket = self.queue_handle(tp, part, group).await?;
        let qh = ticket.resolve()?;
        let wq = qh.work_queue()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        let snapshot = self.read_queue_snapshot(&self.snap_file(tp,part,group))?.map(|(boundary,_)|boundary.event_next()).unwrap_or(0);
        let live = wq.lowest_not_settled_offset().await;
        let floor = self.checkpoint_truncation_floor(&qh, live, snapshot).await?;
        qh.event_log().truncate_before(before.min(floor)).await.map_err(io_err)
    }

    /// Drop stream records below `before`. Refuses queue partitions.
    pub async fn truncate_stream_records_before(
        &self,
        tp: &str,
        part: u32,
        before: Offset,
    ) -> Result<u64> {
        let ticket = self.queue_handle(tp, part, None).await?;
        let qh = ticket.resolve()?;
        qh.stream()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        qh.msg_log()?.truncate_before(before).await.map_err(io_err)
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
        // This deletable-floor is an ack/settlement concept. A stream prunes its
        // message log through retention instead, so it offers no floor here.
        let Some(wq) = qh.as_work_queue() else {
            return Ok(0);
        };
        let settled_until = wq.settled_until().await;
        let min = settled_until.min(wq.lowest_not_settled_offset().await);

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

    pub async fn is_settled(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        off: Offset,
    ) -> Result<bool> {
        let q = self.queue_handle(tp, part, group).await?;
        let q = q.resolve()?;
        Ok(q.work_queue()?.is_settled(off).await)
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
        Ok(self
            .estimate_disk_used_breakdown()
            .await?
            .into_iter()
            .map(|entry| entry.message_bytes + entry.event_bytes)
            .sum())
    }

    /// Disk use per queue (partitions aggregated under topic + group), split
    /// into message-log and event-log bytes. Covers every partition on disk:
    /// loaded partitions report through their logs, unloaded ones are measured
    /// straight from their directories so an evicted queue's bytes stay
    /// visible without waking it.
    pub async fn estimate_disk_used_breakdown(&self) -> Result<Vec<DiskUsedBreakdownEntry>> {
        let loaded: HashSet<(String, u32, Option<String>)> = self
            .queue_keys_snapshot()
            .into_iter()
            .map(|(tp, part, group)| (tp.to_string(), part, group.map(|g| g.to_string())))
            .collect();

        let mut per_queue: HashMap<(String, Option<String>), (u64, u64)> = HashMap::new();
        for (group, tp, part) in self.discover_partitions()? {
            let (message, event) = if loaded.contains(&(tp.clone(), part, group.clone())) {
                let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
                let qh = qh.resolve()?;
                (
                    match qh.optional_message_log() { Some(log) => log.estimate_disk_used().await.map_err(io_err)?, None => 0 },
                    qh.event_log().estimate_disk_used().await.map_err(io_err)?,
                )
            } else {
                (
                    dir_size_bytes(&self.msg_tp_part_dir(&tp, part, group.as_deref())),
                    dir_size_bytes(&self.tp_part_dir(&tp, part, group.as_deref())),
                )
            };
            let slot = per_queue.entry((tp, group)).or_insert((0, 0));
            slot.0 += message;
            slot.1 += event;
        }
        let mut entries: Vec<DiskUsedBreakdownEntry> = per_queue
            .into_iter()
            .map(
                |((topic, group), (message_bytes, event_bytes))| DiskUsedBreakdownEntry {
                    topic,
                    group,
                    message_bytes,
                    event_bytes,
                },
            )
            .collect();
        entries.sort_by(|a, b| {
            (b.message_bytes + b.event_bytes)
                .cmp(&(a.message_bytes + a.event_bytes))
                .then_with(|| a.topic.cmp(&b.topic))
        });
        Ok(entries)
    }

    pub async fn get_queues_stats(
        &self,
    ) -> Result<HashMap<(Box<str>, Option<Box<str>>), std::result::Result<QueueStatusReport, String>>>
    {
        let mut stats: HashMap<
            (Box<str>, Option<Box<str>>),
            std::result::Result<QueueStatusReport, String>,
        > = HashMap::new();
        for (tp, part, group) in self.queue_keys_snapshot() {
            // Report per queue so one unreachable partition does not blank the whole
            // snapshot. Skip Plexus streams: they run a StreamEngine, not the
            // work-queue actor, so a status report routed to one finds no actor.
            // Streams report under the stream stats path. A queue spans partitions
            // under one (tp, group), so prefer a successful report over an error: a
            // single dead partition must not mask healthy ones.
            let outcome: std::result::Result<Option<QueueStatusReport>, StromaError> = async {
                let qh = self.queue_handle(&tp, part, group.as_deref()).await?;
                let qh = qh.resolve()?;
                let Some(wq) = qh.as_work_queue() else {
                    return Ok(None);
                };
                Ok(Some(wq.status_report().await.map_err(io_err)?))
            }
            .await;

            let report = match outcome {
                Ok(None) => continue, // not a work queue
                Ok(Some(report)) => Ok(report),
                Err(err) => Err(err.to_string()),
            };

            let key = (tp, group);
            // Keep a successful report once we have one. Otherwise record the latest,
            // so an all-dead queue still surfaces its error.
            if matches!(stats.get(&key), Some(Ok(_))) {
                continue;
            }
            stats.insert(key, report);
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
        let ticket = self.queue_handle(tp, part, group).await?;
        let qh = ticket.resolve()?;
        if qh.has_ordered_apply() {
            return self.write_exact_queue_checkpoint(ticket.clone(), false).await;
        }
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        let upto = qh.applied_upto().load(Ordering::Acquire);
        self.write_snapshots_for_partition(tp, part, group, upto).await
    }

    pub async fn truncate_partition_log(
        &self,
        qh: QueueHandle,
        before_event: Offset,
    ) -> Result<u64> {
        let qh = qh.resolve()?;
        let _apply = qh.follower_apply_state().await;
        qh.ensure_not_recovery_sealed()?;
        let tp = qh.topic();
        let part = qh.partition();
        let group = qh.group();
        if qh.has_combined_log() {
            let snapshot = self.read_queue_snapshot(&self.snap_file(tp,part,group))?.map(|(boundary,_)|boundary.event_next()).unwrap_or(0);
            let live = qh.work_queue()?.lowest_not_settled_offset().await;
            let floor = self.checkpoint_truncation_floor(&qh, live, snapshot).await?;
            return qh.event_log().truncate_before(before_event.min(floor)).await.map_err(io_err);
        }
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
        let msg_log = qh.msg_log()?;
        // let safe_msg_truncate = self.safe_truncate_before(tp, part, group).await?;
        // A stream has no ack frontier and prunes via retention, so it offers no
        // message-log floor here (0 truncates nothing).
        let safe_msg_truncate = match qh.as_work_queue() {
            Some(wq) => wq.lowest_not_settled_offset().await,
            None => 0,
        };
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
        Ok(format!("{:#?}", q.work_queue()?.canonical().await))
    }

    pub async fn validate(&self) -> Result<()> {
        let keys = self.queue_keys_snapshot();
        for (k_tp, k_part, k_group) in keys {
            let qh = self.queue_handle(&k_tp, k_part, k_group.as_deref()).await?;
            let qh = qh.resolve()?;
            // The invariants below are ack-lifecycle properties of a work queue.
            // Streams have no inflight or ack frontier, so they are skipped.
            let Some(wq) = qh.as_work_queue() else {
                continue;
            };
            for (off, _) in wq.dump_inflight().await {
                if off < wq.settled_until().await {
                    return Err(StromaError::Decode("inflight < ack frontier".into()));
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn replicated_append_outcome_allows_state_apply(
    outcome: &ReplicatedAppendOutcome,
) -> bool {
    matches!(
        outcome,
        ReplicatedAppendOutcome::Applied(_)
            | ReplicatedAppendOutcome::AppliedSuffix { .. }
            | ReplicatedAppendOutcome::AlreadyPresent { .. }
    )
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

/// Compute the segment-granular truncation boundary for a stream from its
/// retention policy. Returns the new logical head (the offset everything below
/// which may be dropped), or `None` when no whole sealed segment qualifies.
///
/// Truncation is segment-granular (only whole sealed segments are dropped, never
/// the active one), so the boundary is always a sealed segment's exclusive end.
/// Each enabled axis independently selects how many of the oldest sealed segments
/// to drop; the result is the largest prefix any axis wants:
/// - max_records: drop sealed segments whose records all sit below `tail - N`.
/// - max_bytes: drop oldest sealed segments while the bytes kept from a segment
///   onward (including the active segment) still exceed the cap.
/// - max_age: drop a sealed segment when the next segment was created at or before
///   `now - max_age`, so every record in it predates the age cutoff.
fn compute_stream_truncate_before(
    infos: &[SegmentInfo],
    config: RetentionConfig,
    tail: Offset,
    now_ms: u64,
) -> Option<Offset> {
    let sealed: Vec<&SegmentInfo> = infos.iter().filter(|s| s.sealed).collect();
    if sealed.is_empty() {
        return None;
    }
    let total_bytes: u64 = infos.iter().map(|s| s.bytes).sum();

    let mut before: Offset = 0;

    if let Some(max_records) = config.max_records {
        let keep_from = tail.saturating_sub(max_records);
        for s in &sealed {
            if s.end_offset <= keep_from {
                before = before.max(s.end_offset);
            }
        }
    }

    if let Some(max_bytes) = config.max_bytes {
        let mut dropped_bytes = 0u64;
        for s in &sealed {
            let kept_from_here = total_bytes - dropped_bytes;
            if kept_from_here > max_bytes {
                before = before.max(s.end_offset);
                dropped_bytes += s.bytes;
            } else {
                break;
            }
        }
    }

    if let Some(max_age_ms) = config.max_age_ms {
        let cutoff = now_ms.saturating_sub(max_age_ms);
        for (idx, s) in sealed.iter().enumerate() {
            // The records in `s` all predate the next segment's creation, so `s`
            // is fully older than the cutoff when that next creation is.
            let next_created = if idx + 1 < sealed.len() {
                sealed[idx + 1].created_ts_ms
            } else {
                // The active segment is the next one after the last sealed.
                infos
                    .last()
                    .map(|a| a.created_ts_ms)
                    .unwrap_or(s.created_ts_ms)
            };
            if next_created <= cutoff {
                before = before.max(s.end_offset);
            }
        }
    }

    if before == 0 { None } else { Some(before) }
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
#[derive(Debug)]
struct PublishApplyPause {
    topic: &'static str,
    base: Offset,
    entered: Notify,
    release: Notify,
}

#[cfg(test)]
#[path = "stroma/event_order_tests.rs"]
mod event_order_tests;

#[cfg(all(test, feature = "early-replication-experiment"))]
#[path = "early_replication_tests.rs"]
mod early_replication_tests;

#[cfg(test)]
mod tests {
    use keratin_log::{KeratinReplicaExt, test_dir};

    use super::*;
    use crate::state::QueueInternalState;

    fn test_history_binding() -> StorageHistoryBinding {
        StorageHistoryBinding { resource_incarnation: [1; 16], accepted_history: [2; 16], writer_session: [3; 16] }
    }
    async fn test_history(st: &Stroma, topic: &str) -> PreparedStorageHistory {
        st.initialize_empty_storage_history(topic, 0, None, PartitionKind::Queue, test_history_binding()).await.unwrap();
        st.durable_storage_history_receipt(topic, 0, None).unwrap().unwrap()
    }
    async fn test_learner(st: &Stroma, topic: &str) -> PreparedStorageHistory {
        st.prepare_queue_learner_storage(topic, 0, None, test_history_binding(), [4; 32]).await.unwrap()
    }
    fn test_payload(bytes: &[u8]) -> StromaEvent {
        StromaEvent::PayloadEnqueue { not_before: None, expire_at: None, headers: MessageHeaders { published: 0, publish_received: 0, content_type: None, extra: Default::default() }.encode().unwrap(), payload: bytes.to_vec() }
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn later_durable_batch_cannot_overtake_paused_earlier_apply() {
        let dir = test_dir!("publish_apply_order");
        let st = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let pause = Arc::new(PublishApplyPause {
            topic: "ordered",
            base: 0,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *st.publish_apply_pause.lock().unwrap() = Some(pause.clone());
        let mut confirms = Vec::new();
        for id in 0..2 {
            let (completion, rx) = KeratinAppendCompletion::pair();
            st.append_message_batch(
                "ordered",
                0,
                None,
                vec![PublishItem {
                    headers: MessageHeaders {
                        published: Default::default(),
                        publish_received: Default::default(),
                        content_type: None,
                        extra: Default::default(),
                    },
                    payload: vec![id],
                    completion,
                    not_before: None,
                    expire_at: None,
                }],
            )
            .await
            .unwrap();
            confirms.push(rx);
            if id == 0 {
                tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
                    .await
                    .unwrap();
            }
        }
        // The writer can finish batch 1 while application of batch 0 is paused.
        let queue = st.queue_handle("ordered", 0, None).await.unwrap();
        let handle = queue.resolve().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while handle.event_log().durable_offset() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut second = confirms.pop().unwrap();
        let overtook = tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .is_ok();
        let visible = st.is_ready("ordered", 0, None, 1).await.unwrap();
        pause.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), confirms.pop().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if !overtook {
            tokio::time::timeout(Duration::from_secs(5), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        st.shutdown().await.unwrap();
        assert!(
            !overtook && !visible,
            "later batch overtook paused earlier apply: confirmed={overtook}, visible={visible}"
        );
    }

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


    async fn checkpoint_backfill_case(export: bool) {
        let dir = test_dir!("single_log_checkpoint_backfill");
        let owner = Stroma::open(&dir.root.join("owner"), test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        let follower_root = dir.root.join("follower");
        let mut follower = Stroma::open(&follower_root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        publish_one(&owner, "topic", 0, None).await;
        publish_one(&owner, "topic", 0, None).await;
        let owner_ticket = owner.queue_handle("topic", 0, None).await.unwrap();
        let h = owner_ticket.resolve().unwrap();
        let generation = h.role_generation();
        let cut = owner.export_queue_state_checkpoint("topic", 0, None).await.unwrap();
        assert_eq!((cut.start_offset, cut.next_offset), (0, 2));
        let repeated = owner.export_queue_state_checkpoint("topic", 0, None).await.unwrap();
        let mut first_state = QueueInternalState::new("topic".into(), 0);
        let mut second_state = QueueInternalState::new("topic".into(), 0);
        first_state.load_snapshot(&cut.state_snapshot).unwrap();
        second_state.load_snapshot(&repeated.state_snapshot).unwrap();
        assert_eq!(first_state.recovery_state_digest(), second_state.recovery_state_digest());
        assert_eq!(repeated.next_offset, 2);
        assert_eq!(h.role_generation(), generation);
        assert_eq!(h.role(), QueueRole::Owner);
        let install = QueueStateCheckpoint {
            epoch: cut.epoch, start_offset: 0, next_offset: 2,
            state_snapshot: if export { cut.state_snapshot } else { h.encode_snapshot(1).await.unwrap() },
        };
        let receipt = test_learner(&follower, "topic").await;
        follower.become_queue_follower("topic", 0, None).await.unwrap();
        let result = follower.install_queue_learner_checkpoint(receipt, install.clone()).await.unwrap();
        assert_eq!(result.next_offset, 0);
        assert_eq!(result.snapshot_meta.last_snapshot_event_offset, 1);
        assert_eq!(follower.promote_queue_follower_if_caught_up("topic", 0, None, 2).await.unwrap(),
            QueuePromotionOutcome::LogBehind { local_next: 0, expected_next: 2 });
        assert!(!matches!(follower.promote_queue_follower_to_local_tail("topic", 0, None, 1).await.unwrap(), QueuePromotionOutcome::Promoted { .. }));
        assert!(follower.become_queue_owner("topic", 0, None).await.is_err());
        follower.shutdown().await.unwrap();
        drop(follower);
        follower = Stroma::open(&follower_root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        assert!(follower.queue_handle("topic", 0, None).await.is_err());
        let receipt = test_learner(&follower, "topic").await;
        assert!(follower.become_queue_owner("topic", 0, None).await.is_err());
        assert!(!matches!(follower.promote_queue_follower_to_local_tail("topic", 0, None, 1).await.unwrap(), QueuePromotionOutcome::Promoted { .. }));
        let OwnerReplicationRead::Batch(batch) = owner.read_owner_queue_records("topic", 0, None, 0, 10, usize::MAX).await.unwrap() else { panic!("retained events") };
        assert_eq!(batch.records.len(), 2);
        for (offset, event) in batch.records {
            follower.apply_replicated_queue_batch("topic", 0, None, ReplicatedEventBatch {
                epoch: batch.epoch, first_offset: offset, events: vec![event], durability: Some(KDurability::AfterFsync),
            }).await.unwrap();
            if offset == 0 {
                assert_eq!(follower.promote_queue_follower_if_caught_up("topic", 0, None, 2).await.unwrap(),
                    QueuePromotionOutcome::LogBehind { local_next: 1, expected_next: 2 });
            }
        }
        follower.install_queue_learner_checkpoint(receipt, install).await.unwrap();
        assert_eq!(follower.promote_queue_follower_if_caught_up("topic", 0, None, 2).await.unwrap(),
            QueuePromotionOutcome::Promoted { next: 2 });
        let delivered = follower.poll_ready("topic", 0, None, 10, unix_millis()+30_000, u64::MAX).await.unwrap();
        assert_eq!(delivered.len(), 2);
        assert_eq!((delivered[0].0, delivered[1].0), (0, 1));
        assert_eq!(delivered[0].2, b"x");
        assert_eq!(delivered[1].2, b"x");
        owner.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runtime_policy_reaches_metadata_message_event_and_future_logs() {
        let dir = test_dir!("stroma_runtime_policy");
        let runtime = Arc::new(
            keratin_log::LogRuntimeSettings::new(keratin_log::LogRuntimeConfig {
                segment_preallocate_bytes: 0,
            })
            .unwrap(),
        );
        let st = Stroma::open_with_runtime(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
            runtime.clone(),
        )
        .await
        .unwrap();
        st.global_store().await.unwrap();
        let messages = st.msg_log_init("runtime-a", 0, None).await.unwrap();
        let events = st.event_log_init("runtime-a", 0, None).await.unwrap();
        assert_eq!(runtime.logs().len(), 3);
        runtime
            .replace(
                0,
                keratin_log::LogRuntimeConfig {
                    segment_preallocate_bytes: 4096,
                },
            )
            .unwrap();
        assert!(runtime.logs().iter().all(|log| log.applied.revision == 0));
        let future = st.msg_log_init("runtime-b", 0, None).await.unwrap();
        assert_eq!(
            runtime
                .logs()
                .iter()
                .filter(|log| log.applied.revision == 1)
                .count(),
            1
        );
        messages.shutdown().await.unwrap();
        events.shutdown().await.unwrap();
        future.shutdown().await.unwrap();
        st.shutdown().await.unwrap();
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
        let qh = qh.work_queue().unwrap();
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
    async fn owner_replication_byte_budget_keeps_contiguous_queue_prefix() {
        let dir = test_dir!("owner_replication_byte_budget");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        for _ in 0..5 {
            publish_one(&stroma, "topic", 0, None).await;
        }
        let OwnerReplicationRead::Batch(full) = stroma
            .read_owner_queue_records("topic", 0, None, 0, 100, usize::MAX)
            .await
            .unwrap()
        else {
            panic!("event batch");
        };
        assert!(full.records.len() > 2);
        let OwnerReplicationRead::Batch(events) = stroma
            .read_owner_queue_records("topic", 0, None, 0, 100, 1)
            .await
            .unwrap()
        else {
            panic!("event batch");
        };
        assert_eq!(events.records, full.records[..1]);
        assert_eq!(events.next_offset, 1);
        let OwnerReplicationRead::Batch(next) = stroma
            .read_owner_queue_records("topic", 0, None, events.next_offset, 100, 1)
            .await
            .unwrap()
        else {
            panic!("next event batch");
        };
        assert_eq!(next.records, full.records[1..2]);
    }

    #[tokio::test]
    async fn owner_replication_read_returns_queue_records() {
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

        let events = stroma
            .read_owner_queue_records("topic", 0, None, 0, 10, usize::MAX)
            .await
            .unwrap();
        let OwnerReplicationRead::Batch(events) = events else {
            panic!("expected event batch");
        };
        assert_eq!(events.epoch, 0);
        assert_eq!(events.requested_offset, 0);
        assert_eq!(events.next_offset, 1);
        assert_eq!(events.records.len(), 1);
        assert!(matches!(&events.records[0], (0, StromaEvent::PayloadEnqueue { payload, .. }) if payload == b"x"));

        shutdown_stroma("owner_replication_read_records", &stroma).await;
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

        let events = stroma
            .read_owner_queue_records("topic", 0, None, 0, 1, usize::MAX)
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
            .read_owner_queue_records("topic", 0, None, events.next_offset, 1, usize::MAX)
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
            .read_owner_queue_records("topic", 0, None, 0, 10, usize::MAX)
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
            .apply_replicated_queue_batch(
                "topic",
                0,
                None,
                ReplicatedEventBatch {
                    epoch: 0,
                    first_offset: 0,
                    events: vec![test_payload(b"payload")],
                    durability: None,
                },
            )
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
        qh.event_log().become_follower();
        qh.event_log()
            .destructive_reset_to_checkpoint(7)
            .await
            .unwrap();
        qh.event_log().become_owner();

        let read = owner_replication_gap::<StromaEvent>("event", &qh.event_log(), 5, 5, 7).unwrap();
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

        let read = owner_replication_gap::<StromaEvent>("event", &qh.event_log(), 0, 0, 7).unwrap();
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
        let err = owner_replication_gap::<StromaEvent>("event", &qh.event_log(), 0, 3, 7).unwrap_err();
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
        qh.event_log().become_follower();
        qh.event_log().destructive_reset_to_checkpoint(1).await.unwrap();
        qh.event_log().become_owner();

        let events = stroma
            .read_owner_queue_records("topic", 0, None, 0, 10, usize::MAX)
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
    async fn follower_checkpoint_rejects_either_epoch_mismatch_before_resetting_any_log() {
        let dir = test_dir!("checkpoint_epoch_pair");
        let stroma = Stroma::open(
            &dir.root,
            test_keratin_config(),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap();
        let receipt = test_history(&stroma, "topic").await;
        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let old_snapshot = qh.encode_snapshot(0).await.unwrap();
        publish_one(&stroma, "topic", 0, None).await;
        stroma
            .become_queue_follower_with_epoch("topic", 0, None, 7)
            .await
            .unwrap();
        let event_records = qh.event_log().reader().scan_from(0, 10).unwrap();
        let before_state = qh.full_debug_info().await;
        // The log is at epoch 7. An older or newer checkpoint epoch is refused
        // before anything changes.
        for epoch in [6, 8, 0] {
            let error = stroma
                .install_queue_learner_checkpoint(
                    receipt.clone(),
                    QueueStateCheckpoint {
                        epoch,
                        start_offset: 0,
                        next_offset: 1,
                        state_snapshot: old_snapshot.clone(),
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, StromaError::InvalidArgument(_)),
                "{error:?}"
            );
            assert!(error.to_string().contains("epoch"), "{error}");
            for log in [qh.event_log()] {
                assert_eq!(log.current_epoch(), 7);
                assert_eq!(log.head_offset(), 0);
                assert_eq!(log.next_offset(), 2);
            }
            for (log, before) in [
                (qh.event_log(), &event_records),
            ] {
                let after = log.reader().scan_from(0, 10).unwrap();
                assert_eq!(after.len(), before.len());
                for (a, b) in after.iter().zip(before) {
                    assert_eq!(a.offset, b.offset);
                    assert_eq!(a.payload, b.payload);
                }
            }
            let after = qh.full_debug_info().await;
            assert_eq!(after.applied_upto, before_state.applied_upto);
            assert_eq!(after.state.ready_count, before_state.state.ready_count);
            assert_eq!(after.state.settled_until, before_state.state.settled_until);
        }
        shutdown_stroma("checkpoint_epoch_pair", &stroma).await;
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

        let receipt = test_history(&stroma, "topic").await;
        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let snapshot = qh.encode_snapshot(0).await.unwrap();

        let err = stroma
            .install_queue_learner_checkpoint(
                receipt.clone(),
                QueueStateCheckpoint {
                    epoch: 0,
                    start_offset: 0,
                    next_offset: 1,
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

        let receipt = test_history(&stroma, "topic").await;
        publish_one(&stroma, "topic", 0, None).await;
        let qh = stroma.queue_handle("topic", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        let snapshot = qh.encode_snapshot(0).await.unwrap();
        stroma
            .become_queue_follower("topic", 0, None)
            .await
            .unwrap();

        let err = stroma
            .install_queue_learner_checkpoint(
                receipt.clone(),
                QueueStateCheckpoint {
                    epoch: 0,
                    start_offset: 0,
                    next_offset: 2,
                    state_snapshot: snapshot,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StromaError::InvalidArgument(_)));

        // Backfill cannot start past the applied boundary.
        let err = stroma
            .install_queue_learner_checkpoint(
                receipt.clone(),
                QueueStateCheckpoint {
                    epoch: 0,
                    start_offset: 2,
                    next_offset: 1,
                    state_snapshot: qh.encode_snapshot(0).await.unwrap(),
                },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("past its applied boundary"), "{err}");

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

        let receipt = test_history(&stroma, "topic").await;
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
            .install_queue_learner_checkpoint(
                receipt.clone(),
                QueueStateCheckpoint {
                    epoch: 0,
                    start_offset: 2,
                    next_offset: 2,
                    state_snapshot: snapshot,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StromaError::InvalidArgument(_)));
        assert!(err.to_string().contains("checkpoint payload"), "{err}");

        shutdown_stroma("follower_state_checkpoint_validates_messages", &stroma).await;
    }

    #[tokio::test]
    async fn follower_state_checkpoint_install_resets_logs_but_messages_still_need_replication() {
        checkpoint_backfill_case(false).await;
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
            .export_queue_state_checkpoint("topic", 0, None)
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
    async fn checkpoint_cancel_while_draining_resumes_owner_operations() {
        let dir = test_dir!("checkpoint_cancel_during_drain");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        let handle = stroma.queue_handle("topic", 0, None).await.unwrap();
        let h = handle.resolve().unwrap();
        let active = h.begin_owner_operation().await.unwrap();
        let mut pause = Box::pin(h.pause_owner_operations_and_wait());
        assert!(futures::poll!(&mut pause).is_pending());
        // Cancellation happens after admission was paused, before existing work
        // drains. Dropping the future must release the pause that it acquired.
        drop(pause);
        let next = tokio::time::timeout(std::time::Duration::from_millis(200), h.begin_owner_operation()).await
            .expect("cancelled checkpoint left owner operations permanently paused").unwrap();
        drop(next);
        drop(active);
        let pause = h.pause_owner_operations_and_wait().await.unwrap();
        drop(pause);
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_pause_rechecks_owner_after_drain() {
        let dir = test_dir!("checkpoint_pause_role_change");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        let handle = stroma.queue_handle("topic", 0, None).await.unwrap();
        let h = handle.resolve().unwrap();
        let active = h.begin_owner_operation().await.unwrap();
        let mut pause = Box::pin(h.pause_owner_operations_and_wait());
        assert!(futures::poll!(&mut pause).is_pending());
        h.become_follower();
        drop(active);
        assert!(pause.await.is_err(), "a demoted owner must not finish an owner capture");
        h.become_owner();
        let next = tokio::time::timeout(std::time::Duration::from_millis(200), h.begin_owner_operation()).await.unwrap().unwrap();
        drop(next);
        stroma.shutdown().await.unwrap();
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
        checkpoint_backfill_case(true).await;
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

        let OwnerReplicationRead::Batch(read) = owner
            .read_owner_queue_records("topic", 0, None, 0, 10, usize::MAX)
            .await
            .unwrap()
        else {
            panic!("expected owner queue batch");
        };

        follower
            .apply_replicated_queue_batch(
                "topic",
                0,
                None,
                ReplicatedEventBatch {
                    epoch: read.epoch,
                    first_offset: read.records[0].0,
                    events: read.records.into_iter().map(|(_, event)| event).collect(),
                    durability: None,
                },
            )
            .await
            .unwrap();

        let outcome = follower
            .promote_queue_follower_if_caught_up("topic", 0, None, 2)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            QueuePromotionOutcome::Promoted { next: 2 }
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
    async fn follower_writer_failure_blocks_application_and_promotion() {
        let dir = test_dir!("follower_writer_failure");
        let st = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        st.become_queue_follower("t", 0, None).await.unwrap();
        let ticket = st.queue_handle("t", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        h.event_log().shutdown().await.unwrap();
        assert!(st.apply_replicated_queue_batch(
            "t",
            0,
            None,
            ReplicatedEventBatch {
            epoch: 0, first_offset: 0, events: vec![test_payload(b"payload")], durability: Some(KDurability::AfterFsync),
        },
).await.is_err());
        assert_eq!(h.event_log().next_offset(), 0);
        assert_eq!(h.full_debug_info().await.state.ready_count, 0);
        assert!(*h.follower_apply_state().await);
        assert!(st.promote_queue_follower_to_local_tail("t", 0, None, 1).await.is_err());
        assert!(st.promote_queue_follower_if_caught_up("t", 0, None, 0).await.is_err());
        assert!(st.become_queue_owner("t", 0, None).await.is_err());
        assert_eq!(h.role(), QueueRole::Follower);
        st.shutdown().await.unwrap();
        drop(h);
        drop(st);
        let st = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        let ticket = st.queue_handle("t", 0, None).await.unwrap();
        assert_eq!(ticket.resolve().unwrap().full_debug_info().await.state.ready_count, 0);
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_replays_event_zero_after_an_empty_checkpoint() {
        let dir = test_dir!("empty_checkpoint_event_zero");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        let receipt = test_learner(&stroma, "t").await;
        stroma.become_queue_follower("t", 0, None).await.unwrap();
        let snapshot = QueueInternalState::new("t".into(), 0).encode_snapshot(0);
        stroma
            .install_queue_learner_checkpoint(
                receipt,
                QueueStateCheckpoint {
                    epoch: 0,
                    start_offset: 0,
                    next_offset: 0,
                    state_snapshot: snapshot,
                },
            )
            .await
            .unwrap();
        stroma
            .apply_replicated_queue_batch(
                "t",
                0,
                None,
                ReplicatedEventBatch {
                    epoch: 0,
                    first_offset: 0,
                    events: vec![test_payload(b"first")],
                    durability: None,
                },
            )
            .await
            .unwrap();
        // Keep the old empty checkpoint after the queue log commits
        // and before a newer state snapshot replaces it.
        let qh = stroma.queue_handle("t", 0, None).await.unwrap();
        qh.resolve().unwrap().set_dirty_snapshot(false);
        shutdown_stroma("empty_checkpoint_before_restart", &stroma).await;
        drop(stroma);
        let reopened = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        test_learner(&reopened, "t").await;
        let qh = reopened.queue_handle("t", 0, None).await.unwrap();
        assert_eq!(
            qh.resolve()
                .unwrap()
                .full_debug_info()
                .await
                .state
                .ready_count,
            1
        );
        shutdown_stroma("empty_checkpoint_after_restart", &reopened).await;
    }

    #[tokio::test]
    async fn follower_promotion_refuses_partial_replication() {
        let dir = test_dir!("partial_single_log_replication");
        let st = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        st.become_queue_follower("topic", 0, None).await.unwrap();
        for next in 0..2 {
            assert_eq!(st.promote_queue_follower_if_caught_up("topic", 0, None, 2).await.unwrap(),
                QueuePromotionOutcome::LogBehind { local_next: next, expected_next: 2 });
            st.apply_replicated_queue_batch(
                "topic",
                0,
                None,
                ReplicatedEventBatch {
                epoch: 0, first_offset: next, events: vec![test_payload(&[next as u8])], durability: Some(KDurability::AfterFsync),
            },
).await.unwrap();
        }
        assert_eq!(st.promote_queue_follower_if_caught_up("topic", 0, None, 2).await.unwrap(),
            QueuePromotionOutcome::Promoted { next: 2 });
        assert_eq!(st.poll_ready("topic", 0, None, 10, unix_millis()+30_000, u64::MAX).await.unwrap().len(), 2);
        st.shutdown().await.unwrap();
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
        qh.resolve()
            .unwrap()
            .recovery_complete
            .store(false, Ordering::Release);

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
        let qh = qh.work_queue().unwrap();

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
            let qh = qh.work_queue().unwrap();
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
            assert_eq!(second, 2);
            let third = publish_one(&stroma, "topic-a", 0, None).await;
            assert_eq!(third, 3);

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
            .find(|item| item.state.offset == 2)
            .expect("offset 1 should be present after recovery");
        let third = page
            .items
            .iter()
            .find(|item| item.state.offset == 3)
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
            let qh = qh.work_queue().unwrap();
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
            let qh = qh.work_queue().unwrap();
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
            let qh = qh.work_queue().unwrap();
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
    async fn poisoned_queue_event_in_a_stream_log_does_not_brick_recovery() {
        let dir = test_dir!("stream_log_poison");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        stroma.create_stream("sensors", 0, None).await.unwrap();
        let headers = MessageHeaders {
            published: unix_millis(),
            publish_received: unix_millis(),
            content_type: None,
            extra: Default::default(),
        };
        let off = stroma
            .append_stream_record("sensors", 0, &headers, b"r1".to_vec())
            .await
            .unwrap();
        stroma
            .commit_stream_cursor("sensors", 0, "reader", off)
            .await
            .unwrap();

        // Inject what an older broker could write: a queue-kind enqueue event
        // in the stream partition's log (a mis-routed publish - the append
        // guard refuses this now, but existing logs can carry it).
        {
            let qh = stroma.queue_handle("sensors", 0, None).await.unwrap();
            let h = qh.resolve().unwrap();
            let poison = StromaEvent::Enqueue {
                off: 999,
                retries: 0,
                expire_at: None,
            };
            let (completion, rx) = KeratinAppendCompletion::pair();
            h.event_log()
                .append_enqueue(event_msg(&poison).unwrap(), None, completion)
                .unwrap();
            rx.await.unwrap().unwrap();
        }
        shutdown_stroma("stream_log_poison/shutdown", &stroma).await;

        // Recovery must skip the poison instead of refusing to open, and the
        // stream's own state must be intact.
        let reopened = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .expect("a poisoned event log must not brick the engine open");
        let (_head, tail) = reopened.stream_head_tail("sensors", 0).await.unwrap();
        assert_eq!(tail, off + 1, "stream tail survives recovery");
        assert_eq!(
            reopened
                .stream_cursor("sensors", 0, "reader")
                .await
                .unwrap(),
            Some(off),
            "durable cursor survives recovery"
        );
        shutdown_stroma("stream_log_poison/reopen", &reopened).await;
    }

    #[tokio::test]
    async fn partition_tree_refuses_unknown_entries_and_sweeps_trash_at_open() {
        let dir = test_dir!("partition_tree_scan");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        stroma.materialize("orders", 0, None).await.unwrap();
        shutdown_stroma("partition_tree/shutdown", &stroma).await;
        drop(stroma);

        // A destroy that crashed after its rename into trash.
        let trashed = dir.root.join("trash").join(uuid::Uuid::now_v7().to_string());
        fs::create_dir_all(trashed.join("g-x/log")).unwrap();
        fs::write(dir.root.join("tmp/snapshot.tmp"), b"partial").unwrap();
        let reopened = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        assert!(!trashed.exists(), "open sweeps interrupted destroys");
        assert!(!dir.root.join("tmp/snapshot.tmp").exists(), "open clears scratch");
        assert_eq!(
            reopened.discover_partitions().unwrap(),
            vec![(None, "orders".to_string(), 0)]
        );
        let topic_dir = reopened
            .partition_dir("orders", 0, None)
            .parent()
            .unwrap()
            .to_path_buf();
        shutdown_stroma("partition_tree/reopen", &reopened).await;
        drop(reopened);

        // Anything Stroma would not write is refused, naming the entry.
        for stray in [
            topic_dir.join("deleted"),
            topic_dir.join("0000000000").join("unexpected"),
            topic_dir.join("0000000000").join(layout::NO_GENERATION),
            dir.root.join("partitions").join("Bad%zz"),
        ] {
            fs::create_dir_all(&stray).unwrap();
            let err = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
                .await
                .err()
                .expect("an unknown partition tree entry must refuse open");
            assert!(
                matches!(&err, StromaError::Corruption(m) if m.contains("unexpected entry")),
                "{stray:?}: {err:?}"
            );
            fs::remove_dir_all(&stray).unwrap();
        }
        let reopened = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        shutdown_stroma("partition_tree/final", &reopened).await;
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

        test_step(
            "destroy_partition/materialize",
            stroma.materialize("orders", 3, None),
        )
        .await
        .unwrap();
        assert_eq!(stroma.indexed_queue_count(), 1);

        let msg_dir = stroma.msg_tp_part_dir("orders", 3, None);
        let event_dir = stroma.tp_part_dir("orders", 3, None);
        assert!(
            !msg_dir.exists(),
            "queue materialization must not create a payload-only directory"
        );
        assert!(
            event_dir.exists(),
            "event dir should exist after materialize"
        );

        // Drop a marker into the on-disk tree so we can prove the storage is
        // actually deleted (not just unindexed) and a recreate starts fresh.
        let marker = event_dir.join("marker.proof");
        fs::write(&marker, b"present").unwrap();

        let outcome = test_step(
            "destroy_partition/destroy",
            stroma.destroy_partition("orders", 3, None),
        )
        .await
        .unwrap();
        assert_eq!(outcome, DestroyOutcome::Destroyed);
        assert_eq!(stroma.indexed_queue_count(), 0);
        assert!(
            !msg_dir.exists(),
            "message dir should be deleted after destroy"
        );
        assert!(
            !event_dir.exists(),
            "event dir should be deleted after destroy"
        );

        assert!(
            !stroma.partition_dir("orders", 3, None).exists(),
            "the whole partition moves out of the tree"
        );
        assert_eq!(
            fs::read_dir(dir.root.join("trash")).unwrap().count(),
            0,
            "the trashed partition is deleted after the move"
        );

        // A recreate (later grow reusing the index) starts from empty storage.
        test_step(
            "destroy_partition/rematerialize",
            stroma.materialize("orders", 3, None),
        )
        .await
        .unwrap();
        assert_eq!(stroma.indexed_queue_count(), 1);
        let recreated = stroma.tp_part_dir("orders", 3, None);
        assert_ne!(recreated, event_dir, "a recreated partition gets a new generation");
        assert!(
            recreated.exists(),
            "queue log directory should be recreated on materialize"
        );
        assert!(
            !marker.exists(),
            "recreated partition must not see the destroyed partition's files"
        );

        shutdown_stroma(
            "destroy_partition_removes_index_entry_and_on_disk_storage",
            &stroma,
        )
        .await;
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
            assert!(
                stroma.indexed_queue_count() <= 1,
                "round {round}: duplicate slot"
            );
        }

        shutdown_stroma(
            "destroy_and_materialize_race_never_corrupts_or_leaks",
            &stroma,
        )
        .await;
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
    async fn current_generation_switch_survives_reopen_and_a_torn_switch_keeps_the_old_one() {
        let dir = test_dir!("current_switch_reopen");
        let open = || Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default());
        let stroma = open().await.unwrap();
        stroma.materialize("q", 0, None).await.unwrap();
        let first = stroma.partition_generation("q", 0, None).unwrap();
        assert!(matches!(first, layout::Generation::Initial(_)));
        let first_log = stroma.tp_part_dir("q", 0, None);
        assert!(first_log.is_dir());
        shutdown_stroma("current_switch/first", &stroma).await;
        drop(stroma);

        let part_dir = layout::partition_dir(&dir.root, "q", 0, None);
        let second = layout::Generation::Recovery {
            plan: [3; 32],
            instance: [4; 16],
        };
        fs::create_dir(part_dir.join(second.name())).unwrap();

        // Crash after current.next was written but before its rename.
        fs::write(part_dir.join("current.next"), format!("{}\n", second.name())).unwrap();
        let reopened = open().await.unwrap();
        assert_eq!(reopened.partition_generation("q", 0, None), Some(first));
        assert_eq!(reopened.tp_part_dir("q", 0, None), first_log);
        shutdown_stroma("current_switch/torn", &reopened).await;
        drop(reopened);

        // A completed switch is what the next open serves.
        layout::write_current(&part_dir, &second).unwrap();
        let reopened = open().await.unwrap();
        assert_eq!(reopened.partition_generation("q", 0, None), Some(second));
        assert_eq!(
            reopened.tp_part_dir("q", 0, None),
            part_dir.join(second.name()).join("log")
        );
        // The previous generation stays until a retention policy removes it.
        assert!(first_log.is_dir());
        shutdown_stroma("current_switch/switched", &reopened).await;
    }

    #[tokio::test]
    async fn groups_resolve_to_separate_partition_trees_and_queues_have_no_message_log() {
        let dir = test_dir!("group_trees");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        for group in [None, Some("@none"), Some("none"), Some("default")] {
            stroma.materialize("q", 0, group).await.unwrap();
        }
        stroma.create_stream("s", 0, None).await.unwrap();
        let none = stroma.partition_dir("q", 0, None);
        assert_eq!(none, dir.root.join("partitions/@none/q/0000000000"));
        assert_eq!(stroma.partition_dir("q", 0, Some("default")), none);
        let named = stroma.partition_dir("q", 0, Some("@none"));
        assert_eq!(named, dir.root.join("partitions/%40none/q/0000000000"));
        assert_ne!(stroma.tp_part_dir("q", 0, None), stroma.tp_part_dir("q", 0, Some("@none")));
        for group in [None, Some("@none"), Some("none")] {
            assert!(stroma.tp_part_dir("q", 0, group).is_dir());
            assert!(!stroma.msg_tp_part_dir("q", 0, group).exists());
            assert_eq!(
                fs::read(stroma.partition_dir("q", 0, group).join("kind")).unwrap(),
                b"queue\n"
            );
        }
        // Streams keep a separate message log in the same partition shape.
        assert!(stroma.msg_tp_part_dir("s", 0, None).is_dir());
        assert_eq!(
            fs::read(stroma.partition_dir("s", 0, None).join("kind")).unwrap(),
            b"stream\n"
        );
        assert_eq!(
            stroma.discover_partitions().unwrap(),
            vec![
                (None, "q".to_string(), 0),
                (None, "s".to_string(), 0),
                (Some("@none".to_string()), "q".to_string(), 0),
                (Some("none".to_string()), "q".to_string(), 0),
            ]
        );
        // The kind never changes once written.
        assert!(stroma.create_stream("q", 0, None).await.is_err());
        shutdown_stroma("group_trees", &stroma).await;
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
                expire_at: None,
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
                    expire_at: None,
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
        let qh = qh.work_queue().unwrap();
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

    /// Teardown (evict/destroy) must drain in-flight owner operations before
    /// shutting the logs down, so a durable-but-not-yet-applied append is never
    /// cut off mid-flight. `unmaterialize` and `destroy_partition` share the
    /// same `quiesce_for_teardown` path, so this covers both.
    #[tokio::test]
    async fn unmaterialize_waits_for_in_flight_owner_operation_to_drain() {
        let dir = test_dir!("evict_waits_for_owner_op_drain");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();

        let ticket = stroma.queue_handle("topic-a", 0, None).await.unwrap();
        // Hold an owner-operation lease: stands in for an append in progress.
        // The lease is self-contained, so it outlives the `Resolved` temporary.
        let lease = ticket
            .resolve()
            .unwrap()
            .begin_owner_operation()
            .await
            .unwrap();

        let stroma2 = stroma.clone();
        let evict = tokio::spawn(async move { stroma2.unmaterialize("topic-a", 0, None).await });

        // Teardown must not complete while an owner operation is in flight.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !evict.is_finished(),
            "evict must wait for the in-flight owner operation to drain"
        );

        // Once the operation finishes, teardown proceeds.
        drop(lease);
        let outcome = evict.await.unwrap().unwrap();
        assert_eq!(outcome, EvictOutcome::Evicted);

        shutdown_stroma(
            "unmaterialize_waits_for_in_flight_owner_operation_to_drain",
            &stroma,
        )
        .await;
    }

    /// Impossible single-log references preserve evidence until explicit repair.
    #[tokio::test]
    async fn recovery_quarantines_forward_reference_until_explicit_repair() {
        let dir = test_dir!("single_log_forward_reference");
        let st = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        {
            let ticket = st.queue_handle("t", 0, None).await.unwrap();
            ticket.resolve().unwrap().event_log().append_batch(vec![event_msg(&StromaEvent::Enqueue { off: 0, retries: 0, expire_at: None }).unwrap()], Some(KDurability::AfterFsync)).await.unwrap();
        }
        st.unmaterialize("t", 0, None).await.unwrap();
        assert!(matches!(st.queue_handle("t", 0, None).await, Err(StromaError::RecoveryMismatch { .. })));
        assert!(st.is_quarantined("t", 0, None));
        let info = st.quarantined_partitions();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].truncate_event_offset, 0);
        assert!(info[0].reason.contains("forward payload reference"));
        assert!(st.queue_handle("healthy", 0, None).await.is_ok());
        let log = st.event_log_init("t", 0, None).await.unwrap();
        assert_eq!(log.next_offset(), 1);
        log.shutdown().await.unwrap();
        st.repair_partition("t", 0, None).await.unwrap();
        assert!(!st.is_quarantined("t", 0, None));
        assert!(st.queue_handle("t", 0, None).await.is_ok());
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_dangling_enqueue_does_not_mask_corrupt_or_missing_settlement_history() {
        for non_enqueue in [false, true] {
            let dir = test_dir!("dangling_must_not_mask_corruption");
            let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
            {
                let handle = stroma.queue_handle("t", 0, None).await.unwrap();
                let qh = handle.resolve().unwrap();
                let bad = if non_enqueue {
                    event_msg(&StromaEvent::Ack { off: 0 }).unwrap()
                } else {
                    Message { flags: 0, headers: vec![], payload: b"invalid event".to_vec() }
                };
                qh.event_log().append_batch(vec![
                    event_msg(&StromaEvent::Enqueue { off: 0, retries: 0, expire_at: None }).unwrap(), bad,
                ], Some(KDurability::AfterFsync)).await.unwrap();
            }
            stroma.unmaterialize("t", 0, None).await.unwrap();
            let snapshot = stroma.snap_file("t", 0, None);
            if snapshot.exists() { std::fs::remove_file(snapshot).unwrap(); }
            assert!(stroma.queue_handle("t", 0, None).await.is_err());
            assert!(stroma.is_quarantined("t", 0, None));
            // The original evidence is retained, including the unexplained event.
            let log = stroma.event_log_init("t", 0, None).await.unwrap();
            assert_eq!(log.head_offset(), 0);
            assert_eq!(log.next_offset(), 2);
            log.shutdown().await.unwrap();
            stroma.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn suffix_repair_preserves_durable_event_prefix_across_restarts() {
        let dir = test_dir!("suffix_repair_preserves_prefix");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default()).await.unwrap();
        {
            let handle = stroma.queue_handle("t", 0, None).await.unwrap();
            let qh = handle.resolve().unwrap();
            qh.event_log().append_batch(vec![
                event_msg(&test_payload(b"confirmed")).unwrap(),
                event_msg(&StromaEvent::Enqueue { off: 1, retries: 0, expire_at: None }).unwrap(),
            ], Some(KDurability::AfterFsync)).await.unwrap();
        }
        stroma.unmaterialize("t", 0, None).await.unwrap();
        // Fixture represents a crash before any snapshot of the durable prefix.
        let snapshot = stroma.snap_file("t", 0, None);
        if snapshot.exists() { std::fs::remove_file(&snapshot).unwrap(); }
        assert!(matches!(stroma.queue_handle("t", 0, None).await, Err(StromaError::RecoveryMismatch { .. })));
        assert_eq!(stroma.quarantined_partitions()[0].truncate_event_offset, 1);
        stroma.repair_partition("t", 0, None).await.unwrap();
        {
            let handle = stroma.queue_handle("t", 0, None).await.unwrap();
            let qh = handle.resolve().unwrap();
            assert!(qh.work_queue().unwrap().is_ready(0).await);
            assert_eq!(qh.event_log().head_offset(), 0, "repair must retain the valid prefix on disk");
            assert_eq!(qh.event_log().next_offset(), 1);
            assert_eq!(qh.event_log().reader().scan_from(0, 10).unwrap().len(), 1);
        }
        stroma.unmaterialize("t", 0, None).await.unwrap();
        if snapshot.exists() { std::fs::remove_file(&snapshot).unwrap(); }
        assert!(stroma.is_ready("t", 0, None, 0).await.unwrap());
        stroma.shutdown().await.unwrap();
    }

    /// A valid payload enqueue followed by cancellation remains replayable.
    #[tokio::test]
    async fn recovery_replays_cancellation_of_retained_payload() {
        let dir = test_dir!("recovery_fold_cancel");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();

        {
            let qh = stroma.queue_handle("t", 0, None).await.unwrap();
            let qh = qh.resolve().unwrap();
            let enqueue = test_payload(b"cancelled");
            let cancel = StromaEvent::CancelEnqueueMany { offs: vec![0] };
            qh.event_log()
                .append_batch(
                    vec![event_msg(&enqueue).unwrap(), event_msg(&cancel).unwrap()],
                    None,
                )
                .await
                .unwrap();
        }
        assert_eq!(
            stroma.unmaterialize("t", 0, None).await.unwrap(),
            EvictOutcome::Evicted
        );

        // Cancellation removes the work without truncating either record.
        let qh = stroma.queue_handle("t", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        assert!(qh.recovery_complete());
        assert!(!qh.work_queue().unwrap().is_ready(0).await);
        assert_eq!(qh.full_debug_info().await.state.ready_count, 0);
        assert!(!stroma.is_quarantined("t", 0, None));
        assert_eq!(
            qh.event_log().next_offset(),
            2,
            "the fold must not truncate the event log"
        );
        assert_eq!(
            stroma.metrics.recovery.quarantined.load(Ordering::Relaxed),
            0
        );

        shutdown_stroma("recovery_folds_cancel_against_dangling_enqueue", &stroma).await;
    }

    /// A corrupt (undecodable) event record is the genuinely mid-log failure and
    /// goes through the SAME quarantine+truncate machinery as a dangling ref.
    #[tokio::test]
    async fn recovery_quarantines_and_repairs_corrupt_event_record() {
        let dir = test_dir!("recovery_corrupt_event_record");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();

        // Inject a record whose payload is not a valid encoded StromaEvent.
        {
            let qh = stroma.queue_handle("t", 0, None).await.unwrap();
            let qh = qh.resolve().unwrap();
            let garbage = Message {
                flags: 0,
                headers: vec![],
                payload: vec![0xFF, 0xFF, 0xFF, 0xFF],
            };
            qh.event_log()
                .append_batch(vec![garbage], None)
                .await
                .unwrap();
        }
        assert_eq!(
            stroma.unmaterialize("t", 0, None).await.unwrap(),
            EvictOutcome::Evicted
        );

        let err = stroma.queue_handle("t", 0, None).await.unwrap_err();
        assert!(
            matches!(err, StromaError::RecoveryMismatch { .. }),
            "expected RecoveryMismatch (corrupt record), got {err:?}"
        );
        assert!(stroma.is_quarantined("t", 0, None));

        stroma.repair_partition("t", 0, None).await.unwrap();
        assert!(!stroma.is_quarantined("t", 0, None));

        let qh = stroma.queue_handle("t", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        assert!(qh.recovery_complete());

        shutdown_stroma(
            "recovery_quarantines_and_repairs_corrupt_event_record",
            &stroma,
        )
        .await;
    }

    /// With on_mismatch=Ignore, recovery auto-truncates the dangling suffix and
    /// continues (no quarantine), with the loss accepted by the operator.
    #[tokio::test]
    async fn recovery_ignore_policy_auto_truncates_and_continues() {
        let dir = test_dir!("recovery_dangling_ignore");
        let stroma = Stroma::open(&dir.root, test_keratin_config(), SnapshotConfig::default())
            .await
            .unwrap();
        stroma.set_recovery_mismatch_policy(RecoveryMismatchPolicy::Ignore);

        {
            let qh = stroma.queue_handle("t", 0, None).await.unwrap();
            let qh = qh.resolve().unwrap();
            let bogus = StromaEvent::Enqueue {
                off: 0,
                retries: 0,
                expire_at: None,
            };
            qh.event_log()
                .append_batch(vec![event_msg(&bogus).unwrap()], None)
                .await
                .unwrap();
        }
        assert_eq!(
            stroma.unmaterialize("t", 0, None).await.unwrap(),
            EvictOutcome::Evicted
        );

        // Ignore -> auto-repair + continue, so queue_handle succeeds and nothing
        // is quarantined.
        let qh = stroma.queue_handle("t", 0, None).await.unwrap();
        let qh = qh.resolve().unwrap();
        assert!(qh.recovery_complete());
        assert!(!stroma.is_quarantined("t", 0, None));

        shutdown_stroma(
            "recovery_ignore_policy_auto_truncates_and_continues",
            &stroma,
        )
        .await;
    }
}

#[cfg(test)]
mod retention_tests {
    use super::{SegmentInfo, compute_stream_truncate_before};
    use crate::stream_state::RetentionConfig;

    fn seg(base: u64, end: u64, bytes: u64, created_ts_ms: u64, sealed: bool) -> SegmentInfo {
        SegmentInfo {
            base_offset: base,
            end_offset: end,
            bytes,
            created_ts_ms,
            sealed,
        }
    }

    // Three sealed segments [0,10) [10,20) [20,30) plus an active [30,35), tail=35.
    fn infos() -> Vec<SegmentInfo> {
        vec![
            seg(0, 10, 1000, 1_000, true),
            seg(10, 20, 1000, 2_000, true),
            seg(20, 30, 1000, 3_000, true),
            seg(30, 35, 500, 4_000, false),
        ]
    }

    #[test]
    fn no_policy_drops_nothing() {
        assert_eq!(
            compute_stream_truncate_before(&infos(), RetentionConfig::default(), 35, 10_000),
            None
        );
    }

    #[test]
    fn max_records_drops_oldest_whole_segments() {
        // Keep newest 20 records => keep_from = 15 => drop the [0,10) segment only
        // ([10,20) ends at 20 > 15 so it stays).
        let cfg = RetentionConfig {
            max_records: Some(20),
            ..Default::default()
        };
        assert_eq!(
            compute_stream_truncate_before(&infos(), cfg, 35, 10_000),
            Some(10)
        );
    }

    #[test]
    fn max_bytes_keeps_newest_within_cap() {
        // total bytes = 3500. Cap 1500: keeping from [0,10) (3500) > cap -> drop;
        // from [10,20) (2500) > cap -> drop; from [20,30) (1500) <= cap -> stop.
        // So drop through [10,20): before = 20.
        let cfg = RetentionConfig {
            max_bytes: Some(1500),
            ..Default::default()
        };
        assert_eq!(
            compute_stream_truncate_before(&infos(), cfg, 35, 10_000),
            Some(20)
        );
    }

    #[test]
    fn max_age_drops_segments_fully_older_than_cutoff() {
        // now=10_000, max_age=7_000 => cutoff=3_000. A sealed segment goes when the
        // NEXT segment was created at/under 3_000: [0,10) next=2_000<=3_000 drop;
        // [10,20) next=3_000<=3_000 drop; [20,30) next=4_000>3_000 keep. before=20.
        let cfg = RetentionConfig {
            max_age_ms: Some(7_000),
            ..Default::default()
        };
        assert_eq!(
            compute_stream_truncate_before(&infos(), cfg, 35, 10_000),
            Some(20)
        );
    }

    #[test]
    fn axes_combine_to_the_largest_prefix() {
        // records wants drop through 10, bytes wants drop through 20 -> union = 20.
        let cfg = RetentionConfig {
            max_records: Some(25),
            max_bytes: Some(1500),
            max_age_ms: None,
        };
        assert_eq!(
            compute_stream_truncate_before(&infos(), cfg, 35, 10_000),
            Some(20)
        );
    }

    #[test]
    fn active_segment_is_never_dropped() {
        // A tiny cap that even one segment exceeds still keeps the active segment:
        // the boundary never passes the last sealed end (30).
        let cfg = RetentionConfig {
            max_bytes: Some(1),
            ..Default::default()
        };
        assert_eq!(
            compute_stream_truncate_before(&infos(), cfg, 35, 10_000),
            Some(30)
        );
    }

    #[test]
    fn nothing_to_drop_returns_none() {
        let cfg = RetentionConfig {
            max_records: Some(10_000),
            max_bytes: Some(10_000_000),
            max_age_ms: Some(10_000_000),
        };
        assert_eq!(
            compute_stream_truncate_before(&infos(), cfg, 35, 10_000),
            None
        );
    }
}


#[cfg(all(test, feature = "io-fault-injection"))]
mod open_batch_fault_tests {
    use super::*;
    use futures::StreamExt;
    use keratin_log::io_fault_injection::{arm, Fault};

    #[tokio::test]
    async fn open_batch_sync_failures_block_actor_apply_and_promotion() {
        for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
            let dir = keratin_log::test_dir!("open_batch_sync_failure");
            let st = Stroma::open(&dir.root,
                crate::StromaKeratinConfig::from_message_log(keratin_log::KeratinConfig::test_default()),
                crate::SnapshotConfig::default()).await.unwrap();
            st.become_queue_follower("q", 0, None).await.unwrap();
            let qh = st.queue_handle("q", 0, None).await.unwrap();
            let h = qh.resolve().unwrap();
            let path = st.tp_part_dir("q", 0, None).join("segments/00000000000000000000.log");
            let armed = arm(&path, fault).unwrap();
            let frame = ReplicatedEventBatch {
                epoch: 0, first_offset: 0,
                events: vec![StromaEvent::PayloadEnqueue { not_before: None, expire_at: None, headers: vec![], payload: vec![42; 1024] }],
                durability: Some(KDurability::AfterFsync),
            };
            let result = st.apply_open_replicated_queue_batch("q", 0, None, futures::stream::iter([Ok(frame)]).boxed()).await;
            assert!(result.is_err());
            assert_eq!(armed.hits(), 1);
            assert!(!h.work_queue().unwrap().is_ready(0).await);
            assert!(*h.follower_apply_state().await);
            assert!(st.become_queue_owner("q", 0, None).await.is_err());
            assert!(st.promote_queue_follower_to_local_tail("q", 0, None, 1).await.is_err());
            drop(armed);
            let _ = st.shutdown().await;
        }
    }
}
