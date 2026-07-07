use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use arc_swap::ArcSwap;

use keratin_log::Keratin;
use keratin_log::util::unix_millis;
use rangemap::{RangeMap, RangeSet};
use serde::Serialize;
use tokio::sync::{Notify, RwLock, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::StromaError;
use crate::engine::PartitionKind;
use crate::event::{
    AckEventMeta, DLQDiscardPolicyWire, DeadLetterReason, DeclareMeta, EnqueueDelayedEventMeta,
    EnqueueEventMeta, MarkInflightEventMeta, NackEventMeta,
};
use crate::metrics::{
    CommandMetricsSnapshot, LogMetricsSnapshot, RecoveryMetricsSnapshot, SnapshotMetricsSnapshot,
    StromaMetrics,
};
use crate::stream_state::{RetentionConfig, StreamCommand, StreamState, run_stream_control};
use crate::stroma::{GlobalDLQ, QueueKey, Registry, TaskGroup};

pub type Offset = u64;
pub type UnixMillis = u64;

pub const FORMAT_VERSION: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueRole {
    Owner,
    Follower,
    Frozen,
}

impl QueueRole {
    const OWNER: u8 = 0;
    const FOLLOWER: u8 = 1;
    const FROZEN: u8 = 2;

    fn from_u8(value: u8) -> Self {
        match value {
            Self::FOLLOWER => Self::Follower,
            Self::FROZEN => Self::Frozen,
            _ => Self::Owner,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Owner => Self::OWNER,
            Self::Follower => Self::FOLLOWER,
            Self::Frozen => Self::FROZEN,
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueueHandleError {
    ActorGone,
    WrongRole {
        expected: QueueRole,
        actual: QueueRole,
    },
    WrongKind {
        expected: PartitionKind,
        actual: PartitionKind,
    },
    LoadSnapshotFailed(String),
    SnapshotNotCreated,
    SnapshotLoadFailed(String),
    Internal(String),
}

#[derive(Debug)]
pub struct OwnerOperationLease {
    active: Arc<AtomicU64>,
    drained: Arc<Notify>,
}

#[derive(Debug)]
pub struct OwnerOperationPauseGuard {
    paused: Arc<AtomicBool>,
    resumed: Arc<Notify>,
}

impl OwnerOperationLease {
    pub(crate) fn clone_for_continuation(&self) -> Self {
        self.active.fetch_add(1, Ordering::AcqRel);
        Self {
            active: self.active.clone(),
            drained: self.drained.clone(),
        }
    }
}

impl Drop for OwnerOperationPauseGuard {
    fn drop(&mut self) {
        self.paused.store(false, Ordering::Release);
        self.resumed.notify_waiters();
    }
}

impl Drop for OwnerOperationLease {
    fn drop(&mut self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
}

impl std::fmt::Display for QueueHandleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueHandleError::ActorGone => write!(f, "queue actor is gone"),
            QueueHandleError::WrongRole { expected, actual } => {
                write!(
                    f,
                    "queue role mismatch: expected {expected:?}, current role is {actual:?}"
                )
            }
            QueueHandleError::WrongKind { expected, actual } => {
                write!(
                    f,
                    "partition kind mismatch: expected {expected:?}, this partition is {actual:?}"
                )
            }
            QueueHandleError::LoadSnapshotFailed(reason) => {
                write!(f, "snapshot load failed: {reason}")
            }
            QueueHandleError::SnapshotNotCreated => write!(f, "snapshot not created"),
            QueueHandleError::SnapshotLoadFailed(reason) => {
                write!(f, "snapshot load failed: {reason}")
            }
            QueueHandleError::Internal(reason) => write!(f, "internal queue error: {reason}"),
        }
    }
}

impl From<QueueHandleError> for StromaError {
    fn from(value: QueueHandleError) -> Self {
        match value {
            QueueHandleError::ActorGone => StromaError::QueueActorGone,
            QueueHandleError::WrongRole { expected, actual } => {
                StromaError::WrongQueueRole { expected, actual }
            }
            QueueHandleError::WrongKind { expected, actual } => {
                StromaError::WrongPartitionKind { expected, actual }
            }
            QueueHandleError::LoadSnapshotFailed(reason) => StromaError::Internal(reason),
            QueueHandleError::SnapshotNotCreated => {
                StromaError::Internal("snapshot not created".to_string())
            }
            QueueHandleError::SnapshotLoadFailed(reason) => StromaError::Internal(reason),
            QueueHandleError::Internal(reason) => StromaError::Internal(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum NackOutcome {
    /// already settled / not in lifecycle
    NoOp,
    /// back to ready, retries++
    Requeued,
    /// back to ready with delay, retries++
    RequeuedLater { not_before: UnixMillis },
    /// retries exhausted OR requeue=false
    DeadLetterRequested {
        retry_count: u32,
        reason: DeadLetterReason,
    },
}

#[derive(Debug, Clone)]
pub struct NackBatchOutcome {
    pub dead_letter_offsets: Vec<Offset>,
}

#[derive(Debug, Clone)]
pub struct DLQDiscardSettings {
    pub max_retries: u32,
}

impl Default for DLQDiscardSettings {
    fn default() -> Self {
        Self { max_retries: 5 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CustomDLQ {
    pub tp: String,
    pub part: u32,
    pub group: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DLQDiscardPolicy {
    #[default]
    Discard,
    GlobalDQL,
    CustomDQL(CustomDLQ), // tp, part
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDlqTarget {
    pub tp: String,
    pub part: u32,
    pub group: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectMode {
    ActiveOnly,
    IncludeSettled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueInspectionSnapshot {
    pub next_offset_hint: Offset,
    pub items: Vec<QueueInspectionState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueInspectionState {
    pub offset: Offset,
    pub status: MessageInspectionStatus,
    pub retry_count: u32,
    pub inflight_deadline_ms: Option<UnixMillis>,
    pub available_at_ms: Option<UnixMillis>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageInspectionStatus {
    Ready,
    Inflight,
    Delayed,
    PendingDlq,
    Settled,
}

/// QueueState invariants and transitions:
///
/// States (per offset):
/// - Absent        : no enqueue, no inflight, not acked
/// - Ready         : enqueued, eligible for delivery
/// - Inflight      : leased to a consumer
/// - Acked         : terminal
///
/// Terminal rule:
/// - ACK is final. Once acked, an offset can never re-enter Ready or Inflight.
///
/// Transitions:
/// - enqueue:
///   Absent -> Ready
///   Acked  -> (ignored)
///
/// - mark_inflight:
///   Ready     -> Inflight
///   Inflight  -> Inflight (deadline update)
///   others    -> (ignored)
///
/// - collect_expired:
///   Inflight -> Ready
///
/// - nack(requeue=true):
///   Inflight -> Ready (retry++)
///   others   -> (ignored)
///
/// - nack(requeue=false) / reject:
///   Inflight -> Acked
///
/// - ack:
///   Ready    -> Acked
///   Inflight -> Acked
///   others   -> (idempotent)
///
/// Safety invariants:
/// - An offset is never in Ready and Inflight at the same time
/// - Inflight offsets are always >= settled_until
/// - settled_until is monotonic
///
/// QueueState semantics:
///
/// Offsets move through a strict state machine:
///
///   Enqueue -> Ready -> Inflight -> (Ack | Nack | Expiry)
///
/// Rules:
/// - An offset exists iff it is Enqueued or has delivery history.
/// - READY is authoritative for delivery eligibility.
/// - MARK_INFLIGHT requires READY.
/// - ACK / REJECT / DLQ are terminal and advance the frontier.
/// - NACK never creates offsets; it only transforms existing state.
/// - EXPIRY requeues inflight offsets but never ACKs.
/// - An offset may exist in at most one of {ready, inflight, acked}.
/// - The ACK frontier is contiguous and monotonic.
///
/// All operations are idempotent and replay-safe.
///
/// Important invariants:
/// - NACK never creates offsets; it only transforms existing state.
/// - READY is the sole authority for delivery eligibility.
/// - ACK / REJECT / DLQ are terminal.
#[derive(Debug, Clone)]
pub struct QueueInternalState {
    topic: String,
    partition: u32,

    last_snapshot_timestamp: u64,
    last_snapshot_event_offset: u64,

    // ----- settlement state -----
    // Terminal settlements (ack, terminal nack, DLQ commit) as offset ranges,
    // stored from 0. The contiguous run covering offset 0 is the frontier
    // (`settled_until`, derived); out-of-order settlements live as separate ranges
    // above it and coalesce into the frontier as it advances. Mirrors `ready`.
    settled: RangeSet<Offset>,

    // ----- inflight -----
    // offset -> deadline_ts
    inflight: BTreeMap<Offset, UnixMillis>,

    // awaiting DLQ-copy + commit
    pending_dlq: BTreeMap<Offset, Option<ResolvedDlqTarget>>,

    // ----- Ready -----
    // offset -> retries
    ready: RangeSet<Offset>,       // readiness only
    retries: HashMap<Offset, u32>, // retry metadata only

    // Per-message drop deadline (message TTL), keyed by offset. Offset-keyed (not
    // deadline-keyed) so it persists across ready->inflight->ready and is removed
    // only on terminal settle. Contiguous same-deadline spans collapse into one
    // entry, so a uniform/queue-default TTL stays compact even for a large
    // backlog. Consulted reactively (skip-and-drop at delivery) and proactively
    // (the expiry worker drops expired ready offsets).
    ttl_deadlines: RangeMap<Offset, UnixMillis>,

    // min-heap via Reverse(deadline), contains stale entries, validated against inflight map
    expiry_heap: BinaryHeap<(Reverse<UnixMillis>, Offset)>,

    // min-heap via Reverse(deadline), serving delayed message publishing
    delayed_enqueue_heap: BinaryHeap<(Reverse<UnixMillis>, Offset)>,

    // min-heap via Reverse(deadline), serving delayed message retries
    delayed_retry_heap: BinaryHeap<(Reverse<UnixMillis>, Offset)>,

    // best-effort hint
    min_deadline_hint: Option<UnixMillis>,

    // what to do on DLQ
    dlq_policy: DLQDiscardPolicy,

    // when to send to DLQ
    dlq_discard_max_retries: u32,

    // per-queue default message TTL (ms); applied at publish when a message
    // carries no explicit deadline. None = no default.
    default_message_ttl_ms: Option<u64>,

    deadline_waker: Arc<Notify>,
}

// Every QueueState method will be processed by a relevant Command sequentially on a single task, so we don't need to worry about concurrent mutations or complex locking.
#[derive(Debug)]
pub enum QueueCommand {
    Shutdown {
        response: Option<oneshot::Sender<()>>,
    },
    Enqueue {
        offset: Offset,
        retries: u32,
        expire_at: Option<UnixMillis>,
        response: Option<oneshot::Sender<()>>,
    }, // offset, retries, drop deadline
    EnqueueMany {
        reqs: Vec<EnqueueEventMeta>,
        response: Option<oneshot::Sender<()>>,
    }, // list[offset, retries]
    CancelEnqueueMany {
        offs: Vec<Offset>,
        response: Option<oneshot::Sender<()>>,
    }, // annihilate enqueues whose payload never became durable
    EnqueueDelayed {
        offset: Offset,
        not_before: UnixMillis,
        response: Option<oneshot::Sender<()>>,
    }, // offset, not_before
    EnqueueDelayedMany {
        reqs: Vec<EnqueueDelayedEventMeta>,
        response: Option<oneshot::Sender<()>>,
    }, // list[offset, not_before]
    MarkInflight {
        offset: Offset,
        deadline: UnixMillis,
        response: Option<oneshot::Sender<()>>,
    }, // offset, deadline
    MarkInflightMany {
        reqs: Vec<MarkInflightEventMeta>,
        response: Option<oneshot::Sender<()>>,
    }, // entries
    Ack {
        offset: Offset,
        response: Option<oneshot::Sender<()>>,
    }, // offset
    AckMany {
        reqs: Vec<AckEventMeta>,
        response: Option<oneshot::Sender<()>>,
    }, // list[offset]
    ReleaseInflightMany {
        reqs: Vec<AckEventMeta>,
        response: Option<oneshot::Sender<()>>,
    },
    Nack {
        offset: Offset,
        requeue: bool,
        not_before: Option<UnixMillis>,
        response: Option<oneshot::Sender<NackOutcome>>,
    }, // offset, requeue, optional retry deadline
    NackMany {
        reqs: Vec<NackEventMeta>,
        response: Option<oneshot::Sender<Vec<(Offset, NackOutcome)>>>,
    }, // offset, requeue?
    DeadLetterCommit {
        offsets: Vec<Offset>,
        response: Option<oneshot::Sender<()>>,
    },
    MarkPendingDlq {
        offsets: Vec<Offset>,
        response: Option<oneshot::Sender<()>>,
    },
    DiscardPendingDlq {
        offsets: Vec<Offset>,
        response: Option<oneshot::Sender<()>>,
    },
    Declare {
        meta: DeclareMeta,
        response: Option<oneshot::Sender<()>>,
    },
    GetPendingDlq {
        response: Option<oneshot::Sender<Vec<(Offset, Option<ResolvedDlqTarget>)>>>,
    },
    InspectOffsets {
        from: Offset,
        limit: usize,
        mode: InspectMode,
        response: Option<oneshot::Sender<QueueInspectionSnapshot>>,
    },
    GetDlqTarget {
        global: Option<GlobalDLQ>,
        response: Option<oneshot::Sender<Option<(String, u32, Option<String>)>>>,
    },
    Reset {
        response: Option<oneshot::Sender<()>>,
    },
    EncodeSnapshot {
        last_snapshot_event_offset: u64,
        force: bool,
        response: Option<oneshot::Sender<Option<Vec<u8>>>>,
    },
    ExportStateCheckpoint {
        last_snapshot_event_offset: u64,
        response: Option<oneshot::Sender<QueueStateCheckpointSnapshot>>,
    },
    LoadSnapshot {
        data: Vec<u8>,
        response: Option<oneshot::Sender<std::io::Result<SnapshotMeta>>>,
    }, // data
    InstallSnapshotState {
        state: QueueInternalState,
        meta: SnapshotMeta,
        response: Option<oneshot::Sender<SnapshotMeta>>,
    },

    IsSettled {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    IsInflight {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    IsInflightOrSettled {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    IsReady {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    GetDebugInfo {
        response: Option<oneshot::Sender<QueueInternalDebugInfo>>,
    }, // debug info
    GetRetries {
        offset: Offset,
        response: Option<oneshot::Sender<u32>>,
    }, // offset
    GetSettledUntil {
        response: Option<oneshot::Sender<Offset>>,
    },
    PollReadyAndMark {
        max: usize,
        lease_deadline: UnixMillis,
        upper: Offset,
        response: Option<oneshot::Sender<Vec<(Offset, u32)>>>,
    }, // max, lease_deadline, upper (exclusive deliverable ceiling)
    GetLowestUnsettled {
        response: Option<oneshot::Sender<Offset>>,
    },
    GetLowestNotSettled {
        response: Option<oneshot::Sender<Offset>>,
    },
    GetNextDeliverable {
        from: Offset,
        upper: Offset,
        response: Option<oneshot::Sender<Option<Offset>>>,
    }, // from, upper
    GetInflightLen {
        response: Option<oneshot::Sender<usize>>,
    },
    GetNextExpiryHint {
        response: Option<oneshot::Sender<Option<UnixMillis>>>,
    },
    GetCanonicalQueueState {
        response: Option<oneshot::Sender<CanonicalQueueState>>,
    },
    GetStatusReport {
        response: Option<oneshot::Sender<QueueStatusReport>>,
    },
    CollectExpired {
        now: UnixMillis,
        max: usize,
        response: Option<oneshot::Sender<Vec<Offset>>>,
    }, // now, max

    CollectTtlExpired {
        now: UnixMillis,
        max: usize,
        response: Option<oneshot::Sender<Vec<Offset>>>,
    }, // now, max - ready offsets past their message-TTL deadline

    DumpInflight {
        response: Option<oneshot::Sender<Vec<(Offset, UnixMillis)>>>,
    },
}

#[derive(Debug)]
pub struct QueueStateCheckpointSnapshot {
    pub message_checkpoint_offset: Offset,
    pub state_snapshot: Vec<u8>,
}

impl QueueCommand {
    pub fn prio(&self) -> CommandPrio {
        match self {
            // === Lifecycle / control — must preempt everything ===

            // === Recovery / loading — one-shot at startup, not in contention ===
            // Put at Express so they can't be blocked if something else is in the queue
            QueueCommand::LoadSnapshot { .. } => CommandPrio::Express,
            QueueCommand::InstallSnapshotState { .. } => CommandPrio::Express,
            QueueCommand::Reset { .. } => CommandPrio::Express,
            QueueCommand::GetDlqTarget { .. } => CommandPrio::Express,

            // === Observability / admin queries — fast, cheap, must stay responsive ===
            QueueCommand::GetDebugInfo { .. } => CommandPrio::Express,
            QueueCommand::GetStatusReport { .. } => CommandPrio::Express,
            QueueCommand::InspectOffsets { .. } => CommandPrio::Express,
            QueueCommand::GetInflightLen { .. } => CommandPrio::Express,
            QueueCommand::GetSettledUntil { .. } => CommandPrio::Express,
            QueueCommand::GetLowestUnsettled { .. } => CommandPrio::Express,
            QueueCommand::GetLowestNotSettled { .. } => CommandPrio::Express,
            QueueCommand::GetCanonicalQueueState { .. } => CommandPrio::Express,
            QueueCommand::DumpInflight { .. } => CommandPrio::Express,
            QueueCommand::GetRetries { .. } => CommandPrio::Express,

            // === Point-reads used in hot paths — cheap but not observability-critical ===
            QueueCommand::IsSettled { .. } => CommandPrio::High,
            QueueCommand::IsInflight { .. } => CommandPrio::High,
            QueueCommand::IsInflightOrSettled { .. } => CommandPrio::High,
            QueueCommand::IsReady { .. } => CommandPrio::High,
            QueueCommand::GetNextDeliverable { .. } => CommandPrio::High,
            QueueCommand::GetNextExpiryHint { .. } => CommandPrio::High,

            // === Delivery path — the consumer-facing hot path ===
            // Higher than publish so consumers drain the queue under load
            QueueCommand::PollReadyAndMark { .. } => CommandPrio::High,
            QueueCommand::MarkInflight { .. } => CommandPrio::High,
            QueueCommand::MarkInflightMany { .. } => CommandPrio::High,

            // === Settlement — finishes in-progress work, matches delivery priority ===
            // Ack/nack completing work frees up consumer prefetch slots; if this is
            // low priority, consumers stall waiting for acks to register
            QueueCommand::Ack { .. } => CommandPrio::High,
            QueueCommand::AckMany { .. } => CommandPrio::High,
            QueueCommand::ReleaseInflightMany { .. } => CommandPrio::High,
            QueueCommand::Nack { .. } => CommandPrio::High,
            QueueCommand::NackMany { .. } => CommandPrio::High,
            QueueCommand::DeadLetterCommit { .. } => CommandPrio::High,
            QueueCommand::MarkPendingDlq { .. } => CommandPrio::High,
            QueueCommand::DiscardPendingDlq { .. } => CommandPrio::High,
            QueueCommand::Declare { .. } => CommandPrio::High,
            QueueCommand::GetPendingDlq { .. } => CommandPrio::High,

            // === Producer path — must accept writes but yield to delivery/settlement ===
            // Under overload, throttling publish is correct. Natural backpressure upstream.
            QueueCommand::Enqueue { .. } => CommandPrio::Medium,
            QueueCommand::EnqueueMany { .. } => CommandPrio::Medium,
            // Same priority as EnqueueMany so a cancel never overtakes the enqueue
            // it annihilates (Medium is FIFO, and the cancel is always enqueued
            // after its enqueue).
            QueueCommand::CancelEnqueueMany { .. } => CommandPrio::Medium,
            QueueCommand::EnqueueDelayed { .. } => CommandPrio::Medium,
            QueueCommand::EnqueueDelayedMany { .. } => CommandPrio::Medium,

            // === Background maintenance — wait for quiet periods ===
            QueueCommand::CollectExpired { .. } => CommandPrio::Low,
            QueueCommand::CollectTtlExpired { .. } => CommandPrio::Low,

            // === Snapshots — lowest priority, run only when other work is drained ===
            // This assumes snapshot encoding can tolerate being delayed under load.
            // If you need snapshots to run on schedule regardless of load, raise this.
            QueueCommand::EncodeSnapshot { .. } => CommandPrio::SuperLow,
            QueueCommand::ExportStateCheckpoint { .. } => CommandPrio::SuperLow,
            // SuperLow: shutdown drains all queued commands before exiting. Each queued
            // command may have a oneshot response sender that callers are awaiting, if
            // shutdown jumped ahead (Express), those callers would see their rx future
            // return Err and panic on the unwrap.
            //
            // The in memory state mutations performed during drain are wasted work, the
            // caller (or evictor) is about to discard the state, but the *responses*
            // matter. Draining is the cheapest way to honor the protocol.
            //
            // TODO: a separate ShutdownNow variant (Express) for cases where we accept
            // caller visible errors in exchange for fast exit (e.g., process shutdown
            // with hard deadline).
            QueueCommand::Shutdown { .. } => CommandPrio::SuperLow,
        }
    }

    pub fn variant_name(&self) -> &str {
        match self {
            QueueCommand::Shutdown { .. } => "Shutdown",
            QueueCommand::Enqueue { .. } => "Enqueue",
            QueueCommand::EnqueueMany { .. } => "EnqueueMany",
            QueueCommand::CancelEnqueueMany { .. } => "CancelEnqueueMany",
            QueueCommand::MarkInflight { .. } => "MarkInflight",
            QueueCommand::MarkInflightMany { .. } => "MarkInflightMany",
            QueueCommand::Ack { .. } => "Ack",
            QueueCommand::AckMany { .. } => "AckMany",
            QueueCommand::ReleaseInflightMany { .. } => "ReleaseInflightMany",
            QueueCommand::Nack { .. } => "Nack",
            QueueCommand::NackMany { .. } => "NackMany",
            QueueCommand::Reset { .. } => "Reset",
            QueueCommand::EncodeSnapshot { .. } => "EncodeSnapshot",
            QueueCommand::ExportStateCheckpoint { .. } => "ExportStateCheckpoint",
            QueueCommand::LoadSnapshot { .. } => "LoadSnapshot",
            QueueCommand::InstallSnapshotState { .. } => "InstallSnapshotState",
            QueueCommand::IsSettled { .. } => "IsSettled",
            QueueCommand::IsInflight { .. } => "IsInflight",
            QueueCommand::IsInflightOrSettled { .. } => "IsInflightOrSettled",
            QueueCommand::IsReady { .. } => "IsReady",
            QueueCommand::GetRetries { .. } => "GetRetries",
            QueueCommand::GetSettledUntil { .. } => "GetSettledUntil",
            QueueCommand::PollReadyAndMark { .. } => "PollReadyAndMark",
            QueueCommand::GetLowestUnsettled { .. } => "GetLowestUnsettled",
            QueueCommand::GetLowestNotSettled { .. } => "GetLowestNotSettled",
            QueueCommand::GetNextDeliverable { .. } => "GetNextDeliverable",
            QueueCommand::GetInflightLen { .. } => "GetInflightLen",
            QueueCommand::GetNextExpiryHint { .. } => "GetNextExpiryHint",
            QueueCommand::GetCanonicalQueueState { .. } => "GetCanonicalQueueState",
            QueueCommand::GetStatusReport { .. } => "GetStatusReport",
            QueueCommand::InspectOffsets { .. } => "InspectOffsets",
            QueueCommand::CollectExpired { .. } => "CollectExpired",
            QueueCommand::CollectTtlExpired { .. } => "CollectTtlExpired",
            QueueCommand::DumpInflight { .. } => "DumpInflight",
            QueueCommand::GetDebugInfo { .. } => "GetDebugInfo",
            QueueCommand::DeadLetterCommit { .. } => "DeadLetterCommit",
            QueueCommand::MarkPendingDlq { .. } => "MarkPendingDlq",
            QueueCommand::DiscardPendingDlq { .. } => "DiscardPendingDlq",
            QueueCommand::Declare { .. } => "Declare",
            QueueCommand::GetPendingDlq { .. } => "GetPendingDlq",
            QueueCommand::GetDlqTarget { .. } => "GetDlqTarget",
            QueueCommand::EnqueueDelayed { .. } => "EnqueueDelayed",
            QueueCommand::EnqueueDelayedMany { .. } => "EnqueueDelayedMany",
        }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Copy, Clone)]
pub enum CommandPrio {
    Express,
    High,
    Medium,
    Low,
    SuperLow,
}

impl CommandPrio {
    pub fn all() -> [Self; 5] {
        use CommandPrio::*;
        [SuperLow, Low, Medium, High, Express]
    }

    pub fn name(&self) -> &'static str {
        match self {
            CommandPrio::Express => "express",
            CommandPrio::High => "high",
            CommandPrio::Medium => "medium",
            CommandPrio::Low => "low",
            CommandPrio::SuperLow => "super_low",
        }
    }

    pub fn idx(&self) -> usize {
        match self {
            CommandPrio::Express => 0,
            CommandPrio::High => 1,
            CommandPrio::Medium => 2,
            CommandPrio::Low => 3,
            CommandPrio::SuperLow => 4,
        }
    }
}

#[derive(Debug)]
pub struct QueueCommandPackage {
    pub command: QueueCommand,
    pub enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub last_snapshot_timestamp: u64,
    pub last_snapshot_event_offset: u64,
    /// Per-queue default message TTL (ms) restored from the snapshot, so the
    /// handle-side cache can be repopulated on recovery from a snapshot that
    /// already compacted the original Declare event. None = no default.
    pub default_message_ttl_ms: Option<u64>,
}

#[derive(Debug)]
pub struct CommandReceiver {
    metrics: Arc<StromaMetrics>,
    express: mpsc::Receiver<QueueCommandPackage>,
    high_prio: mpsc::Receiver<QueueCommandPackage>,
    medium_prio: mpsc::Receiver<QueueCommandPackage>,
    low_prio: mpsc::Receiver<QueueCommandPackage>,
    super_low_prio: mpsc::Receiver<QueueCommandPackage>,
}

impl CommandReceiver {
    /// Receive the highest-priority available command.
    ///
    /// Priority is strict: Express > High > Medium > Low > SuperLow.
    /// Selection happens at command boundaries only; a long-running command
    /// cannot be preempted by a higher-priority arrival.
    ///
    /// Returns `None` only when all senders have been dropped AND all queues
    /// are empty (i.e. the actor should shut down).
    async fn recv_inner(&mut self) -> Option<QueueCommandPackage> {
        loop {
            // Fast path: drain anything already queued, highest-prio first.
            // This avoids waking up the scheduler when work is already available.
            if let Ok(pkg) = self.express.try_recv() {
                return Some(pkg);
            }
            if let Ok(pkg) = self.high_prio.try_recv() {
                return Some(pkg);
            }
            if let Ok(pkg) = self.medium_prio.try_recv() {
                return Some(pkg);
            }
            if let Ok(pkg) = self.low_prio.try_recv() {
                return Some(pkg);
            }
            if let Ok(pkg) = self.super_low_prio.try_recv() {
                return Some(pkg);
            }

            // All channels empty — wait for any of them.
            // `biased` makes select! check branches in order, so if multiple
            // wake up simultaneously, the highest-priority one wins.
            tokio::select! {
                biased;
                pkg = self.express.recv() => {
                    if let Some(p) = pkg { return Some(p) }
                }
                pkg = self.high_prio.recv() => {
                    if let Some(p) = pkg { return Some(p) }
                }
                pkg = self.medium_prio.recv() => {
                    if let Some(p) = pkg { return Some(p) }
                }
                pkg = self.low_prio.recv() => {
                    if let Some(p) = pkg { return Some(p) }
                }
                pkg = self.super_low_prio.recv() => {
                    if let Some(p) = pkg { return Some(p) }
                }
            }

            // If we got here, at least one channel closed with nothing available.
            // Check if ALL are closed — if so, we're done.
            if self.express.is_closed()
                && self.high_prio.is_closed()
                && self.medium_prio.is_closed()
                && self.low_prio.is_closed()
                && self.super_low_prio.is_closed()
            {
                return None;
            }
            // Otherwise loop: some senders still alive, keep waiting.
        }
    }

    pub async fn recv(&mut self) -> Option<QueueCommandPackage> {
        let result = self.recv_inner().await;

        // When returning a command, record wait time and update depth gauge
        if let Some(ref pkg) = result {
            let prio = pkg.command.prio();
            // depth was already decremented implicitly by the recv; we track by counting
            self.metrics.cmd_wait_latency[prio.idx()].observe(pkg.enqueued_at.elapsed());
            self.metrics.cmd_queue_depth[prio.idx()].fetch_sub(1, Ordering::Relaxed);
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct CommandSender {
    metrics: Arc<StromaMetrics>,
    express: mpsc::Sender<QueueCommandPackage>,
    high_prio: mpsc::Sender<QueueCommandPackage>,
    medium_prio: mpsc::Sender<QueueCommandPackage>,
    low_prio: mpsc::Sender<QueueCommandPackage>,
    super_low_prio: mpsc::Sender<QueueCommandPackage>,
}

impl CommandSender {
    pub fn channel_pair(metrics: Arc<StromaMetrics>) -> (CommandSender, CommandReceiver) {
        let (express_tx, express_rx) = mpsc::channel(2048);
        let (high_tx, high_rx) = mpsc::channel(16384);
        let (medium_tx, medium_rx) = mpsc::channel(8192);
        let (low_tx, low_rx) = mpsc::channel(2048);
        let (super_low_tx, super_low_rx) = mpsc::channel(512);

        let sender = CommandSender {
            metrics: metrics.clone(),
            express: express_tx,
            high_prio: high_tx,
            medium_prio: medium_tx,
            low_prio: low_tx,
            super_low_prio: super_low_tx,
        };

        let receiver = CommandReceiver {
            metrics,
            express: express_rx,
            high_prio: high_rx,
            medium_prio: medium_rx,
            low_prio: low_rx,
            super_low_prio: super_low_rx,
        };

        (sender, receiver)
    }

    pub async fn send(
        &self,
        mut pkg: QueueCommandPackage,
    ) -> Result<(), mpsc::error::SendError<QueueCommandPackage>> {
        pkg.enqueued_at = Instant::now();

        let prio = pkg.command.prio();
        // increment depth gauge for this lane
        self.metrics.cmd_queue_depth[prio.idx()].fetch_add(1, Ordering::Relaxed);
        let res = match prio {
            CommandPrio::Express => self.express.send(pkg).await,
            CommandPrio::High => self.high_prio.send(pkg).await,
            CommandPrio::Medium => self.medium_prio.send(pkg).await,
            CommandPrio::Low => self.low_prio.send(pkg).await,
            CommandPrio::SuperLow => self.super_low_prio.send(pkg).await,
        };
        // if send failed, decrement; if succeeded, will be decremented on recv
        if res.is_err() {
            self.metrics.cmd_queue_depth[prio.idx()].fetch_sub(1, Ordering::Relaxed);
        }

        res
    }

    pub fn blocking_send(
        &self,
        mut pkg: QueueCommandPackage,
    ) -> Result<(), mpsc::error::SendError<QueueCommandPackage>> {
        pkg.enqueued_at = Instant::now();

        let prio = pkg.command.prio();
        // increment depth gauge for this lane
        self.metrics.cmd_queue_depth[prio.idx()].fetch_add(1, Ordering::Relaxed);
        let res = match pkg.command.prio() {
            CommandPrio::Express => self.express.blocking_send(pkg),
            CommandPrio::High => self.high_prio.blocking_send(pkg),
            CommandPrio::Medium => self.medium_prio.blocking_send(pkg),
            CommandPrio::Low => self.low_prio.blocking_send(pkg),
            CommandPrio::SuperLow => self.super_low_prio.blocking_send(pkg),
        };
        // if send failed, decrement; if succeeded, will be decremented on recv
        if res.is_err() {
            self.metrics.cmd_queue_depth[prio.idx()].fetch_sub(1, Ordering::Relaxed);
        }

        res
    }
}

fn command_send_error(err: mpsc::error::SendError<QueueCommandPackage>) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        format!(
            "queue actor is gone while sending {}",
            err.0.command.variant_name()
        ),
    )
}

#[derive(Debug, Clone)]
pub struct QueueSharedBundle {
    pub msg_log: Arc<Keratin>,
    pub event_log: Arc<Keratin>,
    pub task_group: Arc<TaskGroup>,
    pub metrics: Arc<StromaMetrics>,
    pub global_dlq: Arc<RwLock<Option<GlobalDLQ>>>,
    pub deadline_waker: Arc<Notify>,
}

/// Bound on the stream control actor's command channel. The work queue uses
/// five priority lanes (latency-sensitive ack/publish interleaving); a stream has
/// no such interleaving, so a single plain channel is enough.
const STREAM_COMMAND_CHANNEL_CAPACITY: usize = 8192;

/// The per-partition engine the substrate hosts. Both engines share the same
/// substrate (logs, durable append, replication, recovery, snapshot IO) but each
/// runs its own control actor with its own command vocabulary, so the handle to
/// that actor is the one field that differs by kind. The work queue is the
/// default; streams (Plexus) are the second.
#[derive(Debug, Clone)]
enum EngineHandle {
    Queue(CommandSender),
    Stream(mpsc::Sender<StreamCommand>),
}

impl EngineHandle {
    fn kind(&self) -> PartitionKind {
        match self {
            EngineHandle::Queue(_) => PartitionKind::Queue,
            EngineHandle::Stream(_) => PartitionKind::Stream,
        }
    }
}

#[derive(Debug)]
pub struct QueueHandleInner {
    engine: EngineHandle,

    topic: String,
    partition: u32,
    group: Option<String>,

    msg_log: Arc<Keratin>,
    event_log: Arc<Keratin>,

    applied_upto: Arc<AtomicU64>,

    // TODO: Set on startup and on encode snapshot, then pass them as arguments to the internal state, methods for easy access at stroma level
    last_snapshot_timestamp: Arc<AtomicU64>,
    last_snapshot_event_offset: Arc<AtomicU64>,
    creating_snapshot: Arc<AtomicBool>,
    dirty_since_snapshot: Arc<AtomicBool>,

    pub(crate) recovery_complete: Arc<AtomicBool>,
    recovery_notify: Arc<Notify>,
    snapshot_task_started: Arc<AtomicBool>,
    background_tasks: CancellationToken,
    role: Arc<AtomicU8>,
    role_generation: Arc<AtomicU64>,
    owner_operations: Arc<AtomicU64>,
    owner_operations_drained: Arc<Notify>,
    owner_operations_paused: Arc<AtomicBool>,
    owner_operations_resumed: Arc<Notify>,

    // Serializes the parallel-publish event-log APPEND per partition so events
    // reach the event log in msg-offset order. The recovery fold and the owner
    // replication gate both assume event-log order matches msg-offset order;
    // without this, concurrent publishes race to append their EnqueueMany and a
    // crash that strands a middle publish can truncate a confirmed one. Held only
    // across staging + the event send, never the fsync waits, so the two fsyncs
    // still overlap.
    publish_event_order: Arc<tokio::sync::Mutex<()>>,

    // Hot-path cache of the per-queue default message TTL (ms). 0 = none.
    // Populated by the actor on Declare and snapshot load so the publish path
    // can resolve a default deadline without a command roundtrip.
    default_message_ttl_ms: Arc<AtomicU64>,

    global_dlq: Arc<RwLock<Option<GlobalDLQ>>>,
    metrics: Arc<StromaMetrics>,

    deadline_waker: Arc<Notify>,
}

/// Handed-out handle to a queue partition: a TICKET, not the state itself.
///
/// The log-owning `QueueHandleInner` lives only in the registry slot (held
/// strong there). A `QueueHandle` holds a `Weak` to the current incarnation
/// plus the registry ref and key, so it can re-resolve. This means a handed-out
/// handle (or a long-lived task that clones it) can NEVER pin a dead incarnation
/// alive: when the slot drops the `Inner`, the logs close and the flock releases
/// even if tickets still exist. Resolution upgrades the `Weak`; if the
/// incarnation has rotated (destroy then recreate) it re-looks-up the live slot
/// by key, so a stale ticket transparently rebinds to the current incarnation.
#[derive(Debug, Clone)]
pub struct QueueHandle {
    registry: Arc<ArcSwap<Registry>>,
    key: QueueKey,
    incarnation: Weak<QueueHandleInner>,
}

impl QueueHandle {
    /// Build a ticket pointing at `inner` (the slot's strong incarnation).
    pub(crate) fn from_inner(
        registry: Arc<ArcSwap<Registry>>,
        key: QueueKey,
        inner: &Arc<QueueHandleInner>,
    ) -> Self {
        Self {
            registry,
            key,
            incarnation: Arc::downgrade(inner),
        }
    }

    /// Resolve the ticket to the live incarnation for the duration of one
    /// operation. Cheap on the common path (one `Weak::upgrade`). On a
    /// rotated/gone incarnation it re-looks-up the slot by key (cold path),
    /// returning the current incarnation or `ActorGone` if the partition no
    /// longer exists.
    ///
    /// The returned [`Resolved`] borrows the ticket, so it cannot be moved into
    /// a `'static` task nor stashed in a longer-lived struct: a resolved handle
    /// cannot outlive the ticket it came from, which makes "park a strong handle
    /// somewhere and pin the logs forever" a COMPILE error rather than a latent
    /// leak. Resolve once per operation/batch scope and let it drop.
    pub fn resolve(&self) -> Result<Resolved<'_>, QueueHandleError> {
        let inner = if let Some(inner) = self.incarnation.upgrade() {
            inner
        } else {
            let current = self.registry.load();
            match current
                .get(&self.key)
                .and_then(|slot| slot.handle.get().cloned())
            {
                Some(inner) => inner,
                None => return Err(QueueHandleError::ActorGone),
            }
        };
        Ok(Resolved {
            inner,
            _ticket: PhantomData,
        })
    }

    /// Resolve and run `f` against the live incarnation. The borrow cannot
    /// escape, so callers cannot accidentally pin the incarnation.
    pub fn with<R>(&self, f: impl FnOnce(&QueueHandleInner) -> R) -> Result<R, QueueHandleError> {
        let inner = self.resolve()?;
        Ok(f(&inner))
    }

    /// Identity, served from the key without resolving.
    pub fn topic(&self) -> &str {
        &self.key.0
    }

    pub fn partition(&self) -> u32 {
        self.key.1
    }

    pub fn group(&self) -> Option<&str> {
        self.key.2.as_deref()
    }
}

/// A ticket resolved to its live incarnation for the span of one operation.
///
/// Holds a strong `Arc<QueueHandleInner>` (so the incarnation cannot vanish
/// mid-operation) but is lifetime-bound to the originating [`QueueHandle`], so
/// it cannot escape into a `'static` task or a longer-lived field. Deref gives
/// the full `QueueHandleInner` API. Drop it promptly (one per op/batch scope).
pub struct Resolved<'a> {
    inner: Arc<QueueHandleInner>,
    _ticket: PhantomData<&'a QueueHandle>,
}

impl std::ops::Deref for Resolved<'_> {
    type Target = QueueHandleInner;
    fn deref(&self) -> &QueueHandleInner {
        &self.inner
    }
}

/// A resolved incarnation projected to the WORK-QUEUE command surface.
///
/// Constructed only via [`QueueHandleInner::as_work_queue`] /
/// [`QueueHandleInner::work_queue`], which hand one back exactly when the
/// partition runs the work-queue engine. The work-queue ops (enqueue, ack,
/// nack, lease, status, expiry sweeps, ...) live on this type, so they are
/// unreachable on a stream partition by construction rather than failing at
/// runtime when a queue command is sent to a stream actor. Derefs to the shared
/// substrate ([`QueueHandleInner`]: role, logs, snapshot, recovery), so a holder
/// keeps full access to everything that is kind-agnostic.
#[derive(Clone, Copy)]
pub struct WorkQueueHandle<'a>(&'a QueueHandleInner);

impl std::ops::Deref for WorkQueueHandle<'_> {
    type Target = QueueHandleInner;
    fn deref(&self) -> &QueueHandleInner {
        self.0
    }
}

/// A resolved incarnation projected to the STREAM command surface, the mirror of
/// [`WorkQueueHandle`]. Constructed only via [`QueueHandleInner::as_stream`] /
/// [`QueueHandleInner::stream`], so the stream command vocabulary is unreachable
/// on a work-queue partition. Derefs to the shared substrate.
#[derive(Clone, Copy)]
pub struct StreamHandle<'a>(&'a QueueHandleInner);

impl std::ops::Deref for StreamHandle<'_> {
    type Target = QueueHandleInner;
    fn deref(&self) -> &QueueHandleInner {
        self.0
    }
}

impl QueueHandleInner {
    pub fn init(
        topic: String,
        partition: u32,
        group: Option<String>,
        bundle: QueueSharedBundle,
        kind: PartitionKind,
    ) -> Arc<QueueHandleInner> {
        let QueueSharedBundle {
            msg_log,
            event_log,
            task_group,
            metrics,
            global_dlq,
            deadline_waker,
        } = bundle;

        // Build the engine handle for this kind, keeping the receiver so its
        // control actor can be spawned once the Arc exists. The queue actor needs
        // a Weak back-reference (it calls process_command with the handle), so it
        // is spawned after the Arc; the stream actor operates purely on its own
        // state and needs no back-reference.
        let (engine, pending) = match kind {
            PartitionKind::Queue => {
                let (tx, rx) = CommandSender::channel_pair(metrics.clone());
                (EngineHandle::Queue(tx), PendingActor::Queue(rx))
            }
            PartitionKind::Stream => {
                let (tx, rx) = mpsc::channel(STREAM_COMMAND_CHANNEL_CAPACITY);
                (EngineHandle::Stream(tx), PendingActor::Stream(rx))
            }
        };

        let dirty_since_snapshot = Arc::new(AtomicBool::new(false));

        let topic_clone = topic.clone();
        let dirty_since_snapshot_loop = dirty_since_snapshot.clone();

        let applied_upto = Arc::new(AtomicU64::new(0));
        let last_snapshot_timestamp = Arc::new(AtomicU64::new(0));
        let last_snapshot_event_offset = Arc::new(AtomicU64::new(0));

        let creating_snapshot = Arc::new(AtomicBool::new(false));
        let recovery_complete = Arc::new(AtomicBool::new(false));
        let recovery_notify = Arc::new(Notify::new());
        let snapshot_task_started = Arc::new(AtomicBool::new(false));
        let background_tasks = CancellationToken::new();
        // A freshly created or recovered queue defaults to Owner, and the role is
        // in-memory only (not persisted). Latent footgun: a queue that was
        // Frozen/Follower loses that role on eviction or restart and comes back
        // as Owner. In a coordinated cluster the broker's ownership gate masks it
        // (it refuses owner traffic for queues it does not own), so this is
        // defense-in-depth, not the primary guard. Robust fix (tracked in the
        // fibril FOLLOWUPS Clients/coordination notes): persist the role (or at
        // least "not owner") so recovery restores a non-owner state and ownership
        // is always coordination's decision, never a default.
        let role = Arc::new(AtomicU8::new(QueueRole::Owner.as_u8()));
        let role_generation = Arc::new(AtomicU64::new(0));
        let owner_operations = Arc::new(AtomicU64::new(0));
        let owner_operations_drained = Arc::new(Notify::new());
        let owner_operations_paused = Arc::new(AtomicBool::new(false));
        let owner_operations_resumed = Arc::new(Notify::new());

        let task_group_clone = task_group.clone();

        let waker_for_state = deadline_waker.clone();
        let result = Arc::new(QueueHandleInner {
            engine,
            topic,
            partition,
            group,
            msg_log,
            event_log,
            applied_upto,
            last_snapshot_timestamp,
            last_snapshot_event_offset,
            creating_snapshot,
            dirty_since_snapshot,
            recovery_complete,
            recovery_notify,
            snapshot_task_started,
            background_tasks,
            role,
            role_generation,
            owner_operations,
            owner_operations_drained,
            owner_operations_paused,
            owner_operations_resumed,
            publish_event_order: Arc::new(tokio::sync::Mutex::new(())),
            default_message_ttl_ms: Arc::new(AtomicU64::new(0)),
            global_dlq,
            metrics,
            deadline_waker,
        });

        // Spawn the control actor for this engine. Both hold only the receiver
        // (and, for the queue, a Weak to the Inner), never a strong clone: the
        // sender lives in the Inner, so when the slot drops the Inner the sender
        // drops, `recv()` returns None, and the loop exits, so the task never pins
        // a retired incarnation.
        match pending {
            PendingActor::Queue(mut rx) => {
                let weak = Arc::downgrade(&result);
                task_group_clone.spawn("queue control", async move {
                    let mut state: QueueInternalState =
                        QueueInternalState::new_with_waker(topic_clone, partition, waker_for_state);

                    while let Some(pkg) = rx.recv().await {
                        let cmd = pkg.command;
                        let Some(handle) = weak.upgrade() else {
                            break;
                        };
                        let (processed, dirty) =
                            QueueHandleInner::process_command(&mut state, cmd, &handle);
                        drop(handle);
                        let _old_val = dirty_since_snapshot_loop
                            .fetch_or(dirty, std::sync::atomic::Ordering::Relaxed);

                        if processed.is_none() {
                            break;
                        }
                    }
                    // If the loop exits, the channel was closed.
                });
            }
            PendingActor::Stream(rx) => {
                let state = StreamState::new(topic_clone, partition);
                task_group_clone.spawn("stream control", run_stream_control(state, rx));
            }
        }

        result
    }
}

/// Carries the spawned engine's command receiver from `init`'s kind match down to
/// the actor spawn after the Arc is built.
enum PendingActor {
    Queue(CommandReceiver),
    Stream(mpsc::Receiver<StreamCommand>),
}

impl QueueHandleInner {
    pub async fn full_debug_info(&self) -> QueueDebugInfo {
        // The internal debug snapshot is a work-queue command. Streams have no
        // equivalent here, so report the default for them.
        let state = match self.as_work_queue() {
            Some(wq) => wq.debug_info().await,
            None => QueueInternalDebugInfo::default(),
        };

        QueueDebugInfo {
            topic: self.topic.clone(),
            partition: self.partition,
            group: self.group.clone(),
            kind: self.kind(),
            materialized: true,
            exists_on_disk: true,
            evicting: false,
            role: self.role(),
            role_generation: self.role_generation(),
            applied_upto: self.applied_upto.load(Ordering::Relaxed),
            last_snapshot_timestamp: self.last_snapshot_timestamp(),
            last_snapshot_event_offset: self.last_snapshot_event_offset(),
            dirty_since_snapshot: self.dirty_snapshot(),
            creating_snapshot: self.creating_snapshot(),
            state,
        }
    }

    pub fn mark_recovery_complete(&self) {
        self.recovery_complete.store(true, Ordering::Release);
        self.recovery_notify.notify_waiters();
    }

    pub fn role(&self) -> QueueRole {
        QueueRole::from_u8(self.role.load(Ordering::Acquire))
    }

    pub fn role_generation(&self) -> u64 {
        self.role_generation.load(Ordering::Acquire)
    }

    pub fn active_owner_operations(&self) -> u64 {
        self.owner_operations.load(Ordering::Acquire)
    }

    pub fn become_owner(&self) {
        self.set_role(QueueRole::Owner);
    }

    pub fn become_follower(&self) {
        self.set_role(QueueRole::Follower);
    }

    pub fn freeze(&self) {
        self.set_role(QueueRole::Frozen);
    }

    pub fn try_freeze_owner(&self) -> Result<(), QueueHandleError> {
        match self.role.compare_exchange(
            QueueRole::Owner.as_u8(),
            QueueRole::Frozen.as_u8(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.role_generation.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Err(actual) => Err(QueueHandleError::WrongRole {
                expected: QueueRole::Owner,
                actual: QueueRole::from_u8(actual),
            }),
        }
    }

    fn set_role(&self, role: QueueRole) {
        let old = self.role.swap(role.as_u8(), Ordering::AcqRel);
        if old != role.as_u8() {
            self.role_generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub fn ensure_owner(&self) -> Result<(), QueueHandleError> {
        let actual = self.role();
        if actual == QueueRole::Owner {
            return Ok(());
        }
        Err(QueueHandleError::WrongRole {
            expected: QueueRole::Owner,
            actual,
        })
    }

    pub async fn begin_owner_operation(&self) -> Result<OwnerOperationLease, QueueHandleError> {
        loop {
            self.ensure_owner()?;

            while self.owner_operations_paused.load(Ordering::Acquire) {
                let resumed = self.owner_operations_resumed.notified();
                if !self.owner_operations_paused.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
                self.ensure_owner()?;
            }

            self.ensure_owner()?;
            if self.owner_operations_paused.load(Ordering::Acquire) {
                continue;
            }

            self.owner_operations.fetch_add(1, Ordering::AcqRel);

            if let Err(err) = self.ensure_owner() {
                if self.owner_operations.fetch_sub(1, Ordering::AcqRel) == 1 {
                    self.owner_operations_drained.notify_waiters();
                }
                return Err(err);
            }

            if self.owner_operations_paused.load(Ordering::Acquire) {
                if self.owner_operations.fetch_sub(1, Ordering::AcqRel) == 1 {
                    self.owner_operations_drained.notify_waiters();
                }
                continue;
            }

            return Ok(OwnerOperationLease {
                active: self.owner_operations.clone(),
                drained: self.owner_operations_drained.clone(),
            });
        }
    }

    pub async fn pause_owner_operations_and_wait(
        &self,
    ) -> Result<OwnerOperationPauseGuard, QueueHandleError> {
        self.ensure_owner()?;
        loop {
            match self.owner_operations_paused.compare_exchange(
                false,
                true,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => {
                    let resumed = self.owner_operations_resumed.notified();
                    if !self.owner_operations_paused.load(Ordering::Acquire) {
                        continue;
                    }
                    resumed.await;
                    self.ensure_owner()?;
                }
            }
        }

        if let Err(err) = self.ensure_owner() {
            self.owner_operations_paused.store(false, Ordering::Release);
            self.owner_operations_resumed.notify_waiters();
            return Err(err);
        }

        loop {
            let drained = self.owner_operations_drained.notified();
            if self.active_owner_operations() == 0 {
                break;
            }
            drained.await;
        }

        Ok(OwnerOperationPauseGuard {
            paused: self.owner_operations_paused.clone(),
            resumed: self.owner_operations_resumed.clone(),
        })
    }

    pub async fn freeze_owner_and_wait_operations(&self) -> Result<(), QueueHandleError> {
        self.try_freeze_owner()?;
        loop {
            let drained = self.owner_operations_drained.notified();
            if self.active_owner_operations() == 0 {
                break;
            }
            drained.await;
        }
        Ok(())
    }

    /// Quiesce the queue for teardown (evict/destroy): stop admitting new owner
    /// operations and wait for any in-flight ones to finish before the logs are
    /// shut down. Role-agnostic and hang-free:
    /// - If Owner, freeze first (best-effort) so new `begin_owner_operation`
    ///   calls return `WrongRole` rather than blocking on a resume that will
    ///   never come (which `pause` would risk during teardown).
    /// - For Follower/Frozen there are no owner operations (begin requires
    ///   Owner), so `active_owner_operations` is already 0 and this returns at
    ///   once.
    pub async fn quiesce_for_teardown(&self) {
        let _ = self.try_freeze_owner();
        loop {
            let drained = self.owner_operations_drained.notified();
            if self.active_owner_operations() == 0 {
                break;
            }
            drained.await;
        }
    }

    pub fn recovery_complete(&self) -> bool {
        self.recovery_complete.load(Ordering::Acquire)
    }

    pub async fn wait_recovery_complete(&self) {
        while !self.recovery_complete() {
            self.recovery_notify.notified().await;
        }
    }

    pub fn try_start_snapshot_task(&self) -> bool {
        self.snapshot_task_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn snapshot_task_started(&self) -> bool {
        self.snapshot_task_started.load(Ordering::Acquire)
    }

    pub fn deadline_waker(&self) -> Arc<Notify> {
        self.deadline_waker.clone()
    }

    #[tracing::instrument(skip(state, handle), fields(
        queue = %handle.topic(),
        part = handle.partition(),
        cmd = cmd.variant_name(),
    ))]
    fn process_command(
        state: &mut QueueInternalState,
        cmd: QueueCommand,
        handle: &QueueHandleInner,
    ) -> (Option<bool>, bool) {
        let mut dirty = false;
        let start = Instant::now();
        let prio = cmd.prio();

        match cmd {
            QueueCommand::Shutdown { response } => {
                // No more commands will be processed after this, so we can ignore the rest of the channel.
                if let Some(r) = response {
                    let _ = r.send(());
                }
                return (None, false); // signal to break the loop
            }
            QueueCommand::Enqueue {
                offset,
                retries,
                expire_at,
                response,
            } => {
                state.enqueue(offset, retries, expire_at);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::EnqueueMany { reqs, response } => {
                state.enqueue_many(&reqs);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !reqs.is_empty();
            }
            QueueCommand::CancelEnqueueMany { offs, response } => {
                state.cancel_enqueue_many(&offs);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !offs.is_empty();
            }
            QueueCommand::EnqueueDelayed {
                offset,
                not_before,
                response,
            } => {
                state.enqueue_delayed(offset, not_before);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::EnqueueDelayedMany { reqs, response } => {
                state.enqueue_delayed_many(&reqs);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !reqs.is_empty();
            }
            QueueCommand::MarkInflight {
                offset,
                deadline,
                response,
            } => {
                state.mark_inflight(offset, deadline);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::MarkInflightMany { reqs, response } => {
                state.mark_inflight_many(&reqs);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !reqs.is_empty();
            }
            QueueCommand::Ack { offset, response } => {
                state.ack(offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::AckMany { reqs, response } => {
                state.ack_many(&reqs);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !reqs.is_empty();
            }
            QueueCommand::ReleaseInflightMany { reqs, response } => {
                let released = state.release_inflight_many(&reqs);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = released > 0;
            }
            QueueCommand::Nack {
                offset,
                requeue,
                not_before,
                response,
            } => {
                let outcome = state.nack_at(offset, requeue, not_before);
                if let Some(r) = response {
                    let _ = r.send(outcome);
                }
                dirty = true;
            }
            QueueCommand::NackMany { reqs, response } => {
                let outcomes = state.nack_many(&reqs);
                if let Some(r) = response {
                    let _ = r.send(outcomes);
                }
                dirty = !reqs.is_empty();
            }
            QueueCommand::DeadLetterCommit { offsets, response } => {
                for o in &offsets {
                    state.commit_dlq(*o);
                }
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !offsets.is_empty();
            }
            QueueCommand::DiscardPendingDlq { offsets, response } => {
                for o in &offsets {
                    state.discard_pending_dlq(*o);
                }
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !offsets.is_empty();
            }
            QueueCommand::Declare { meta, response } => {
                state.apply_declare(&meta);
                handle.set_default_message_ttl_ms(state.default_message_ttl_ms());
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::MarkPendingDlq { offsets, response } => {
                state.mark_pending_dlq_many(&offsets);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !offsets.is_empty();
            }
            QueueCommand::GetDlqTarget { global, response } => {
                let v = state.resolve_dlq_target(global.as_ref());
                if let Some(r) = response {
                    let _ = r.send(v);
                }
            }
            QueueCommand::GetPendingDlq { response } => {
                let v = state.pending_dlq_iter().collect();
                if let Some(r) = response {
                    let _ = r.send(v);
                }
            }
            QueueCommand::InspectOffsets {
                from,
                limit,
                mode,
                response,
            } => {
                let v = state.inspect_offsets(from, limit, mode);
                if let Some(r) = response {
                    let _ = r.send(v);
                }
            }
            QueueCommand::IsSettled { offset, response } => {
                let result = state.is_settled(offset);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::IsInflight { offset, response } => {
                let result = state.is_inflight(offset);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::IsReady { offset, response } => {
                let result = state.is_ready(offset);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetDebugInfo { response } => {
                let result = state.debug_info();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetRetries { offset, response } => {
                let result = state.get_retries(offset);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetSettledUntil { response } => {
                let result = state.settled_until();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetNextDeliverable {
                from,
                upper,
                response,
            } => {
                let result = state.next_deliverable(from, upper);
                if let Some(r) = response {
                    let _ = r.send(Some(result));
                }
            }
            QueueCommand::DumpInflight { response } => {
                let result = state.dump_inflight();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::CollectExpired { now, max, response } => {
                let result = state.collect_expired(now, max);
                dirty = !result.is_empty();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::CollectTtlExpired { now, max, response } => {
                // Read-only: the durable drop is an Ack emitted by the caller.
                let result = state.collect_ttl_expired(now, max);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::Reset { response } => {
                state.reset();
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::EncodeSnapshot {
                last_snapshot_event_offset,
                force,
                response,
            } => {
                let trigger_time = Instant::now();
                handle.metrics.snapshot.attempts.incr();

                if !force && !handle.dirty_snapshot() {
                    handle
                        .metrics
                        .snapshot
                        .skipped_not_dirty
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(r) = response {
                        let _ = r.send(None);
                    }
                    return (Some(true), false);
                }

                state.last_snapshot_event_offset = last_snapshot_event_offset;
                state.last_snapshot_timestamp = unix_millis();

                let clone_start = Instant::now();
                let state_clone = state.clone();
                let clone_duration = clone_start.elapsed();
                handle
                    .metrics
                    .snapshot
                    .clone_latency
                    .observe(clone_duration);

                let metrics_bg = handle.metrics.clone(); // Arc clone
                tokio::task::spawn_blocking(move || {
                    let encode_start = Instant::now();
                    let blob = state_clone.encode_snapshot(last_snapshot_event_offset);
                    let encode_duration = encode_start.elapsed();
                    metrics_bg.snapshot.encode_latency.observe(encode_duration);
                    metrics_bg
                        .snapshot
                        .bytes_written
                        .fetch_add(blob.len() as u64, Ordering::Relaxed);
                    metrics_bg
                        .snapshot
                        .last_snapshot_size_bytes
                        .store(blob.len() as u64, Ordering::Relaxed);

                    let total = trigger_time.elapsed();
                    metrics_bg.snapshot.total_latency.observe(total);

                    if let Some(r) = response {
                        let _ = r.send(Some(blob));
                    }
                });
            }
            QueueCommand::ExportStateCheckpoint {
                last_snapshot_event_offset,
                response,
            } => {
                let trigger_time = Instant::now();
                handle.metrics.snapshot.attempts.incr();

                state.last_snapshot_event_offset = last_snapshot_event_offset;
                state.last_snapshot_timestamp = unix_millis();

                let message_checkpoint_offset = state.lowest_not_settled_offset();
                let clone_start = Instant::now();
                let state_clone = state.clone();
                let clone_duration = clone_start.elapsed();
                handle
                    .metrics
                    .snapshot
                    .clone_latency
                    .observe(clone_duration);

                let metrics_bg = handle.metrics.clone();
                tokio::task::spawn_blocking(move || {
                    let encode_start = Instant::now();
                    let blob = state_clone.encode_snapshot(last_snapshot_event_offset);
                    let encode_duration = encode_start.elapsed();
                    metrics_bg.snapshot.encode_latency.observe(encode_duration);
                    metrics_bg
                        .snapshot
                        .bytes_written
                        .fetch_add(blob.len() as u64, Ordering::Relaxed);
                    metrics_bg
                        .snapshot
                        .last_snapshot_size_bytes
                        .store(blob.len() as u64, Ordering::Relaxed);

                    let total = trigger_time.elapsed();
                    metrics_bg.snapshot.total_latency.observe(total);

                    if let Some(r) = response {
                        let _ = r.send(QueueStateCheckpointSnapshot {
                            message_checkpoint_offset,
                            state_snapshot: blob,
                        });
                    }
                });
            }
            QueueCommand::LoadSnapshot { data, response } => {
                let result = state.load_snapshot(&data);
                if result.is_ok() {
                    handle.set_default_message_ttl_ms(state.default_message_ttl_ms());
                }
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::InstallSnapshotState {
                state: mut loaded_state,
                meta,
                response,
            } => {
                loaded_state.deadline_waker = state.deadline_waker.clone();
                *state = loaded_state;
                handle.set_default_message_ttl_ms(state.default_message_ttl_ms());
                if let Some(r) = response {
                    let _ = r.send(meta);
                }
            }
            QueueCommand::IsInflightOrSettled { offset, response } => {
                let result = state.is_inflight_or_settled(offset);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::PollReadyAndMark {
                max,
                lease_deadline,
                upper,
                response,
            } => {
                let result = state.poll_ready_and_mark(max, lease_deadline, upper);
                dirty = !result.is_empty();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetLowestUnsettled { response } => {
                let result = state.lowest_unsettled_offset();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetLowestNotSettled { response } => {
                let result = state.lowest_not_settled_offset();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetInflightLen { response } => {
                let result = state.inflight_len();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetNextExpiryHint { response } => {
                let result = state.next_expiry_hint();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetCanonicalQueueState { response } => {
                let result = state.canonical();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetStatusReport { response } => {
                let result = state.status_report();

                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
        }

        let elapsed = start.elapsed();
        handle.metrics.cmd_process_latency[prio.idx()].observe(elapsed);

        (Some(true), dirty) // signal to continue processing
    }

    /// Which engine owns this partition.
    pub fn kind(&self) -> PartitionKind {
        self.engine.kind()
    }

    /// Project to the work-queue command surface, `Some` iff this partition runs
    /// the work-queue engine. The work-queue ops live on [`WorkQueueHandle`], so
    /// callers that iterate partitions of mixed kind (stats/expiry sweeps) get a
    /// natural skip for streams, and a caller that knows it holds a queue uses
    /// [`work_queue`](Self::work_queue) to propagate a typed error instead.
    pub fn as_work_queue(&self) -> Option<WorkQueueHandle<'_>> {
        match &self.engine {
            EngineHandle::Queue(_) => Some(WorkQueueHandle(self)),
            EngineHandle::Stream(_) => None,
        }
    }

    /// Project to the stream command surface, `Some` iff this partition runs the
    /// stream engine. Mirror of [`as_work_queue`](Self::as_work_queue).
    pub fn as_stream(&self) -> Option<StreamHandle<'_>> {
        match &self.engine {
            EngineHandle::Stream(_) => Some(StreamHandle(self)),
            EngineHandle::Queue(_) => None,
        }
    }

    /// Project to the work-queue surface or fail with [`QueueHandleError::WrongKind`].
    /// For callers that already expect a work queue and want to `?`-propagate.
    pub fn work_queue(&self) -> Result<WorkQueueHandle<'_>, QueueHandleError> {
        self.as_work_queue().ok_or(QueueHandleError::WrongKind {
            expected: PartitionKind::Queue,
            actual: self.kind(),
        })
    }

    /// Project to the stream surface or fail with [`QueueHandleError::WrongKind`].
    pub fn stream(&self) -> Result<StreamHandle<'_>, QueueHandleError> {
        self.as_stream().ok_or(QueueHandleError::WrongKind {
            expected: PartitionKind::Stream,
            actual: self.kind(),
        })
    }

    fn queue_sender(&self) -> std::io::Result<&CommandSender> {
        match &self.engine {
            EngineHandle::Queue(s) => Ok(s),
            EngineHandle::Stream(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "queue command on a stream partition",
            )),
        }
    }

    pub(crate) async fn command_enqueue(&self, cmd: QueueCommand) -> std::io::Result<()> {
        self.queue_sender()?
            .send(QueueCommandPackage {
                command: cmd,
                enqueued_at: Instant::now(),
            })
            .await
            .map_err(command_send_error)
    }

    pub(crate) fn blocking_command_enqueue(&self, cmd: QueueCommand) -> std::io::Result<()> {
        self.queue_sender()?
            .blocking_send(QueueCommandPackage {
                command: cmd,
                enqueued_at: Instant::now(),
            })
            .map_err(command_send_error)
    }

    /// Send a command to the stream control actor. Errors if this partition is a
    /// work queue.
    pub(crate) async fn stream_command_enqueue(&self, cmd: StreamCommand) -> std::io::Result<()> {
        match &self.engine {
            EngineHandle::Stream(s) => s.send(cmd).await.map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "stream actor is gone while sending a command",
                )
            }),
            EngineHandle::Queue(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "stream command on a queue partition",
            )),
        }
    }

    /// Blocking counterpart to [`stream_command_enqueue`], for the synchronous
    /// event-apply path (recovery replay and follower ingest), mirroring
    /// `blocking_command_enqueue`.
    pub(crate) fn blocking_stream_command_enqueue(
        &self,
        cmd: StreamCommand,
    ) -> std::io::Result<()> {
        match &self.engine {
            EngineHandle::Stream(s) => s.blocking_send(cmd).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "stream actor is gone while sending a command",
                )
            }),
            EngineHandle::Queue(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "stream command on a queue partition",
            )),
        }
    }
}

/// Work-queue command surface. Reachable only through a [`WorkQueueHandle`],
/// which is handed out exclusively for work-queue partitions, so none of these
/// ops can be issued against a stream actor: the misroute is a type error rather
/// than a runtime channel failure.
impl WorkQueueHandle<'_> {
    pub async fn debug_info(&self) -> QueueInternalDebugInfo {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetDebugInfo { response: Some(tx) })
            .await;
        rx.await
            .unwrap_or_else(|_| QueueInternalDebugInfo::default())
    }

    pub async fn enqueue(&self, offset: Offset, retries: u32) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();

        let _ = self
            .command_enqueue(QueueCommand::Enqueue {
                offset,
                retries,
                expire_at: None,
                response: Some(tx),
            })
            .await;

        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn mark_inflight(
        &self,
        offset: Offset,
        deadline: UnixMillis,
    ) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::MarkInflight {
                offset,
                deadline,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        self.deadline_waker().notify_one();
        Ok(())
    }

    pub async fn mark_inflight_batch(
        &self,
        reqs: Vec<MarkInflightEventMeta>,
    ) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::MarkInflightMany {
                reqs,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        self.deadline_waker().notify_one();
        Ok(())
    }

    pub async fn ack(&self, offset: Offset) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::Ack {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn release_inflight_many(
        &self,
        reqs: Vec<AckEventMeta>,
    ) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::ReleaseInflightMany {
                reqs,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        self.deadline_waker().notify_one();
        Ok(())
    }

    pub async fn nack(
        &self,
        offset: Offset,
        requeue: bool,
    ) -> Result<NackOutcome, QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::Nack {
                offset,
                requeue,
                not_before: None,
                response: Some(tx),
            })
            .await;
        let outcome = rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        if let NackOutcome::RequeuedLater { .. } = outcome {
            self.deadline_waker().notify_one();
        }
        Ok(outcome)
    }

    pub async fn dead_letter_commit(&self, offsets: Vec<Offset>) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::DeadLetterCommit {
                offsets,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn get_dlq_target(
        &self,
    ) -> Result<Option<(String, u32, Option<String>)>, QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetDlqTarget {
                global: self.global_dlq.read().await.clone(),
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    pub async fn discard_pending_dlq(&self, offsets: Vec<Offset>) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::DiscardPendingDlq {
                offsets,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn declare(&self, meta: DeclareMeta) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::Declare {
                meta,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn pending_dlq(
        &self,
    ) -> Result<Vec<(Offset, Option<ResolvedDlqTarget>)>, QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetPendingDlq { response: Some(tx) })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    pub async fn inspect_offsets(
        &self,
        from: Offset,
        limit: usize,
        mode: InspectMode,
    ) -> QueueInspectionSnapshot {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::InspectOffsets {
                from,
                limit,
                mode,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(QueueInspectionSnapshot {
            next_offset_hint: from,
            items: Vec::new(),
        })
    }
}

impl QueueHandleInner {
    pub async fn reset(&self) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        match &self.engine {
            EngineHandle::Queue(_) => {
                let _ = self
                    .command_enqueue(QueueCommand::Reset { response: Some(tx) })
                    .await;
            }
            EngineHandle::Stream(_) => {
                let _ = self
                    .stream_command_enqueue(StreamCommand::Reset { response: Some(tx) })
                    .await;
            }
        }
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }
}

impl QueueHandleInner {
    pub async fn encode_snapshot(
        &self,
        last_snapshot_event_offset: u64,
    ) -> Result<Vec<u8>, QueueHandleError> {
        self.encode_snapshot_inner(last_snapshot_event_offset, false)
            .await
    }

    pub async fn force_encode_snapshot(
        &self,
        last_snapshot_event_offset: u64,
    ) -> Result<Vec<u8>, QueueHandleError> {
        self.encode_snapshot_inner(last_snapshot_event_offset, true)
            .await
    }

    async fn encode_snapshot_inner(
        &self,
        last_snapshot_event_offset: u64,
        force: bool,
    ) -> Result<Vec<u8>, QueueHandleError> {
        self.creating_snapshot
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let result = match &self.engine {
            EngineHandle::Queue(_) => {
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .command_enqueue(QueueCommand::EncodeSnapshot {
                        last_snapshot_event_offset,
                        force,
                        response: Some(tx),
                    })
                    .await;
                rx.await
                    .map_err(|_| QueueHandleError::ActorGone)?
                    .ok_or(QueueHandleError::SnapshotNotCreated)
            }
            EngineHandle::Stream(_) => {
                // The stream engine always encodes; it has no dirty/skip gate, so
                // `force` does not apply.
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .stream_command_enqueue(StreamCommand::EncodeSnapshot {
                        last_event_offset: last_snapshot_event_offset,
                        response: tx,
                    })
                    .await;
                rx.await.map_err(|_| QueueHandleError::ActorGone)
            }
        };
        self.last_snapshot_event_offset.store(
            last_snapshot_event_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_snapshot_timestamp
            .store(unix_millis(), std::sync::atomic::Ordering::Relaxed);
        self.creating_snapshot
            .store(false, std::sync::atomic::Ordering::SeqCst);
        result
    }
}

impl WorkQueueHandle<'_> {
    pub async fn export_state_checkpoint_snapshot(
        &self,
        last_snapshot_event_offset: u64,
    ) -> Result<QueueStateCheckpointSnapshot, QueueHandleError> {
        self.creating_snapshot
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::ExportStateCheckpoint {
                last_snapshot_event_offset,
                response: Some(tx),
            })
            .await;
        let res = rx.await;
        self.last_snapshot_event_offset.store(
            last_snapshot_event_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_snapshot_timestamp
            .store(unix_millis(), std::sync::atomic::Ordering::Relaxed);
        self.creating_snapshot
            .store(false, std::sync::atomic::Ordering::SeqCst);
        res.map_err(|_| QueueHandleError::ActorGone)
    }
}

impl QueueHandleInner {
    pub async fn load_snapshot(&self, data: Vec<u8>) -> Result<SnapshotMeta, QueueHandleError> {
        let snapmeta = match &self.engine {
            EngineHandle::Queue(_) => {
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .command_enqueue(QueueCommand::LoadSnapshot {
                        data,
                        response: Some(tx),
                    })
                    .await;
                rx.await
                    .map_err(|_| QueueHandleError::ActorGone)?
                    .map_err(|_| {
                        QueueHandleError::SnapshotLoadFailed("Failed to load snapshot".into())
                    })?
            }
            EngineHandle::Stream(_) => {
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .stream_command_enqueue(StreamCommand::LoadSnapshot {
                        bytes: data,
                        response: tx,
                    })
                    .await;
                rx.await
                    .map_err(|_| QueueHandleError::ActorGone)?
                    .map_err(|e| {
                        QueueHandleError::SnapshotLoadFailed(format!(
                            "Failed to load snapshot: {e}"
                        ))
                    })?
            }
        };

        self.last_snapshot_event_offset.store(
            snapmeta.last_snapshot_event_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_snapshot_timestamp.store(
            snapmeta.last_snapshot_timestamp,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(snapmeta)
    }
}

impl WorkQueueHandle<'_> {
    pub async fn install_snapshot_state(
        &self,
        state: QueueInternalState,
        meta: SnapshotMeta,
    ) -> Result<SnapshotMeta, QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::InstallSnapshotState {
                state,
                meta,
                response: Some(tx),
            })
            .await;
        let snapmeta = rx.await.map_err(|_| QueueHandleError::ActorGone)?;

        self.last_snapshot_event_offset.store(
            snapmeta.last_snapshot_event_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_snapshot_timestamp.store(
            snapmeta.last_snapshot_timestamp,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(snapmeta)
    }

    pub async fn is_settled(&self, offset: Offset) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::IsSettled {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(false)
    }

    pub async fn is_inflight(&self, offset: Offset) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::IsInflight {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(false)
    }

    pub async fn is_inflight_or_settled(&self, offset: Offset) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::IsInflightOrSettled {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(false)
    }

    pub async fn is_ready(&self, offset: Offset) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::IsReady {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(false)
    }

    pub async fn retries(&self, offset: Offset) -> u32 {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetRetries {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn settled_until(&self) -> Offset {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetSettledUntil { response: Some(tx) })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn poll_ready_and_mark(
        &self,
        max: usize,
        lease_deadline: UnixMillis,
        upper: Offset,
    ) -> Result<Vec<(Offset, u32)>, QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::PollReadyAndMark {
                max,
                lease_deadline,
                upper,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    pub async fn lowest_unsettled_offset(&self) -> Offset {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetLowestUnsettled { response: Some(tx) })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn lowest_not_settled_offset(&self) -> Offset {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetLowestNotSettled { response: Some(tx) })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn next_deliverable(&self, from: Offset, upper: Offset) -> Option<Offset> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetNextDeliverable {
                from,
                upper,
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or(None)
    }

    pub async fn inflight_len(&self) -> usize {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetInflightLen { response: Some(tx) })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn next_expiry_hint(&self) -> Option<UnixMillis> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetNextExpiryHint { response: Some(tx) })
            .await;
        rx.await.unwrap_or(None)
    }

    pub async fn canonical(&self) -> CanonicalQueueState {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetCanonicalQueueState { response: Some(tx) })
            .await;
        rx.await.unwrap_or_default()
    }

    pub async fn status_report(&self) -> Result<QueueStatusReport, std::io::Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetStatusReport { response: Some(tx) })
            .await;
        rx.await
            .map_err(|err| std::io::Error::other(format!("Status report failed: {err}")))
    }

    pub async fn collect_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<Vec<Offset>, QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::CollectExpired {
                now,
                max,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    pub async fn collect_ttl_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<Vec<Offset>, QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::CollectTtlExpired {
                now,
                max,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    pub async fn dump_inflight(&self) -> Vec<(Offset, UnixMillis)> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::DumpInflight { response: Some(tx) })
            .await;
        rx.await.unwrap_or_default()
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::Shutdown { response: Some(tx) })
            .await;
        let _ = rx.await;
    }
}

impl QueueHandleInner {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn msg_log(&self) -> Arc<Keratin> {
        self.msg_log.clone()
    }

    pub fn event_log(&self) -> Arc<Keratin> {
        self.event_log.clone()
    }

    /// Per-partition lock serializing the parallel-publish event-log append order.
    /// See the field docs on `QueueHandleInner`.
    pub(crate) fn publish_event_order(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.publish_event_order.clone()
    }

    pub fn applied_upto(&self) -> Arc<AtomicU64> {
        self.applied_upto.clone()
    }

    /// Per-queue default message TTL (ms), or `None`. Hot-path read for the
    /// publish path. Kept in sync by the actor on Declare and snapshot load.
    pub fn default_message_ttl_ms(&self) -> Option<u64> {
        match self.default_message_ttl_ms.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    fn set_default_message_ttl_ms(&self, value: Option<u64>) {
        self.default_message_ttl_ms
            .store(value.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn last_snapshot_timestamp(&self) -> u64 {
        self.last_snapshot_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn last_snapshot_event_offset(&self) -> u64 {
        self.last_snapshot_event_offset
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn creating_snapshot(&self) -> bool {
        self.creating_snapshot
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn dirty_snapshot(&self) -> bool {
        self.dirty_since_snapshot
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_dirty_snapshot(&self, dirty: bool) {
        self.dirty_since_snapshot
            .store(dirty, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn background_cancellation_token(&self) -> CancellationToken {
        self.background_tasks.clone()
    }

    pub fn cancel_background_tasks(&self) {
        self.background_tasks.cancel();
    }
}

/// Stream command surface, the mirror of [`WorkQueueHandle`]. Reachable only
/// through a [`StreamHandle`], handed out exclusively for stream partitions, so
/// these ops cannot be issued against a work-queue actor. Each method is a thin
/// round trip to the stream control actor over the same transport the kind-
/// dispatched apply path uses.
impl StreamHandle<'_> {
    /// Move the tail forward after records were appended, waiting for the apply.
    pub async fn advance_tail(&self, next_offset: Offset) -> Result<(), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        self.stream_command_enqueue(StreamCommand::AdvanceTail {
            next_offset,
            response: Some(tx),
        })
        .await
        .map_err(|err| QueueHandleError::Internal(err.to_string()))?;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    /// Move the tail forward without waiting for confirmation. Tail advances are
    /// monotonic (max-semantics), so a fire-and-forget advance from the durable
    /// append path cannot regress the tail.
    pub async fn advance_tail_unconfirmed(&self, next_offset: Offset) -> std::io::Result<()> {
        self.stream_command_enqueue(StreamCommand::AdvanceTail {
            next_offset,
            response: None,
        })
        .await
    }

    pub async fn set_retention(&self, config: RetentionConfig) -> Result<(), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        self.stream_command_enqueue(StreamCommand::SetRetention {
            config,
            response: Some(tx),
        })
        .await
        .map_err(|err| QueueHandleError::Internal(err.to_string()))?;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    /// Read a durable named cursor position, or `None` if it has none.
    pub async fn cursor(&self, name: &str) -> Result<Option<Offset>, QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        self.stream_command_enqueue(StreamCommand::GetCursor {
            name: name.to_string(),
            response: tx,
        })
        .await
        .map_err(|err| QueueHandleError::Internal(err.to_string()))?;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    pub async fn head_tail(&self) -> Result<(Offset, Offset), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        self.stream_command_enqueue(StreamCommand::GetHeadTail { response: tx })
            .await
            .map_err(|err| QueueHandleError::Internal(err.to_string()))?;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }

    /// Read the retention policy plus the head/tail watermarks in one round trip.
    pub async fn retention_state(
        &self,
    ) -> Result<(RetentionConfig, Offset, Offset), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        self.stream_command_enqueue(StreamCommand::GetRetentionState { response: tx })
            .await
            .map_err(|err| QueueHandleError::Internal(err.to_string()))?;
        rx.await.map_err(|_| QueueHandleError::ActorGone)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    NoopAlreadyAcked,
    Applied, // advanced frontier or set bit
}

impl QueueInternalState {
    pub fn new(topic: String, partition: u32) -> Self {
        Self {
            topic,
            partition,
            last_snapshot_timestamp: 0,
            last_snapshot_event_offset: 0,
            settled: RangeSet::new(),
            inflight: BTreeMap::new(),
            pending_dlq: BTreeMap::new(),
            ready: RangeSet::new(),
            retries: HashMap::new(),
            ttl_deadlines: RangeMap::new(),
            expiry_heap: BinaryHeap::new(),
            delayed_enqueue_heap: BinaryHeap::new(),
            delayed_retry_heap: BinaryHeap::new(),
            min_deadline_hint: None,
            dlq_policy: DLQDiscardPolicy::Discard,
            dlq_discard_max_retries: 5,
            default_message_ttl_ms: None,
            deadline_waker: Arc::new(Notify::new()),
        }
    }

    pub fn new_with_waker(topic: String, partition: u32, deadline_waker: Arc<Notify>) -> Self {
        Self {
            topic,
            partition,
            last_snapshot_timestamp: 0,
            last_snapshot_event_offset: 0,
            settled: RangeSet::new(),
            inflight: BTreeMap::new(),
            pending_dlq: BTreeMap::new(),
            ready: RangeSet::new(),
            retries: HashMap::new(),
            ttl_deadlines: RangeMap::new(),
            expiry_heap: BinaryHeap::new(),
            delayed_enqueue_heap: BinaryHeap::new(),
            delayed_retry_heap: BinaryHeap::new(),
            min_deadline_hint: None,
            dlq_policy: DLQDiscardPolicy::Discard,
            dlq_discard_max_retries: 5,
            default_message_ttl_ms: None,
            deadline_waker,
        }
    }

    pub fn debug_info(&self) -> QueueInternalDebugInfo {
        QueueInternalDebugInfo {
            settled_until: self.settled_until(),
            ready_count: self.ready.iter().map(|r| (r.end - r.start) as usize).sum(),
            ready_set_fragments: self.ready.len(),
            inflight_count: self.inflight.len(),
            retries_count: self.retries.len(),
            min_ready: self.ready.first().map(|r| r.start),
            max_ready: self.ready.last().map(|r| r.end),
            min_inflight: self.inflight.keys().next().copied(),
            max_inflight: self.inflight.keys().next_back().copied(),
            expiry_heap_size: self.expiry_heap.len(),
            next_expiry_hint: self.peek_next_expiry_hint(),
            dlq_policy: format!("{:?}", self.dlq_policy),
            dlq_max_retries: self.dlq_discard_max_retries,
        }
    }

    #[inline]
    pub fn next_offset(&self) -> Offset {
        self.ready.last().map(|r| r.end).unwrap_or(0)
    }

    #[inline]
    pub fn last_snapshot_timestamp(&self) -> Offset {
        self.last_snapshot_timestamp
    }

    #[inline]
    pub fn last_snapshot_event_offset(&self) -> Offset {
        self.last_snapshot_event_offset
    }

    #[inline]
    pub fn min_ready_offset(&self) -> Option<Offset> {
        self.ready.first().map(|r| r.start)
    }

    #[inline]
    pub fn min_inflight_offset(&self) -> Option<Offset> {
        self.inflight.keys().copied().min()
    }

    #[inline]
    pub fn safe_message_truncate_before(&self) -> Offset {
        let min_ready = self.ready.first().map(|r| r.start).unwrap_or(u64::MAX);
        let min_inflight = self.inflight.keys().copied().min().unwrap_or(u64::MAX);
        let min_pending = self.pending_dlq.keys().copied().next().unwrap_or(u64::MAX);

        let min_delayed_enq = self
            .delayed_enqueue_heap
            .iter()
            .map(|(_, off)| *off)
            .min()
            .unwrap_or(u64::MAX);
        let min_delayed_retry = self
            .delayed_retry_heap
            .iter()
            .map(|(_, off)| *off)
            .min()
            .unwrap_or(u64::MAX);
        let result = min_ready
            .min(min_inflight)
            .min(min_pending)
            .min(min_delayed_enq)
            .min(min_delayed_retry);
        if result == u64::MAX {
            return self.settled_until();
        }
        if result == 0 {
            return self.settled_until();
        }
        result
    }

    // ---------------- ACK API ----------------

    /// The ACK frontier: the end of the contiguous settled run covering offset 0,
    /// or 0 if offset 0 is not settled. Derived from `settled` (single source of
    /// truth), so it is always current with no separate field to keep in sync.
    #[inline]
    pub fn settled_until(&self) -> Offset {
        match self.settled.first() {
            Some(r) if r.start == 0 => r.end,
            _ => 0,
        }
    }

    // pub fn iter_ready_from(&self, from: Offset) -> impl Iterator<Item = Offset> + '_ {
    //     // Iterate overlapping ranges from `from` onwards, flatten to individual offsets
    //     let range  = from..u64::MAX ;
    //     self.ready
    //         .overlapping(&range)
    //         .flat_map(|range| range.start.max(from)..range.end)
    // }

    pub fn poll_ready_and_mark(
        &mut self,
        max: usize,
        lease_deadline: UnixMillis,
        upper: Offset,
    ) -> Vec<(Offset, u32)> {
        tracing::debug!(
            "Polling ready for ({}, {}), settled_until={}, upper={}",
            self.topic,
            self.partition,
            self.settled_until(),
            upper,
        );
        let mut offs = Vec::with_capacity(max);
        let from = self.settled_until();
        // `upper` is an exclusive deliverable ceiling: a replica-durable queue
        // passes its committed-replicated watermark so consumers never see an
        // offset that is not yet durable on enough replicas. u64::MAX disables it.
        let range = from..upper;
        // Iterate overlapping ranges from `from` onwards, flatten to individual
        // offsets, capping each interval's end at `upper` (a ready interval can
        // extend past the deliverable ceiling).
        let iter = self
            .ready
            .overlapping(&range)
            .flat_map(|range| range.start.max(from)..range.end.min(upper));
        for off in iter {
            if offs.len() >= max {
                break;
            }
            let retries = self.retries.get(&off).copied().unwrap_or(0);
            offs.push((off, retries));
        }

        self.mark_inflight_uniform_deadline_with_retries(&offs, lease_deadline);

        offs
    }

    /// True if this offset is known settled (acked, terminal-nacked, or DLQ'd).
    #[inline]
    pub fn is_settled(&self, offset: Offset) -> bool {
        self.settled.contains(&offset)
    }

    #[inline]
    pub fn is_inflight(&self, offset: Offset) -> bool {
        self.inflight.contains_key(&offset)
    }

    #[inline]
    pub fn is_inflight_or_settled(&self, offset: Offset) -> bool {
        self.is_settled(offset)
            || self.is_inflight(offset)
            || self.pending_dlq.contains_key(&offset)
    }

    #[inline]
    pub fn is_ready(&self, offset: Offset) -> bool {
        self.ready.contains(&offset)
    }

    pub fn ack(&mut self, offset: u64) {
        if self.settled.contains(&offset) {
            // already settled
            self.inflight.remove(&offset); // best-effort cleanup
            return;
        }

        // SETTLE beats inflight: always remove inflight if present
        let removed = self.inflight.remove(&offset);
        self.ready.remove(offset..offset + 1);
        self.retries.remove(&offset);
        self.ttl_deadlines.remove(offset..offset + 1);
        if removed.is_some() {
            // heap can have stale entries now
            self.recompute_hint_if_needed();
        }

        // Record the settlement. Inserting coalesces with adjacent ranges, so an
        // ack at the frontier extends it and out-of-order acks merge in as the
        // frontier catches up. No window, no explicit frontier advance.
        self.settled.insert(offset..offset + 1);
    }

    pub fn release_inflight(&mut self, offset: u64) -> bool {
        if self.settled.contains(&offset) || !self.inflight.contains_key(&offset) {
            return false;
        }

        self.inflight.remove(&offset);
        self.ready.insert(offset..offset + 1);
        self.recompute_hint_if_needed();
        true
    }

    pub fn release_inflight_many(&mut self, reqs: &[AckEventMeta]) -> usize {
        reqs.iter()
            .filter(|req| self.release_inflight(req.off))
            .count()
    }

    pub fn nack(&mut self, offset: u64, requeue: bool) -> NackOutcome {
        self.nack_at(offset, requeue, None)
    }

    pub fn nack_at(
        &mut self,
        offset: u64,
        requeue: bool,
        not_before: Option<UnixMillis>,
    ) -> NackOutcome {
        if offset < self.settled_until() {
            self.inflight.remove(&offset);
            return NackOutcome::NoOp;
        }

        let exists = self.inflight.contains_key(&offset)
            || self.ready.contains(&offset)
            || self.retries.contains_key(&offset);
        if !exists {
            return NackOutcome::NoOp;
        }

        self.inflight.remove(&offset);

        if !requeue {
            let retry_count = self.retries.get(&offset).copied().unwrap_or(0);
            // Policy-driven: caller (Stroma) decides DLQ vs discard based on dlq_policy.
            // We only mark pending; the local ack happens later via commit_dlq or
            // discard_pending_dlq depending on policy.
            self.ready.remove(offset..offset + 1);
            self.retries.remove(&offset);
            self.ttl_deadlines.remove(offset..offset + 1);
            self.pending_dlq.insert(offset, None);
            self.recompute_hint_if_needed();
            // if let DLQDiscardPolicy::Discard = self.dlq_policy {
            //     return NackOutcome::NoOp;
            // }
            return NackOutcome::DeadLetterRequested {
                retry_count,
                reason: DeadLetterReason::TerminalNack,
            };
        }

        let retries = self.retries.entry(offset).or_insert(0);
        if *retries >= self.dlq_discard_max_retries {
            let retry_count = *retries;
            self.ready.remove(offset..offset + 1);
            self.retries.remove(&offset);
            self.ttl_deadlines.remove(offset..offset + 1);
            self.pending_dlq.insert(offset, None);
            self.recompute_hint_if_needed();
            return NackOutcome::DeadLetterRequested {
                retry_count,
                reason: DeadLetterReason::RetriesExhausted,
            };
        }

        *retries += 1;
        if let Some(not_before) = not_before {
            self.delayed_retry_heap.push((Reverse(not_before), offset));
            self.recompute_hint_if_needed();
            return NackOutcome::RequeuedLater { not_before };
        }

        self.ready.insert(offset..offset + 1);
        self.recompute_hint_if_needed();
        NackOutcome::Requeued
    }

    pub fn nack_many(&mut self, reqs: &[NackEventMeta]) -> Vec<(Offset, NackOutcome)> {
        reqs.iter()
            .map(|r| (r.off, self.nack_at(r.off, r.requeue, r.not_before)))
            .collect()
    }

    pub fn mark_pending_dlq_many(&mut self, offsets: &[Offset]) {
        for &o in offsets {
            if o < self.settled_until() {
                continue;
            }
            self.inflight.remove(&o);
            self.ready.remove(o..o + 1);
            self.retries.remove(&o);
            self.ttl_deadlines.remove(o..o + 1);
            self.pending_dlq.entry(o).or_insert(None);
        }
        self.recompute_hint_if_needed();
    }

    /// DLQ copy succeeded -> finalize as ack.
    pub fn commit_dlq(&mut self, offset: Offset) {
        if !self.pending_dlq.remove(&offset).is_some() {
            return; // not pending; idempotent no-op
        }
        self.ack(offset); // reuse existing frontier-advance logic
    }

    /// Policy says discard, OR DLQ copy permanently failed -> ack locally without DLQ.
    /// Identical mechanics to commit_dlq right now, but kept separate for clarity
    /// and future divergence (e.g. metrics, logging, poison set).
    pub fn discard_pending_dlq(&mut self, offset: Offset) {
        if !self.pending_dlq.remove(&offset).is_some() {
            return;
        }
        self.ack(offset);
    }

    /// Returns (offset, target). target == None means "needs re-resolution".
    pub fn pending_dlq_iter(
        &self,
    ) -> impl Iterator<Item = (Offset, Option<ResolvedDlqTarget>)> + '_ {
        self.pending_dlq.iter().map(|(&o, t)| (o, t.clone()))
    }

    pub fn inspect_offsets(
        &self,
        from: Offset,
        limit: usize,
        mode: InspectMode,
    ) -> QueueInspectionSnapshot {
        let limit = limit.min(10_000);
        let Some(end) = from.checked_add(limit as Offset) else {
            return QueueInspectionSnapshot {
                next_offset_hint: Offset::MAX,
                items: Vec::new(),
            };
        };

        let mut states: BTreeMap<Offset, QueueInspectionState> = BTreeMap::new();

        for range in self.ready.iter() {
            let start = range.start.max(from);
            let stop = range.end.min(end);
            for offset in start..stop {
                states.insert(
                    offset,
                    self.inspection_state(offset, MessageInspectionStatus::Ready),
                );
            }
        }

        for (&offset, &deadline) in self.inflight.range(from..end) {
            states.insert(
                offset,
                QueueInspectionState {
                    offset,
                    status: MessageInspectionStatus::Inflight,
                    retry_count: self.get_retries(offset),
                    inflight_deadline_ms: Some(deadline),
                    available_at_ms: None,
                },
            );
        }

        for &(Reverse(deadline), offset) in &self.delayed_enqueue_heap {
            if (from..end).contains(&offset) && !self.is_settled(offset) {
                states.entry(offset).or_insert(QueueInspectionState {
                    offset,
                    status: MessageInspectionStatus::Delayed,
                    retry_count: self.get_retries(offset),
                    inflight_deadline_ms: None,
                    available_at_ms: Some(deadline),
                });
            }
        }

        for &(Reverse(deadline), offset) in &self.delayed_retry_heap {
            if (from..end).contains(&offset) && !self.is_settled(offset) {
                states.entry(offset).or_insert(QueueInspectionState {
                    offset,
                    status: MessageInspectionStatus::Delayed,
                    retry_count: self.get_retries(offset),
                    inflight_deadline_ms: None,
                    available_at_ms: Some(deadline),
                });
            }
        }

        for (&offset, _) in self.pending_dlq.range(from..end) {
            states.insert(
                offset,
                self.inspection_state(offset, MessageInspectionStatus::PendingDlq),
            );
        }

        if mode == InspectMode::IncludeSettled {
            for offset in from..end {
                states.entry(offset).or_insert(QueueInspectionState {
                    offset,
                    status: MessageInspectionStatus::Settled,
                    retry_count: 0,
                    inflight_deadline_ms: None,
                    available_at_ms: None,
                });
            }
        }

        QueueInspectionSnapshot {
            next_offset_hint: end,
            items: states.into_values().collect(),
        }
    }

    fn inspection_state(
        &self,
        offset: Offset,
        status: MessageInspectionStatus,
    ) -> QueueInspectionState {
        QueueInspectionState {
            offset,
            status,
            retry_count: self.get_retries(offset),
            inflight_deadline_ms: None,
            available_at_ms: None,
        }
    }

    pub fn is_pending_dlq(&self, offset: Offset) -> bool {
        self.pending_dlq.contains_key(&offset)
    }

    pub fn resolve_dlq_target(
        &self,
        global: Option<&GlobalDLQ>,
    ) -> Option<(String, u32, Option<String>)> {
        match &self.dlq_policy {
            DLQDiscardPolicy::Discard => None,
            DLQDiscardPolicy::CustomDQL(c) => Some((c.tp.clone(), c.part, c.group.clone())),
            DLQDiscardPolicy::GlobalDQL => global.map(|g| (g.tp.clone(), g.part, g.group.clone())),
        }
    }

    pub fn apply_declare(&mut self, meta: &DeclareMeta) {
        if let Some(p) = &meta.dlq_policy {
            self.dlq_policy = match p {
                DLQDiscardPolicyWire::Discard => DLQDiscardPolicy::Discard,
                DLQDiscardPolicyWire::GlobalDQL => DLQDiscardPolicy::GlobalDQL,
                DLQDiscardPolicyWire::CustomDQL { tp, part, group } => {
                    DLQDiscardPolicy::CustomDQL(CustomDLQ {
                        tp: tp.to_string(),
                        part: *part,
                        group: group.as_deref().map(|s| s.into()),
                    })
                }
            };
        }
        if let Some(n) = meta.dlq_max_retries {
            self.dlq_discard_max_retries = n;
        }
        if let Some(ttl) = meta.default_message_ttl_ms {
            self.default_message_ttl_ms = Some(ttl);
        }
    }

    pub fn default_message_ttl_ms(&self) -> Option<u64> {
        self.default_message_ttl_ms
    }

    pub fn ack_many(&mut self, reqs: &[AckEventMeta]) {
        for e in reqs {
            self.ack(e.off);
        }
    }

    #[inline]
    pub fn lowest_unsettled_offset(&self) -> Offset {
        self.settled_until()
    }

    #[inline]
    pub fn lowest_not_settled_offset(&self) -> Offset {
        self.safe_message_truncate_before()
    }

    pub fn get_retries(&self, offset: Offset) -> u32 {
        self.retries.get(&offset).copied().unwrap_or(0)
    }

    pub fn enqueue(&mut self, offset: Offset, retries: u32, expire_at: Option<UnixMillis>) {
        // We assume it is only used on messages that have been properly stored earlier
        // TODO: possibly use different checks as ack window has limited trust
        if self.is_settled(offset) {
            return;
        }

        self.ready.insert(offset..offset + 1);
        if retries > 0 {
            self.retries.insert(offset, retries);
        }
        // Only set on the original enqueue. The requeue paths pass None so a
        // message keeps its first deadline across ready->inflight->ready.
        if let Some(deadline) = expire_at {
            self.set_ttl_deadline(offset, deadline);
        }
    }

    pub fn enqueue_many(&mut self, reqs: &[EnqueueEventMeta]) {
        for req in reqs {
            self.enqueue(req.off, req.retries, req.expire_at);
        }
    }

    /// Undo enqueues for offsets whose message payload never became durable (the
    /// parallel append cancel path). Removes them from the ready set and every
    /// per-offset tracker so they are never delivered. Only ever targets
    /// never-settled tail offsets (the producer was not confirmed); a settled
    /// offset is left untouched.
    pub fn cancel_enqueue_many(&mut self, offs: &[Offset]) {
        for &o in offs {
            if o < self.settled_until() {
                continue;
            }
            self.inflight.remove(&o);
            self.ready.remove(o..o + 1);
            self.retries.remove(&o);
            self.ttl_deadlines.remove(o..o + 1);
        }
        self.recompute_hint_if_needed();
    }

    /// Record a message's drop deadline (message TTL) and wake the deadline
    /// worker if this is the new earliest deadline.
    fn set_ttl_deadline(&mut self, offset: Offset, deadline: UnixMillis) {
        if self.is_settled(offset) {
            return;
        }
        let was_earlier_or_empty = self.min_ttl_deadline().is_none_or(|d| deadline < d);
        self.ttl_deadlines.insert(offset..offset + 1, deadline);
        if was_earlier_or_empty {
            self.deadline_waker.notify_one();
        }
    }

    /// Earliest TTL deadline across all tracked offsets, or `None` if no message
    /// carries a TTL.
    fn min_ttl_deadline(&self) -> Option<UnixMillis> {
        self.ttl_deadlines.iter().map(|(_, &d)| d).min()
    }

    /// Ready offsets whose drop deadline has passed (`deadline <= now`), up to
    /// `max`. Pure query - the durable drop is an Ack emitted by the caller,
    /// which clears the entry via `ack`. Inflight/acked offsets are skipped so we
    /// never drop work in flight.
    pub fn collect_ttl_expired(&self, now: UnixMillis, max: usize) -> Vec<Offset> {
        let mut out = Vec::new();
        for (range, &deadline) in self.ttl_deadlines.iter() {
            if deadline > now {
                continue;
            }
            for off in range.clone() {
                if out.len() >= max {
                    return out;
                }
                if self.ready.contains(&off) && !self.inflight.contains_key(&off) {
                    out.push(off);
                }
            }
        }
        out
    }

    pub fn enqueue_delayed(&mut self, offset: Offset, not_before: u64) {
        let was_earlier_or_empty = self
            .delayed_enqueue_heap
            .peek()
            .is_none_or(|(Reverse(d), _)| not_before < *d);
        self.delayed_enqueue_heap
            .push((Reverse(not_before), offset));
        if was_earlier_or_empty {
            self.deadline_waker.notify_one();
        }
    }

    pub fn enqueue_delayed_many(&mut self, reqs: &[EnqueueDelayedEventMeta]) {
        for req in reqs {
            self.enqueue_delayed(req.off, req.not_before);
        }
    }

    // ---------------- Inflight API ----------------

    /// Mark inflight for an offset. If offset is already ACKed, no-op.
    pub fn mark_inflight(&mut self, offset: Offset, deadline: UnixMillis) -> bool {
        // Below frontier is always acked
        if offset < self.settled_until() {
            return false;
        }

        // Case 1: update existing inflight lease
        if let Some(cur) = self.inflight.get_mut(&offset) {
            *cur = deadline;
            self.expiry_heap.push((Reverse(deadline), offset));
            // self.min_deadline_hint = Some(match self.min_deadline_hint {
            //     None => deadline,
            //     Some(m) => m.min(deadline),
            // });
            // TODO: test
            self.recompute_hint_if_needed();
            return self
                .expiry_heap
                .peek()
                .map(|(Reverse(d), _)| *d == deadline)
                .unwrap_or(false);
        }

        // Case 2: initial delivery, must be READY
        if !self.ready.contains(&offset) {
            return false;
        }
        self.ready.remove(offset..offset + 1);

        self.inflight.insert(offset, deadline);
        self.expiry_heap.push((Reverse(deadline), offset));
        self.min_deadline_hint = Some(match self.min_deadline_hint {
            None => deadline,
            Some(cur) => cur.min(deadline),
        });
        self.expiry_heap
            .peek()
            .map(|(Reverse(d), _)| *d == deadline)
            .unwrap_or(false)
    }

    pub fn mark_inflight_many(&mut self, reqs: &[MarkInflightEventMeta]) {
        for e in reqs {
            self.mark_inflight(e.off, e.deadline);
        }
    }

    pub fn mark_inflight_uniform_deadline(&mut self, offsets: &[Offset], deadline: UnixMillis) {
        for &o in offsets {
            self.mark_inflight(o, deadline);
        }
    }

    pub fn mark_inflight_uniform_deadline_with_retries(
        &mut self,
        offsets: &[(Offset, u32)],
        deadline: UnixMillis,
    ) {
        for &(o, _) in offsets {
            self.mark_inflight(o, deadline);
        }
    }

    /// Clear inflight for an offset (e.g. expired worker clears it before requeue).
    pub fn clear_inflight(&mut self, offset: Offset) {
        let was = self.inflight.remove(&offset);
        if was.is_some() {
            // heap might now be stale; recompute hint lazily but safely
            self.recompute_hint_if_needed();
        }
    }

    #[inline]
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    #[inline]
    pub fn next_expiry_hint(&mut self) -> Option<UnixMillis> {
        self.recompute_hint_if_needed();
        let inflight_min = self.min_deadline_hint;
        let delayed_enq_min = self.delayed_enqueue_heap.peek().map(|(Reverse(d), _)| *d);
        let delayed_retry_min = self.delayed_retry_heap.peek().map(|(Reverse(d), _)| *d);
        let ttl_min = self.min_ttl_deadline();
        [inflight_min, delayed_enq_min, delayed_retry_min, ttl_min]
            .into_iter()
            .flatten()
            .min()
    }

    pub fn collect_expired(&mut self, now: UnixMillis, max: usize) -> Vec<Offset> {
        let mut out = Vec::new();

        // TODO: Since we now use this to handle delayed things in one convenient place we might need to find a way to make the expiry worker go earlier
        // TODO: in the case where a message is enqueued/retries with delay and will be available before the next schedule expiry worker run.
        // TODO: At the very least, it should be documented that the guarantee is not published before timestamp, but guaranteed after,
        // TODO: with maximum delayed equal to expiry worker period
        let mut to_enqueue = Vec::new();
        // Handle delayed publishes and retries
        while let Some(&(Reverse(deadline), off)) = self.delayed_enqueue_heap.peek() {
            if deadline > now {
                break;
            }

            self.delayed_enqueue_heap.pop();

            let meta = EnqueueEventMeta {
                off,
                retries: 0,
                expire_at: None,
            };
            to_enqueue.push(meta);
        }

        while let Some(&(Reverse(deadline), off)) = self.delayed_retry_heap.peek() {
            if deadline > now {
                break;
            }

            self.delayed_retry_heap.pop();

            let meta = EnqueueEventMeta {
                off,
                retries: self.retries.get(&off).copied().unwrap_or(0),
                expire_at: None,
            };
            to_enqueue.push(meta);
        }

        self.enqueue_many(&to_enqueue);

        while let Some(&(Reverse(deadline), off)) = self.expiry_heap.peek() {
            if deadline > now || out.len() >= max {
                break;
            }

            self.expiry_heap.pop();

            // validate against inflight (skip stale heap entries)
            match self.inflight.get(&off).copied() {
                Some(cur_deadline) if cur_deadline == deadline => {
                    self.inflight.remove(&off);
                    self.ready.insert(off..off + 1);
                    out.push(off);
                }
                _ => continue,
            }
        }

        // heap may now be stale
        self.recompute_hint_if_needed();

        out
    }

    /// Returns true iff this offset ever entered the delivery lifecycle
    /// (i.e. enqueue or inflight).
    /// Terminal-only operations (ack/reject without enqueue) do NOT create history.
    /// Only used as a test probe for the ready/inflight/retries history semantics.
    #[cfg(test)]
    #[inline]
    fn has_history(&self, offset: Offset) -> bool {
        self.ready.contains(&offset)
            || self.inflight.contains_key(&offset)
            || self.retries.contains_key(&offset)
    }

    /// Walk heap until we find a valid inflight entry, rebuild if heap fully stale.
    /// Keeps `min_deadline_hint` in sync with the live inflight set.
    fn recompute_hint_if_needed(&mut self) {
        if self.inflight.is_empty() {
            self.min_deadline_hint = None;
            return;
        }

        while let Some(&(Reverse(ts), off)) = self.expiry_heap.peek() {
            match self.inflight.get(&off).copied() {
                Some(cur) if cur == ts => {
                    self.min_deadline_hint = Some(ts);
                    return;
                }
                _ => {
                    self.expiry_heap.pop(); // stale
                }
            }
        }

        // Heap drained but inflight not empty: rebuild heap from inflight.
        self.expiry_heap.clear();
        let mut min: Option<UnixMillis> = None;
        for (&off, &deadline) in self.inflight.iter() {
            self.expiry_heap.push((Reverse(deadline), off));
            min = Some(min.map_or(deadline, |m| m.min(deadline)));
        }
        self.min_deadline_hint = min;
    }

    // ---------------- Delivery helper ----------------

    /// Find next deliverable offset in [from, upper).
    /// Skips inflight and (bounded) acked entries.
    pub fn next_deliverable(&self, from: Offset, upper: Offset) -> Offset {
        let start = from.max(self.settled_until());

        for range in self.ready.overlapping(&(start..upper)) {
            let range_start = range.start.max(start);
            for off in range_start..range.end.min(upper) {
                if self.inflight.contains_key(&off) {
                    continue;
                }
                if self.is_settled(off) {
                    continue;
                }
                return off;
            }
        }
        upper
    }

    // ---------------- Debug / maintenance ----------------

    pub fn dump_inflight(&self) -> Vec<(Offset, UnixMillis)> {
        let mut v: Vec<_> = self.inflight.iter().map(|(&o, &d)| (o, d)).collect();
        v.sort_unstable_by_key(|x| x.0);
        v
    }

    pub fn reset(&mut self) {
        let waker = self.deadline_waker.clone();
        *self = QueueInternalState::new_with_waker(self.topic.clone(), self.partition, waker);
    }

    // TODO: Add enqueued state?
    pub fn encode_snapshot(&self, last_snapshot_event_offset: u64) -> Vec<u8> {
        let start = Instant::now();

        let mut out = Vec::new();

        // version
        out.extend_from_slice(&FORMAT_VERSION.to_be_bytes());

        // snapshot meta
        out.extend_from_slice(&self.last_snapshot_timestamp.to_be_bytes());
        out.extend_from_slice(&last_snapshot_event_offset.to_be_bytes());

        // settled ranges (from 0; the contiguous run covering 0 is the frontier)
        let settled_ranges: Vec<_> = self.settled.iter().collect();
        out.extend_from_slice(&(settled_ranges.len() as u64).to_be_bytes());
        for range in settled_ranges {
            out.extend_from_slice(&range.start.to_be_bytes());
            out.extend_from_slice(&range.end.to_be_bytes());
        }

        // inflight
        out.extend_from_slice(&(self.inflight.len() as u64).to_be_bytes());
        for (&off, e) in self.inflight.iter() {
            out.extend_from_slice(&off.to_be_bytes());
            out.extend_from_slice(&e.to_be_bytes());
        }

        // pending delayed enqueues
        out.extend_from_slice(&(self.delayed_enqueue_heap.len() as u64).to_be_bytes());
        for (Reverse(deadline), off) in self.delayed_enqueue_heap.iter() {
            out.extend_from_slice(&deadline.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }

        // pending delayed retries
        out.extend_from_slice(&(self.delayed_retry_heap.len() as u64).to_be_bytes());
        for (Reverse(deadline), off) in self.delayed_retry_heap.iter() {
            out.extend_from_slice(&deadline.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }

        // retries
        out.extend_from_slice(&(self.retries.len() as u64).to_be_bytes());
        for (&off, e) in self.retries.iter() {
            out.extend_from_slice(&off.to_be_bytes());
            out.extend_from_slice(&e.to_be_bytes());
        }
        // ready ranges
        let ranges: Vec<_> = self.ready.iter().collect();
        out.extend_from_slice(&(ranges.len() as u64).to_be_bytes());
        for range in ranges {
            out.extend_from_slice(&range.start.to_be_bytes());
            out.extend_from_slice(&range.end.to_be_bytes());
        }

        // ttl deadlines (message TTL): offset ranges -> absolute drop deadline.
        // Snapshots compact away the Enqueue events that would otherwise rebuild
        // this, so it has to round-trip here.
        let ttl_ranges: Vec<_> = self.ttl_deadlines.iter().collect();
        out.extend_from_slice(&(ttl_ranges.len() as u64).to_be_bytes());
        for (range, &deadline) in ttl_ranges {
            out.extend_from_slice(&range.start.to_be_bytes());
            out.extend_from_slice(&range.end.to_be_bytes());
            out.extend_from_slice(&deadline.to_be_bytes());
        }

        // pending dlq
        out.extend_from_slice(&(self.pending_dlq.len() as u64).to_be_bytes());
        for (off, target) in &self.pending_dlq {
            out.extend_from_slice(&off.to_be_bytes());
            match target {
                None => out.push(0),
                Some(t) => {
                    out.push(1);
                    let tp_bytes = t.tp.as_bytes();
                    out.extend_from_slice(&(tp_bytes.len() as u32).to_be_bytes());
                    out.extend_from_slice(tp_bytes);
                    out.extend_from_slice(&t.part.to_be_bytes());
                    let grp = t.group.as_deref().unwrap_or_default();
                    let grp_bytes = grp.as_bytes();
                    out.extend_from_slice(&(grp_bytes.len() as u32).to_be_bytes());
                    out.extend_from_slice(grp_bytes);
                }
            }
        }

        // dlq policy
        match &self.dlq_policy {
            DLQDiscardPolicy::Discard => {
                out.push(0);
            }
            DLQDiscardPolicy::GlobalDQL => {
                out.push(1);
            }
            DLQDiscardPolicy::CustomDQL(c) => {
                out.push(2);

                let tp_bytes = c.tp.as_bytes();
                out.extend_from_slice(&(tp_bytes.len() as u32).to_be_bytes());
                out.extend_from_slice(tp_bytes);

                out.extend_from_slice(&c.part.to_be_bytes());

                let group_tmp = c.group.as_deref().unwrap_or_default();
                let group_bytes = group_tmp.as_bytes();
                out.extend_from_slice(&(group_bytes.len() as u32).to_be_bytes());
                out.extend_from_slice(group_bytes);
            }
        }

        // dlq max retries
        out.extend_from_slice(&self.dlq_discard_max_retries.to_be_bytes());

        // per-queue default message TTL (presence byte + value)
        match self.default_message_ttl_ms {
            Some(ttl) => {
                out.push(1);
                out.extend_from_slice(&ttl.to_be_bytes());
            }
            None => out.push(0),
        }

        tracing::info!(
            "ms taken to encode snapshot: {}",
            start.elapsed().as_millis()
        );

        out
    }

    fn rebuild_derived(&mut self) {
        // rebuild expiry heap
        self.expiry_heap.clear();
        for (&off, &dl) in &self.inflight {
            self.expiry_heap.push((Reverse(dl), off));
        }

        // rebuild min_deadline_hint
        self.min_deadline_hint = self.inflight.values().min().copied();
    }

    // TODO: Add enqueued state?
    pub fn load_snapshot(&mut self, mut bytes: &[u8]) -> std::io::Result<SnapshotMeta> {
        use std::io::{Error, ErrorKind};

        fn take<const N: usize>(b: &mut &[u8]) -> std::io::Result<[u8; N]> {
            if b.len() < N {
                return Err(Error::new(ErrorKind::UnexpectedEof, "snapshot"));
            }
            let (a, rest) = b.split_at(N);
            *b = rest;
            Ok(a.try_into().expect("exact-length slice"))
        }

        // Version
        const VERSION_SIZE: usize = size_of::<u64>();
        let version = u64::from_be_bytes(take::<VERSION_SIZE>(&mut bytes)?);
        if version != FORMAT_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unsupported snapshot version {version}, expected {FORMAT_VERSION}"),
            ));
        }

        self.reset();

        self.last_snapshot_timestamp = u64::from_be_bytes(take::<8>(&mut bytes)?);
        self.last_snapshot_event_offset = u64::from_be_bytes(take::<8>(&mut bytes)?);

        // settled ranges (from 0; the contiguous run covering 0 is the frontier)
        let settled_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..settled_len {
            let start = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let end = u64::from_be_bytes(take::<8>(&mut bytes)?);
            if end > start {
                self.settled.insert(start..end);
            }
        }

        // inflight
        let inflight_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..inflight_len {
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let dl = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.inflight.insert(off, dl);
        }

        // pending delayed enqueues
        let delayed_enq_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..delayed_enq_len {
            let deadline = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.delayed_enqueue_heap.push((Reverse(deadline), off));
        }

        // pending delayed retries
        let delayed_retry_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..delayed_retry_len {
            let deadline = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.delayed_retry_heap.push((Reverse(deadline), off));
        }

        // retries
        let retries_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..retries_len {
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let retries = u32::from_be_bytes(take::<4>(&mut bytes)?);
            self.retries.insert(off, retries);
        }

        // ready ranges
        let ranges_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..ranges_len {
            let start = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let end = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.ready.insert(start..end);
        }

        // ttl deadlines (message TTL)
        let ttl_len = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..ttl_len {
            let start = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let end = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let deadline = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.ttl_deadlines.insert(start..end, deadline);
        }

        // pending dlq
        let n = u64::from_be_bytes(take::<8>(&mut bytes)?) as usize;
        for _ in 0..n {
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let target = {
                let tag = take::<1>(&mut bytes)?[0];
                match tag {
                    0 => None,
                    1 => {
                        let tp_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
                        if bytes.len() < tp_len {
                            return Err(Error::new(ErrorKind::UnexpectedEof, "pending tp"));
                        }
                        let tp = String::from_utf8(bytes[..tp_len].to_vec())
                            .map_err(|_| Error::new(ErrorKind::InvalidData, "utf8"))?;
                        bytes = &bytes[tp_len..];
                        let part = u32::from_be_bytes(take::<4>(&mut bytes)?);
                        let g_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
                        if bytes.len() < g_len {
                            return Err(Error::new(ErrorKind::UnexpectedEof, "pending group"));
                        }
                        let g = String::from_utf8(bytes[..g_len].to_vec())
                            .map_err(|_| Error::new(ErrorKind::InvalidData, "utf8"))?;
                        bytes = &bytes[g_len..];
                        Some(ResolvedDlqTarget {
                            tp,
                            part,
                            group: if g.is_empty() { None } else { Some(g) },
                        })
                    }
                    _ => return Err(Error::new(ErrorKind::InvalidData, "pending tag")),
                }
            };
            self.pending_dlq.insert(off, target);
        }

        let tag = take::<1>(&mut bytes)?[0];

        self.dlq_policy = match tag {
            0 => DLQDiscardPolicy::Discard,
            1 => DLQDiscardPolicy::GlobalDQL,
            2 => {
                let len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
                if bytes.len() < len {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "dlq tp"));
                }
                let tp = String::from_utf8(bytes[..len].to_vec())
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "utf8"))?;
                bytes = &bytes[len..];

                let part = u32::from_be_bytes(take::<4>(&mut bytes)?);

                let len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
                let group_tmp = String::from_utf8(bytes[..len].to_vec())
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "utf8"))?;
                let group = if group_tmp.is_empty() {
                    None
                } else {
                    Some(group_tmp)
                };
                bytes = &bytes[len..];

                DLQDiscardPolicy::CustomDQL(CustomDLQ { tp, part, group })
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "dlq tag")),
        };

        self.dlq_discard_max_retries = u32::from_be_bytes(take::<4>(&mut bytes)?);

        // per-queue default message TTL (presence byte + value)
        self.default_message_ttl_ms = match take::<1>(&mut bytes)?[0] {
            0 => None,
            _ => Some(u64::from_be_bytes(take::<8>(&mut bytes)?)),
        };

        if !bytes.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("{} trailing bytes", bytes.len()),
            ));
        }

        // --- enforce invariants ---
        if self.inflight.keys().any(|&o| o < self.settled_until()) {
            return Err(Error::new(ErrorKind::InvalidData, "inflight < frontier"));
        }

        for off in self.inflight.keys().copied() {
            self.ready.remove(off..off + 1);
        }

        self.rebuild_derived();

        Ok(SnapshotMeta {
            last_snapshot_event_offset: self.last_snapshot_event_offset,
            last_snapshot_timestamp: self.last_snapshot_timestamp,
            default_message_ttl_ms: self.default_message_ttl_ms,
        })
    }

    // TODO: Add enqueued state?
    pub fn canonical(&self) -> CanonicalQueueState {
        let mut inflight: Vec<_> = self.inflight.iter().map(|(&o, &d)| (o, d)).collect();
        inflight.sort_unstable();

        CanonicalQueueState {
            settled: self.settled.iter().map(|r| (r.start, r.end)).collect(),
            inflight,
        }
    }

    pub fn peek_next_expiry_hint(&self) -> Option<UnixMillis> {
        let heap_iter = self.expiry_heap.iter();

        let mut candidate = None;

        for (Reverse(deadline), offset) in heap_iter {
            match self.inflight.get(offset) {
                Some(&d) if d == *deadline => {
                    candidate = Some(*deadline);
                    break;
                }
                _ => continue, // skip stale
            }
        }

        candidate
    }

    pub fn status_report(&self) -> QueueStatusReport {
        QueueStatusReport {
            topic: self.topic.clone(),
            partition: self.partition,
            inflight_count: self.inflight.len(),
            ready_count: self.ready.iter().map(|r| (r.end - r.start) as usize).sum(),
            next_expiry_hint: self.peek_next_expiry_hint(),
            lowest_unacked: self.lowest_unsettled_offset(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStatusReport {
    pub topic: String,
    pub partition: u32,
    pub inflight_count: usize,
    pub ready_count: usize,
    /// Best-effort next expiry (validated, may be slightly stale but never incorrect)
    pub next_expiry_hint: Option<UnixMillis>,
    pub lowest_unacked: Offset,
}

#[derive(Debug, Serialize)]
pub struct StromaDebugSnapshot {
    pub queues: Vec<QueueDebugInfo>,
    pub queue_count: usize,
    pub materialized_queue_count: usize,
    pub cmd_queue_depths: HashMap<String, usize>, // lane name -> depth
    pub snapshot_metrics: SnapshotMetricsSnapshot,
    pub recovery_metrics: RecoveryMetricsSnapshot,
    pub log_metrics: LogMetricsSnapshot,
    pub command_metrics: CommandMetricsSnapshot,
    pub uptime_seconds: u64,
}
#[derive(Debug, Serialize)]
pub struct QueueDebugInfo {
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    /// Whether this partition is a work queue or a Plexus stream. Streams reuse
    /// the queue handle, so they appear in this snapshot; the kind lets a viewer
    /// (e.g. the admin UI) route them to the stream view instead of the queue list.
    pub kind: PartitionKind,
    pub materialized: bool,
    pub exists_on_disk: bool,
    pub evicting: bool,
    pub role: QueueRole,
    pub role_generation: u64,

    pub applied_upto: u64,
    pub last_snapshot_timestamp: u64,
    pub last_snapshot_event_offset: u64,
    pub dirty_since_snapshot: bool,
    pub creating_snapshot: bool,

    pub state: QueueInternalDebugInfo,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueInternalDebugInfo {
    pub settled_until: Offset,
    pub ready_count: usize,
    pub ready_set_fragments: usize,
    pub inflight_count: usize,
    pub retries_count: usize,
    pub min_ready: Option<Offset>,
    pub max_ready: Option<Offset>,
    pub min_inflight: Option<Offset>,
    pub max_inflight: Option<Offset>,
    pub expiry_heap_size: usize,
    pub next_expiry_hint: Option<UnixMillis>,
    pub dlq_policy: String,
    pub dlq_max_retries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CanonicalQueueState {
    /// Settled offset ranges (acked / terminal-nacked / DLQ'd), from 0. The frontier
    /// is the end of the range covering 0.
    pub settled: Vec<(u64, u64)>,
    pub inflight: Vec<(u64, u64)>,
}

#[cfg(test)]
mod tests {
    use super::{InspectMode, MessageInspectionStatus, QueueInternalState};
    use crate::event::DeadLetterReason;
    use crate::{
        event::{AckEventMeta, DLQDiscardPolicyWire, DeclareMeta},
        state::{CustomDLQ, DLQDiscardPolicy, NackOutcome},
        stroma::GlobalDLQ,
    };

    #[test]
    fn next_deliverable_requires_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(5, 10); // ignored
        s.ack(3);

        assert_eq!(s.next_deliverable(0, 10), 10);

        s.enqueue(5, 0, None);
        assert_eq!(s.next_deliverable(0, 10), 5);
    }

    #[test]
    fn inspect_offsets_active_only_returns_tracked_messages() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.enqueue(2, 2, None);
        s.mark_inflight(2, 500);
        s.enqueue_delayed(3, 700);
        s.enqueue(4, 1, None);
        s.mark_pending_dlq_many(&[4]);
        s.enqueue(5, 0, None);
        s.ack(5);

        let snapshot = s.inspect_offsets(0, 8, InspectMode::ActiveOnly);
        let statuses: Vec<_> = snapshot
            .items
            .iter()
            .map(|item| (item.offset, item.status, item.retry_count))
            .collect();

        assert_eq!(snapshot.next_offset_hint, 8);
        assert_eq!(
            statuses,
            vec![
                (1, MessageInspectionStatus::Ready, 0),
                (2, MessageInspectionStatus::Inflight, 2),
                (3, MessageInspectionStatus::Delayed, 0),
                (4, MessageInspectionStatus::PendingDlq, 0),
            ]
        );
        assert_eq!(snapshot.items[1].inflight_deadline_ms, Some(500));
        assert_eq!(snapshot.items[2].available_at_ms, Some(700));
    }

    #[test]
    fn inspect_offsets_can_include_settled_records() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.enqueue(2, 0, None);

        let snapshot = s.inspect_offsets(0, 4, InspectMode::IncludeSettled);
        let statuses: Vec<_> = snapshot
            .items
            .iter()
            .map(|item| (item.offset, item.status))
            .collect();

        assert_eq!(
            statuses,
            vec![
                (0, MessageInspectionStatus::Settled),
                (1, MessageInspectionStatus::Ready),
                (2, MessageInspectionStatus::Ready),
                (3, MessageInspectionStatus::Settled),
            ]
        );
    }

    #[test]
    fn nack_after_ack_is_noop() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 10);
        s.ack(1);

        s.nack(1, true);

        assert!(s.is_settled(1));
        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert_eq!(s.get_retries(1), 0);
    }

    #[test]
    fn ack_without_enqueue_is_terminal_but_not_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(3);

        assert!(s.is_settled(3));
        assert!(!s.is_ready(3));
        assert!(!s.is_inflight(3));
        assert_eq!(s.settled_until(), 0); // frontier doesn't move unless contiguous
    }

    #[test]
    fn has_history_only_after_enqueue_or_delivery() {
        let mut s = QueueInternalState::new("test".into(), 0);

        // No history initially
        assert!(!s.has_history(5));

        // ACK alone does not create history
        s.ack(5);
        assert!(!s.has_history(5));

        // ACK dominates future enqueue
        s.enqueue(5, 0, None);
        assert!(!s.has_history(5));
    }

    #[test]
    fn enqueue_creates_history_if_not_acked() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0, None);
        assert!(s.has_history(5));

        s.mark_inflight(5, 10);
        assert!(s.has_history(5));

        s.ack(5);
        assert!(!s.has_history(5));
    }

    #[test]
    fn cancel_enqueue_removes_only_the_targeted_offset() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0, None);
        s.enqueue(6, 0, None);
        assert!(s.is_ready(5));
        assert!(s.is_ready(6));

        s.cancel_enqueue_many(&[5]);

        // The cancelled offset is annihilated: not ready and no history, as if it
        // had never been enqueued.
        assert!(!s.is_ready(5));
        assert!(!s.has_history(5));
        // The sibling enqueue is untouched.
        assert!(s.is_ready(6));
    }

    #[test]
    fn ack_without_enqueue_is_allowed() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(5);

        assert!(s.is_settled(5));
        assert_eq!(s.settled_until(), 0); // frontier does not advance
        assert!(!s.is_ready(5));
        assert!(!s.is_inflight(5));
    }

    #[test]
    fn enqueue_after_ack_is_ignored() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(3);
        s.enqueue(3, 0, None);

        assert!(s.is_settled(3));
        assert!(!s.is_ready(3));
        assert_eq!(s.next_deliverable(0, 10), 10);
    }

    #[test]
    fn ack_before_enqueue_advances_frontier_normally() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(0);
        s.ack(1);
        s.ack(2);

        assert_eq!(s.settled_until(), 3);

        s.enqueue(1, 0, None);
        s.enqueue(2, 0, None);

        assert!(!s.is_ready(1));
        assert!(!s.is_ready(2));
    }

    #[test]
    fn nack_without_enqueue_does_not_make_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.nack(5, true);

        assert!(!s.is_ready(5));
        assert!(!s.is_inflight(5));
        assert!(!s.is_settled(5));
    }

    #[test]
    fn inflight_update_does_not_make_offset_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 10);
        s.mark_inflight(1, 20);

        assert!(s.is_inflight(1));
        assert!(!s.is_ready(1));
    }

    #[test]
    fn expiry_makes_offset_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(3, 0, None);
        s.mark_inflight(3, 10);
        s.collect_expired(10, 10);

        assert!(s.is_ready(3));
        assert_eq!(s.next_deliverable(0, 10), 3);
    }

    #[test]
    fn only_ready_offsets_are_delivered() {
        let mut s = QueueInternalState::new("test".into(), 0);

        assert_eq!(s.next_deliverable(0, 10), 10);

        s.enqueue(5, 0, None);
        assert_eq!(s.next_deliverable(0, 10), 5);
    }

    #[test]
    fn nack_hits_dlq_at_max_retries() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 10);
        for _ in 0..s.dlq_discard_max_retries {
            s.nack(1, true);
            s.mark_inflight(1, 10);
        }

        s.nack(1, true);

        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert!(!s.is_settled(1));

        s.commit_dlq(1);

        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert!(s.is_settled(1));
    }

    #[test]
    fn offset_in_exactly_one_state() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ready.insert(5..5 + 1);
        assert!(s.is_ready(5));
        assert!(!s.is_inflight(5));

        s.mark_inflight(5, 100);
        assert!(!s.is_ready(5));
        assert!(s.is_inflight(5));

        s.nack(5, true);
        assert!(s.is_ready(5));
        assert!(!s.is_inflight(5));

        s.ack(5);
        assert!(s.is_settled(5));
        assert!(!s.is_ready(5));
        assert!(!s.is_inflight(5));
    }

    #[test]
    fn retries_persist_across_redelivery() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ready.insert(1..1 + 1);
        s.mark_inflight(1, 100);
        s.nack(1, true);

        s.mark_inflight(1, 200);
        s.nack(1, true);

        assert_eq!(s.get_retries(1), 2);
    }

    #[test]
    fn release_inflight_returns_ready_without_retry_accounting() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 2, None);
        s.mark_inflight(1, 100);
        assert!(s.release_inflight(1));

        assert!(s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert_eq!(s.get_retries(1), 2);
    }

    #[test]
    fn expired_then_nacked_is_still_not_acked() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(2, 0, None);
        s.mark_inflight(2, 10);
        s.collect_expired(10, 10);
        s.nack(2, true);

        assert!(!s.is_settled(2));
        assert_eq!(s.next_deliverable(0, 10), 2);
    }

    #[test]
    fn nack_requeue_increments_retry() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 100);
        s.nack(1, true);
        assert_eq!(s.get_retries(1), 1);

        s.mark_inflight(1, 100);
        s.nack(1, true);
        assert_eq!(s.get_retries(1), 2);
    }

    #[test]
    fn nack_allows_redelivery() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(0, 0, None);
        s.mark_inflight(0, 100);
        s.nack(0, true);

        assert_eq!(s.next_deliverable(0, 10), 0);
    }

    #[test]
    fn nack_never_acks() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(3, 100);
        s.nack(3, true);

        assert!(!s.is_settled(3));
        assert!(!s.is_inflight(3));
        assert_eq!(s.settled_until(), 0);
    }

    #[test]
    fn expiry_never_acks() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0, None);
        s.mark_inflight(5, 10);
        assert!(!s.is_settled(5));

        let ex = s.collect_expired(10, 10);
        assert_eq!(ex, vec![5]);

        // Still NOT acked
        assert!(!s.is_settled(5));
        assert_eq!(s.settled_until(), 0);

        // Offset 5 is now eligible again, but ordering is preserved
        assert!(!s.is_inflight(5));
        assert!(!s.is_settled(5));

        // Earliest deliverable is still 0
        let d = s.next_deliverable(0, 100);
        assert_eq!(d, 5);
    }

    #[test]
    fn expired_offset_delivered_after_frontier_advances() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0, None);
        s.mark_inflight(5, 10);
        s.collect_expired(10, 10);

        // ACK 0..4
        for i in 0..5 {
            s.ack(i);
        }

        assert_eq!(s.settled_until(), 5);
        assert_eq!(s.next_deliverable(0, 100), 5);
    }

    #[test]
    fn expired_offsets_do_not_reappear_without_redelivery() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(3, 10);
        s.collect_expired(10, 10);

        assert!(!s.is_inflight(3));

        // No operation should magically reinsert it
        for _ in 0..10 {
            s.collect_expired(20, 10);
            s.clear_inflight(3);
        }

        assert!(!s.is_inflight(3));
    }

    #[test]
    fn ack_before_expiry_prevents_expiry() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(7, 100);
        s.ack(7);

        let ex = s.collect_expired(200, 10);
        assert!(ex.is_empty());

        assert!(s.is_settled(7));
        assert!(!s.is_inflight(7));
    }

    #[test]
    fn expiry_does_not_interact_with_ack_window() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.ack(1); // out of order
        s.enqueue(0, 0, None);
        s.mark_inflight(0, 10);

        let ex = s.collect_expired(10, 10);
        assert_eq!(ex, vec![0]);

        // ACK window still intact
        assert!(s.is_settled(1));
        assert!(!s.is_settled(0));
        assert_eq!(s.settled_until(), 0);
    }

    #[test]
    fn expiry_hint_is_none_after_full_drain() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..50 {
            s.mark_inflight(i, i + 10);
        }

        s.collect_expired(100, 100);

        assert_eq!(s.inflight_len(), 0);
        assert_eq!(s.next_expiry_hint(), None);
    }

    #[test]
    fn next_deliverable_progresses_after_expiry() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(10, 0, None);
        s.mark_inflight(0, 10);
        s.enqueue(1, 0, None);
        s.mark_inflight(1, 10);
        s.enqueue(12, 0, None);
        s.mark_inflight(2, 10);

        s.collect_expired(10, 10);

        let d = s.next_deliverable(0, 10);
        assert_eq!(d, 1);
    }

    #[test]
    fn random_ops_with_expiry_never_violate_invariants() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for _ in 0..200_000 {
            let o = fastrand::u64(0..1000);
            match fastrand::u8(0..6) {
                0 => {
                    s.mark_inflight(o, fastrand::u64(0..1000));
                }
                1 => s.clear_inflight(o),
                2 => s.ack(o),
                _ => {
                    let _ = s.collect_expired(fastrand::u64(0..1000), 10);
                }
            }

            // Invariants
            if let Some(h) = s.next_expiry_hint() {
                assert!(s.inflight.values().any(|&d| d == h));
            }
            assert!(s.settled_until() <= 1000);
        }
    }

    #[test]
    fn expired_offsets_are_removed_exactly_once() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..1000 {
            s.enqueue(i, 0, None);
            s.mark_inflight(i, 100);
        }

        let e1 = s.collect_expired(100, 2000);
        let e2 = s.collect_expired(100, 2000);

        assert_eq!(e1.len(), 1000);
        assert!(e2.is_empty());
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn updating_inflight_deadline_does_not_expire_early() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 10);
        s.mark_inflight(1, 100); // update

        let expired = s.collect_expired(50, 10);
        assert!(expired.is_empty());

        let expired = s.collect_expired(100, 10);
        assert_eq!(expired, vec![1]);
    }

    #[test]
    fn acked_offsets_never_expire() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(5, 10);
        s.ack(5);

        let expired = s.collect_expired(100, 10);
        assert!(expired.is_empty());
        assert!(s.is_settled(5));
    }

    #[test]
    fn inflight_below_frontier_is_ignored() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(0);
        s.ack(1);
        s.ack(2);
        assert_eq!(s.settled_until(), 3);

        s.mark_inflight(1, 10); // should be ignored
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn offset_not_in_multiple_states() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0, None);
        s.mark_inflight(5, 10);

        assert!(s.is_inflight(5));
        assert!(!s.is_ready(5));

        s.ack(5);
        assert!(s.is_settled(5));
        assert!(!s.is_inflight(5));
        assert!(!s.is_ready(5));
    }

    #[test]
    fn frontier_is_monotonic() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for _i in 0..10_000 {
            s.ack(fastrand::u64(0..5000));
            assert!(s.settled_until() <= 5000);
        }
    }

    #[test]
    fn next_deliverable_never_returns_acked_or_inflight() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..2000 {
            if i % 3 == 0 {
                s.ack(i);
            } else if i % 3 == 1 {
                s.mark_inflight(i, 1000);
            }
        }

        for _i in 0..2000 {
            let d = s.next_deliverable(0, 2000);
            if d >= 2000 {
                break;
            }

            assert!(!s.is_settled(d));
            assert!(!s.is_inflight(d));

            s.mark_inflight(d, 2000);
        }
    }

    #[test]
    fn snapshot_roundtrip_is_identity() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..1000 {
            s.mark_inflight(i, 1000 + i);
            if i % 2 == 0 {
                s.ack(i);
            }
        }

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert_eq!(s.canonical(), s2.canonical());
    }

    #[test]
    fn snapshot_records_supplied_event_offset() {
        let s = QueueInternalState::new("test".into(), 0);
        let snap = s.encode_snapshot(42);

        let mut loaded = QueueInternalState::new("test".into(), 0);
        let meta = loaded.load_snapshot(&snap).unwrap();

        assert_eq!(meta.last_snapshot_event_offset, 42);
        assert_eq!(loaded.last_snapshot_event_offset, 42);
    }

    #[test]
    fn snapshot_preserves_dlq_policy() {
        let mut s = QueueInternalState::new("test".into(), 0);
        s.dlq_policy = DLQDiscardPolicy::CustomDQL(CustomDLQ {
            tp: "dlq-topic".into(),
            part: 42,
            group: Some("dlq-group".into()),
        });
        s.dlq_discard_max_retries = 7;

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert_eq!(s.dlq_policy, s2.dlq_policy);
        assert_eq!(s.dlq_discard_max_retries, s2.dlq_discard_max_retries);
    }

    #[test]
    fn snapshot_rebuilds_expiry_heap() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 5, None);
        s.enqueue(2, 5, None);
        s.mark_inflight(1, 100);
        s.mark_inflight(2, 50);

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert_eq!(s2.min_deadline_hint, Some(50));
    }

    #[test]
    fn random_ops_never_break_invariants() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for _ in 0..1_000_000 {
            let o = fastrand::u64(0..30000);
            match fastrand::u8(0..8) {
                0 => {
                    s.mark_inflight(o, fastrand::u64(0..100_000));
                }
                1 => s.clear_inflight(o),
                2 => s.ack(o),
                _ => {
                    let _ = s.collect_expired(fastrand::u64(0..100_000), 100);
                }
            }

            // Hard invariants:
            assert!(s.settled_until() <= 30000);
            if let Some(h) = s.next_expiry_hint() {
                let assert_cond = s.inflight.values().any(|&d| d == h);
                if !assert_cond {
                    dbg!(s.dump_inflight());
                    dbg!(h);
                    assert!(assert_cond);
                }
            }
        }
    }

    #[test]
    fn snapshot_truncated_fails() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 100);

        let snap = s.encode_snapshot(0);

        for i in 0..snap.len() {
            let mut s2 = QueueInternalState::new("test".into(), 0);
            let res = s2.load_snapshot(&snap[..i]);
            assert!(res.is_err(), "should fail at truncation {i}");
        }
    }

    #[test]
    fn snapshot_with_trailing_bytes_is_rejected() {
        let q = QueueInternalState::new("test".into(), 0);
        let mut snap = q.encode_snapshot(0);

        snap.extend_from_slice(&[1, 2, 3, 4]);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        let res = s2.load_snapshot(&snap);

        assert!(res.is_err()); // or assert ok if you *intentionally* allow this
    }

    #[test]
    fn snapshot_inconsistent_ready_retries() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ready.insert(5..5 + 1); // no retries entry

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert!(s2.get_retries(5) == 0 || s2.is_ready(5));
    }

    #[test]
    fn snapshot_preserves_ack_window_behavior() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..20000 {
            s.ack(i);
        }

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert_eq!(s.settled_until(), s2.settled_until());

        for i in 0..20000 {
            assert_eq!(s.is_settled(i), s2.is_settled(i));
        }
    }

    #[test]
    fn snapshot_handles_stale_heap_entries() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..1000 {
            s.enqueue(i, 0, None);
            s.mark_inflight(i, i + 100);
            s.mark_inflight(i, i + 200); // create stale heap entries
        }

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        for _ in 0..1000 {
            let _ = s2.next_expiry_hint();
        }

        assert_eq!(s.inflight_len(), s2.inflight_len());
    }

    #[test]
    fn snapshot_all_dlq_variants() {
        let cases = vec![
            DLQDiscardPolicy::Discard,
            DLQDiscardPolicy::GlobalDQL,
            DLQDiscardPolicy::CustomDQL(CustomDLQ {
                tp: "x".into(),
                part: 1,
                group: Some("y".into()),
            }),
        ];

        for policy in cases {
            let mut s = QueueInternalState::new("test".into(), 0);
            s.dlq_policy = policy.clone();

            let snap = s.encode_snapshot(0);

            let mut s2 = QueueInternalState::new("test".into(), 0);
            s2.load_snapshot(&snap).unwrap();

            assert_eq!(s2.dlq_policy, policy);
        }
    }

    #[test]
    fn snapshot_load_is_idempotent() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..100 {
            s.enqueue(i, i as u32, None);
            s.mark_inflight(i, i + 100);
        }

        let snap = s.encode_snapshot(0);

        let mut s2 = QueueInternalState::new("test".into(), 0);
        s2.load_snapshot(&snap).unwrap();
        let snap2 = s2.encode_snapshot(0);

        let mut s3 = QueueInternalState::new("test".into(), 0);
        s3.load_snapshot(&snap2).unwrap();

        assert_eq!(s2.canonical(), s3.canonical());
    }

    #[test]
    fn out_of_order_acks_advance_frontier() {
        let mut s = QueueInternalState::new("test".into(), 0);

        // ACK 2 and 4 out-of-order; frontier stays at 0
        s.ack(2);
        s.ack(4);
        assert_eq!(s.settled_until(), 0);
        assert!(s.is_settled(2));
        assert!(s.is_settled(4));
        assert!(!s.is_settled(0));

        // ACK 0 should advance frontier to 1
        s.ack(0);
        assert_eq!(s.settled_until(), 1);

        // ACK 1 should advance to 3 (because 2 was already acked)
        s.ack(1);
        assert_eq!(s.settled_until(), 3);

        // ACK 3 should advance to 5 (because 4 was already acked)
        s.ack(3);
        assert_eq!(s.settled_until(), 5);

        // sanity: everything < 5 is acked now
        for o in 0..5 {
            assert!(s.is_settled(o));
        }
    }

    #[test]
    fn ack_removes_inflight() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(10, 0, None);
        s.mark_inflight(10, 1000);
        assert!(s.is_inflight_or_settled(10));
        assert!(!s.is_settled(10));

        s.ack(10);
        assert!(s.is_settled(10));
        assert!(s.is_inflight_or_settled(10 /* still true via ack */)); // just to show logic
        // More direct:
        assert!(s.is_inflight_or_settled(10));
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn mark_inflight_ignored_if_already_acked() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack_many(&[0, 1, 2, 3, 4].map(|off| AckEventMeta { off }));
        assert_eq!(s.settled_until(), 5);

        s.enqueue(2, 0, None);
        s.mark_inflight(2, 123);
        s.enqueue(4, 0, None);
        s.mark_inflight(4, 123);
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn expiry_hint_tracks_min_deadline_and_handles_updates() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(10, 0, None);
        s.mark_inflight(10, 500);
        assert_eq!(s.next_expiry_hint(), Some(500));

        s.enqueue(11, 0, None);
        s.mark_inflight(11, 400);
        assert_eq!(s.next_expiry_hint(), Some(400));

        // update 11 to later deadline; heap now has stale(400) + current(700).
        s.mark_inflight(11, 700);

        // hint may still be 400 until recompute/pop, force recompute
        assert_eq!(s.next_expiry_hint(), Some(500));
    }

    #[test]
    fn collect_expired_is_idempotent() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..1000 {
            s.enqueue(i, 0, None);
            s.mark_inflight(i, 100);
        }

        let ex1 = s.collect_expired(100, 2000);
        let ex2 = s.collect_expired(100, 2000);

        assert_eq!(ex1.len(), 1000);
        assert!(ex2.is_empty());
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn clear_inflight_removes_and_hint_updates() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0, None);
        s.mark_inflight(1, 10);
        s.enqueue(2, 0, None);
        s.mark_inflight(2, 20);
        assert_eq!(s.next_expiry_hint(), Some(10));

        s.clear_inflight(1);
        assert_eq!(s.inflight_len(), 1);
        assert_eq!(s.next_expiry_hint(), Some(20));

        s.clear_inflight(2);
        assert_eq!(s.inflight_len(), 0);
        assert_eq!(s.next_expiry_hint(), None);
    }

    #[test]
    fn is_inflight_or_settled_behaves() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0, None);
        s.mark_inflight(5, 100);
        assert!(s.is_inflight_or_settled(5));
        assert!(!s.is_settled(5));

        s.ack(5);
        assert!(s.is_inflight_or_settled(5));
        assert!(s.is_settled(5));

        // below frontier is always acked
        s.ack(0);
        s.ack(1);
        s.ack(2);
        s.ack(3);
        s.ack(4);
        assert_eq!(s.settled_until(), 6);
        for o in 0..6 {
            assert!(s.is_inflight_or_settled(o));
            assert!(s.is_settled(o));
        }
    }

    #[test]
    fn ack_batch_handles_duplicates() {
        let mut s = QueueInternalState::new("test".into(), 0);
        let v: Vec<AckEventMeta> = [2, 2, 0, 1, 1, 3]
            .into_iter()
            .map(|off| AckEventMeta { off })
            .collect();
        s.ack_many(&v);
        assert_eq!(s.settled_until(), 4);
    }

    #[test]
    fn nack_requeue_under_max_returns_requeued() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.mark_inflight(1, 100);

        let out = s.nack(1, true);

        assert_eq!(out, NackOutcome::Requeued);
        assert!(s.is_ready(1));
        assert!(!s.is_pending_dlq(1));
        assert_eq!(s.get_retries(1), 1);
    }

    #[test]
    fn nack_requeue_later_waits_until_deadline() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.mark_inflight(1, 100);

        let out = s.nack_at(1, true, Some(500));

        assert_eq!(out, NackOutcome::RequeuedLater { not_before: 500 });
        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert_eq!(s.get_retries(1), 1);

        let _ = s.collect_expired(499, 100);
        assert!(!s.is_ready(1));

        let _ = s.collect_expired(500, 100);
        assert!(s.is_ready(1));
        assert_eq!(s.get_retries(1), 1);
    }

    #[test]
    fn nack_requeue_at_max_returns_dead_letter() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.dlq_discard_max_retries = 2;
        s.enqueue(1, 0, None);

        s.mark_inflight(1, 100);
        assert_eq!(s.nack(1, true), NackOutcome::Requeued); // retries=1
        s.mark_inflight(1, 100);
        assert_eq!(s.nack(1, true), NackOutcome::Requeued); // retries=2
        s.mark_inflight(1, 100);
        let out = s.nack(1, true); // retries==max -> DLQ

        assert_eq!(
            out,
            NackOutcome::DeadLetterRequested {
                retry_count: 2,
                reason: DeadLetterReason::RetriesExhausted,
            }
        );
        assert!(s.is_pending_dlq(1));
        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert!(!s.is_settled(1)); // NOT acked yet — phase 2 hasn't happened
    }

    #[test]
    fn nack_no_requeue_goes_to_pending_dlq() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.mark_inflight(1, 100);

        let out = s.nack(1, false);

        assert_eq!(
            out,
            NackOutcome::DeadLetterRequested {
                retry_count: 0,
                reason: DeadLetterReason::TerminalNack,
            }
        );
        assert!(s.is_pending_dlq(1));
        assert!(!s.is_settled(1));
    }

    #[test]
    fn nack_unknown_offset_is_noop() {
        let mut s = QueueInternalState::new("t".into(), 0);
        let out = s.nack(42, true);
        assert_eq!(out, NackOutcome::NoOp);
        assert!(!s.is_pending_dlq(42));
        assert!(!s.is_settled(42));
    }

    #[test]
    fn nack_below_frontier_is_noop() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.ack(0);
        s.ack(1);
        s.ack(2);
        assert_eq!(s.settled_until(), 3);

        let out = s.nack(1, true);
        assert_eq!(out, NackOutcome::NoOp);
        assert!(!s.is_pending_dlq(1));
    }

    #[test]
    fn commit_dlq_acks_locally_and_advances_frontier() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, None);
        s.mark_inflight(0, 100);
        assert_eq!(
            s.nack(0, false),
            NackOutcome::DeadLetterRequested {
                retry_count: 0,
                reason: DeadLetterReason::TerminalNack,
            }
        );

        s.commit_dlq(0);

        assert!(!s.is_pending_dlq(0));
        assert!(s.is_settled(0));
        assert_eq!(s.settled_until(), 1);
    }

    #[test]
    fn commit_dlq_unknown_offset_is_idempotent() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.commit_dlq(99); // never been pending
        assert!(!s.is_settled(99));
        assert_eq!(s.settled_until(), 0);
    }

    #[test]
    fn commit_dlq_twice_is_idempotent() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, None);
        s.mark_inflight(0, 100);
        s.nack(0, false);

        s.commit_dlq(0);
        s.commit_dlq(0); // second call: pending_dlq no longer contains, no-op

        assert!(s.is_settled(0));
        assert_eq!(s.settled_until(), 1);
    }

    #[test]
    fn discard_pending_dlq_acks_locally() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, None);
        s.mark_inflight(0, 100);
        s.nack(0, false);

        s.discard_pending_dlq(0);

        assert!(!s.is_pending_dlq(0));
        assert!(s.is_settled(0));
    }

    #[test]
    fn pending_dlq_blocks_msg_truncation() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(5, 0, None);
        s.mark_inflight(5, 100);
        s.nack(5, false); // pending_dlq = {5}

        // Frontier is 0, but pending_dlq holds 5, so safe truncation must not pass 5.
        assert!(s.safe_message_truncate_before() <= 5);
        assert!(s.is_inflight_or_settled(5)); // delivery must skip it
    }

    #[test]
    fn pending_dlq_does_not_count_as_ready() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(3, 0, None);
        s.mark_inflight(3, 100);
        s.nack(3, false);

        assert!(!s.is_ready(3));
        assert_eq!(s.next_deliverable(0, 100), 100); // nothing deliverable
    }

    #[test]
    fn poll_ready_skips_pending_dlq() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, None);
        s.enqueue(1, 0, None);
        s.enqueue(2, 0, None);
        s.mark_inflight(1, 100);
        s.nack(1, false); // 1 -> pending_dlq

        let polled = s.poll_ready_and_mark(10, 200, u64::MAX);
        let offsets: Vec<_> = polled.iter().map(|(o, _)| *o).collect();

        assert!(!offsets.contains(&1));
        assert_eq!(offsets, vec![0, 2]);
    }

    #[test]
    fn mark_pending_dlq_many_clears_other_states() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.enqueue(2, 0, None);
        s.mark_inflight(1, 100); // 1 inflight, 2 ready

        s.mark_pending_dlq_many(&[1, 2]);

        assert!(s.is_pending_dlq(1));
        assert!(s.is_pending_dlq(2));
        assert!(!s.is_inflight(1));
        assert!(!s.is_ready(2));
        assert!(!s.is_settled(1));
        assert!(!s.is_settled(2));
    }

    #[test]
    fn apply_declare_updates_only_provided_fields() {
        let mut s = QueueInternalState::new("t".into(), 0);
        let original_max = s.dlq_discard_max_retries;

        s.apply_declare(&DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
            dlq_max_retries: None,
            default_message_ttl_ms: None,
        });

        assert_eq!(s.dlq_policy, DLQDiscardPolicy::GlobalDQL);
        assert_eq!(s.dlq_discard_max_retries, original_max); // untouched
    }

    #[test]
    fn apply_declare_custom_dlq_roundtrips_through_wire() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.apply_declare(&DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::CustomDQL {
                tp: "dlq-x".into(),
                part: 7,
                group: Some("g1".into()), // assumes you added the group field
            }),
            dlq_max_retries: Some(99),
            default_message_ttl_ms: None,
        });

        assert_eq!(
            s.dlq_policy,
            DLQDiscardPolicy::CustomDQL(CustomDLQ {
                tp: "dlq-x".into(),
                part: 7,
                group: Some("g1".into()),
            })
        );
        assert_eq!(s.dlq_discard_max_retries, 99);
    }

    #[test]
    fn resolve_dlq_target_discard_returns_none() {
        let s = QueueInternalState::new("t".into(), 0);
        assert!(matches!(s.dlq_policy, DLQDiscardPolicy::Discard));
        assert!(s.resolve_dlq_target(None).is_none());
        // Even with a global, discard means discard.
        let global = GlobalDLQ {
            tp: "g".into(),
            part: 0,
            group: None,
        };
        assert!(s.resolve_dlq_target(Some(&global)).is_none());
    }

    #[test]
    fn resolve_dlq_target_global_falls_back_to_global() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.dlq_policy = DLQDiscardPolicy::GlobalDQL;

        assert!(s.resolve_dlq_target(None).is_none());

        let g = GlobalDLQ {
            tp: "global-dlq".into(),
            part: 3,
            group: None,
        };
        let r = s.resolve_dlq_target(Some(&g)).unwrap();
        assert_eq!(r, ("global-dlq".into(), 3, None));
    }

    #[test]
    fn resolve_dlq_target_custom_ignores_global() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.dlq_policy = DLQDiscardPolicy::CustomDQL(CustomDLQ {
            tp: "custom".into(),
            part: 1,
            group: None,
        });
        let g = GlobalDLQ {
            tp: "global".into(),
            part: 9,
            group: None,
        };

        let r = s.resolve_dlq_target(Some(&g)).unwrap();
        assert_eq!(r, ("custom".into(), 1, None));
    }

    #[test]
    fn snapshot_roundtrips_pending_dlq() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.enqueue(2, 0, None);
        s.mark_inflight(1, 100);
        s.nack(1, false);
        s.nack(2, false);

        let snap = s.encode_snapshot(0);
        let mut s2 = QueueInternalState::new("t".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert!(s2.is_pending_dlq(1));
        assert!(s2.is_pending_dlq(2));
        assert_eq!(s.canonical(), s2.canonical());
    }

    #[test]
    fn snapshot_roundtrips_declare_settings() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.dlq_discard_max_retries = 17;
        s.dlq_policy = DLQDiscardPolicy::CustomDQL(CustomDLQ {
            tp: "x".into(),
            part: 2,
            group: Some("g".into()),
        });

        let snap = s.encode_snapshot(0);
        let mut s2 = QueueInternalState::new("t".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        assert_eq!(s2.dlq_discard_max_retries, 17);
        assert_eq!(s2.dlq_policy, s.dlq_policy);
    }

    #[test]
    fn nack_after_dead_letter_requested_is_noop() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, None);
        s.mark_inflight(0, 100);
        s.nack(0, false); // -> pending_dlq

        let out = s.nack(0, true); // already pending, not in lifecycle
        assert_eq!(out, NackOutcome::NoOp);
        assert!(s.is_pending_dlq(0));
    }

    #[test]
    fn collect_expired_drains_delayed_enqueue_heap() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue_delayed(5, 100);
        s.enqueue_delayed(6, 200);

        let _ = s.collect_expired(150, 100);

        assert!(s.is_ready(5));
        assert!(!s.is_ready(6));
        assert_eq!(s.delayed_enqueue_heap.len(), 1); // only 6 remains

        let _ = s.collect_expired(250, 100);
        assert!(s.is_ready(6));
        assert_eq!(s.delayed_enqueue_heap.len(), 0);
    }

    #[test]
    fn next_expiry_hint_considers_all_heaps() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.mark_inflight(1, 500); // inflight expiry
        s.enqueue_delayed(2, 300); // delayed enqueue
        // (when delayed_retry exists, add one at 400 too)

        assert_eq!(s.next_expiry_hint(), Some(300));
    }

    #[test]
    fn next_expiry_hint_when_inflight_is_earliest() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0, None);
        s.mark_inflight(1, 100);
        s.enqueue_delayed(2, 500);

        assert_eq!(s.next_expiry_hint(), Some(100));
    }

    #[test]
    fn safe_truncate_blocked_by_delayed_message() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue_delayed(5, 999_999_999); // far future
        s.ack(10); // some higher offset done
        s.ack(11);

        assert!(s.safe_message_truncate_before() <= 5);
    }

    #[test]
    fn safe_truncate_without_retained_offsets_uses_frontier() {
        let mut s = QueueInternalState::new("t".into(), 0);

        assert_eq!(s.safe_message_truncate_before(), 0);

        s.ack(0);
        s.ack(1);

        assert_eq!(s.settled_until(), 2);
        assert_eq!(s.safe_message_truncate_before(), 2);
    }

    #[test]
    fn snapshot_preserves_delayed_enqueue() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue_delayed(5, 1_000_000);
        s.enqueue_delayed(6, 2_000_000);

        let snap = s.encode_snapshot(0);
        let mut s2 = QueueInternalState::new("t".into(), 0);
        s2.load_snapshot(&snap).unwrap();

        // After load, the delayed entries should still be tracked
        let _ = s2.collect_expired(1_500_000, 10);
        assert!(s2.is_ready(5));
        assert!(!s2.is_ready(6));
    }

    #[test]
    fn ttl_collects_ready_offsets_past_deadline() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, Some(100));
        s.enqueue(1, 0, Some(200));
        s.enqueue(2, 0, None); // no TTL, never collected

        assert_eq!(s.collect_ttl_expired(50, 10), Vec::<u64>::new());
        assert_eq!(s.collect_ttl_expired(150, 10), vec![0]);
        assert_eq!(s.collect_ttl_expired(250, 10), vec![0, 1]);
    }

    #[test]
    fn ttl_never_drops_inflight() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, Some(100));
        assert!(s.mark_inflight(0, 5_000));
        // Past its deadline but leased - must not be collected for drop.
        assert_eq!(s.collect_ttl_expired(1_000, 10), Vec::<u64>::new());
    }

    #[test]
    fn ttl_deadline_cleared_on_ack() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, Some(100));
        s.ack(0);
        assert_eq!(s.collect_ttl_expired(1_000, 10), Vec::<u64>::new());
        assert!(s.ttl_deadlines.is_empty());
    }

    #[test]
    fn ttl_respects_max_bound() {
        let mut s = QueueInternalState::new("t".into(), 0);
        for off in 0..5 {
            s.enqueue(off, 0, Some(100));
        }
        assert_eq!(s.collect_ttl_expired(150, 2), vec![0, 1]);
    }

    #[test]
    fn ttl_feeds_next_expiry_hint() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, Some(300));
        s.enqueue(1, 0, Some(120));
        assert_eq!(s.next_expiry_hint(), Some(120));
    }

    #[test]
    fn ttl_deadlines_survive_snapshot_round_trip() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0, Some(100));
        s.enqueue(1, 0, Some(100));
        s.enqueue(2, 0, Some(500));
        let snap = s.encode_snapshot(0);

        let mut restored = QueueInternalState::new("t".into(), 0);
        restored.load_snapshot(&snap).unwrap();
        assert_eq!(restored.collect_ttl_expired(150, 10), vec![0, 1]);
        assert_eq!(restored.collect_ttl_expired(600, 10), vec![0, 1, 2]);
    }
}
