use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use arc_swap::ArcSwap;

use bitvec::vec::BitVec;
use keratin_log::Keratin;
use keratin_log::util::unix_millis;
use rangemap::RangeSet;
use serde::Serialize;
use tokio::sync::{Notify, RwLock, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::StromaError;
use crate::event::{
    AckEventMeta, DLQDiscardPolicyWire, DeadLetterReason, DeclareMeta, EnqueueDelayedEventMeta,
    EnqueueEventMeta, MarkInflightEventMeta, NackEventMeta,
};
use crate::metrics::{
    CommandMetricsSnapshot, LogMetricsSnapshot, RecoveryMetricsSnapshot,
    ReplicationCacheMetricsSnapshot, SnapshotMetricsSnapshot, StromaMetrics,
};
use crate::stroma::{GlobalDLQ, QueueKey, Registry, TaskGroup};

pub type ClientId = Uuid;
pub type ConsumerId = u64;
pub type Offset = u64;
pub type UnixMillis = u64;

pub const ACK_WINDOW: usize = 16384; // fixed bounded memory

pub const FORMAT_VERSION: u64 = 2;

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
    LoadSnapshotFailed(String),
    SnapshotNotCreated,
    SnapshotLoadFailed(String),
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
            QueueHandleError::LoadSnapshotFailed(reason) => {
                write!(f, "snapshot load failed: {reason}")
            }
            QueueHandleError::SnapshotNotCreated => write!(f, "snapshot not created"),
            QueueHandleError::SnapshotLoadFailed(reason) => {
                write!(f, "snapshot load failed: {reason}")
            }
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
            QueueHandleError::LoadSnapshotFailed(reason) => StromaError::Internal(reason),
            QueueHandleError::SnapshotNotCreated => {
                StromaError::Internal("snapshot not created".to_string())
            }
            QueueHandleError::SnapshotLoadFailed(reason) => StromaError::Internal(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum ExpiryDeadlineOutcome {
    Updated(UnixMillis),
    NoChange,
}

impl ExpiryDeadlineOutcome {
    pub fn is_updated(&self) -> bool {
        matches!(self, ExpiryDeadlineOutcome::Updated(_))
    }

    pub fn deadline(&self) -> Option<UnixMillis> {
        match self {
            ExpiryDeadlineOutcome::Updated(ts) => Some(*ts),
            ExpiryDeadlineOutcome::NoChange => None,
        }
    }

    pub fn min(a: Self, b: Self) -> Self {
        match (a, b) {
            (ExpiryDeadlineOutcome::Updated(ts_a), ExpiryDeadlineOutcome::Updated(ts_b)) => {
                ExpiryDeadlineOutcome::Updated(ts_a.min(ts_b))
            }
            (ExpiryDeadlineOutcome::Updated(ts), ExpiryDeadlineOutcome::NoChange)
            | (ExpiryDeadlineOutcome::NoChange, ExpiryDeadlineOutcome::Updated(ts)) => {
                ExpiryDeadlineOutcome::Updated(ts)
            }
            (ExpiryDeadlineOutcome::NoChange, ExpiryDeadlineOutcome::NoChange) => {
                ExpiryDeadlineOutcome::NoChange
            }
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

    // ----- ACK state -----
    // Lowest offset that is NOT ACKed (frontier).
    settled_until: Offset,

    // Bounded window of out-of-order ACKs for offsets in [ack_window_base, ack_window_base + ACK_WINDOW)
    ack_window_base: Offset,
    ack_bits: BitVec,

    // ----- inflight -----
    // offset -> deadline_ts
    inflight: BTreeMap<Offset, UnixMillis>,

    // awaiting DLQ-copy + commit
    pending_dlq: BTreeMap<Offset, Option<ResolvedDlqTarget>>,

    // ----- Ready -----
    // offset -> retries
    ready: RangeSet<Offset>,       // readiness only
    retries: HashMap<Offset, u32>, // retry metadata only

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
        response: Option<oneshot::Sender<()>>,
    }, // offset, retries
    EnqueueMany {
        reqs: Vec<EnqueueEventMeta>,
        response: Option<oneshot::Sender<()>>,
    }, // list[offset, retries]
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
    AdvanceFrontier {
        response: Option<oneshot::Sender<()>>,
    },
    Reset {
        response: Option<oneshot::Sender<()>>,
    },
    SetAckedUntil {
        offset: Offset,
        response: Option<oneshot::Sender<()>>,
    }, // offset
    SetAckWindow {
        base: Offset,
        bits: BitVec,
        response: Option<oneshot::Sender<()>>,
    }, // base, bits
    SetAckWindowFromBytes {
        base: Offset,
        bits_bytes: Vec<u8>,
        response: Option<oneshot::Sender<std::io::Result<()>>>,
    }, // base, bits_bytes
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

    IsAcked {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    IsInflight {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    IsInflightOrAcked {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    IsReady {
        offset: Offset,
        response: Option<oneshot::Sender<bool>>,
    }, // offset
    FilterNotEnqueued {
        items: Vec<(Offset, Vec<u8>)>,
        response: Option<oneshot::Sender<Vec<(Offset, Vec<u8>)>>>,
    }, // items
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
    GetLowestUnacked {
        response: Option<oneshot::Sender<Offset>>,
    },
    GetLowestNotAcked {
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
    GetAckWindowBase {
        response: Option<oneshot::Sender<Offset>>,
    },
    GetAckBitsBytes {
        response: Option<oneshot::Sender<Vec<u8>>>,
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
            QueueCommand::SetAckedUntil { .. } => CommandPrio::Express,
            QueueCommand::SetAckWindow { .. } => CommandPrio::Express,
            QueueCommand::SetAckWindowFromBytes { .. } => CommandPrio::Express,
            QueueCommand::Reset { .. } => CommandPrio::Express,
            QueueCommand::GetDlqTarget { .. } => CommandPrio::Express,

            // === Observability / admin queries — fast, cheap, must stay responsive ===
            QueueCommand::GetDebugInfo { .. } => CommandPrio::Express,
            QueueCommand::GetStatusReport { .. } => CommandPrio::Express,
            QueueCommand::InspectOffsets { .. } => CommandPrio::Express,
            QueueCommand::GetInflightLen { .. } => CommandPrio::Express,
            QueueCommand::GetSettledUntil { .. } => CommandPrio::Express,
            QueueCommand::GetLowestUnacked { .. } => CommandPrio::Express,
            QueueCommand::GetLowestNotAcked { .. } => CommandPrio::Express,
            QueueCommand::GetAckWindowBase { .. } => CommandPrio::Express,
            QueueCommand::GetAckBitsBytes { .. } => CommandPrio::Express,
            QueueCommand::GetCanonicalQueueState { .. } => CommandPrio::Express,
            QueueCommand::DumpInflight { .. } => CommandPrio::Express,
            QueueCommand::GetRetries { .. } => CommandPrio::Express,

            // === Point-reads used in hot paths — cheap but not observability-critical ===
            QueueCommand::IsAcked { .. } => CommandPrio::High,
            QueueCommand::IsInflight { .. } => CommandPrio::High,
            QueueCommand::IsInflightOrAcked { .. } => CommandPrio::High,
            QueueCommand::IsReady { .. } => CommandPrio::High,
            QueueCommand::FilterNotEnqueued { .. } => CommandPrio::High,
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
            QueueCommand::EnqueueDelayed { .. } => CommandPrio::Medium,
            QueueCommand::EnqueueDelayedMany { .. } => CommandPrio::Medium,

            // === Background maintenance — wait for quiet periods ===
            QueueCommand::CollectExpired { .. } => CommandPrio::Low,
            QueueCommand::AdvanceFrontier { .. } => CommandPrio::Low,

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
            QueueCommand::MarkInflight { .. } => "MarkInflight",
            QueueCommand::MarkInflightMany { .. } => "MarkInflightMany",
            QueueCommand::Ack { .. } => "Ack",
            QueueCommand::AckMany { .. } => "AckMany",
            QueueCommand::ReleaseInflightMany { .. } => "ReleaseInflightMany",
            QueueCommand::Nack { .. } => "Nack",
            QueueCommand::NackMany { .. } => "NackMany",
            QueueCommand::AdvanceFrontier { .. } => "AdvanceFrontier",
            QueueCommand::Reset { .. } => "Reset",
            QueueCommand::SetAckedUntil { .. } => "SetAckedUntil",
            QueueCommand::SetAckWindow { .. } => "SetAckWindow",
            QueueCommand::SetAckWindowFromBytes { .. } => "SetAckWindowFromBytes",
            QueueCommand::EncodeSnapshot { .. } => "EncodeSnapshot",
            QueueCommand::ExportStateCheckpoint { .. } => "ExportStateCheckpoint",
            QueueCommand::LoadSnapshot { .. } => "LoadSnapshot",
            QueueCommand::InstallSnapshotState { .. } => "InstallSnapshotState",
            QueueCommand::IsAcked { .. } => "IsAcked",
            QueueCommand::IsInflight { .. } => "IsInflight",
            QueueCommand::IsInflightOrAcked { .. } => "IsInflightOrAcked",
            QueueCommand::IsReady { .. } => "IsReady",
            QueueCommand::FilterNotEnqueued { .. } => "FilterNotEnqueued",
            QueueCommand::GetRetries { .. } => "GetRetries",
            QueueCommand::GetSettledUntil { .. } => "GetSettledUntil",
            QueueCommand::PollReadyAndMark { .. } => "PollReadyAndMark",
            QueueCommand::GetLowestUnacked { .. } => "GetLowestUnacked",
            QueueCommand::GetLowestNotAcked { .. } => "GetLowestNotAcked",
            QueueCommand::GetNextDeliverable { .. } => "GetNextDeliverable",
            QueueCommand::GetInflightLen { .. } => "GetInflightLen",
            QueueCommand::GetNextExpiryHint { .. } => "GetNextExpiryHint",
            QueueCommand::GetAckWindowBase { .. } => "GetAckWindowBase",
            QueueCommand::GetAckBitsBytes { .. } => "GetAckBitsBytes",
            QueueCommand::GetCanonicalQueueState { .. } => "GetCanonicalQueueState",
            QueueCommand::GetStatusReport { .. } => "GetStatusReport",
            QueueCommand::InspectOffsets { .. } => "InspectOffsets",
            QueueCommand::CollectExpired { .. } => "CollectExpired",
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

#[derive(Debug)]
pub struct QueueHandleInner {
    command_sender: CommandSender,

    pub(crate) task_group: Arc<TaskGroup>,

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

impl QueueHandleInner {
    pub fn init(
        topic: String,
        partition: u32,
        group: Option<String>,
        bundle: QueueSharedBundle,
    ) -> Arc<QueueHandleInner> {
        let QueueSharedBundle {
            msg_log,
            event_log,
            task_group,
            metrics,
            global_dlq,
            deadline_waker,
        } = bundle;
        let (tx, mut rx) = CommandSender::channel_pair(metrics.clone());
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
        let role = Arc::new(AtomicU8::new(QueueRole::Owner.as_u8()));
        let role_generation = Arc::new(AtomicU64::new(0));
        let owner_operations = Arc::new(AtomicU64::new(0));
        let owner_operations_drained = Arc::new(Notify::new());
        let owner_operations_paused = Arc::new(AtomicBool::new(false));
        let owner_operations_resumed = Arc::new(Notify::new());

        let task_group_clone = task_group.clone();

        let waker_for_state = deadline_waker.clone();
        let result = Arc::new(QueueHandleInner {
            command_sender: tx,
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
            task_group,
            global_dlq,
            metrics,
            deadline_waker,
        });

        // The control task holds only a Weak to the Inner, never a strong clone:
        // the command `tx` lives in the Inner, so while the Inner is alive (held
        // strong by the registry slot) `recv()` yields and the upgrade succeeds.
        // When the slot drops the Inner, `tx` drops, `recv()` returns None, and
        // the loop exits, so the task never pins a retired incarnation.
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
                let _old_val =
                    dirty_since_snapshot_loop.fetch_or(dirty, std::sync::atomic::Ordering::Relaxed);

                if processed.is_none() {
                    break;
                }
            }
            // If the loop exits, the channel was closed.
        });

        result
    }
}

impl QueueHandleInner {
    pub async fn full_debug_info(&self) -> QueueDebugInfo {
        let state = self.debug_info().await;

        QueueDebugInfo {
            topic: self.topic.clone(),
            partition: self.partition,
            group: self.group.clone(),
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
                response,
            } => {
                state.enqueue(offset, retries);
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
            QueueCommand::IsAcked { offset, response } => {
                let result = state.is_acked(offset);
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
            QueueCommand::AdvanceFrontier { response } => {
                state.advance_frontier();
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::Reset { response } => {
                state.reset();
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::SetAckedUntil { offset, response } => {
                state.set_acked_until(offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::SetAckWindow {
                base,
                bits,
                response,
            } => {
                state.set_ack_window(base, bits);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::SetAckWindowFromBytes {
                base,
                bits_bytes,
                response,
            } => {
                let result = state.set_ack_window_from_bytes(base, &bits_bytes);
                if let Some(r) = response {
                    let _ = r.send(result);
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

                let message_checkpoint_offset = state.lowest_not_acked_offset();
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
                if let Some(r) = response {
                    let _ = r.send(meta);
                }
            }
            QueueCommand::IsInflightOrAcked { offset, response } => {
                let result = state.is_inflight_or_acked(offset);
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::FilterNotEnqueued {
                mut items,
                response,
            } => {
                state.filter_not_enqueued(&mut items);
                if let Some(r) = response {
                    let _ = r.send(items);
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
            QueueCommand::GetLowestUnacked { response } => {
                let result = state.lowest_unacked_offset();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetLowestNotAcked { response } => {
                let result = state.lowest_not_acked_offset();
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
            QueueCommand::GetAckWindowBase { response } => {
                let result = state.ack_window_base();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetAckBitsBytes { response } => {
                let result = state.ack_bits_bytes();
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

    pub async fn command_enqueue(&self, cmd: QueueCommand) -> std::io::Result<()> {
        self.command_sender
            .send(QueueCommandPackage {
                command: cmd,
                enqueued_at: Instant::now(),
            })
            .await
            .map_err(command_send_error)
    }

    pub fn blocking_command_enqueue(&self, cmd: QueueCommand) -> std::io::Result<()> {
        self.command_sender
            .blocking_send(QueueCommandPackage {
                command: cmd,
                enqueued_at: Instant::now(),
            })
            .map_err(command_send_error)
    }

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
                response: Some(tx),
            })
            .await;

        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn enqueue_many(&self, reqs: Vec<EnqueueEventMeta>) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();

        let _ = self
            .command_enqueue(QueueCommand::EnqueueMany {
                reqs,
                response: Some(tx),
            })
            .await;

        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn enqueue_delayed(
        &self,
        offset: Offset,
        not_before: UnixMillis,
    ) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();

        let _ = self
            .command_enqueue(QueueCommand::EnqueueDelayed {
                offset,
                not_before,
                response: Some(tx),
            })
            .await;

        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn enqueue_delayed_many(
        &self,
        reqs: Vec<EnqueueDelayedEventMeta>,
    ) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();

        let _ = self
            .command_enqueue(QueueCommand::EnqueueDelayedMany {
                reqs,
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

    pub async fn ack_many(&self, reqs: Vec<AckEventMeta>) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::AckMany {
                reqs,
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

    pub async fn nack_many(
        &self,
        reqs: Vec<NackEventMeta>,
    ) -> Result<Vec<(Offset, NackOutcome)>, QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::NackMany {
                reqs,
                response: Some(tx),
            })
            .await;
        let outcomes = rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        for (_offset, outcome) in &outcomes {
            if let NackOutcome::RequeuedLater { .. } = outcome {
                self.deadline_waker().notify_one();
                break;
            }
        }
        Ok(outcomes)
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

    pub async fn mark_pending_dlq_many(
        &self,
        offsets: Vec<Offset>,
    ) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::MarkPendingDlq {
                offsets,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn advance_frontier(&self) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::AdvanceFrontier { response: Some(tx) })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn reset(&self) -> Result<(), QueueHandleError> {
        let _owner_operation = self.begin_owner_operation().await?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::Reset { response: Some(tx) })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn set_acked_until(&self, offset: Offset) -> Result<(), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::SetAckedUntil {
                offset,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn set_ack_window(&self, base: Offset, bits: BitVec) -> Result<(), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::SetAckWindow {
                base,
                bits,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?;
        Ok(())
    }

    pub async fn set_ack_window_from_bytes(
        &self,
        base: Offset,
        bits_bytes: Vec<u8>,
    ) -> Result<(), QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::SetAckWindowFromBytes {
                base,
                bits_bytes,
                response: Some(tx),
            })
            .await;
        rx.await.map_err(|_| QueueHandleError::ActorGone)?.unwrap();
        Ok(())
    }

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
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::EncodeSnapshot {
                last_snapshot_event_offset,
                force,
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
        res.map_err(|_| QueueHandleError::ActorGone)?
            .ok_or_else(|| QueueHandleError::SnapshotNotCreated)
    }

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

    pub async fn load_snapshot(&self, data: Vec<u8>) -> Result<SnapshotMeta, QueueHandleError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::LoadSnapshot {
                data,
                response: Some(tx),
            })
            .await;
        let snapmeta = rx
            .await
            .map_err(|_| QueueHandleError::ActorGone)?
            .map_err(|_| QueueHandleError::SnapshotLoadFailed("Failed to load snapshot".into()))?;

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

    pub async fn is_acked(&self, offset: Offset) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::IsAcked {
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

    pub async fn is_inflight_or_acked(&self, offset: Offset) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::IsInflightOrAcked {
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

    pub async fn filter_not_enqueued(
        &self,
        items: Vec<(Offset, Vec<u8>)>,
    ) -> Vec<(Offset, Vec<u8>)> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::FilterNotEnqueued {
                items: items.clone(),
                response: Some(tx),
            })
            .await;
        rx.await.unwrap_or_default()
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

    pub async fn lowest_unacked_offset(&self) -> Offset {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetLowestUnacked { response: Some(tx) })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn lowest_not_acked_offset(&self) -> Offset {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetLowestNotAcked { response: Some(tx) })
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

    pub async fn ack_window_base(&self) -> Offset {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetAckWindowBase { response: Some(tx) })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn ack_bits_bytes(&self) -> Vec<u8> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_enqueue(QueueCommand::GetAckBitsBytes { response: Some(tx) })
            .await;
        rx.await.unwrap_or_default()
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

    pub fn applied_upto(&self) -> Arc<AtomicU64> {
        self.applied_upto.clone()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InflightEntry {
    pub deadline_ts: UnixMillis,
    pub epoch: u32, // optional; ok to keep at 0 for now
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpiryItem {
    deadline_rev: Reverse<UnixMillis>,
    offset: Offset,
    epoch: u32,
}

impl Ord for ExpiryItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is max-heap; we want min-deadline => compare Reverse(deadline) first.
        self.deadline_rev
            .cmp(&other.deadline_rev)
            // tie-breakers to make ordering total/deterministic
            .then_with(|| self.offset.cmp(&other.offset))
            .then_with(|| self.epoch.cmp(&other.epoch))
    }
}
impl PartialOrd for ExpiryItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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
            settled_until: 0,
            ack_window_base: 0,
            ack_bits: BitVec::repeat(false, ACK_WINDOW),
            inflight: BTreeMap::new(),
            pending_dlq: BTreeMap::new(),
            ready: RangeSet::new(),
            retries: HashMap::new(),
            expiry_heap: BinaryHeap::new(),
            delayed_enqueue_heap: BinaryHeap::new(),
            delayed_retry_heap: BinaryHeap::new(),
            min_deadline_hint: None,
            dlq_policy: DLQDiscardPolicy::Discard,
            dlq_discard_max_retries: 5,
            deadline_waker: Arc::new(Notify::new()),
        }
    }

    pub fn new_with_waker(topic: String, partition: u32, deadline_waker: Arc<Notify>) -> Self {
        Self {
            topic,
            partition,
            last_snapshot_timestamp: 0,
            last_snapshot_event_offset: 0,
            settled_until: 0,
            ack_window_base: 0,
            ack_bits: BitVec::repeat(false, ACK_WINDOW),
            inflight: BTreeMap::new(),
            pending_dlq: BTreeMap::new(),
            ready: RangeSet::new(),
            retries: HashMap::new(),
            expiry_heap: BinaryHeap::new(),
            delayed_enqueue_heap: BinaryHeap::new(),
            delayed_retry_heap: BinaryHeap::new(),
            min_deadline_hint: None,
            dlq_policy: DLQDiscardPolicy::Discard,
            dlq_discard_max_retries: 5,
            deadline_waker,
        }
    }

    pub fn debug_info(&self) -> QueueInternalDebugInfo {
        QueueInternalDebugInfo {
            settled_until: self.settled_until,
            ack_window_base: self.ack_window_base,
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
            return self.settled_until;
        }
        if result == 0 {
            return self.settled_until;
        }
        result
    }

    // ---------------- ACK API ----------------

    #[inline]
    pub fn settled_until(&self) -> Offset {
        self.settled_until
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

    /// True if this offset is known ACKed.
    #[inline]
    pub fn is_acked(&self, offset: Offset) -> bool {
        if offset < self.settled_until {
            return true;
        }

        if offset < self.ack_window_base {
            // Out of tracked window and >= acked_until implies "unknown / not acked"
            return false;
        }

        let idx = (offset - self.ack_window_base) as usize;
        idx < ACK_WINDOW && self.ack_bits[idx]
    }

    #[inline]
    pub fn is_inflight(&self, offset: Offset) -> bool {
        self.inflight.contains_key(&offset)
    }

    #[inline]
    pub fn is_inflight_or_acked(&self, offset: Offset) -> bool {
        self.is_acked(offset) || self.is_inflight(offset) || self.pending_dlq.contains_key(&offset)
    }

    #[inline]
    pub fn is_ready(&self, offset: Offset) -> bool {
        self.ready.contains(&offset)
    }

    pub fn filter_not_enqueued<T>(&self, items: &mut Vec<(Offset, T)>) {
        items.retain(|(off, _)| self.ready.contains(off));
    }

    pub fn ack(&mut self, offset: u64) {
        if offset < self.settled_until {
            // already settled
            self.inflight.remove(&offset); // best-effort cleanup
            return;
        }

        // SETTLE beats inflight: always remove inflight if present
        let removed = self.inflight.remove(&offset);
        self.ready.remove(offset..offset + 1);
        self.retries.remove(&offset);
        if removed.is_some() {
            // heap can have stale entries now
            self.recompute_hint_if_needed();
        }

        if offset == self.settled_until {
            self.settled_until += 1;
            self.advance_frontier();
            return;
        }

        let end = self.ack_window_base + ACK_WINDOW as u64;
        if offset < end {
            let idx = (offset - self.ack_window_base) as usize;
            self.ack_bits.set(idx, true);
        } else {
            // far settle: leave for persistence/event log (still applied logically by replay later)
            // (Materialized model can ignore or store it.)
        }
    }

    pub fn release_inflight(&mut self, offset: u64) -> bool {
        if offset < self.settled_until || !self.inflight.contains_key(&offset) {
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
        if offset < self.settled_until {
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
            if o < self.settled_until {
                continue;
            }
            self.inflight.remove(&o);
            self.ready.remove(o..o + 1);
            self.retries.remove(&o);
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
            if (from..end).contains(&offset) && !self.is_acked(offset) {
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
            if (from..end).contains(&offset) && !self.is_acked(offset) {
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
    }

    pub fn ack_many(&mut self, reqs: &[AckEventMeta]) {
        for e in reqs {
            self.ack(e.off);
        }
    }

    /// Slide frontier through contiguous acked bits and slide the window accordingly.
    fn advance_frontier(&mut self) {
        loop {
            // Is acked_until represented inside window?
            if self.settled_until < self.ack_window_base {
                break;
            }
            let idx = (self.settled_until - self.ack_window_base) as usize;
            if idx >= ACK_WINDOW || !self.ack_bits[idx] {
                break;
            }
            self.ack_bits.set(idx, false);
            self.settled_until += 1;
        }

        // Slide the window so its base follows acked_until (keeps bits "near" the frontier).
        let new_base = self.settled_until;
        let delta = new_base.saturating_sub(self.ack_window_base);
        if delta == 0 {
            return;
        }

        if delta as usize >= ACK_WINDOW {
            // We've advanced more than the window; just clear it.
            self.ack_bits.fill(false);
            self.ack_window_base = new_base;
            return;
        }

        // Rotate left by delta and clear new tail.
        self.ack_bits.rotate_left(delta as usize);
        for i in (ACK_WINDOW - delta as usize)..ACK_WINDOW {
            self.ack_bits.set(i, false);
        }
        self.ack_window_base = new_base;
    }

    #[inline]
    pub fn lowest_unacked_offset(&self) -> Offset {
        self.settled_until
    }

    #[inline]
    pub fn lowest_not_acked_offset(&self) -> Offset {
        self.safe_message_truncate_before()
    }

    pub fn get_retries(&self, offset: Offset) -> u32 {
        self.retries.get(&offset).copied().unwrap_or(0)
    }

    pub fn enqueue(&mut self, offset: Offset, retries: u32) {
        // We assume it is only used on messages that have been properly stored earlier
        // TODO: possibly use different checks as ack window has limited trust
        if self.is_acked(offset) {
            return;
        }

        self.ready.insert(offset..offset + 1);
        if retries > 0 {
            self.retries.insert(offset, retries);
        }
    }

    pub fn enqueue_many(&mut self, reqs: &[EnqueueEventMeta]) {
        for req in reqs {
            self.enqueue(req.off, req.retries);
        }
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
        if offset < self.settled_until {
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
        [inflight_min, delayed_enq_min, delayed_retry_min]
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

            let meta = EnqueueEventMeta { off, retries: 0 };
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
    #[inline]
    fn has_history(&self, offset: Offset) -> bool {
        self.ready.contains(&offset)
            || self.inflight.contains_key(&offset)
            || self.retries.contains_key(&offset)
    }

    /// Walk heap until we find a valid inflight entry, rebuild if heap fully stale.
    fn recompute_hint_if_needed(&mut self) -> ExpiryDeadlineOutcome {
        if self.inflight.is_empty() {
            self.min_deadline_hint = None;
            return ExpiryDeadlineOutcome::NoChange;
        }

        while let Some(&(Reverse(ts), off)) = self.expiry_heap.peek() {
            match self.inflight.get(&off).copied() {
                Some(cur) if cur == ts => {
                    self.min_deadline_hint = Some(ts);
                    return ExpiryDeadlineOutcome::Updated(ts);
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
        let new_min = if self.min_deadline_hint > min {
            if let Some(m) = min {
                ExpiryDeadlineOutcome::Updated(m)
            } else {
                ExpiryDeadlineOutcome::NoChange
            }
        } else {
            ExpiryDeadlineOutcome::NoChange
        };
        self.min_deadline_hint = min;
        new_min
    }

    fn recompute_hint_full(&mut self) {
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

        if self.inflight.is_empty() {
            self.min_deadline_hint = None;
            return;
        }

        // rebuild heap from inflight
        self.expiry_heap.clear();
        let mut min = None;
        for (&off, &deadline) in self.inflight.iter() {
            self.expiry_heap.push((Reverse(deadline), off));
            min = Some(min.map_or(deadline, |m: u64| m.min(deadline)));
        }
        self.min_deadline_hint = min;
    }

    // ---------------- Delivery helper ----------------

    /// Find next deliverable offset in [from, upper).
    /// Skips inflight and (bounded) acked entries.
    pub fn next_deliverable(&self, from: Offset, upper: Offset) -> Offset {
        let start = from.max(self.settled_until);

        for range in self.ready.overlapping(&(start..upper)) {
            let range_start = range.start.max(start);
            for off in range_start..range.end.min(upper) {
                if self.inflight.contains_key(&off) {
                    continue;
                }
                if self.is_acked(off) {
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

    // Snapshot/recovery setters (used by recover.rs)
    pub fn set_acked_until(&mut self, v: Offset) {
        self.settled_until = v;
        // keep window consistent with new frontier
        self.ack_window_base = v;
        self.ack_bits.fill(false);
    }

    pub fn set_ack_window(&mut self, base: Offset, bits: BitVec) {
        self.ack_window_base = base;
        self.ack_bits = bits;
        if self.ack_bits.len() != ACK_WINDOW {
            self.ack_bits.resize(ACK_WINDOW, false);
        }
    }

    pub fn ack_window_base(&self) -> u64 {
        self.ack_window_base
    }

    pub fn ack_bits_bytes(&self) -> Vec<u8> {
        // Convert BitVec<usize,Lsb0> → Vec<u8> in a stable packed form
        self.ack_bits
            .as_raw_slice()
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect()
    }

    pub fn set_ack_window_from_bytes(&mut self, base: u64, bytes: &[u8]) -> std::io::Result<()> {
        let word_bytes = std::mem::size_of::<usize>();

        if !bytes.len().is_multiple_of(word_bytes) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad ack window bytes",
            ));
        }

        let mut words = Vec::with_capacity(bytes.len() / word_bytes);
        for chunk in bytes.chunks_exact(word_bytes) {
            words.push(usize::from_le_bytes(chunk.try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "failed to convert chunk to usize",
                )
            })?));
        }

        let mut bits = bitvec::vec::BitVec::<usize, bitvec::order::Lsb0>::from_vec(words);

        if bits.len() < ACK_WINDOW {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ack window too small",
            ));
        }

        bits.truncate(ACK_WINDOW);

        self.ack_window_base = base;
        self.ack_bits = bits;
        Ok(())
    }

    // TODO: Add enqueued state?
    pub fn encode_snapshot(&self, last_snapshot_event_offset: u64) -> Vec<u8> {
        let start = Instant::now();

        // let last_snapshot_timestamp = self.last_snapshot_timestamp;
        // let last_snapshot_event_offset = self.last_snapshot_event_offset;
        // let settled_until = self.settled_until;
        // let ack_window_base = self.ack_window_base;
        // let inflight = self.inflight.clone();
        // let ready = self.ready.clone();
        // let retries = self.retries.clone();
        let bits = self.ack_bits_bytes();

        let mut out = Vec::new();

        // version
        out.extend_from_slice(&FORMAT_VERSION.to_be_bytes());

        // snapshot meta
        out.extend_from_slice(&self.last_snapshot_timestamp.to_be_bytes());
        out.extend_from_slice(&last_snapshot_event_offset.to_be_bytes());

        // acked_until
        out.extend_from_slice(&self.settled_until.to_be_bytes());

        // ack window
        out.extend_from_slice(&self.ack_window_base.to_be_bytes());
        out.extend_from_slice(&(bits.len() as u32).to_be_bytes());
        out.extend_from_slice(&bits);

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
            Ok(a.try_into().unwrap())
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

        self.settled_until = u64::from_be_bytes(take::<8>(&mut bytes)?);
        let base = u64::from_be_bytes(take::<8>(&mut bytes)?);

        // ack window
        let win_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        if bytes.len() < win_len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "ack window"));
        }
        let win = &bytes[..win_len];
        bytes = &bytes[win_len..];
        self.set_ack_window_from_bytes(base, win)?;

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

        if !bytes.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("{} trailing bytes", bytes.len()),
            ));
        }

        // --- enforce invariants ---
        if self.ack_window_base > self.settled_until {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ack window base > frontier",
            ));
        }

        if self.inflight.keys().any(|&o| o < self.settled_until) {
            return Err(Error::new(ErrorKind::InvalidData, "inflight < frontier"));
        }

        for off in self.inflight.keys().copied() {
            self.ready.remove(off..off + 1);
        }

        self.rebuild_derived();

        Ok(SnapshotMeta {
            last_snapshot_event_offset: self.last_snapshot_event_offset,
            last_snapshot_timestamp: self.last_snapshot_timestamp,
        })
    }

    // TODO: Add enqueued state?
    pub fn canonical(&self) -> CanonicalQueueState {
        let mut inflight: Vec<_> = self.inflight.iter().map(|(&o, &d)| (o, d)).collect();
        inflight.sort_unstable();

        CanonicalQueueState {
            acked_until: self.settled_until,
            ack_window_base: self.ack_window_base,
            ack_bits: self.ack_bits_bytes(),
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
            lowest_unacked: self.lowest_unacked_offset(),
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
    pub replication_cache_metrics: ReplicationCacheMetricsSnapshot,
    pub command_metrics: CommandMetricsSnapshot,
    pub uptime_seconds: u64,
}
#[derive(Debug, Serialize)]
pub struct QueueDebugInfo {
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
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
    pub ack_window_base: Offset,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalQueueState {
    pub acked_until: u64,
    pub ack_window_base: u64,
    pub ack_bits: Vec<u8>,
    pub inflight: Vec<(u64, u64)>,
}

impl Default for CanonicalQueueState {
    fn default() -> Self {
        Self {
            acked_until: 0,
            ack_window_base: 0,
            ack_bits: vec![0; ACK_WINDOW / 8],
            inflight: Vec::new(),
        }
    }
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

        s.enqueue(5, 0);
        assert_eq!(s.next_deliverable(0, 10), 5);
    }

    #[test]
    fn inspect_offsets_active_only_returns_tracked_messages() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0);
        s.enqueue(2, 2);
        s.mark_inflight(2, 500);
        s.enqueue_delayed(3, 700);
        s.enqueue(4, 1);
        s.mark_pending_dlq_many(&[4]);
        s.enqueue(5, 0);
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

        s.enqueue(1, 0);
        s.enqueue(2, 0);

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

        s.enqueue(1, 0);
        s.mark_inflight(1, 10);
        s.ack(1);

        s.nack(1, true);

        assert!(s.is_acked(1));
        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert_eq!(s.get_retries(1), 0);
    }

    #[test]
    fn ack_without_enqueue_is_terminal_but_not_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(3);

        assert!(s.is_acked(3));
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
        s.enqueue(5, 0);
        assert!(!s.has_history(5));
    }

    #[test]
    fn enqueue_creates_history_if_not_acked() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0);
        assert!(s.has_history(5));

        s.mark_inflight(5, 10);
        assert!(s.has_history(5));

        s.ack(5);
        assert!(!s.has_history(5));
    }

    #[test]
    fn ack_without_enqueue_is_allowed() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(5);

        assert!(s.is_acked(5));
        assert_eq!(s.settled_until(), 0); // frontier does not advance
        assert!(!s.is_ready(5));
        assert!(!s.is_inflight(5));
    }

    #[test]
    fn enqueue_after_ack_is_ignored() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack(3);
        s.enqueue(3, 0);

        assert!(s.is_acked(3));
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

        s.enqueue(1, 0);
        s.enqueue(2, 0);

        assert!(!s.is_ready(1));
        assert!(!s.is_ready(2));
    }

    #[test]
    fn nack_without_enqueue_does_not_make_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.nack(5, true);

        assert!(!s.is_ready(5));
        assert!(!s.is_inflight(5));
        assert!(!s.is_acked(5));
    }

    #[test]
    fn inflight_update_does_not_make_offset_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0);
        s.mark_inflight(1, 10);
        s.mark_inflight(1, 20);

        assert!(s.is_inflight(1));
        assert!(!s.is_ready(1));
    }

    #[test]
    fn expiry_makes_offset_ready() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(3, 0);
        s.mark_inflight(3, 10);
        s.collect_expired(10, 10);

        assert!(s.is_ready(3));
        assert_eq!(s.next_deliverable(0, 10), 3);
    }

    #[test]
    fn only_ready_offsets_are_delivered() {
        let mut s = QueueInternalState::new("test".into(), 0);

        assert_eq!(s.next_deliverable(0, 10), 10);

        s.enqueue(5, 0);
        assert_eq!(s.next_deliverable(0, 10), 5);
    }

    #[test]
    fn nack_hits_dlq_at_max_retries() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0);
        s.mark_inflight(1, 10);
        for _ in 0..s.dlq_discard_max_retries {
            s.nack(1, true);
            s.mark_inflight(1, 10);
        }

        s.nack(1, true);

        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert!(!s.is_acked(1));

        s.commit_dlq(1);

        assert!(!s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert!(s.is_acked(1));
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
        assert!(s.is_acked(5));
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

        s.enqueue(1, 2);
        s.mark_inflight(1, 100);
        assert!(s.release_inflight(1));

        assert!(s.is_ready(1));
        assert!(!s.is_inflight(1));
        assert_eq!(s.get_retries(1), 2);
    }

    #[test]
    fn expired_then_nacked_is_still_not_acked() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(2, 0);
        s.mark_inflight(2, 10);
        s.collect_expired(10, 10);
        s.nack(2, true);

        assert!(!s.is_acked(2));
        assert_eq!(s.next_deliverable(0, 10), 2);
    }

    #[test]
    fn nack_requeue_increments_retry() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0);
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

        s.enqueue(0, 0);
        s.mark_inflight(0, 100);
        s.nack(0, true);

        assert_eq!(s.next_deliverable(0, 10), 0);
    }

    #[test]
    fn nack_never_acks() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(3, 100);
        s.nack(3, true);

        assert!(!s.is_acked(3));
        assert!(!s.is_inflight(3));
        assert_eq!(s.settled_until(), 0);
    }

    #[test]
    fn expiry_never_acks() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0);
        s.mark_inflight(5, 10);
        assert!(!s.is_acked(5));

        let ex = s.collect_expired(10, 10);
        assert_eq!(ex, vec![5]);

        // Still NOT acked
        assert!(!s.is_acked(5));
        assert_eq!(s.settled_until(), 0);

        // Offset 5 is now eligible again, but ordering is preserved
        assert!(!s.is_inflight(5));
        assert!(!s.is_acked(5));

        // Earliest deliverable is still 0
        let d = s.next_deliverable(0, 100);
        assert_eq!(d, 5);
    }

    #[test]
    fn expired_offset_delivered_after_frontier_advances() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0);
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

        assert!(s.is_acked(7));
        assert!(!s.is_inflight(7));
    }

    #[test]
    fn expiry_does_not_interact_with_ack_window() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(1, 0);
        s.ack(1); // out of order
        s.enqueue(0, 0);
        s.mark_inflight(0, 10);

        let ex = s.collect_expired(10, 10);
        assert_eq!(ex, vec![0]);

        // ACK window still intact
        assert!(s.is_acked(1));
        assert!(!s.is_acked(0));
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

        s.enqueue(10, 0);
        s.mark_inflight(0, 10);
        s.enqueue(1, 0);
        s.mark_inflight(1, 10);
        s.enqueue(12, 0);
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
            s.enqueue(i, 0);
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

        s.enqueue(1, 0);
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
        assert!(s.is_acked(5));
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

        s.enqueue(5, 0);
        s.mark_inflight(5, 10);

        assert!(s.is_inflight(5));
        assert!(!s.is_ready(5));

        s.ack(5);
        assert!(s.is_acked(5));
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

            assert!(!s.is_acked(d));
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

        s.enqueue(1, 5);
        s.enqueue(2, 5);
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

        s.enqueue(1, 0);
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
            assert_eq!(s.is_acked(i), s2.is_acked(i));
        }
    }

    #[test]
    fn snapshot_handles_stale_heap_entries() {
        let mut s = QueueInternalState::new("test".into(), 0);

        for i in 0..1000 {
            s.enqueue(i, 0);
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
            s.enqueue(i, i as u32);
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
        assert!(s.is_acked(2));
        assert!(s.is_acked(4));
        assert!(!s.is_acked(0));

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
            assert!(s.is_acked(o));
        }
    }

    #[test]
    fn ack_removes_inflight() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(10, 0);
        s.mark_inflight(10, 1000);
        assert!(s.is_inflight_or_acked(10));
        assert!(!s.is_acked(10));

        s.ack(10);
        assert!(s.is_acked(10));
        assert!(s.is_inflight_or_acked(10 /* still true via ack */)); // just to show logic
        // More direct:
        assert!(s.is_inflight_or_acked(10));
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn mark_inflight_ignored_if_already_acked() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ack_many(&[0, 1, 2, 3, 4].map(|off| AckEventMeta { off }));
        assert_eq!(s.settled_until(), 5);

        s.enqueue(2, 0);
        s.mark_inflight(2, 123);
        s.enqueue(4, 0);
        s.mark_inflight(4, 123);
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn expiry_hint_tracks_min_deadline_and_handles_updates() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(10, 0);
        s.mark_inflight(10, 500);
        assert_eq!(s.next_expiry_hint(), Some(500));

        s.enqueue(11, 0);
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
            s.enqueue(i, 0);
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

        s.enqueue(1, 0);
        s.mark_inflight(1, 10);
        s.enqueue(2, 0);
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
    fn is_inflight_or_acked_behaves() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.enqueue(5, 0);
        s.mark_inflight(5, 100);
        assert!(s.is_inflight_or_acked(5));
        assert!(!s.is_acked(5));

        s.ack(5);
        assert!(s.is_inflight_or_acked(5));
        assert!(s.is_acked(5));

        // below frontier is always acked
        s.ack(0);
        s.ack(1);
        s.ack(2);
        s.ack(3);
        s.ack(4);
        assert_eq!(s.settled_until(), 6);
        for o in 0..6 {
            assert!(s.is_inflight_or_acked(o));
            assert!(s.is_acked(o));
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
        s.enqueue(1, 0);
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
        s.enqueue(1, 0);
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
        s.enqueue(1, 0);

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
        assert!(!s.is_acked(1)); // NOT acked yet — phase 2 hasn't happened
    }

    #[test]
    fn nack_no_requeue_goes_to_pending_dlq() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0);
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
        assert!(!s.is_acked(1));
    }

    #[test]
    fn nack_unknown_offset_is_noop() {
        let mut s = QueueInternalState::new("t".into(), 0);
        let out = s.nack(42, true);
        assert_eq!(out, NackOutcome::NoOp);
        assert!(!s.is_pending_dlq(42));
        assert!(!s.is_acked(42));
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
        s.enqueue(0, 0);
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
        assert!(s.is_acked(0));
        assert_eq!(s.settled_until(), 1);
    }

    #[test]
    fn commit_dlq_unknown_offset_is_idempotent() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.commit_dlq(99); // never been pending
        assert!(!s.is_acked(99));
        assert_eq!(s.settled_until(), 0);
    }

    #[test]
    fn commit_dlq_twice_is_idempotent() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0);
        s.mark_inflight(0, 100);
        s.nack(0, false);

        s.commit_dlq(0);
        s.commit_dlq(0); // second call: pending_dlq no longer contains, no-op

        assert!(s.is_acked(0));
        assert_eq!(s.settled_until(), 1);
    }

    #[test]
    fn discard_pending_dlq_acks_locally() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0);
        s.mark_inflight(0, 100);
        s.nack(0, false);

        s.discard_pending_dlq(0);

        assert!(!s.is_pending_dlq(0));
        assert!(s.is_acked(0));
    }

    #[test]
    fn pending_dlq_blocks_msg_truncation() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(5, 0);
        s.mark_inflight(5, 100);
        s.nack(5, false); // pending_dlq = {5}

        // Frontier is 0, but pending_dlq holds 5, so safe truncation must not pass 5.
        assert!(s.safe_message_truncate_before() <= 5);
        assert!(s.is_inflight_or_acked(5)); // delivery must skip it
    }

    #[test]
    fn pending_dlq_does_not_count_as_ready() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(3, 0);
        s.mark_inflight(3, 100);
        s.nack(3, false);

        assert!(!s.is_ready(3));
        assert_eq!(s.next_deliverable(0, 100), 100); // nothing deliverable
    }

    #[test]
    fn poll_ready_skips_pending_dlq() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(0, 0);
        s.enqueue(1, 0);
        s.enqueue(2, 0);
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
        s.enqueue(1, 0);
        s.enqueue(2, 0);
        s.mark_inflight(1, 100); // 1 inflight, 2 ready

        s.mark_pending_dlq_many(&[1, 2]);

        assert!(s.is_pending_dlq(1));
        assert!(s.is_pending_dlq(2));
        assert!(!s.is_inflight(1));
        assert!(!s.is_ready(2));
        assert!(!s.is_acked(1));
        assert!(!s.is_acked(2));
    }

    #[test]
    fn apply_declare_updates_only_provided_fields() {
        let mut s = QueueInternalState::new("t".into(), 0);
        let original_max = s.dlq_discard_max_retries;

        s.apply_declare(&DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
            dlq_max_retries: None,
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
        s.enqueue(1, 0);
        s.enqueue(2, 0);
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
        s.enqueue(0, 0);
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
        s.enqueue(1, 0);
        s.mark_inflight(1, 500); // inflight expiry
        s.enqueue_delayed(2, 300); // delayed enqueue
        // (when delayed_retry exists, add one at 400 too)

        assert_eq!(s.next_expiry_hint(), Some(300));
    }

    #[test]
    fn next_expiry_hint_when_inflight_is_earliest() {
        let mut s = QueueInternalState::new("t".into(), 0);
        s.enqueue(1, 0);
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
}
