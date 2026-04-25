use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::thread;

use bitvec::vec::BitVec;
use keratin_log::Keratin;
use keratin_log::util::unix_millis;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::stroma::TaskGroup;

pub type Offset = u64;
pub type UnixMillis = u64;

pub const ACK_WINDOW: usize = 16384; // fixed bounded memory

pub const FORMAT_VERSION: u64 = 1;

#[derive(Debug, Clone)]
pub struct DLQDiscordSettings {
    pub max_retries: u32,
}

impl Default for DLQDiscordSettings {
    fn default() -> Self {
        Self { max_retries: 5 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CustomDLQ {
    pub tp: String,
    pub part: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DLQDiscardPolicy {
    #[default]
    Discard,
    GlobalDQL,
    CustomDQL(CustomDLQ), // tp, part
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

    // ----- Ready -----
    // offset -> retries
    ready: BTreeSet<Offset>,                  // readiness only
    retries: hashbrown::HashMap<Offset, u32>, // retry metadata only

    // min-heap via Reverse(deadline), contains stale entries, validated against inflight map
    expiry_heap: BinaryHeap<(Reverse<UnixMillis>, Offset)>,

    // best-effort hint
    min_deadline_hint: Option<UnixMillis>,

    // what to do on DLQ
    dlq_policy: DLQDiscardPolicy,

    // when to send to DLQ
    dlq_discard_max_retries: u32,
}

// Every QueueState method will be processed by a relevant Command sequentially on a single task, so we don't need to worry about concurrent mutations or complex locking.
#[derive(Debug)]
pub enum QueueCommand {
    Shutdown {
        response: Option<tokio::sync::oneshot::Sender<()>>,
    },
    Enqueue {
        offset: Offset,
        retries: u32,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset, retries
    MarkInflight {
        offset: Offset,
        deadline: UnixMillis,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset, deadline
    MarkInflightBatch {
        entries: Vec<(Offset, UnixMillis)>,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // entries
    ClearInflight {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset
    Ack {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset
    AckBatch {
        offsets: Vec<Offset>,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offsets
    Nack {
        offset: Offset,
        requeue: bool,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset, requeue?
    DeadLetter {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset
    Reject {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset

    AdvanceFrontier {
        response: Option<tokio::sync::oneshot::Sender<()>>,
    },
    Reset {
        response: Option<tokio::sync::oneshot::Sender<()>>,
    },
    SetAckedUntil {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // offset
    SetAckWindow {
        base: Offset,
        bits: BitVec,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // base, bits
    SetAckWindowFromBytes {
        base: Offset,
        bits_bytes: Vec<u8>,
        response: Option<tokio::sync::oneshot::Sender<std::io::Result<()>>>,
    }, // base, bits_bytes
    LoadInflight {
        entries: Vec<(Offset, UnixMillis)>,
        response: Option<tokio::sync::oneshot::Sender<()>>,
    }, // entries
    EncodeSnapshot {
        last_snapshot_event_offset: u64,
        response: Option<tokio::sync::oneshot::Sender<Option<Vec<u8>>>>,
    },
    LoadSnapshot {
        data: Vec<u8>,
        response: Option<tokio::sync::oneshot::Sender<std::io::Result<SnapshotMeta>>>,
    }, // data

    IsAcked {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<bool>>,
    }, // offset
    IsInflight {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<bool>>,
    }, // offset
    IsInflightOrAcked {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<bool>>,
    }, // offset
    IsReady {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<bool>>,
    }, // offset
    FilterNotEnqueued {
        items: Vec<(Offset, Vec<u8>)>,
        response: Option<tokio::sync::oneshot::Sender<Vec<(Offset, Vec<u8>)>>>,
    }, // items
    GetRetries {
        offset: Offset,
        response: Option<tokio::sync::oneshot::Sender<u32>>,
    }, // offset
    GetNextOffset {
        response: Option<tokio::sync::oneshot::Sender<Offset>>,
    },
    GetSettledUntil {
        response: Option<tokio::sync::oneshot::Sender<Offset>>,
    },
    PollReadyAndMark {
        max: usize,
        lease_deadline: UnixMillis,
        response: Option<tokio::sync::oneshot::Sender<Vec<(Offset, u32)>>>,
    }, // max, lease_deadline
    GetLowestUnacked {
        response: Option<tokio::sync::oneshot::Sender<Offset>>,
    },
    GetLowestNotAcked {
        response: Option<tokio::sync::oneshot::Sender<Offset>>,
    },
    GetNextDeliverable {
        from: Offset,
        upper: Offset,
        response: Option<tokio::sync::oneshot::Sender<Option<Offset>>>,
    }, // from, upper
    GetInflightLen {
        response: Option<tokio::sync::oneshot::Sender<usize>>,
    },
    GetNextExpiryHint {
        response: Option<tokio::sync::oneshot::Sender<Option<UnixMillis>>>,
    },
    GetAckWindowBase {
        response: Option<tokio::sync::oneshot::Sender<Offset>>,
    },
    GetAckBitsBytes {
        response: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    },
    GetCanonicalQueueState {
        response: Option<tokio::sync::oneshot::Sender<CanonicalQueueState>>,
    },
    GetStatusReport {
        response: Option<tokio::sync::oneshot::Sender<QueueStatusReport>>,
    },
    CollectExpired {
        now: UnixMillis,
        max: usize,
        response: Option<tokio::sync::oneshot::Sender<Vec<Offset>>>,
    }, // now, max

    DumpInflight {
        response: Option<tokio::sync::oneshot::Sender<Vec<(Offset, UnixMillis)>>>,
    },
}

impl QueueCommand {
    pub fn prio(&self) -> CommandPrio {
        match self {
            // === Lifecycle / control — must preempt everything ===
            QueueCommand::Shutdown { .. } => CommandPrio::Express,

            // === Recovery / loading — one-shot at startup, not in contention ===
            // Put at Express so they can't be blocked if something else is in the queue
            QueueCommand::LoadSnapshot { .. } => CommandPrio::Express,
            QueueCommand::SetAckedUntil { .. } => CommandPrio::Express,
            QueueCommand::SetAckWindow { .. } => CommandPrio::Express,
            QueueCommand::SetAckWindowFromBytes { .. } => CommandPrio::Express,
            QueueCommand::LoadInflight { .. } => CommandPrio::Express,
            QueueCommand::Reset { .. } => CommandPrio::Express,

            // === Observability / admin queries — fast, cheap, must stay responsive ===
            QueueCommand::GetStatusReport { .. } => CommandPrio::Express,
            QueueCommand::GetInflightLen { .. } => CommandPrio::Express,
            QueueCommand::GetSettledUntil { .. } => CommandPrio::Express,
            QueueCommand::GetLowestUnacked { .. } => CommandPrio::Express,
            QueueCommand::GetLowestNotAcked { .. } => CommandPrio::Express,
            QueueCommand::GetNextOffset { .. } => CommandPrio::Express,
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
            QueueCommand::MarkInflightBatch { .. } => CommandPrio::High,

            // === Settlement — finishes in-progress work, matches delivery priority ===
            // Ack/nack completing work frees up consumer prefetch slots; if this is
            // low priority, consumers stall waiting for acks to register
            QueueCommand::Ack { .. } => CommandPrio::High,
            QueueCommand::AckBatch { .. } => CommandPrio::High,
            QueueCommand::Nack { .. } => CommandPrio::High,
            QueueCommand::Reject { .. } => CommandPrio::High,
            QueueCommand::DeadLetter { .. } => CommandPrio::High,

            // === Producer path — must accept writes but yield to delivery/settlement ===
            // Under overload, throttling publish is correct. Natural backpressure upstream.
            QueueCommand::Enqueue { .. } => CommandPrio::Medium,

            // === Background maintenance — wait for quiet periods ===
            QueueCommand::CollectExpired { .. } => CommandPrio::Low,
            QueueCommand::ClearInflight { .. } => CommandPrio::Low,
            QueueCommand::AdvanceFrontier { .. } => CommandPrio::Low,

            // === Snapshots — lowest priority, run only when other work is drained ===
            // This assumes snapshot encoding can tolerate being delayed under load.
            // If you need snapshots to run on schedule regardless of load, raise this.
            QueueCommand::EncodeSnapshot { .. } => CommandPrio::SuperLow,
        }
    }
}

pub enum CommandPrio {
    Express,
    High,
    Medium,
    Low,
    SuperLow,
}

pub struct QueueCommandPackage {
    pub command: QueueCommand,
}

impl QueueCommand {
    pub fn into_package(self) -> QueueCommandPackage {
        QueueCommandPackage { command: self }
    }
}

#[derive(Debug)]
pub struct SnapshotMeta {
    pub last_snapshot_timestamp: u64,
    pub last_snapshot_event_offset: u64,
}

#[derive(Debug)]
pub struct CommandReceiver {
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
    pub async fn recv(&mut self) -> Option<QueueCommandPackage> {
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
                    match pkg {
                        Some(p) => return Some(p),
                        None => {} // closed, try others
                    }
                }
                pkg = self.high_prio.recv() => {
                    match pkg {
                        Some(p) => return Some(p),
                        None => {}
                    }
                }
                pkg = self.medium_prio.recv() => {
                    match pkg {
                        Some(p) => return Some(p),
                        None => {}
                    }
                }
                pkg = self.low_prio.recv() => {
                    match pkg {
                        Some(p) => return Some(p),
                        None => {}
                    }
                }
                pkg = self.super_low_prio.recv() => {
                    match pkg {
                        Some(p) => return Some(p),
                        None => {}
                    }
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
}

#[derive(Debug, Clone)]
pub struct CommandSender {
    express: mpsc::Sender<QueueCommandPackage>,
    high_prio: mpsc::Sender<QueueCommandPackage>,
    medium_prio: mpsc::Sender<QueueCommandPackage>,
    low_prio: mpsc::Sender<QueueCommandPackage>,
    super_low_prio: mpsc::Sender<QueueCommandPackage>,
}

impl CommandSender {
    pub fn channel_pair() -> (CommandSender, CommandReceiver) {
        let (express_tx, express_rx) = mpsc::channel(2048);
        let (high_tx, high_rx) = mpsc::channel(16384);
        let (medium_tx, medium_rx) = mpsc::channel(8192);
        let (low_tx, low_rx) = mpsc::channel(2048);
        let (super_low_tx, super_low_rx) = mpsc::channel(512);

        let sender = CommandSender {
            express: express_tx,
            high_prio: high_tx,
            medium_prio: medium_tx,
            low_prio: low_tx,
            super_low_prio: super_low_tx,
        };

        let receiver = CommandReceiver {
            express: express_rx,
            high_prio: high_rx,
            medium_prio: medium_rx,
            low_prio: low_rx,
            super_low_prio: super_low_rx,
        };

        (sender, receiver)

    }

    pub async fn send(&self, pkg: QueueCommandPackage) -> Result<(), mpsc::error::SendError<QueueCommandPackage>> {
        match pkg.command.prio() {
            CommandPrio::Express => self.express.send(pkg).await,
            CommandPrio::High => self.high_prio.send(pkg).await,
            CommandPrio::Medium => self.medium_prio.send(pkg).await,
            CommandPrio::Low => self.low_prio.send(pkg).await,
            CommandPrio::SuperLow => self.super_low_prio.send(pkg).await,
        }
    }

    pub fn blocking_send(&self, pkg: QueueCommandPackage) -> Result<(), mpsc::error::SendError<QueueCommandPackage>> {
        match pkg.command.prio() {
            CommandPrio::Express => self.express.blocking_send(pkg),
            CommandPrio::High => self.high_prio.blocking_send(pkg),
            CommandPrio::Medium => self.medium_prio.blocking_send(pkg),
            CommandPrio::Low => self.low_prio.blocking_send(pkg),
            CommandPrio::SuperLow => self.super_low_prio.blocking_send(pkg),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueueHandle {
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
}

impl QueueHandle {
    pub fn init(
        topic: String,
        partition: u32,
        group: Option<String>,
        msg_log: Arc<Keratin>,
        event_log: Arc<Keratin>,
        task_group: Arc<TaskGroup>,
    ) -> Self {
        let (tx, mut rx) = CommandSender::channel_pair();
        let dirty_since_snapshot = Arc::new(AtomicBool::new(false));

        let topic_clone = topic.clone();
        let dirty_since_snapshot_loop = dirty_since_snapshot.clone();

        let applied_upto = Arc::new(AtomicU64::new(0));
        let last_snapshot_timestamp = Arc::new(AtomicU64::new(0));
        let last_snapshot_event_offset = Arc::new(AtomicU64::new(0));
        let creating_snapshot = Arc::new(AtomicBool::new(false));

        let task_group_clone = task_group.clone();

        let result = Self {
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
            task_group,
        };

        let handle = result.clone();

        task_group_clone.spawn("queue control", async move {
            let mut state = QueueInternalState::new(topic_clone, partition);

            while let Some(pkg) = rx.recv().await {
                let cmd = pkg.command;
                let (processed, dirty) = Self::process_command(&mut state, cmd, &handle);
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

    fn process_command(
        state: &mut QueueInternalState,
        cmd: QueueCommand,
        handle: &QueueHandle,
    ) -> (Option<bool>, bool) {
        let mut dirty = false;

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
            QueueCommand::MarkInflightBatch { entries, response } => {
                state.mark_inflight_batch(&entries);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !entries.is_empty();
            }
            QueueCommand::ClearInflight { offset, response } => {
                state.clear_inflight(offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::Ack { offset, response } => {
                state.ack(offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::AckBatch { offsets, response } => {
                state.ack_batch(&offsets);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !offsets.is_empty();
            }
            QueueCommand::Nack {
                offset,
                requeue,
                response,
            } => {
                state.nack(offset, requeue);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::DeadLetter { offset, response } => {
                state.dead_letter(offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
            }
            QueueCommand::Reject { offset, response } => {
                state.reject(offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = true;
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
            QueueCommand::LoadInflight { entries, response } => {
                state.load_inflight(&entries);
                if let Some(r) = response {
                    let _ = r.send(());
                }
                dirty = !entries.is_empty();
            }
            QueueCommand::EncodeSnapshot {
                last_snapshot_event_offset,
                response,
            } => {
                let start = Instant::now();
                // state.expiry_heap.shrink_to_fit();
                // state.retries.shrink_to_fit();
                // state.ready = state.ready.clone().into_iter().collect();
                // state.inflight = state.inflight.clone().into_iter().collect();
                state.last_snapshot_event_offset = last_snapshot_event_offset;
                state.last_snapshot_timestamp = unix_millis();
                let start2 = Instant::now();
                let state = state.clone();
                let handle = handle.clone();
                tracing::info!(
                    "ms taken cloning on encode snapshot command: {}",
                    start2.elapsed().as_millis()
                );
                tokio::task::spawn_blocking(move || {
                    if let Some(r) = response {
                        if handle.dirty_snapshot() {
                            let result = state.encode_snapshot(last_snapshot_event_offset);
                            let _ = r.send(Some(result));
                        } else {
                            let _ = r.send(None);
                        }
                    }
                });
                tracing::info!(
                    "ms taken on encode snapshot command: {}",
                    start.elapsed().as_millis()
                );
            }
            QueueCommand::LoadSnapshot { data, response } => {
                let result = state.load_snapshot(&data);
                if let Some(r) = response {
                    let _ = r.send(result);
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
                response,
            } => {
                let result = state.poll_ready_and_mark(max, lease_deadline);
                dirty = !result.is_empty();
                if let Some(r) = response {
                    let _ = r.send(result);
                }
            }
            QueueCommand::GetNextOffset { response } => {
                let result = state.next_offset();
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

        (Some(true), dirty) // signal to continue processing
    }

    pub async fn command_enqueue(&self, cmd: QueueCommand) -> std::io::Result<()> {
        let _ = self
            .command_sender
            .send(QueueCommandPackage { command: cmd }).await;
        Ok(())
    }

    pub fn blocking_command_enqueue(&self, cmd: QueueCommand) -> std::io::Result<()> {
        let _ = self
            .command_sender
            .blocking_send(QueueCommandPackage { command: cmd });
        Ok(())
    }

    pub async fn enqueue(&self, offset: Offset, retries: u32) {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = self.command_enqueue(QueueCommand::Enqueue {
            offset,
            retries,
            response: Some(tx),
        }).await;

        rx.await.unwrap();
    }

    pub async fn mark_inflight(&self, offset: Offset, deadline: UnixMillis) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::MarkInflight {
            offset,
            deadline,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn mark_inflight_batch(&self, entries: Vec<(Offset, UnixMillis)>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::MarkInflightBatch {
            entries,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn clear_inflight(&self, offset: Offset) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::ClearInflight {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn ack(&self, offset: Offset) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::Ack {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn ack_batch(&self, offsets: Vec<Offset>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::AckBatch {
            offsets,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn nack(&self, offset: Offset, requeue: bool) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::Nack {
            offset,
            requeue,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn dead_letter(&self, offset: Offset) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::DeadLetter {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn reject(&self, offset: Offset) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::Reject {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn advance_frontier(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::AdvanceFrontier { response: Some(tx) }).await;
        rx.await.unwrap();
    }

    pub async fn reset(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::Reset { response: Some(tx) }).await;
        rx.await.unwrap();
    }

    pub async fn set_acked_until(&self, offset: Offset) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::SetAckedUntil {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn set_ack_window(&self, base: Offset, bits: BitVec) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::SetAckWindow {
            base,
            bits,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn set_ack_window_from_bytes(&self, base: Offset, bits_bytes: Vec<u8>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::SetAckWindowFromBytes {
            base,
            bits_bytes,
            response: Some(tx),
        }).await;
        rx.await.unwrap().unwrap();
    }

    pub async fn load_inflight(&self, entries: Vec<(Offset, UnixMillis)>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::LoadInflight {
            entries,
            response: Some(tx),
        }).await;
        rx.await.unwrap();
    }

    pub async fn encode_snapshot(&self, last_snapshot_event_offset: u64) -> Option<Vec<u8>> {
        self.creating_snapshot
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::EncodeSnapshot {
            last_snapshot_event_offset,
            response: Some(tx),
        }).await;
        let res = rx.await;
        self.last_snapshot_event_offset.store(
            last_snapshot_event_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_snapshot_timestamp
            .store(unix_millis(), std::sync::atomic::Ordering::Relaxed);
        self.creating_snapshot
            .store(false, std::sync::atomic::Ordering::SeqCst);
        res.ok().flatten()
    }

    pub async fn load_snapshot(&self, data: Vec<u8>) -> std::io::Result<SnapshotMeta> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::LoadSnapshot {
            data,
            response: Some(tx),
        }).await;
        let snapmeta = rx
            .await
            .unwrap_or_else(|_| Err(std::io::Error::other("Snapshot load failed")))?;

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
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::IsAcked {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or(false)
    }

    pub async fn is_inflight(&self, offset: Offset) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::IsInflight {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or(false)
    }

    pub async fn is_inflight_or_acked(&self, offset: Offset) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::IsInflightOrAcked {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or(false)
    }

    pub async fn is_ready(&self, offset: Offset) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::IsReady {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or(false)
    }

    pub async fn filter_not_enqueued(
        &self,
        items: Vec<(Offset, Vec<u8>)>,
    ) -> Vec<(Offset, Vec<u8>)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::FilterNotEnqueued {
            items: items.clone(),
            response: Some(tx),
        }).await;
        rx.await.unwrap_or_default()
    }

    pub async fn retries(&self, offset: Offset) -> u32 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetRetries {
            offset,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn settled_until(&self) -> Offset {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetSettledUntil { response: Some(tx) }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn next_offset(&self) -> Offset {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetNextOffset { response: Some(tx) }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn poll_ready_and_mark(
        &self,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Vec<(Offset, u32)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::PollReadyAndMark {
            max,
            lease_deadline,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or_default()
    }

    pub async fn lowest_unacked_offset(&self) -> Offset {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetLowestUnacked { response: Some(tx) }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn lowest_not_acked_offset(&self) -> Offset {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetLowestNotAcked { response: Some(tx) }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn next_deliverable(&self, from: Offset, upper: Offset) -> Option<Offset> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetNextDeliverable {
            from,
            upper,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or(None)
    }

    pub async fn inflight_len(&self) -> usize {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetInflightLen { response: Some(tx) }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn next_expiry_hint(&self) -> Option<UnixMillis> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetNextExpiryHint { response: Some(tx) }).await;
        rx.await.unwrap_or(None)
    }

    pub async fn ack_window_base(&self) -> Offset {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetAckWindowBase { response: Some(tx) }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn ack_bits_bytes(&self) -> Vec<u8> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetAckBitsBytes { response: Some(tx) }).await;
        rx.await.unwrap_or_default()
    }

    pub async fn canonical(&self) -> CanonicalQueueState {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetCanonicalQueueState { response: Some(tx) }).await;
        rx.await.unwrap_or_default()
    }

    pub async fn status_report(&self) -> Result<QueueStatusReport, std::io::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::GetStatusReport { response: Some(tx) }).await;
        rx.await
            .map_err(|err| std::io::Error::other(format!("Status report failed: {err}")))
    }

    pub async fn collect_expired(&self, now: UnixMillis, max: usize) -> Vec<Offset> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::CollectExpired {
            now,
            max,
            response: Some(tx),
        }).await;
        rx.await.unwrap_or_default()
    }

    pub async fn dump_inflight(&self) -> Vec<(Offset, UnixMillis)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::DumpInflight { response: Some(tx) }).await;
        rx.await.unwrap_or_default()
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.command_enqueue(QueueCommand::Shutdown { response: Some(tx) }).await;
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
            ready: BTreeSet::new(),
            retries: hashbrown::HashMap::new(),
            expiry_heap: BinaryHeap::new(),
            min_deadline_hint: None,
            dlq_policy: DLQDiscardPolicy::Discard,
            dlq_discard_max_retries: 5,
        }
    }

    #[inline]
    pub fn next_offset(&self) -> Offset {
        self.ready.last().copied().unwrap_or(0)
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
        self.ready.first().copied()
    }

    #[inline]
    pub fn min_inflight_offset(&self) -> Option<Offset> {
        self.inflight.keys().copied().min()
    }

    #[inline]
    pub fn safe_message_truncate_before(&self) -> Offset {
        let min_ready = self.ready.first().copied().unwrap_or(u64::MAX);
        let min_inflight = self.inflight.keys().copied().min().unwrap_or(u64::MAX);

        let result = min_ready.min(min_inflight);

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

    pub fn iter_ready_from(&self, from: Offset) -> impl Iterator<Item = Offset> + '_ {
        self.ready.range(from..).copied()
    }

    pub fn poll_ready_and_mark(
        &mut self,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Vec<(Offset, u32)> {
        tracing::debug!(
            "Polling ready for ({}, {}), settled_until={}",
            self.topic,
            self.partition,
            self.settled_until()
        );
        let mut offs = Vec::new();
        for off in self.iter_ready_from(self.settled_until()) {
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
        self.is_acked(offset) || self.is_inflight(offset)
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
        self.ready.remove(&offset);
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

    // TODO: move non storage related logic elsewhere?
    pub fn nack(&mut self, offset: u64, requeue: bool) {
        if offset < self.settled_until {
            self.inflight.remove(&offset);
            return;
        }

        if !requeue {
            self.ready.remove(&offset);
            self.retries.remove(&offset);
            self.dead_letter(offset);
            return;
        }

        let exists = self.inflight.contains_key(&offset)
            || self.ready.contains(&offset);
        if !exists {
            return;
        }

        self.inflight.remove(&offset);

        let retries = self.retries.entry(offset).or_insert(0);
        if *retries >= self.dlq_discard_max_retries {
            self.ready.remove(&offset);
            self.retries.remove(&offset);
            self.dead_letter(offset);
            return;
        }

        *retries += 1;
        self.ready.insert(offset);
        self.recompute_hint_if_needed();
    }

    // TODO: move non storage related logic elsewhere?
    pub fn dead_letter(&mut self, offset: u64) {
        // let global_dlq = &stroma.global_dlq;

        // // Do fallible parts first
        // let dlq = match &self.dlq_policy {
        //     DLQDiscardPolicy::Discard => {
        //         // no DLQ; just discard
        //     return Ok(());
        //     }
        //     DLQDiscardPolicy::GlobalDQL => match global_dlq.blocking_read().as_ref().map(|d| d.to_custom_dlq()) {
        //         Some(c) => c,
        //         None => {
        //             // no global DLQ configured; discard
        //             return Ok(());
        //         }
        //     },
        //     DLQDiscardPolicy::CustomDQL(c) => c.clone(),
        // };

        // // We enqueue the message to the DLQ topic/partition.
        // let msg = match stroma.fetch_message_by_offset(&self.topic, self.partition, offset).await? {
        //     Some(msg) => msg,
        //     None => {
        //         // message not found; cannot DLQ
        //         return Ok(());
        //     }
        // };
        // let (completion, _) = KeratinAppendCompletion::pair();
        // let bytes = Message::encode_msg(&msg, offset).map_err(|e| StromaError::Corruption(e.to_string()))?;
        // stroma.append_message(&dlq.tp, dlq.part, &bytes, completion).await?;

        // TODO: WIP
        if offset < self.settled_until {
            // already acked
            self.inflight.remove(&offset); // best-effort cleanup
            return;
        }

        // beats inflight: always remove inflight if present
        let removed = self.inflight.remove(&offset);
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
            // leave for persistence/event log (still applied logically by replay later)
            // (Materialized model can ignore or store it.)
        }
    }

    pub fn reject(&mut self, offset: u64) {
        // TODO: WIP - currently same as nack, must reque into dql which must be implemented
        if offset < self.settled_until {
            // already acked
            self.inflight.remove(&offset); // best-effort cleanup
            return;
        }

        // NACK beats inflight: always remove inflight if present
        let removed = self.inflight.remove(&offset);
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
            // far nack: leave for persistence/event log (still applied logically by replay later)
            // (Materialized model can ignore or store it.)
        }
    }

    pub fn ack_batch(&mut self, offsets: &[Offset]) {
        for &o in offsets {
            self.ack(o);
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
        if self.is_acked(offset) {
            return;
        }

        self.ready.insert(offset);
        if retries > 0 {
            self.retries.insert(offset, retries);
        }
    }

    // ---------------- Inflight API ----------------

    /// Mark inflight for an offset. If offset is already ACKed, no-op.
    pub fn mark_inflight(&mut self, offset: Offset, deadline: UnixMillis) {
        // Below frontier is always acked
        if offset < self.settled_until {
            return;
        }

        // Case 1: update existing inflight lease
        if let Some(cur) = self.inflight.get_mut(&offset) {
            *cur = deadline;
            self.expiry_heap.push((Reverse(deadline), offset));
            self.min_deadline_hint = Some(match self.min_deadline_hint {
                None => deadline,
                Some(m) => m.min(deadline),
            });
            return;
        }

        // Case 2: initial delivery — must be READY
        if !self.ready.remove(&offset) {
            return;
        }

        self.inflight.insert(offset, deadline);
        self.expiry_heap.push((Reverse(deadline), offset));
        self.min_deadline_hint = Some(match self.min_deadline_hint {
            None => deadline,
            Some(cur) => cur.min(deadline),
        });
    }

    pub fn mark_inflight_batch(&mut self, entries: &[(Offset, UnixMillis)]) {
        for &(o, d) in entries {
            self.mark_inflight(o, d);
        }
    }

    pub fn mark_inflight_uniform_deadline(&mut self, offsets: &[Offset], deadline: UnixMillis) {
        for &o in offsets {
            self.mark_inflight(o, deadline);
        }
    }

    pub fn mark_inflight_uniform_deadline_with_retries(&mut self, offsets: &[(Offset, u32)], deadline: UnixMillis) {
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
        // self.recompute_hint_full();
        // self.min_deadline_hint
        while let Some(top) = self.expiry_heap.peek() {
            let (Reverse(deadline), offset) = top;

            match self.inflight.get(offset) {
                Some(d) if *d == *deadline => {
                    return Some(*deadline);
                }
                _ => {
                    // stale heap entry -> drop it
                    self.expiry_heap.pop();
                }
            }
        }

        None
    }

    pub fn collect_expired(&mut self, now: UnixMillis, max: usize) -> Vec<Offset> {
        let mut out = Vec::new();

        while let Some(&(Reverse(deadline), off)) = self.expiry_heap.peek() {
            if deadline > now || out.len() >= max {
                break;
            }

            self.expiry_heap.pop();

            // validate against inflight (skip stale heap entries)
            match self.inflight.get(&off).copied() {
                Some(cur_deadline) if cur_deadline == deadline => {
                    self.inflight.remove(&off);
                    self.ready.insert(off);
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

    /// Walk heap until we find a valid inflight entry; rebuild if heap fully stale.
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
        let mut off = from.max(self.settled_until);

        while off < upper {
            if !self.ready.contains(&off) {
                off += 1;
                continue;
            }

            if self.inflight.contains_key(&off) {
                off += 1;
                continue;
            }

            if self.is_acked(off) {
                off += 1;
                continue;
            }

            return off;
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
        *self = QueueInternalState::new(self.topic.clone(), self.partition);
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

    pub fn load_inflight(&mut self, entries: &[(Offset, UnixMillis)]) {
        self.inflight.clear();
        self.expiry_heap.clear();
        self.min_deadline_hint = None;
        for &(o, d) in entries {
            self.inflight.insert(o, d);
            self.expiry_heap.push((Reverse(d), o));
            self.min_deadline_hint = Some(match self.min_deadline_hint {
                None => d,
                Some(cur) => cur.min(d),
            });
        }
        // ensure heap top is valid
        self.recompute_hint_if_needed();
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
        out.extend_from_slice(&self.last_snapshot_event_offset.to_be_bytes());

        // acked_until
        out.extend_from_slice(&self.settled_until.to_be_bytes());

        // ack window
        out.extend_from_slice(&self.ack_window_base.to_be_bytes());
        out.extend_from_slice(&(bits.len() as u32).to_be_bytes());
        out.extend_from_slice(&bits);

        // inflight
        out.extend_from_slice(&(self.inflight.len() as u32).to_be_bytes());
        for (&off, e) in self.inflight.iter() {
            out.extend_from_slice(&off.to_be_bytes());
            out.extend_from_slice(&e.to_be_bytes());
        }
        out.extend_from_slice(&(self.retries.len() as u32).to_be_bytes());
        for (&off, e) in self.retries.iter() {
            out.extend_from_slice(&off.to_be_bytes());
            out.extend_from_slice(&e.to_be_bytes());
        }
        out.extend_from_slice(&(self.ready.len() as u32).to_be_bytes());
        for off in self.ready.iter() {
            out.extend_from_slice(&off.to_be_bytes());
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

        const VERSION_SIZE: usize = size_of::<u64>();
        let version = u64::from_be_bytes(take::<VERSION_SIZE>(&mut bytes)?);
        if version != FORMAT_VERSION {
            return Err(Error::new(ErrorKind::InvalidData, "unsupported version"));
        }

        self.reset();

        self.last_snapshot_timestamp = u64::from_be_bytes(take::<8>(&mut bytes)?);
        self.last_snapshot_event_offset = u64::from_be_bytes(take::<8>(&mut bytes)?);

        self.settled_until = u64::from_be_bytes(take::<8>(&mut bytes)?);
        let base = u64::from_be_bytes(take::<8>(&mut bytes)?);

        let win_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        if bytes.len() < win_len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "ack window"));
        }
        let win = &bytes[..win_len];
        bytes = &bytes[win_len..];
        self.set_ack_window_from_bytes(base, win)?;

        let inflight_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        for _ in 0..inflight_len {
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let dl = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.inflight.insert(off, dl);
        }

        let retries_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        for _ in 0..retries_len {
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let retries = u32::from_be_bytes(take::<4>(&mut bytes)?);
            self.retries.insert(off, retries);
        }

        let ready_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        for _ in 0..ready_len {
            let off: u64 = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.ready.insert(off);
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

                DLQDiscardPolicy::CustomDQL(CustomDLQ { tp, part })
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "dlq tag")),
        };

        self.dlq_discard_max_retries = u32::from_be_bytes(take::<4>(&mut bytes)?);

        if !bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "trailing bytes"));
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

        for off in self.inflight.keys() {
            self.ready.remove(off);
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
            ready_count: self.ready.len(),
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
    use super::{Offset, QueueInternalState};
    use crate::state::{CustomDLQ, DLQDiscardPolicy};

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
    fn reject_without_enqueue_is_noop() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.reject(7);

        assert!(!s.has_history(7));
    }

    #[test]
    fn reject_without_enqueue_is_terminal() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.reject(7);

        assert!(s.is_acked(7));
        assert!(!s.is_ready(7));
        assert!(!s.is_inflight(7));
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
        assert!(s.is_acked(1));
    }

    #[test]
    fn offset_in_exactly_one_state() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.ready.insert(5);
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

        s.ready.insert(1);
        s.mark_inflight(1, 100);
        s.nack(1, true);

        s.mark_inflight(1, 200);
        s.nack(1, true);

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
    fn reject_advances_frontier() {
        let mut s = QueueInternalState::new("test".into(), 0);

        s.mark_inflight(0, 100);
        s.reject(0);

        assert!(s.is_acked(0));
        assert_eq!(s.settled_until(), 1);
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
                0 => s.mark_inflight(o, fastrand::u64(0..1000)),
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
    fn snapshot_preserves_dlq_policy() {
        let mut s = QueueInternalState::new("test".into(), 0);
        s.dlq_policy = DLQDiscardPolicy::CustomDQL(CustomDLQ {
            tp: "dlq-topic".into(),
            part: 42,
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
                0 => s.mark_inflight(o, fastrand::u64(0..100_000)),
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

        s.ready.insert(5); // no retries entry

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

        s.ack_batch(&[0, 1, 2, 3, 4]);
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
        s.recompute_hint_if_needed();
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
        let v: Vec<Offset> = vec![2, 2, 0, 1, 1, 3];
        s.ack_batch(&v);
        assert_eq!(s.settled_until(), 4);
    }
}
