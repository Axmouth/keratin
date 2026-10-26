use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TryRecvError};

use crate::batcher::{BatcherConfig, BatcherCore, Deadline, FlushReason, PushResult};
use crate::durability::KDurability;
use crate::keratin::WriterCmd;
use crate::log::{
    AppendResult, FsyncJob, Log, LogState, ReplicatedAppendMode, ReplicatedAppendOutcome,
};
use crate::record::Message;
use crate::{AppendCompletion, KeratinConfig};

#[cfg(feature = "writer-stage-trace")]
use crate::writer_stage_trace::WriterStageTracer;

#[cfg(feature = "writer-stage-trace")]
macro_rules! trace_writer_stage {
    ($tracer:ident, $id:expr, $stage:expr, $weight:expr, $body:block) => {{
        let (records, bytes) = if $tracer.enabled() { $weight } else { (0, 0) };
        $tracer.trace($id, $stage, records, bytes, || $body)
    }};
}

#[cfg(not(feature = "writer-stage-trace"))]
macro_rules! trace_writer_stage {
    ($tracer:ident, $id:expr, $stage:expr, $weight:expr, $body:block) => {{ $body }};
}

macro_rules! stage_reqs_then_post {
    (
        $tracer:ident,
        $log:expr,
        $cfg:expr,
        $state:expr,
        $pending:expr,
        $reqs:expr,
        $notify_tx:expr,
        $last_fsync:expr,
        $fsync_interval:expr,
        $fsync_tx:expr,
        $fsync_done_rx:expr,
        $inflight_fsyncs:expr,
        $linger:expr,
        $linger_min:expr,
        $linger_max:expr $(,)?
    ) => {{
        let reqs = $reqs;
        #[cfg(feature = "writer-stage-trace")]
        let work_id = $tracer.next_work_id();

        let total_bytes = stage_reqs(
            $log,
            $cfg,
            $state,
            $pending,
            reqs,
            $notify_tx,
            #[cfg(feature = "writer-stage-trace")]
            &$tracer,
            #[cfg(feature = "writer-stage-trace")]
            work_id,
        );

        post_stage_commit_and_tune(
            $log,
            $cfg,
            $state,
            $pending,
            $last_fsync,
            $fsync_interval,
            total_bytes,
            $notify_tx,
            $fsync_tx,
            $fsync_done_rx,
            $inflight_fsyncs,
            $linger,
            $linger_min,
            $linger_max,
            #[cfg(feature = "writer-stage-trace")]
            &$tracer,
            #[cfg(feature = "writer-stage-trace")]
            work_id,
        );
    }};
}

// TODO: Tests showing guaranteed order
// TODO: Also more tests for failures and edge cases (e.g. batch flush on shutdown, etc.)
// TODO: More pipelining: Batch -> encode and stage buffer -> write file -> fsync -> notify awaiters (estimated possible 40%-60% gain in throughput from not waiting encoding and fsync for large payloads)

#[derive(Debug, Clone)]
pub struct IoError {
    // Keep the error no larger than the former String-only representation;
    // success-path channel items must not grow for failure diagnostics.
    msg: Arc<str>,
    overlap: Option<Arc<crate::ReplicationOverlapDiagnostic>>,
}

impl IoError {
    pub fn new(msg: impl ToString) -> Self {
        Self {
            msg: msg.to_string().into(),
            overlap: None,
        }
    }
    pub fn overlap_diagnostic(&self) -> Option<&crate::ReplicationOverlapDiagnostic> {
        self.overlap.as_deref()
    }
}

impl std::error::Error for IoError {}

impl std::fmt::Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)?;
        Ok(())
    }
}

impl From<io::Error> for IoError {
    fn from(value: io::Error) -> Self {
        Self {
            overlap: value
                .get_ref()
                .and_then(|e| e.downcast_ref::<crate::ReplicationOverlapDiagnostic>())
                .cloned()
                .map(Arc::new),
            msg: value.to_string().into(),
        }
    }
}

pub enum AppendCompletionTarget {
    Boxed(Box<dyn AppendCompletion<IoError> + Send>),
    Oneshot(tokio::sync::oneshot::Sender<Result<AppendResult, IoError>>),
}

impl AppendCompletionTarget {
    fn complete(self, res: Result<AppendResult, IoError>) {
        match self {
            Self::Boxed(completion) => completion.complete(res),
            Self::Oneshot(tx) => {
                let _ = tx.send(res);
            }
        }
    }
}

impl From<Box<dyn AppendCompletion<IoError> + Send>> for AppendCompletionTarget {
    fn from(value: Box<dyn AppendCompletion<IoError> + Send>) -> Self {
        Self::Boxed(value)
    }
}

pub enum AppendPayload {
    One(Message),
    Many(Vec<Message>),
}

pub struct AppendReq {
    pub records: AppendPayload,
    pub durability: Option<KDurability>,
    pub completion: AppendCompletionTarget,
    /// Optional early-offset signal: when set, the writer sends the record's
    /// assigned base offset the moment it is staged (offset assigned, in the
    /// in-memory buffer), BEFORE the durability ack on `completion`. This exposes
    /// the assigned-offset point of the append lifecycle (assigned -> written ->
    /// fsynced) as data, so a caller can act on the offset without waiting for the
    /// flush. `None` for the normal durable path.
    pub staged_offset_tx: Option<tokio::sync::oneshot::Sender<u64>>,
}

impl AppendPayload {
    pub fn len(&self) -> usize {
        match self {
            AppendPayload::One(_) => 1,
            AppendPayload::Many(messages) => messages.len(),
        }
    }

    pub fn bytes_len(&self) -> usize {
        match self {
            AppendPayload::One(message) => message.bytes_len(),
            AppendPayload::Many(messages) => messages.iter().map(|m| m.bytes_len()).sum::<usize>(),
        }
    }
}

#[cfg(feature = "writer-stage-trace")]
fn messages_trace_weight(messages: &[Message]) -> (usize, usize) {
    (
        messages.len(),
        messages
            .iter()
            .map(Message::bytes_len)
            .fold(0usize, usize::saturating_add),
    )
}

pub struct WriterHandle {
    pub tx: Sender<WriterCmd>,
}

struct PendingAck {
    end_offset: u64, // inclusive
    respond_to: AppendCompletionTarget,
    result: AppendResult,
}

/// Only acknowledgements not yet handed to an fsync request live here.
#[derive(Default)]
struct PendingAcks {
    entries: VecDeque<PendingAck>,
}
impl PendingAcks {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn front(&self) -> Option<&PendingAck> {
        self.entries.front()
    }
    fn push_back(&mut self, ack: PendingAck) {
        self.entries.push_back(ack);
    }
    fn pop_front(&mut self) -> Option<PendingAck> {
        self.entries.pop_front()
    }
}

/// Whether recent commits are small enough (fsync-count-bound) to pipeline fsyncs
/// rather than keep a single one in flight. `threshold` is
/// `KeratinConfig::pipeline_commit_records`.
fn should_pipeline(log: &Log, threshold: u64) -> bool {
    log.recent_commit_records < threshold
}

/// Whether the writer should commit now because the fsync worker has capacity
/// with durability acks pending. When `pipeline` (recent commits are small) it
/// allows up to `max_inflight_fsyncs` in-flight commits so the worker can fuse them
/// into one fdatasync; otherwise it keeps a single fsync in flight (the eager
/// cadence that makes fat batches, optimal for bandwidth-bound writes). The idle
/// floor keeps the fsync rate bounded on storage where that matters, and the
/// loaded spacing applies on top while an fsync is in flight.
#[inline]
fn commit_when_idle(
    inflight_fsyncs: usize,
    last_fsync: &Instant,
    min_fsync_interval: Duration,
    loaded_fsync_spacing: Duration,
    pipeline: bool,
    max_inflight_fsyncs: usize,
) -> bool {
    let max_inflight = if pipeline {
        max_inflight_fsyncs.max(1)
    } else {
        1
    };
    let floor = if inflight_fsyncs > 0 {
        min_fsync_interval.max(loaded_fsync_spacing)
    } else {
        min_fsync_interval
    };
    inflight_fsyncs < max_inflight && last_fsync.elapsed() >= floor
}

fn pending_needs_fsync(pending: &PendingAcks) -> bool {
    !pending.is_empty()
}

enum NotifyMsg {
    One { item: NotifyItem },
    Batch(Vec<NotifyItem>),
}

struct NotifyItem {
    completion: AppendCompletionTarget,
    result: Result<AppendResult, IoError>,
}

struct FsyncReq {
    job: FsyncJob,
    ready: Vec<NotifyItem>,
    /// Callers of `WriterCmd::Sync` waiting for this fsync to land. The writer
    /// thread hands the fsync to the worker stage and the worker carries these back
    /// in the `FsyncDone`, so a Sync never blocks staging. Empty for normal commits.
    sync_acks: Vec<tokio::sync::oneshot::Sender<io::Result<()>>>,
    /// Started by the writer's background sync rather than a caller.
    background: bool,
    #[cfg(feature = "writer-stage-trace")]
    work_id: u64,
}

struct FsyncDone {
    durable_end: u64,
    elapsed: Duration,
    ready: Vec<NotifyItem>,
    sync_acks: Vec<tokio::sync::oneshot::Sender<io::Result<()>>>,
    error: Option<String>,
    background: bool,
    #[cfg(feature = "writer-stage-trace")]
    work_id: u64,
}

/// How long a log's fsync or notify thread may sit unused before the writer
/// retires it. An idle log then keeps only its writer thread, and the next
/// send starts a fresh worker.
#[cfg(not(test))]
const WORKER_IDLE: Duration = Duration::from_secs(10);
/// Short in unit tests, so their writers start and retire workers often.
#[cfg(test)]
const WORKER_IDLE: Duration = Duration::from_millis(200);

type FsyncSpawn = Box<dyn Fn(Receiver<FsyncReq>, Sender<FsyncDone>) + Send>;

/// The writer's link to its fsync worker. The worker starts on the first send
/// and the writer retires it once nothing is in flight and nothing was sent
/// for `WORKER_IDLE`, by dropping both channels, so no queued job can be
/// lost. While a worker runs, its completion channel only closes if the
/// worker died, which the writer treats as before.
struct FsyncLink {
    spawn: Option<FsyncSpawn>,
    capacity: usize,
    current: std::cell::RefCell<Option<(Sender<FsyncReq>, Receiver<FsyncDone>)>>,
    last_send: std::cell::Cell<Instant>,
}

impl FsyncLink {
    fn on_demand(capacity: usize, spawn: FsyncSpawn) -> Self {
        Self {
            spawn: Some(spawn),
            capacity,
            current: Default::default(),
            last_send: std::cell::Cell::new(Instant::now()),
        }
    }

    /// Fixed channels to a worker the caller runs, never retired.
    #[cfg(test)]
    fn fixed(tx: Sender<FsyncReq>, rx: Receiver<FsyncDone>) -> Self {
        Self {
            spawn: None,
            capacity: 0,
            current: std::cell::RefCell::new(Some((tx, rx))),
            last_send: std::cell::Cell::new(Instant::now()),
        }
    }

    fn send(&self, req: FsyncReq) -> Result<(), crossbeam_channel::SendError<FsyncReq>> {
        self.last_send.set(Instant::now());
        let tx = {
            let mut current = self.current.borrow_mut();
            if current.is_none()
                && let Some(spawn) = &self.spawn
            {
                let (tx, rx) = crossbeam_channel::bounded::<FsyncReq>(self.capacity);
                let (done_tx, done_rx) = crossbeam_channel::bounded::<FsyncDone>(self.capacity);
                spawn(rx, done_tx);
                *current = Some((tx, done_rx));
            }
            match &*current {
                Some((tx, _)) => tx.clone(),
                None => return Err(crossbeam_channel::SendError(req)),
            }
        };
        tx.send(req)
    }

    fn receiver(&self) -> Option<Receiver<FsyncDone>> {
        self.current.borrow().as_ref().map(|(_, rx)| rx.clone())
    }

    fn try_recv(&self) -> Result<FsyncDone, TryRecvError> {
        match self.receiver() {
            Some(rx) => rx.try_recv(),
            None => Err(TryRecvError::Empty),
        }
    }

    fn recv(&self) -> Result<FsyncDone, crossbeam_channel::RecvError> {
        match self.receiver() {
            Some(rx) => rx.recv(),
            None => Err(crossbeam_channel::RecvError),
        }
    }

    fn running(&self) -> bool {
        self.current.borrow().is_some()
    }

    fn retire_if_idle(&self, inflight: usize) {
        if self.spawn.is_some()
            && inflight == 0
            && self.last_send.get().elapsed() >= WORKER_IDLE
        {
            self.current.borrow_mut().take();
        }
    }
}

type NotifySpawn = Box<dyn Fn(Receiver<NotifyMsg>) + Send>;

/// The writer's link to its notify thread, started on the first send and
/// retired after `WORKER_IDLE` without one. Notifications already queued are
/// still delivered: the thread drains its channel before it exits.
struct NotifyLink {
    spawn: Option<NotifySpawn>,
    capacity: usize,
    current: std::cell::RefCell<Option<Sender<NotifyMsg>>>,
    last_send: std::cell::Cell<Instant>,
}

impl NotifyLink {
    fn on_demand(capacity: usize, spawn: NotifySpawn) -> Self {
        Self {
            spawn: Some(spawn),
            capacity,
            current: Default::default(),
            last_send: std::cell::Cell::new(Instant::now()),
        }
    }

    #[cfg(test)]
    fn fixed(tx: Sender<NotifyMsg>) -> Self {
        Self {
            spawn: None,
            capacity: 0,
            current: std::cell::RefCell::new(Some(tx)),
            last_send: std::cell::Cell::new(Instant::now()),
        }
    }

    fn send(&self, msg: NotifyMsg) -> Result<(), crossbeam_channel::SendError<NotifyMsg>> {
        self.last_send.set(Instant::now());
        let tx = {
            let mut current = self.current.borrow_mut();
            if current.is_none()
                && let Some(spawn) = &self.spawn
            {
                let (tx, rx) = crossbeam_channel::bounded::<NotifyMsg>(self.capacity);
                spawn(rx);
                *current = Some(tx);
            }
            match &*current {
                Some(tx) => tx.clone(),
                None => return Err(crossbeam_channel::SendError(msg)),
            }
        };
        tx.send(msg)
    }

    fn running(&self) -> bool {
        self.current.borrow().is_some()
    }

    fn retire_if_idle(&self) {
        if self.spawn.is_some() && self.last_send.get().elapsed() >= WORKER_IDLE {
            self.current.borrow_mut().take();
        }
    }
}

enum WriterEvent {
    Command(WriterCmd),
    FsyncDone(FsyncDone),
    Timeout,
    WritesDisconnected,
    FsyncDisconnected,
}

fn spawn_named<F, T>(name: &str, f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .expect("failed to spawn a log thread")
}

pub fn spawn_writer(
    mut log: Log,
    cfg: KeratinConfig,
    state: Arc<LogState>,
    channel_capacity: usize,
) -> WriterHandle {

    #[cfg(feature = "writer-stage-trace")]
    let tracer = WriterStageTracer::from_env(log.trace_path(), log.current_epoch());
    #[cfg(feature = "writer-stage-trace")]
    let notifier_tracer = tracer.clone();
    #[cfg(feature = "writer-stage-trace")]
    let fsync_tracer = tracer.clone();

    // Named so profiles and per-thread counters can tell a log's threads
    // from the runtime's. Linux keeps the first 15 bytes. The notify and fsync
    // threads start when the writer first needs them and stop after
    // `WORKER_IDLE` unused, so an idle log keeps only its writer thread.
    let notify_tx = NotifyLink::on_demand(
        channel_capacity,
        Box::new(move |notify_rx| {
            #[cfg(feature = "writer-stage-trace")]
            let notifier_tracer = notifier_tracer.clone();
            spawn_named("keratin-notify", move || {
                notifier_loop(
                    notify_rx,
                    #[cfg(feature = "writer-stage-trace")]
                    notifier_tracer,
                );
            });
        }),
    );

    // Sized to the in-flight fsync budget so the channel capacity does not cap the
    // configured pipeline depth below `max_inflight_fsyncs`.
    let fsync_tx = FsyncLink::on_demand(
        cfg.max_inflight_fsyncs.max(1),
        Box::new(move |fsync_rx, fsync_done_tx| {
            #[cfg(feature = "writer-stage-trace")]
            let fsync_tracer = fsync_tracer.clone();
            spawn_named("keratin-fsync", move || {
                fsync_loop(
                    fsync_rx,
                    fsync_done_tx,
                    #[cfg(feature = "writer-stage-trace")]
                    fsync_tracer,
                );
            });
        }),
    );

    let (tx, rx) = crossbeam_channel::bounded::<WriterCmd>(channel_capacity);

    spawn_named("keratin-writer", move || {
        #[cfg(test)]
        let root = log.root().to_path_buf();
        // The notify and fsync senders move into the loop and drop when it
        // returns, so those helper threads finish on their own.
        let refusal = writer_loop(
            &mut log,
            cfg,
            &rx,
            state,
            notify_tx,
            fsync_tx,
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        // Close the files now. A reopen of the same root may follow while
        // this handle still exists.
        drop(log);
        tracing::info!("Writer loop exited");
        // A bounded crossbeam channel keeps buffered commands alive while any
        // sender exists, even after the receiver drops, so a reply sender in
        // one would never be answered or dropped. Keep receiving and refuse
        // everything until the last sender is gone.
        while let Ok(cmd) = rx.recv() {
            reject_command(cmd, &refusal, None);
        }
        #[cfg(test)]
        test_hooks::writer_exited(&root);
    });

    WriterHandle { tx }
}

fn notifier_loop(
    rx: Receiver<NotifyMsg>,
    #[cfg(feature = "writer-stage-trace")] tracer: WriterStageTracer,
) {
    while let Ok(msg) = rx.recv() {
        #[cfg(feature = "writer-stage-trace")]
        let work_id = tracer.next_work_id();
        trace_writer_stage!(tracer, work_id, "notify", (0, 0), {
            match msg {
                NotifyMsg::One { item } => item.completion.complete(item.result),
                NotifyMsg::Batch(items) => {
                    for item in items {
                        item.completion.complete(item.result);
                    }
                }
            }
        });
    }
}

fn fsync_loop(
    rx: Receiver<FsyncReq>,
    done_tx: Sender<FsyncDone>,
    #[cfg(feature = "writer-stage-trace")] tracer: WriterStageTracer,
) {
    #[cfg(feature = "writer-stage-trace")]
    let job_tracer = tracer.clone();
    fsync_loop_with(
        rx,
        done_tx,
        |job, _id| {
            #[cfg(feature = "writer-stage-trace")]
            {
                job.sync_traced(&job_tracer, _id)
            }
            #[cfg(not(feature = "writer-stage-trace"))]
            {
                job.sync()
            }
        },
        #[cfg(feature = "writer-stage-trace")]
        tracer,
    );
}

fn fsync_loop_with(
    rx: Receiver<FsyncReq>,
    done_tx: Sender<FsyncDone>,
    mut sync: impl FnMut(&FsyncJob, u64) -> io::Result<Duration>,
    #[cfg(feature = "writer-stage-trace")] tracer: WriterStageTracer,
) {
    // Reused across wakeups so the coalescing buffer is not reallocated per fsync.
    let mut reqs: Vec<FsyncReq> = Vec::new();
    while let Ok(first) = rx.recv() {
        // Drain every queued request and make them all durable with one pass:
        // the deferred syncs each job carries from a roll (the outgoing segment,
        // its index, the new directory entry), then the newest job's log and
        // index. Within a segment the newest sync covers everything written
        // before it. Across a roll, the job that carries the outgoing segment
        // may not be the newest, so every job's deferred syncs run. Turns the
        // fsync-count-bound small-batch regime into fat-batch throughput
        // without waiting for a bigger linger.
        reqs.push(first);
        while let Ok(more) = rx.try_recv() {
            reqs.push(more);
        }
        // One sync covers every drained request; fan the single result out.
        #[cfg(feature = "writer-stage-trace")]
        let group_id = tracer.next_work_id();
        #[cfg(not(feature = "writer-stage-trace"))]
        let group_id = 0;
        #[cfg(feature = "writer-stage-trace")]
        for req in &reqs {
            tracer.event(
                req.work_id,
                "fsync_member",
                None,
                0,
                0,
                Some(group_id),
                Some((0, req.job.durable_end())),
            );
        }
        #[cfg(feature = "writer-stage-trace")]
        tracer.event(
            group_id,
            "fsync_begin",
            None,
            reqs.len(),
            0,
            None,
            Some((0, reqs.last().unwrap().job.durable_end())),
        );
        let result = trace_writer_stage!(tracer, group_id, "fsync", (reqs.len(), 0), {
            let (newest, earlier) = reqs.split_last().expect("at least one request");
            earlier
                .iter()
                .try_for_each(|req| req.job.sync_deferred())
                .and_then(|()| sync(&newest.job, group_id))
        });
        #[cfg(feature = "writer-stage-trace")]
        tracer.event(
            group_id,
            if result.is_ok() {
                "fsync_success"
            } else {
                "fsync_error"
            },
            None,
            0,
            0,
            None,
            None,
        );
        match result {
            Ok(elapsed) => {
                // The single sync job's cost belongs to the coalesced group, not to
                // each member: attribute it to the first request and zero to the
                // rest, so `stats.fsync` is not inflated ~N x under fusion.
                let mut fsync_elapsed = elapsed;
                for req in reqs.drain(..) {
                    let done = FsyncDone {
                        durable_end: req.job.durable_end(),
                        elapsed: fsync_elapsed,
                        ready: req.ready,
                        sync_acks: req.sync_acks,
                        error: None,
                        background: req.background,
                        #[cfg(feature = "writer-stage-trace")]
                        work_id: req.work_id,
                    };
                    fsync_elapsed = Duration::ZERO;
                    if done_tx.send(done).is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for req in reqs.drain(..) {
                    let done = FsyncDone {
                        durable_end: req.job.durable_end(),
                        elapsed: Duration::ZERO,
                        ready: fail_notify_items(req.ready, &msg),
                        sync_acks: req.sync_acks,
                        error: Some(msg.clone()),
                        background: req.background,
                        #[cfg(feature = "writer-stage-trace")]
                        work_id: req.work_id,
                    };
                    if done_tx.send(done).is_err() {
                        return;
                    }
                }
            }
        }
    }
}

fn writer_loop(
    log: &mut Log,
    mut cfg: KeratinConfig,
    writes_rx: &Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: NotifyLink,
    fsync_tx: FsyncLink,
    #[cfg(feature = "writer-stage-trace")] tracer: WriterStageTracer,
) -> Refusal {
    // Jobs go out and completions come back through the same link.
    let fsync_done_rx = &fsync_tx;
    crate::drain_diagnostics::init();
    let mut fsync_interval = Duration::from_millis(cfg.fsync_interval_ms.max(1));
    let mut min_fsync_interval = Duration::from_millis(cfg.min_fsync_interval_ms);
    let mut last_fsync = Instant::now();

    let mut pending = PendingAcks::default();
    let mut inflight_fsyncs = 0usize;

    // Adaptive linger
    // TODO: Consider removing due to overlap with fsync window
    let linger_min = Duration::from_millis(0);
    let mut linger_max = Duration::from_millis(cfg.batch_linger_ms.max(1));
    let mut linger = Duration::from_millis(0);

    // Batcher: items are AppendReq; weights are total records + total bytes.
    let mut batcher: BatcherCore<AppendReq, _> = BatcherCore::new(
        BatcherConfig {
            // Inclusive flush thresholds (existing semantics).
            max_items: cfg.max_batch_records, // number of AppendReqs
            max_records: cfg.max_batch_records, // total Message count
            max_bytes: cfg.max_batch_bytes,
            linger,
        },
        |r: &AppendReq| {
            let recs = r.records.len();
            let bytes = r.records.bytes_len();
            (recs, bytes)
        },
    );

    let mut settings_revision = 0u64;
    loop {
        fsync_tx.retire_if_idle(inflight_fsyncs);
        notify_tx.retire_if_idle();
        drain_fsync_done(
            log,
            &state,
            &notify_tx,
            &fsync_done_rx,
            &mut inflight_fsyncs,
            #[cfg(feature = "writer-stage-trace")]
            &tracer,
        );

        if log.storage_failure().is_some() {
            return serve_after_storage_failure(
                log,
                &state,
                batcher.flush(),
                &mut pending,
                writes_rx,
                &notify_tx,
                &fsync_done_rx,
                &mut inflight_fsyncs,
                #[cfg(feature = "writer-stage-trace")]
                &tracer,
            );
        }

        // Keep linger synced with adaptive tuning.
        batcher.set_linger(linger);

        // Close an expired batch.
        if let Some((_, reqs)) = batcher.flush_if_due(Instant::now()) {
            stage_reqs_then_post!(
                tracer,
                log,
                &cfg,
                &state,
                &mut pending,
                reqs,
                &notify_tx,
                &mut last_fsync,
                fsync_interval,
                &fsync_tx,
                &fsync_done_rx,
                &mut inflight_fsyncs,
                &mut linger,
                linger_min,
                linger_max,
            );
            continue;
        }

        // (B) Never block past fsync deadline if we owe durability acks.
        // Commit due work first (and fail pending if commit is repeatedly broken).
        maybe_commit_due(
            log,
            &state,
            &mut pending,
            &mut last_fsync,
            fsync_interval,
            min_fsync_interval,
            cfg.max_inflight_fsyncs,
            cfg.pipeline_commit_records,
            &notify_tx,
            &fsync_tx,
            &fsync_done_rx,
            &mut inflight_fsyncs,
            #[cfg(feature = "writer-stage-trace")]
            &tracer,
        );
        maybe_start_background_sync(
            log,
            &state,
            &mut pending,
            &mut last_fsync,
            cfg.background_sync_interval,
            cfg.expect_every_write_synced,
            &notify_tx,
            &fsync_tx,
            &mut inflight_fsyncs,
            #[cfg(feature = "writer-stage-trace")]
            &tracer,
        );

        // (C) Compute how long we may wait for the next cmd.
        let now = Instant::now();
        log.maintain_staging(now);
        let mut wait = Duration::MAX;
        if let Some(deadline) = log.staging_maintenance_deadline() {
            wait = wait.min(deadline.saturating_duration_since(now));
        }
        // An idle log has nothing unsynced and never sets this timer.
        if let Some(due) =
            background_sync_due(log, &state, cfg.background_sync_interval, inflight_fsyncs)
        {
            wait = wait.min(due.saturating_duration_since(now));
        }

        // Cap by the commit deadline when durability acks are pending. With no
        // fsync in flight the wait only runs to the idle-commit floor, so the
        // writer self-clocks on fsync completions instead of the interval.
        if pending_needs_fsync(&pending)
            // At the hard cap no commit can be submitted until a completion
            // arrives, and the completion wakes the receive below. An expired
            // interval deadline here would otherwise turn into a zero-timeout
            // busy loop for the whole fsync.
            && inflight_fsyncs < cfg.max_inflight_fsyncs.max(1)
        {
            let loaded_spacing = log.loaded_fsync_spacing();
            let horizon = if inflight_fsyncs == 0 {
                min_fsync_interval
            } else if loaded_spacing > Duration::ZERO
                // Only a pipelined commit can start while one is in flight.
                // Otherwise an early wake finds nothing to do and spins.
                && should_pipeline(log, cfg.pipeline_commit_records)
            {
                min_fsync_interval.max(loaded_spacing).min(fsync_interval)
            } else {
                fsync_interval
            };
            // If checked_add overflows, treat as due now.
            let fs_deadline = match last_fsync.checked_add(horizon) {
                Some(d) => d,
                None => now,
            };
            wait = wait.min(if now >= fs_deadline {
                Duration::ZERO
            } else {
                fs_deadline - now
            });
        }

        // Cap by batching deadline when a batch is active.
        match batcher.deadline(now) {
            Deadline::In(d) => wait = wait.min(d),
            Deadline::DueNow => wait = Duration::ZERO,
            Deadline::None => {}
        }

        // Wake in time to retire a worker that has gone idle.
        if fsync_tx.running() || notify_tx.running() {
            wait = wait.min(WORKER_IDLE);
        }

        // (D) Fetch next event (bounded).
        let event = recv_writer_event(writes_rx, fsync_done_rx, wait);

        let cmd = match event {
            WriterEvent::Command(cmd) => cmd,
            WriterEvent::FsyncDone(done) => {
                handle_fsync_done(
                    log,
                    &state,
                    &notify_tx,
                    done,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );
                continue;
            }
            WriterEvent::Timeout => {
                // Either batch deadline or fsync deadline is due; loop will handle.
                #[cfg(test)]
                RECEIVE_TIMEOUTS.with(|count| count.set(count.get() + 1));
                continue;
            }
            WriterEvent::WritesDisconnected => {
                shutdown_fail_unstaged(
                    &mut batcher,
                    "writer shutdown",
                    FlushReason::Shutdown,
                    &notify_tx,
                );
                if !pending.is_empty() {
                    #[cfg(feature = "writer-stage-trace")]
                    let work_id = tracer.next_work_id();
                    if let Err(e) = commit(
                        log,
                        &state,
                        &mut pending,
                        &mut last_fsync,
                        &notify_tx,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        cfg.max_inflight_fsyncs,
                        #[cfg(feature = "writer-stage-trace")]
                        &tracer,
                        #[cfg(feature = "writer-stage-trace")]
                        work_id,
                    ) {
                        fail_all_pending(
                            &mut pending,
                            format!("Internal Error while committing before writer shutdown: {e}"),
                            &notify_tx,
                            true,
                        );
                    }
                }
                wait_for_inflight_fsyncs(
                    log,
                    &state,
                    &notify_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );
                // No sender is left, so nothing can be refused anymore.
                return Refusal::shut_down();
            }
            WriterEvent::FsyncDisconnected => {
                fail_all_pending(&mut pending, "fsync worker disconnected", &notify_tx, true);
                return Refusal::stopped(log, "fsync worker disconnected");
            }
        };

        // (E) Process command.
        match cmd {
            WriterCmd::ReadSettings { respond_to } => {
                let _ = respond_to.send(crate::WriterSettingsSnapshot {
                    revision: settings_revision,
                    settings: cfg.into(),
                });
            }
            WriterCmd::ReplaceSettings {
                expected_revision,
                settings,
                respond_to,
            } => {
                // Refuse stale or invalid changes before touching staging or
                // storage. These leave the writer usable.
                let checked = settings.validate().and_then(|_| {
                    if expected_revision != settings_revision {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "stale writer settings revision",
                        ));
                    }
                    settings_revision
                        .checked_add(1)
                        .ok_or_else(|| io::Error::other("writer settings revision exhausted"))
                });
                let next_revision = match checked {
                    Ok(revision) => revision,
                    Err(error) => {
                        let _ = respond_to.send(Err(error));
                        continue;
                    }
                };
                // Appends already in the batch were accepted under the old
                // settings, so stage them before switching.
                let unstaged = batcher.flush();
                if !unstaged.is_empty() {
                    stage_reqs_then_post!(
                        tracer,
                        log,
                        &cfg,
                        &state,
                        &mut pending,
                        unstaged,
                        &notify_tx,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }
                // Persist everything written under the old settings, so a new
                // interval never stretches the wait of an older append.
                if let Err(error) = settings_sync_boundary(
                    log,
                    &state,
                    &mut pending,
                    &notify_tx,
                    &fsync_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                ) {
                    // The writer cannot tell what state storage is in, so stop
                    // the log. The loop top then fails pending work and every
                    // later command until reopen.
                    log.record_storage_failure(format!("writer settings sync failed: {error}"));
                    let _ = respond_to.send(Err(error));
                    continue;
                }
                settings.apply(&mut cfg);
                settings_revision = next_revision;
                fsync_interval = Duration::from_millis(cfg.fsync_interval_ms.max(1));
                min_fsync_interval = Duration::from_millis(cfg.min_fsync_interval_ms);
                linger_max = Duration::from_millis(cfg.batch_linger_ms.max(1));
                linger = linger.min(linger_max);
                state.sync.set_interval(cfg.background_sync_interval);
                state
                    .sync
                    .set_expect_every_write_synced(cfg.expect_every_write_synced);
                batcher.replace_empty_config(BatcherConfig {
                    max_items: cfg.max_batch_records,
                    max_records: cfg.max_batch_records,
                    max_bytes: cfg.max_batch_bytes,
                    linger,
                });
                last_fsync = Instant::now();
                let _ = respond_to.send(Ok(crate::WriterSettingsSnapshot {
                    revision: settings_revision,
                    settings,
                }));
            }
            WriterCmd::Append(r) => {
                let now = Instant::now();
                match batcher.push(now, r) {
                    PushResult::None => {
                        // nothing flushed yet
                    }
                    PushResult::One((_why, reqs)) => {
                        stage_reqs_then_post!(
                            tracer,
                            log,
                            &cfg,
                            &state,
                            &mut pending,
                            reqs,
                            &notify_tx,
                            &mut last_fsync,
                            fsync_interval,
                            &fsync_tx,
                            &fsync_done_rx,
                            &mut inflight_fsyncs,
                            &mut linger,
                            linger_min,
                            linger_max,
                        );
                        tracing::debug!(
                            items = batcher.len(),
                            records = batcher.total_records(),
                            bytes = batcher.total_bytes(),
                            reason = ?_why,
                            "batcher state after push"
                        );
                    }
                    PushResult::Two((why1, reqs1), (why2, reqs2)) => {
                        // Very rare but must be lossless (stale barrier + size flush).
                        #[cfg(feature = "writer-stage-trace")]
                        let work_id1 = tracer.next_work_id();
                        let b1 = stage_reqs(
                            log,
                            &cfg,
                            &state,
                            &mut pending,
                            reqs1,
                            &notify_tx,
                            #[cfg(feature = "writer-stage-trace")]
                            &tracer,
                            #[cfg(feature = "writer-stage-trace")]
                            work_id1,
                        );

                        #[cfg(feature = "writer-stage-trace")]
                        let work_id2 = tracer.next_work_id();
                        let b2 = stage_reqs(
                            log,
                            &cfg,
                            &state,
                            &mut pending,
                            reqs2,
                            &notify_tx,
                            #[cfg(feature = "writer-stage-trace")]
                            &tracer,
                            #[cfg(feature = "writer-stage-trace")]
                            work_id2,
                        );

                        // Use combined bytes for tuning.
                        let total_bytes = b1.saturating_add(b2);

                        post_stage_commit_and_tune(
                            log,
                            &cfg,
                            &state,
                            &mut pending,
                            &mut last_fsync,
                            fsync_interval,
                            total_bytes,
                            &notify_tx,
                            &fsync_tx,
                            &fsync_done_rx,
                            &mut inflight_fsyncs,
                            &mut linger,
                            linger_min,
                            linger_max,
                            #[cfg(feature = "writer-stage-trace")]
                            &tracer,
                            #[cfg(feature = "writer-stage-trace")]
                            work_id2,
                        );

                        // Regardless of why, post-stage commit scheduling stays the same.
                        let _ = (why1, why2); // keep for tracing if you want
                        tracing::debug!(
                            items = batcher.len(),
                            records = batcher.total_records(),
                            bytes = batcher.total_bytes(),
                            reason1 = ?why1,
                            reason2 = ?why2,
                            "batcher state after push"
                        );
                    }
                }
            }
            WriterCmd::ReplicatedAppend {
                epoch,
                first_offset,
                records,
                mode,
                durability,
                respond_to,
            } => {
                let unstaged = batcher.flush();
                if !unstaged.is_empty() {
                    stage_reqs_then_post!(
                        tracer,
                        log,
                        &cfg,
                        &state,
                        &mut pending,
                        unstaged,
                        &notify_tx,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }

                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                let res = trace_writer_stage!(
                    tracer,
                    work_id,
                    "replicated_append",
                    messages_trace_weight(&records),
                    {
                        stage_replicated_req(
                            log,
                            &state,
                            epoch,
                            first_offset,
                            &records,
                            mode,
                            durability.unwrap_or(cfg.default_durability),
                            #[cfg(feature = "writer-stage-trace")]
                            &tracer,
                            #[cfg(feature = "writer-stage-trace")]
                            work_id,
                        )
                    }
                );
                if let Err(_err) = respond_to.send(res) {
                    tracing::info!("Error sending replicated append response");
                }
            }
            WriterCmd::Truncate { before, respond_to } => {
                tracing::debug!(before, "truncate");
                let done = move |result| {
                    if respond_to.send(result).is_err() {
                        tracing::debug!(before, "truncate caller went away");
                    }
                };
                if let Err(error) = log.truncate_before_deferred(before, done) {
                    tracing::error!(before, %error, "truncate failed");
                }
            }
            WriterCmd::RepairSuffix {
                next_offset,
                expected_epoch,
                respond_to,
            } => {
                #[cfg(feature = "writer-stage-trace")]
                tracer.event(
                    tracer.next_work_id(),
                    "history_control",
                    None,
                    0,
                    0,
                    None,
                    None,
                );
                if expected_epoch != log.current_epoch()
                    || next_offset < log.manifest.head_offset
                    || next_offset > log.next_offset()
                {
                    let _ = respond_to.send(Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "suffix repair requires the current epoch and a retained cut",
                    )));
                    continue;
                }
                shutdown_fail_reqs(batcher.flush(), "writer repairing suffix", &notify_tx);
                fail_all_pending(&mut pending, "writer repairing suffix", &notify_tx, false);
                wait_for_inflight_fsyncs(
                    log,
                    &state,
                    &notify_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );
                let result = log.repair_suffix(next_offset);
                let failure = result
                    .as_ref()
                    .err()
                    .map(|error| format!("suffix repair failed: {error}"));
                let _ = respond_to.send(result);
                if let Some(failure) = failure {
                    // Never append to or persist old metadata over a partially
                    // repaired log. The journal owns recovery from this point.
                    tracing::error!(
                        next_offset,
                        "suffix repair failed; writer stopped until reopen"
                    );
                    return Refusal::stopped(log, failure);
                }
                last_fsync = Instant::now();
            }
            WriterCmd::ResetToCheckpoint {
                next_offset,
                expected_epoch,
                respond_to,
            } => {
                #[cfg(feature = "writer-stage-trace")]
                tracer.event(
                    tracer.next_work_id(),
                    "history_control",
                    None,
                    0,
                    0,
                    None,
                    None,
                );
                // Ordered with AdvanceEpoch on this writer. Reject before
                // touching either persisted records or pending completions.
                if let Some(expected) = expected_epoch {
                    let current = log.current_epoch();
                    if expected != current {
                        let _ = respond_to.send(Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "checkpoint epoch {expected} does not match local epoch {current}"
                            ),
                        )));
                        continue;
                    }
                }
                shutdown_fail_reqs(batcher.flush(), "writer reset to checkpoint", &notify_tx);
                fail_all_pending(
                    &mut pending,
                    "writer reset to checkpoint",
                    &notify_tx,
                    false,
                );
                // No old fsync may publish a frontier or manifest after the
                // checkpoint cut. Draining also completes outstanding Sync
                // replies before their files are replaced.
                wait_for_inflight_fsyncs(
                    log,
                    &state,
                    &notify_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );
                let res = log.reset_to_checkpoint(next_offset, crate::util::unix_millis());
                if res.is_ok() {
                    last_fsync = Instant::now();
                }
                if respond_to.send(res).is_err() {
                    tracing::info!("Error sending reset-to-checkpoint response");
                }
            }
            WriterCmd::AdvanceRetainedHead {
                before,
                epoch,
                respond_to,
            } => {
                let unstaged = batcher.flush();
                if !unstaged.is_empty() {
                    stage_reqs_then_post!(
                        tracer,
                        log,
                        &cfg,
                        &state,
                        &mut pending,
                        unstaged,
                        &notify_tx,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }
                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                let res = commit_all(
                    log,
                    &state,
                    &mut pending,
                    &mut last_fsync,
                    &notify_tx,
                    &fsync_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    cfg.max_inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                    #[cfg(feature = "writer-stage-trace")]
                    work_id,
                )
                .and_then(|_| {
                    wait_for_inflight_fsyncs(
                        log,
                        &state,
                        &notify_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        #[cfg(feature = "writer-stage-trace")]
                        &tracer,
                    );
                    log.advance_retained_head(before, epoch)
                });
                if respond_to.send(res).is_err() {
                    tracing::info!("Error sending checkpoint-floor response");
                }
            }
            WriterCmd::ForkFrozen {
                destination,
                epoch,
                head,
                next,
                respond_to,
            } => {
                let unstaged = batcher.flush();
                if !unstaged.is_empty() {
                    stage_reqs_then_post!(
                        tracer,
                        log,
                        &cfg,
                        &state,
                        &mut pending,
                        unstaged,
                        &notify_tx,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }
                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                let res = commit_all(
                    log,
                    &state,
                    &mut pending,
                    &mut last_fsync,
                    &notify_tx,
                    &fsync_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    cfg.max_inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                    #[cfg(feature = "writer-stage-trace")]
                    work_id,
                )
                .and_then(|_| {
                    wait_for_inflight_fsyncs(
                        log,
                        &state,
                        &notify_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        #[cfg(feature = "writer-stage-trace")]
                        &tracer,
                    );
                    log.fork_frozen(&destination, epoch, head, next)
                });
                if respond_to.send(res).is_err() {
                    tracing::info!("Error sending frozen-fork response");
                }
            }
            WriterCmd::AdvanceEpoch { epoch, respond_to } => {
                #[cfg(feature = "writer-stage-trace")]
                tracer.event(
                    tracer.next_work_id(),
                    "history_control",
                    None,
                    0,
                    0,
                    None,
                    None,
                );
                let unstaged = batcher.flush();
                if !unstaged.is_empty() {
                    stage_reqs_then_post!(
                        tracer,
                        log,
                        &cfg,
                        &state,
                        &mut pending,
                        unstaged,
                        &notify_tx,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }
                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                let res = commit_all(
                    log,
                    &state,
                    &mut pending,
                    &mut last_fsync,
                    &notify_tx,
                    &fsync_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    cfg.max_inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                    #[cfg(feature = "writer-stage-trace")]
                    work_id,
                )
                .and_then(|_| {
                    wait_for_inflight_fsyncs(
                        log,
                        &state,
                        &notify_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        #[cfg(feature = "writer-stage-trace")]
                        &tracer,
                    );
                    log.advance_epoch(epoch)
                });
                if let Ok(epoch) = res {
                    state.epoch.store(epoch, Ordering::Release);
                }
                if respond_to.send(res).is_err() {
                    tracing::info!("Error sending advance-epoch response");
                }
            }
            WriterCmd::Sync { respond_to } => {
                // Make everything staged so far durable. Used by callers that stage
                // with AfterWrite and fsync separately (e.g. the ephemeral stream
                // tier's periodic flush). prepare_fsync_job flushes staged bytes to
                // the file on this (writer) thread, then the actual fsync goes one
                // stage down to the fsync worker so staging keeps chugging. The
                // FsyncDone handler advances the durable watermark and answers the
                // responder when the fsync lands.
                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                // Same no-park rule as commit: a slot must be free before the
                // send below, or the writer can deadlock with the fsync worker.
                if let Err(e) = ensure_fsync_slot(
                    log,
                    &state,
                    &notify_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    cfg.max_inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                ) {
                    let _ = respond_to.send(Err(e));
                    continue;
                }
                #[cfg(feature = "writer-stage-trace")]
                let job = log.prepare_fsync_job_traced(&tracer, work_id);
                #[cfg(not(feature = "writer-stage-trace"))]
                let job = log.prepare_fsync_job();
                match job {
                    Ok(job) => {
                        let req = FsyncReq {
                            job,
                            ready: Vec::new(),
                            sync_acks: vec![respond_to],
                            background: false,
                            #[cfg(feature = "writer-stage-trace")]
                            work_id,
                        };
                        #[cfg(feature = "writer-stage-trace")]
                        tracer.event(
                            work_id,
                            "sync_queued",
                            None,
                            0,
                            0,
                            None,
                            Some((0, req.job.durable_end())),
                        );
                        if let Err(err) = fsync_tx.send(req) {
                            let FsyncReq {
                                sync_acks, ready, ..
                            } = err.into_inner();
                            send_notify_items(
                                &notify_tx,
                                fail_notify_items(ready, "fsync worker disconnected"),
                            );
                            answer_sync_acks(
                                sync_acks,
                                Err(io::Error::other("fsync worker disconnected")),
                            );
                        } else {
                            inflight_fsyncs = inflight_fsyncs.saturating_add(1);
                        }
                    }
                    Err(e) => {
                        let _ = respond_to.send(Err(e));
                    }
                }
            }
            WriterCmd::Shutdown {
                notify_tx: shutdown_tx,
            } => {
                tracing::info!("Writer received shutdown command");
                let unstaged = batcher.flush();
                if !unstaged.is_empty() {
                    stage_reqs_then_post!(
                        tracer,
                        log,
                        &cfg,
                        &state,
                        &mut pending,
                        unstaged,
                        &notify_tx,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }

                if !pending.is_empty() {
                    #[cfg(feature = "writer-stage-trace")]
                    let work_id = tracer.next_work_id();
                    if let Err(e) = commit(
                        log,
                        &state,
                        &mut pending,
                        &mut last_fsync,
                        &notify_tx,
                        &fsync_tx,
                        &fsync_done_rx,
                        &mut inflight_fsyncs,
                        cfg.max_inflight_fsyncs,
                        #[cfg(feature = "writer-stage-trace")]
                        &tracer,
                        #[cfg(feature = "writer-stage-trace")]
                        work_id,
                    ) {
                        fail_all_pending(
                            &mut pending,
                            format!("Internal Error while committing before shutdown: {e}"),
                            &notify_tx,
                            true,
                        );
                    }
                }

                wait_for_inflight_fsyncs(
                    log,
                    &state,
                    &notify_tx,
                    &fsync_done_rx,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );

                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                if let Err(e) = trace_writer_stage!(tracer, work_id, "shutdown_fsync", (0, 0), {
                    log.shutdown()
                }) {
                    tracing::error!("Error during writer shutdown fsync: {e}");
                } else {
                    tracing::info!("Writer shutdown fsync complete");
                }
                if shutdown_tx.send(()).is_err() {
                    tracing::info!("Error sending shutdown notification");
                }
                #[cfg(test)]
                test_hooks::shutdown_acknowledged(log.root());
                return Refusal::shut_down();
            }
            WriterCmd::SizeEstimate { respond_to } => {
                let res = log
                    .estimate_disk_used()
                    .map_err(|e| io::Error::other(format!("size estimate error: {e}")));
                if let Err(_e) = respond_to.send(res) {
                    tracing::info!("Error sending size estimate response");
                }
            }
        }
    }
}

fn stage_replicated_req(
    log: &mut Log,
    state: &Arc<LogState>,
    epoch: u64,
    first_offset: u64,
    records: &[Message],
    mode: ReplicatedAppendMode,
    durability: KDurability,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> Result<ReplicatedAppendOutcome, IoError> {
    let now_ms = crate::util::unix_millis();
    let (outcome, end_offset) =
        trace_writer_stage!(tracer, work_id, "replicated_stage", (records.len(), 0), {
            log.stage_replicated_append_batch(
                epoch,
                first_offset,
                records,
                mode,
                now_ms,
                durability == KDurability::AfterFlush,
            )
        })?;
    state.epoch.store(log.current_epoch(), Ordering::Release);

    // An AfterFsync AlreadyPresent reply tells the leader the range is
    // durable here. The overlap check only accepts durable records today, but
    // if a range above the frontier ever gets through, sync it before replying.
    let end_offset = end_offset.or(match outcome {
        ReplicatedAppendOutcome::AlreadyPresent {
            first_offset,
            count,
            ..
        } if durability == KDurability::AfterFsync
            && count > 0
            && first_offset + u64::from(count)
                > log.durable_end_exclusive().first_non_durable() =>
        {
            Some(first_offset + u64::from(count) - 1)
        }
        _ => None,
    });
    if let Some(end_offset) = end_offset {
        state.tail.fetch_max(end_offset + 1, Ordering::Release);

        match durability {
            // A write failure here is latched by the log.
            KDurability::AfterWrite | KDurability::AfterFlush => {
                let records = (end_offset + 1)
                    .saturating_sub(first_offset)
                    .min(records.len() as u64);
                note_unclaimed_write(log, state, end_offset + 1, records);
                #[cfg(feature = "writer-stage-trace")]
                log.flush_buffers_traced(tracer, work_id)?;
                #[cfg(not(feature = "writer-stage-trace"))]
                log.flush_buffers()?;
                trace_writer_stage!(tracer, work_id, "replicated_flush", (0, 0), { log.flush() })?;
            }
            KDurability::AfterFsync => {
                #[cfg(feature = "writer-stage-trace")]
                log.flush_buffers_traced(tracer, work_id)?;
                #[cfg(not(feature = "writer-stage-trace"))]
                log.flush_buffers()?;
                trace_writer_stage!(tracer, work_id, "replicated_flush", (0, 0), { log.flush() })?;
                #[cfg(feature = "writer-stage-trace")]
                log.fsync_traced(tracer, work_id)?;
                #[cfg(not(feature = "writer-stage-trace"))]
                log.fsync()?;
                state.durable.advance(log.durable_end_exclusive());
            }
        }
    }

    #[cfg(feature = "writer-stage-trace")]
    tracer.event(
        work_id,
        "replicated_done",
        None,
        records.len(),
        0,
        None,
        Some((
            first_offset,
            first_offset.saturating_add(records.len() as u64),
        )),
    );
    Ok(outcome)
}

/// Stage a flushed batch of AppendReqs.
/// Returns total bytes staged (for adaptive linger tuning).
fn stage_reqs(
    log: &mut Log,
    cfg: &KeratinConfig,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    reqs: Vec<AppendReq>,
    notify_tx: &NotifyLink,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> usize {
    crate::drain_diagnostics::stage(
        reqs.iter().map(|r| r.records.len() as u64).sum(),
        reqs.iter().map(|r| r.records.bytes_len() as u64).sum(),
    );
    #[cfg(feature = "writer-stage-trace")]
    let stage_started = tracer.start();
    #[cfg(feature = "writer-stage-trace")]
    let first_offset = log.next_offset();
    #[cfg(feature = "writer-stage-trace")]
    tracer.event(work_id, "stage_begin", None, reqs.len(), 0, None, None);
    let now_ms = crate::util::unix_millis();

    let mut total_bytes: usize = 0;
    let mut successful_bytes = 0u64;
    let mut flush_waiters = Vec::new();

    for r in reqs {
        let request_bytes = r.records.bytes_len();
        total_bytes = total_bytes.saturating_add(request_bytes);

        let dur = r.durability.unwrap_or(cfg.default_durability);
        let completion = r.completion;
        let staged_offset_tx = r.staged_offset_tx;
        // Staging only fills memory buffers, so it would still succeed after
        // an earlier write in this pass stopped the log.
        if let Some(failure) = log.storage_failure() {
            let _ = notify_tx.send(NotifyMsg::One {
                item: NotifyItem {
                    completion,
                    result: Err(IoError::new(format!(
                        "log stopped after storage failure, reopen required: {failure}"
                    ))),
                },
            });
            continue;
        }
        let result = match r.records {
            AppendPayload::One(message) => {
                #[cfg(feature = "writer-stage-trace")]
                let result = log.stage_append_traced(&message, now_ms, tracer, work_id);
                #[cfg(not(feature = "writer-stage-trace"))]
                let result = log.stage_append(&message, now_ms);
                result
            }
            AppendPayload::Many(messages) => {
                #[cfg(feature = "writer-stage-trace")]
                let result = log.stage_append_batch_traced(&messages, now_ms, tracer, work_id);
                #[cfg(not(feature = "writer-stage-trace"))]
                let result = log.stage_append_batch(&messages, now_ms);
                result
            }
        };

        match result {
            Ok((ar, end_offset)) => {
                successful_bytes = successful_bytes.saturating_add(request_bytes as u64);
                // Report the assigned offset as early as possible (staged, pre-ack).
                // A oneshot send is a non-blocking store plus a waker, safe on the
                // writer thread. The durability ack still flows through `completion`.
                if let Some(tx) = staged_offset_tx {
                    let _ = tx.send(ar.base_offset);
                }
                state.tail.store(end_offset + 1, Ordering::Release);
                if dur != KDurability::AfterFsync {
                    note_unclaimed_write(log, state, end_offset + 1, u64::from(ar.count));
                }
                if dur == KDurability::AfterWrite {
                    notify_tx
                        .send(NotifyMsg::One {
                            item: NotifyItem {
                                completion,
                                result: Ok(ar),
                            },
                        })
                        .ok();
                } else if dur == KDurability::AfterFlush {
                    flush_waiters.push(NotifyItem {
                        completion,
                        result: Ok(ar),
                    });
                } else {
                    pending.push_back(PendingAck {
                        end_offset,
                        respond_to: completion,
                        result: ar,
                    });
                }
            }
            Err(e) => {
                #[cfg(feature = "writer-stage-trace")]
                tracer.event(work_id, "stage_error", None, 0, 0, None, None);
                let _ = notify_tx.send(NotifyMsg::One {
                    item: NotifyItem {
                        completion,
                        result: Err(e.into()),
                    },
                });
            }
        }
    }

    if !flush_waiters.is_empty() {
        // One write covers the whole staging group, including any fsync
        // waiters in it. Their persistence wait stays separate. A failure
        // latches the log, so the writer stops on its next pass.
        #[cfg(feature = "writer-stage-trace")]
        let flushed = log
            .flush_buffers_traced(tracer, work_id)
            .and_then(|_| log.flush());
        #[cfg(not(feature = "writer-stage-trace"))]
        let flushed = log.flush_buffers().and_then(|_| log.flush());
        let items = match flushed {
            Ok(()) => flush_waiters,
            Err(error) => {
                log.record_storage_failure(format!("flush failed: {error}"));
                fail_notify_items(flush_waiters, error.to_string())
            }
        };
        let _ = notify_tx.send(NotifyMsg::Batch(items));
    }

    state
        .local_append_bytes
        .fetch_add(successful_bytes, Ordering::Relaxed);
    #[cfg(feature = "writer-stage-trace")]
    tracer.event(
        work_id,
        "stage",
        stage_started,
        log.next_offset().saturating_sub(first_offset) as usize,
        total_bytes,
        None,
        Some((first_offset, log.next_offset())),
    );
    total_bytes
}

/// Commit if due, retry a few times and fail all pending waiters on repeated errors.
#[allow(clippy::too_many_arguments)]
fn maybe_commit_due(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    last_fsync: &mut Instant,
    fsync_interval: Duration,
    min_fsync_interval: Duration,
    max_inflight_fsyncs: usize,
    pipeline_commit_records: u64,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    let needs_commit = pending_needs_fsync(pending);
    let pipeline = should_pipeline(log, pipeline_commit_records);
    // The interval branch may exceed the pipelining soft cap (one in-flight
    // fsync in the fat-batch regime) but never the hard channel capacity: a
    // commit past it cannot make durability progress (the fused fsync already
    // covers the whole queue) and only risks parking the writer in the send.
    let hard_cap = max_inflight_fsyncs.max(1);
    let commit_due = needs_commit
        && (commit_when_idle(
            *inflight_fsyncs,
            last_fsync,
            min_fsync_interval,
            log.loaded_fsync_spacing(),
            pipeline,
            max_inflight_fsyncs,
        ) || (last_fsync.elapsed() >= fsync_interval && *inflight_fsyncs < hard_cap));

    if !commit_due {
        return;
    }

    #[cfg(feature = "writer-stage-trace")]
    let work_id = tracer.next_work_id();

    let mut error_count = 0;
    while let Err(e) = commit(
        log,
        state,
        pending,
        last_fsync,
        notify_tx,
        fsync_tx,
        fsync_done_rx,
        inflight_fsyncs,
        max_inflight_fsyncs,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    ) {
        fail_all_pending(
            pending,
            format!("Internal Error while commiting: {e}"),
            notify_tx,
            true,
        );
        // Retrying cannot succeed once the log has stopped. The loop
        // switches to failed mode on its next pass.
        if log.storage_failure().is_some() {
            break;
        }

        if error_count > 3 {
            fail_all_pending(
                pending,
                "Internal Error while commiting writes over 3 times",
                notify_tx,
                true,
            );
            std::thread::sleep(Duration::from_millis(1000));
            break;
        }

        error_count += 1;
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// After staging, schedule commit / flush buffers and tune linger.
#[allow(clippy::too_many_arguments)]
fn post_stage_commit_and_tune(
    log: &mut Log,
    cfg: &KeratinConfig,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    last_fsync: &mut Instant,
    fsync_interval: Duration,
    total_bytes: usize,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    linger: &mut Duration,
    linger_min: Duration,
    linger_max: Duration,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) {
    // Commit scheduling
    let needs_commit = pending_needs_fsync(pending);
    let min_fsync_interval = Duration::from_millis(cfg.min_fsync_interval_ms);
    let pipeline = should_pipeline(log, cfg.pipeline_commit_records);
    // Interval branch hard-capped for the same reason as in maybe_commit_due.
    let hard_cap = cfg.max_inflight_fsyncs.max(1);
    let commit_due = needs_commit
        && (commit_when_idle(
            *inflight_fsyncs,
            last_fsync,
            min_fsync_interval,
            log.loaded_fsync_spacing(),
            pipeline,
            cfg.max_inflight_fsyncs,
        ) || (last_fsync.elapsed() >= fsync_interval && *inflight_fsyncs < hard_cap));

    if commit_due {
        let _ = commit(
            log,
            state,
            pending,
            last_fsync,
            notify_tx,
            fsync_tx,
            fsync_done_rx,
            inflight_fsyncs,
            cfg.max_inflight_fsyncs,
            #[cfg(feature = "writer-stage-trace")]
            tracer,
            #[cfg(feature = "writer-stage-trace")]
            work_id,
        );
    } else if log.should_flush() {
        #[cfg(feature = "writer-stage-trace")]
        let _ = log.flush_buffers_traced(tracer, work_id);
        #[cfg(not(feature = "writer-stage-trace"))]
        let _ = log.flush_buffers();
    }

    // Adaptive linger tuning (same heuristic)
    if total_bytes >= (cfg.max_batch_bytes / 2) {
        *linger = (*linger + Duration::from_millis(1)).min(linger_max);
    } else if total_bytes < 64 * 1024 && pending.is_empty() {
        *linger = linger
            .saturating_sub(Duration::from_millis(1))
            .max(linger_min);
    }
}

/// On shutdown/disconnect, fail any unstaged requests currently held by the batcher.
fn shutdown_fail_unstaged(
    batcher: &mut BatcherCore<AppendReq, impl FnMut(&AppendReq) -> (usize, usize)>,
    msg: &str,
    _why: FlushReason,
    notify_tx: &NotifyLink,
) {
    shutdown_fail_reqs(batcher.flush(), msg, notify_tx);
}

fn shutdown_fail_reqs(reqs: Vec<AppendReq>, msg: &str, notify_tx: &NotifyLink) {
    let mut items = Vec::new();
    for r in reqs {
        items.push(NotifyItem {
            completion: r.completion,
            result: Err(IoError {
                msg: msg.to_string().into(),
                overlap: None,
            }),
        });
    }
    if !items.is_empty() {
        let _ = notify_tx.send(NotifyMsg::Batch(items));
    }
}

/// The answer for commands that reach a writer after it stopped.
struct Refusal {
    kind: io::ErrorKind,
    msg: String,
}

impl Refusal {
    fn shut_down() -> Self {
        Self {
            kind: io::ErrorKind::BrokenPipe,
            msg: "log is shut down".to_string(),
        }
    }

    /// The writer stopped on an error. A latched storage failure is the more
    /// useful cause when there is one.
    fn stopped(log: &Log, cause: impl std::fmt::Display) -> Self {
        let msg = match log.storage_failure() {
            Some(failure) => storage_failure_message(failure),
            None => format!("log writer stopped, reopen required: {cause}"),
        };
        Self {
            kind: io::ErrorKind::Other,
            msg,
        }
    }

    fn io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.msg.clone())
    }
}

fn storage_failure_message(failure: &str) -> String {
    format!("log stopped after storage failure, reopen required: {failure}")
}

/// Answer one command on a stopped writer, or drop its reply sender so the
/// caller sees a closed writer. Runs on the writer thread, so appends are
/// completed here rather than through the notifier. `log` is present only
/// while the stopped log is still open.
fn reject_command(cmd: WriterCmd, refusal: &Refusal, log: Option<&Log>) {
    match cmd {
        WriterCmd::Append(req) => req.completion.complete(Err(IoError {
            msg: refusal.msg.as_str().into(),
            overlap: None,
        })),
        WriterCmd::ReplicatedAppend { respond_to, .. } => {
            let _ = respond_to.send(Err(IoError {
                msg: refusal.msg.as_str().into(),
                overlap: None,
            }));
        }
        WriterCmd::Truncate { respond_to, .. }
        | WriterCmd::AdvanceRetainedHead { respond_to, .. }
        | WriterCmd::AdvanceEpoch { respond_to, .. } => {
            let _ = respond_to.send(Err(refusal.io_error()));
        }
        WriterCmd::ResetToCheckpoint { respond_to, .. }
        | WriterCmd::RepairSuffix { respond_to, .. }
        | WriterCmd::Sync { respond_to } => {
            let _ = respond_to.send(Err(refusal.io_error()));
        }
        WriterCmd::ForkFrozen { respond_to, .. } => {
            let _ = respond_to.send(Err(refusal.io_error()));
        }
        WriterCmd::ReplaceSettings { respond_to, .. } => {
            let _ = respond_to.send(Err(refusal.io_error()));
        }
        // Dropping the reply reports a closed writer.
        WriterCmd::ReadSettings { .. } => {}
        // Read-only, and still useful to disk accounting while the log is open.
        WriterCmd::SizeEstimate { respond_to } => {
            let _ = respond_to.send(match log {
                Some(log) => log.estimate_disk_used(),
                None => Err(refusal.io_error()),
            });
        }
        // Shutdown work is already done or no longer possible.
        WriterCmd::Shutdown { notify_tx } => {
            let _ = notify_tx.send(());
        }
    }
}

/// Answer every request with an error until shutdown. After a storage
/// failure nothing written since the last good sync can be trusted, so
/// in-flight fsync completions are failed without publishing a frontier and
/// the log is never marked clean. Reopening the log runs the recovery scan.
#[allow(clippy::too_many_arguments)]
fn serve_after_storage_failure(
    log: &mut Log,
    state: &Arc<LogState>,
    unstaged: Vec<AppendReq>,
    pending: &mut PendingAcks,
    writes_rx: &Receiver<WriterCmd>,
    notify_tx: &NotifyLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) -> Refusal {
    let refusal = Refusal::stopped(log, "storage failure");
    shutdown_fail_reqs(unstaged, &refusal.msg, notify_tx);
    fail_all_pending(pending, &refusal.msg, notify_tx, true);
    wait_for_inflight_fsyncs(
        log,
        state,
        notify_tx,
        fsync_done_rx,
        inflight_fsyncs,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
    );
    while let Some(cmd) = writes_rx.recv().ok() {
        let shutdown = matches!(cmd, WriterCmd::Shutdown { .. });
        reject_command(cmd, &refusal, Some(log));
        if shutdown {
            break;
        }
    }
    refusal
}

/// Test-only hooks keyed by log root, so parallel tests do not see each
/// other's writers.
#[cfg(test)]
pub(crate) mod test_hooks {
    use crossbeam_channel::{Receiver, Sender};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct Hooks {
        // (reached, release) for a writer paused after it acknowledges Shutdown.
        shutdown_pause: Option<(Sender<()>, Receiver<()>)>,
        exited: Option<Sender<()>>,
    }

    static HOOKS: LazyLock<Mutex<HashMap<PathBuf, Hooks>>> = LazyLock::new(Default::default);

    /// Hold the writer for `root` right after it acknowledges Shutdown. The
    /// first receiver fires when the writer reaches the pause, and sending on
    /// (or dropping) the returned sender lets it continue.
    pub(crate) fn pause_after_shutdown(root: &Path) -> (Receiver<()>, Sender<()>) {
        let (reached_tx, reached_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        HOOKS
            .lock()
            .unwrap()
            .entry(root.to_path_buf())
            .or_default()
            .shutdown_pause = Some((reached_tx, release_rx));
        (reached_rx, release_tx)
    }

    /// Fires (or disconnects) when the writer thread for `root` exits.
    pub(crate) fn on_writer_exit(root: &Path) -> Receiver<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        HOOKS
            .lock()
            .unwrap()
            .entry(root.to_path_buf())
            .or_default()
            .exited = Some(tx);
        rx
    }

    pub(super) fn shutdown_acknowledged(root: &Path) {
        let pause = HOOKS
            .lock()
            .unwrap()
            .get_mut(root)
            .and_then(|hooks| hooks.shutdown_pause.take());
        if let Some((reached, release)) = pause {
            let _ = reached.send(());
            let _ = release.recv();
        }
    }

    pub(super) fn writer_exited(root: &Path) {
        let exited = HOOKS
            .lock()
            .unwrap()
            .remove(root)
            .and_then(|hooks| hooks.exited);
        if let Some(exited) = exited {
            let _ = exited.send(());
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Timed-out receives on this writer thread. Lets tests detect a busy
    /// loop without platform CPU counters.
    static RECEIVE_TIMEOUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn recv_writer_event(
    writes_rx: &Receiver<WriterCmd>,
    fsync: &FsyncLink,
    wait: Duration,
) -> WriterEvent {
    // With no fsync worker running there is nothing to complete.
    let done_rx = fsync.receiver().unwrap_or_else(crossbeam_channel::never);
    if wait == Duration::MAX {
        crossbeam_channel::select! {
            recv(writes_rx) -> msg => match msg {
                Ok(cmd) => WriterEvent::Command(cmd),
                Err(_) => WriterEvent::WritesDisconnected,
            },
            recv(done_rx) -> msg => match msg {
                Ok(done) => WriterEvent::FsyncDone(done),
                Err(_) => WriterEvent::FsyncDisconnected,
            },
        }
    } else {
        crossbeam_channel::select! {
            recv(writes_rx) -> msg => match msg {
                Ok(cmd) => WriterEvent::Command(cmd),
                Err(_) => WriterEvent::WritesDisconnected,
            },
            recv(done_rx) -> msg => match msg {
                Ok(done) => WriterEvent::FsyncDone(done),
                Err(_) => WriterEvent::FsyncDisconnected,
            },
            default(wait) => WriterEvent::Timeout,
        }
    }
}

fn drain_fsync_done(
    log: &mut Log,
    state: &Arc<LogState>,
    notify_tx: &NotifyLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    loop {
        match fsync_done_rx.try_recv() {
            Ok(done) => handle_fsync_done(
                log,
                state,
                notify_tx,
                done,
                inflight_fsyncs,
                #[cfg(feature = "writer-stage-trace")]
                tracer,
            ),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
}

// Fsync everything written so far for a settings change and wait for the
// result. The sync reply carries the physical sync and manifest errors, so a
// failed barrier can never report the change as adopted.
#[allow(clippy::too_many_arguments)]
fn settings_sync_boundary(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    fsync_done_rx: &FsyncLink,
    inflight: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) -> io::Result<()> {
    wait_for_inflight_fsyncs(
        log,
        state,
        notify_tx,
        fsync_done_rx,
        inflight,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
    );
    #[cfg(feature = "writer-stage-trace")]
    let work_id = tracer.next_work_id();
    #[cfg(feature = "writer-stage-trace")]
    let job = log.prepare_fsync_job_traced(tracer, work_id)?;
    #[cfg(not(feature = "writer-stage-trace"))]
    let job = log.prepare_fsync_job()?;
    let ready = take_covered_acks(pending, job.durable_end());
    let (respond_to, result) = tokio::sync::oneshot::channel();
    let request = FsyncReq {
        job,
        ready,
        sync_acks: vec![respond_to],
        background: false,
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    };
    if let Err(error) = fsync_tx.send(request) {
        send_notify_items(
            notify_tx,
            fail_notify_items(error.0.ready, "fsync worker disconnected"),
        );
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "fsync worker disconnected",
        ));
    }
    *inflight += 1;
    wait_for_inflight_fsyncs(
        log,
        state,
        notify_tx,
        fsync_done_rx,
        inflight,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
    );
    result
        .blocking_recv()
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "sync result dropped"))?
}

fn wait_for_inflight_fsyncs(
    log: &mut Log,
    state: &Arc<LogState>,
    notify_tx: &NotifyLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    while *inflight_fsyncs > 0 {
        match fsync_done_rx.recv() {
            Ok(done) => handle_fsync_done(
                log,
                state,
                notify_tx,
                done,
                inflight_fsyncs,
                #[cfg(feature = "writer-stage-trace")]
                tracer,
            ),
            Err(_) => break,
        }
    }
}

/// Block until the fsync pipeline has a free slot, draining completions while
/// waiting. Every `fsync_tx` send must run behind this gate. The channel holds
/// `max_inflight` requests, and a blocking send on a full channel can deadlock
/// the writer with the fsync worker: the worker's fused drain can owe more
/// FsyncDones than the equally bounded done channel accepts, leaving each
/// thread parked in a send only the other can unblock. Draining here keeps the
/// done channel moving and holds the in-flight count at or under the cap, so
/// queue occupancy (which never exceeds that count) always leaves the send
/// after this gate a free slot. On the scheduled commit paths the callers
/// already gate on the cap, so this loop takes zero iterations. It only waits
/// on the unconditional paths (Sync, epoch advance, shutdown).
fn ensure_fsync_slot(
    log: &mut Log,
    state: &Arc<LogState>,
    notify_tx: &NotifyLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    max_inflight: usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) -> io::Result<()> {
    while *inflight_fsyncs >= max_inflight.max(1) {
        match fsync_done_rx.recv() {
            Ok(done) => handle_fsync_done(
                log,
                state,
                notify_tx,
                done,
                inflight_fsyncs,
                #[cfg(feature = "writer-stage-trace")]
                tracer,
            ),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "fsync worker disconnected",
                ));
            }
        }
    }
    Ok(())
}

fn handle_fsync_done(
    log: &mut Log,
    state: &Arc<LogState>,
    notify_tx: &NotifyLink,
    done: FsyncDone,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    *inflight_fsyncs = inflight_fsyncs.saturating_sub(1);
    if done.background {
        state.sync.background_finished();
    }
    #[cfg(feature = "writer-stage-trace")]
    tracer.event(
        done.work_id,
        "completion_received",
        None,
        done.ready.len(),
        0,
        None,
        Some((0, done.durable_end)),
    );

    if let Some(error) = done.error {
        tracing::error!("fsync job failed: {error}");
        log.record_storage_failure(format!("fsync failed: {error}"));
        send_notify_items(notify_tx, done.ready);
        answer_sync_acks(done.sync_acks, Err(io::Error::other(error)));
        return;
    }

    #[cfg(feature = "writer-stage-trace")]
    let finish_result =
        log.finish_fsync_job_traced(done.durable_end, done.elapsed, tracer, done.work_id);
    #[cfg(not(feature = "writer-stage-trace"))]
    let finish_result = log.finish_fsync_job(done.durable_end, done.elapsed);

    match finish_result {
        Ok(()) => {
            // Publish the durable frontier (unambiguous for empty vs offset 0);
            // `log` already applied this fsync via finish_fsync_job. Monotonic
            // advance: fsync completions may arrive out of order, but the frontier
            // must never rewind on the steady-state path.
            state.durable.advance(log.durable_end_exclusive());
            #[cfg(feature = "writer-stage-trace")]
            tracer.event(
                done.work_id,
                "durable_published",
                None,
                done.ready.len(),
                0,
                None,
                Some((0, done.durable_end)),
            );
            send_notify_items(notify_tx, done.ready);
            answer_sync_acks(done.sync_acks, Ok(()));
        }
        Err(err) => {
            tracing::error!("finishing fsync job failed: {err}");
            let msg = err.to_string();
            send_notify_items(notify_tx, fail_notify_items(done.ready, &msg));
            answer_sync_acks(done.sync_acks, Err(io::Error::other(msg)));
        }
    }
}

/// Fire the `WriterCmd::Sync` responders carried by a finished fsync job. Each ack
/// gets a clone of the result. A dropped receiver (caller gone) is ignored.
fn answer_sync_acks(
    acks: Vec<tokio::sync::oneshot::Sender<io::Result<()>>>,
    result: io::Result<()>,
) {
    for ack in acks {
        let cloned = result
            .as_ref()
            .map(|_| ())
            .map_err(|e| io::Error::new(e.kind(), e.to_string()));
        let _ = ack.send(cloned);
    }
}

fn send_notify_items(notify_tx: &NotifyLink, mut items: Vec<NotifyItem>) {
    if items.is_empty() {
        return;
    }

    if items.len() == 1 {
        if let Some(item) = items.pop() {
            notify_tx.send(NotifyMsg::One { item }).ok();
        }
    } else {
        notify_tx.send(NotifyMsg::Batch(items)).ok();
    }
}

fn fail_notify_items(items: Vec<NotifyItem>, err_msg: impl AsRef<str>) -> Vec<NotifyItem> {
    let err_msg = err_msg.as_ref().to_string();
    items
        .into_iter()
        .map(|item| NotifyItem {
            completion: item.completion,
            result: Err(IoError::new(&err_msg)),
        })
        .collect()
}

/// Track records an owner wrote without anyone waiting for their sync.
/// Records written in the follower role, as replicated appends always are,
/// are left out, so they never count against the log after a promotion.
fn note_unclaimed_write(log: &mut Log, state: &LogState, end: u64, records: u64) {
    if state.sync.is_owner() {
        log.note_unclaimed_write(end, records);
    }
}

/// When the next background sync may start, or `None` when none is wanted.
/// One is wanted when records are written to the OS but not durable, no sync
/// of any kind is in flight and the interval is not zero. Any in-flight sync
/// already moves the durable frontier, and its completion wakes the writer to
/// look again, so at most one background sync is ever outstanding. The time
/// counts from the start of the last sync of any kind, so a commit fsync
/// pushes the next tick back and a slow sync never leaves missed ticks to
/// catch up on.
fn background_sync_due(
    log: &Log,
    state: &LogState,
    interval: Duration,
    inflight_fsyncs: usize,
) -> Option<Instant> {
    if interval.is_zero() || inflight_fsyncs > 0 || log.storage_failure().is_some() {
        return None;
    }
    // The published frontiers, so a durable frontier that an inline sync
    // moved without publishing still gets published by the next tick.
    if state.written.load() <= state.durable.load().first_non_durable() {
        return None;
    }
    log.last_sync_started.checked_add(interval)
}

/// Start a background sync through the fsync worker when one is due. It
/// covers everything written so far, and `AfterFsync` appends still waiting
/// for a commit that it covers complete with it. A failure latches the log in
/// `handle_fsync_done` like any other failed sync.
#[allow(clippy::too_many_arguments)]
fn maybe_start_background_sync(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    last_fsync: &mut Instant,
    interval: Duration,
    expect_every_write_synced: bool,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    let Some(due) = background_sync_due(log, state, interval, *inflight_fsyncs) else {
        return;
    };
    if Instant::now() < due {
        return;
    }
    #[cfg(feature = "writer-stage-trace")]
    let work_id = tracer.next_work_id();
    #[cfg(feature = "writer-stage-trace")]
    let job = log.prepare_fsync_job_traced(tracer, work_id);
    #[cfg(not(feature = "writer-stage-trace"))]
    let job = log.prepare_fsync_job();
    // A failed write latches the log and the loop stops on its next pass.
    let Ok(job) = job else {
        return;
    };
    // The job covers everything staged, so it covers every unclaimed record.
    if expect_every_write_synced && state.sync.is_owner() {
        let unclaimed = log.unclaimed_unsynced_records();
        if unclaimed > 0 && state.sync.note_unexpected_background_sync() {
            tracing::warn!(
                log = %log.root().display(),
                unclaimed_records = unclaimed,
                since_last_sync_ms = state.sync.since_last_sync().as_millis() as u64,
                "background sync covered records that no append waited to sync on a log \
                 that expects every write synced, so some write path skipped its sync"
            );
        }
    }
    let ready = take_covered_acks(pending, job.durable_end());
    if !ready.is_empty() {
        *last_fsync = Instant::now();
    }
    let req = FsyncReq {
        job,
        ready,
        sync_acks: Vec::new(),
        background: true,
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    };
    #[cfg(feature = "writer-stage-trace")]
    tracer.event(
        work_id,
        "background_sync_queued",
        None,
        req.ready.len(),
        0,
        None,
        Some((0, req.job.durable_end())),
    );
    // Nothing is in flight, so the channel has a free slot and this send
    // cannot park (see `ensure_fsync_slot`).
    if let Err(err) = fsync_tx.send(req) {
        let FsyncReq { ready, .. } = err.into_inner();
        send_notify_items(
            notify_tx,
            fail_notify_items(ready, "fsync worker disconnected"),
        );
        return;
    }
    *inflight_fsyncs += 1;
    state.sync.background_started();
}

#[allow(clippy::too_many_arguments)]
/// Sync staged records for the appends waiting on an fsync. Skips the fsync
/// when none waits, so written records nobody waits for stay unsynced.
fn commit(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    last_fsync: &mut Instant,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    max_inflight: usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> Result<(), io::Error> {
    commit_inner(
        log,
        state,
        pending,
        last_fsync,
        notify_tx,
        fsync_tx,
        fsync_done_rx,
        inflight_fsyncs,
        max_inflight,
        false,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    )
}

/// Like `commit`, but syncs everything written or staged even when no append
/// waits, as `AfterFlush` appends never do. For controls that must see the
/// whole log durable: epoch fences, frozen forks and retained head moves.
/// Shutdown syncs the files itself.
fn commit_all(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    last_fsync: &mut Instant,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    max_inflight: usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> Result<(), io::Error> {
    commit_inner(
        log,
        state,
        pending,
        last_fsync,
        notify_tx,
        fsync_tx,
        fsync_done_rx,
        inflight_fsyncs,
        max_inflight,
        true,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    )
}

#[allow(clippy::too_many_arguments)]
fn commit_inner(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut PendingAcks,
    last_fsync: &mut Instant,
    notify_tx: &NotifyLink,
    fsync_tx: &FsyncLink,
    fsync_done_rx: &FsyncLink,
    inflight_fsyncs: &mut usize,
    max_inflight: usize,
    all: bool,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> Result<(), io::Error> {
    #[cfg(feature = "writer-stage-trace")]
    let prepare_started = tracer.start();
    #[cfg(feature = "writer-stage-trace")]
    tracer.event(work_id, "commit_begin", None, 0, 0, None, None);
    // Gate before preparing so the job covers everything staged by the time a
    // slot frees, and so the send below can never park (see ensure_fsync_slot).
    ensure_fsync_slot(
        log,
        state,
        notify_tx,
        fsync_done_rx,
        inflight_fsyncs,
        max_inflight,
        #[cfg(feature = "writer-stage-trace")]
        tracer,
    )?;

    #[cfg(feature = "writer-stage-trace")]
    let job = log.prepare_fsync_job_traced(tracer, work_id)?;
    #[cfg(not(feature = "writer-stage-trace"))]
    let job = log.prepare_fsync_job()?;
    #[cfg(feature = "writer-stage-trace")]
    tracer.event(
        work_id,
        "commit_prepare",
        prepare_started,
        0,
        0,
        None,
        Some((0, job.durable_end())),
    );
    *last_fsync = Instant::now();

    let durable_end = job.durable_end();

    // Track the per-commit record count (EWMA) to gate fsync pipelining. Driven by
    // the linger/batcher (how much accrues per commit), not by the gate, so it does
    // not oscillate with the pipelining decision.
    let commit_records = durable_end.saturating_sub(log.last_commit_through);
    log.last_commit_through = durable_end;
    log.recent_commit_records = (log.recent_commit_records * 3 + commit_records) / 4;

    let ready = take_covered_acks(pending, durable_end);

    if ready.is_empty() && !(all && durable_end > log.durable_end_exclusive().first_non_durable()) {
        return Ok(());
    }

    let req = FsyncReq {
        job,
        ready,
        sync_acks: Vec::new(),
        background: false,
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    };

    #[cfg(feature = "writer-stage-trace")]
    tracer.event(
        work_id,
        "commit_queued",
        None,
        req.ready.len(),
        0,
        None,
        Some((0, req.job.durable_end())),
    );
    if let Err(err) = fsync_tx.send(req) {
        let FsyncReq { ready, .. } = err.into_inner();
        send_notify_items(
            notify_tx,
            fail_notify_items(ready, "fsync worker disconnected"),
        );
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "fsync worker disconnected",
        ));
    }

    *inflight_fsyncs = inflight_fsyncs.saturating_add(1);
    log.stats.batches += 1;

    Ok(())
}

fn take_covered_acks(pending: &mut PendingAcks, durable_end: u64) -> Vec<NotifyItem> {
    let mut ready = Vec::new();
    while let Some(front) = pending.front() {
        if front.end_offset < durable_end {
            let p = pending
                .pop_front()
                .expect("front() returned Some on the previous line");
            ready.push(NotifyItem {
                completion: p.respond_to,
                result: Ok(p.result),
            });
        } else {
            break;
        }
    }
    ready
}

fn fail_all_pending(
    pending: &mut PendingAcks,
    err_msg: impl AsRef<str>,
    notify_tx: &NotifyLink,
    error: bool,
) {
    if error {
        tracing::error!("{}", err_msg.as_ref());
    } else {
        tracing::info!("{}", err_msg.as_ref());
    }
    let mut items = Vec::new();

    while let Some(p) = pending.pop_front() {
        items.push(NotifyItem {
            completion: p.respond_to,
            result: Err(IoError {
                msg: err_msg.as_ref().to_string().into(),
                overlap: None,
            }),
        });
    }

    notify_tx.send(NotifyMsg::Batch(items)).ok();
}

#[cfg(test)]
mod drain_batch_tests {
    use super::*;

    fn batcher() -> BatcherCore<u64, impl FnMut(&u64) -> (usize, usize)> {
        BatcherCore::new(
            BatcherConfig {
                max_items: 8,
                max_records: 8,
                max_bytes: 1024,
                linger: Duration::from_secs(60),
            },
            |_| (1, 8),
        )
    }

    #[test]
    fn drain_preserves_minimum_interval_and_pipeline_capacity() {
        let now = Instant::now();
        assert!(!commit_when_idle(
            0,
            &now,
            Duration::from_secs(60),
            Duration::ZERO,
            true,
            8
        ));
        assert!(!commit_when_idle(
            8,
            &now,
            Duration::ZERO,
            Duration::ZERO,
            true,
            8
        ));
        assert!(!commit_when_idle(
            1,
            &now,
            Duration::ZERO,
            Duration::ZERO,
            false,
            8
        ));
        assert!(commit_when_idle(
            0,
            &now,
            Duration::ZERO,
            Duration::ZERO,
            true,
            8
        ));
    }

    #[test]
    fn loaded_spacing_applies_only_while_an_fsync_is_in_flight() {
        let now = Instant::now();
        let spacing = Duration::from_secs(60);
        assert!(commit_when_idle(0, &now, Duration::ZERO, spacing, true, 8));
        assert!(!commit_when_idle(1, &now, Duration::ZERO, spacing, true, 8));
        let earlier = now - Duration::from_millis(5);
        assert!(commit_when_idle(
            1,
            &earlier,
            Duration::ZERO,
            Duration::from_millis(2),
            true,
            8
        ));
    }
}

#[cfg(test)]
mod worker_link_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A worker starts on the first send, retires once idle with nothing in
    /// flight, and a later send starts a fresh one.
    #[test]
    fn notify_worker_starts_on_demand_and_retires_when_idle() {
        let spawned = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(AtomicUsize::new(0));
        let link = {
            let spawned = spawned.clone();
            let delivered = delivered.clone();
            NotifyLink::on_demand(
                4,
                Box::new(move |rx: Receiver<NotifyMsg>| {
                    spawned.fetch_add(1, Ordering::SeqCst);
                    let delivered = delivered.clone();
                    std::thread::spawn(move || {
                        while rx.recv().is_ok() {
                            delivered.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                }),
            )
        };
        assert!(!link.running());
        link.send(NotifyMsg::Batch(Vec::new())).unwrap();
        assert!(link.running());
        assert_eq!(spawned.load(Ordering::SeqCst), 1);
        link.retire_if_idle();
        assert!(link.running(), "retired before the idle period");
        std::thread::sleep(WORKER_IDLE + Duration::from_millis(50));
        link.retire_if_idle();
        assert!(!link.running());
        link.send(NotifyMsg::Batch(Vec::new())).unwrap();
        assert_eq!(spawned.load(Ordering::SeqCst), 2);
        let deadline = Instant::now() + Duration::from_secs(2);
        while delivered.load(Ordering::SeqCst) < 2 {
            assert!(Instant::now() < deadline, "a notification was lost across the restart");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The fsync worker is never retired with a job in flight.
    #[test]
    fn fsync_worker_stays_while_a_job_is_in_flight() {
        let link = FsyncLink::on_demand(1, Box::new(|_rx, _done| {}));
        let (tx, rx) = crossbeam_channel::bounded::<FsyncReq>(1);
        let (_done_tx, done_rx) = crossbeam_channel::bounded::<FsyncDone>(1);
        drop((tx, rx));
        *link.current.borrow_mut() = Some((crossbeam_channel::bounded(1).0, done_rx));
        link.last_send.set(Instant::now() - WORKER_IDLE * 2);
        link.retire_if_idle(1);
        assert!(link.running());
        link.retire_if_idle(0);
        assert!(!link.running());
    }
}

#[cfg(all(test, feature = "io-fault-injection", not(feature = "writer-stage-trace")))]
mod merged_fsync_test {
    use super::*;
    use crate::DurableFrontier;

    /// A roll hands the outgoing segment's sync to the next fsync job. When
    /// that job is merged with a later one, its deferred syncs must still run:
    /// a failing sync of the old segment has to fail the whole group.
    #[test]
    fn merged_fsync_group_runs_every_jobs_deferred_syncs() {
        let dir = crate::test_dir!("merged_fsync_deferred");
        let cfg = KeratinConfig {
            segment_max_bytes: 64 * 1024,
            tail_cache_bytes: 0,
            ..KeratinConfig::test_default()
        };
        let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
        let (mut log, _) = Log::open(
            &dir.root,
            crate::util::unix_millis(),
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            0,
            0,
            false,
            false,
            state,
            None,
        )
        .unwrap();
        let message = Message {
            flags: 0,
            headers: vec![],
            payload: vec![7; 1024],
        };
        let mut ts = 1;
        while log.active.base_offset == 0 {
            log.stage_append(&message, ts).unwrap();
            ts += 1;
        }
        let old_segment = dir.root.join("segments/00000000000000000000.log");
        // The first job after the roll carries the old segment's sync.
        let first = log.prepare_fsync_job().unwrap();
        log.stage_append(&message, ts).unwrap();
        let second = log.prepare_fsync_job().unwrap();
        let _armed = crate::io_fault_injection::arm(&old_segment, crate::io_fault_injection::Fault::Fsync).unwrap();

        let (jobs, requests) = crossbeam_channel::unbounded::<FsyncReq>();
        let (done, completed) = crossbeam_channel::unbounded();
        for job in [first, second] {
            jobs.send(FsyncReq {
                job,
                ready: Vec::new(),
                sync_acks: Vec::new(),
                background: false,
            })
            .unwrap();
        }
        drop(jobs);
        fsync_loop_with(requests, done, |job, _| job.sync());
        let results: Vec<FsyncDone> = completed.try_iter().collect();
        assert_eq!(results.len(), 2);
        assert!(
            results.iter().all(|r| r.error.is_some()),
            "a merged group reported success without syncing the old segment"
        );
    }
}

#[cfg(all(test, not(feature = "writer-stage-trace")))]
mod held_pipeline_test {
    use super::*;
    use crate::DurableFrontier;

    #[test]
    fn writer_waits_without_spinning_at_fsync_cap() {
        // Holds in both scheduling modes. At the cap only a completion can
        // unblock a commit, so the writer must block rather than poll.
        let dir = crate::test_dir!("held_interval_cap");
        let cfg = KeratinConfig {
            max_inflight_fsyncs: 1,
            fsync_interval_ms: 1,
            min_fsync_interval_ms: 0,
            tail_cache_bytes: 0,
            ..KeratinConfig::test_default()
        };
        let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
        let (mut log, _) = Log::open(
            &dir.root,
            crate::util::unix_millis(),
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            0,
            0,
            false,
            false,
            state.clone(),
            None,
        )
        .unwrap();
        let (writes, input) = crossbeam_channel::bounded(8);
        let (notifications, notified) = crossbeam_channel::unbounded();
        let (jobs, requests) = crossbeam_channel::bounded::<FsyncReq>(1);
        let (done, completed) = crossbeam_channel::bounded(1);
        let writer_state = state.clone();
        let worker = std::thread::spawn(move || {
            writer_loop(
                &mut log,
                cfg,
                &input,
                writer_state,
                NotifyLink::fixed(notifications),
                FsyncLink::fixed(jobs, completed),
            );
            super::RECEIVE_TIMEOUTS.with(|count| count.get())
        });
        let append = |id| {
            let (reply, _) = tokio::sync::oneshot::channel();
            writes
                .send(WriterCmd::Append(AppendReq {
                    records: AppendPayload::One(Message {
                        flags: 0,
                        headers: vec![],
                        payload: vec![id],
                    }),
                    durability: Some(KDurability::AfterFsync),
                    completion: AppendCompletionTarget::Oneshot(reply),
                    staged_offset_tx: None,
                }))
                .unwrap();
        };
        let complete = |request: FsyncReq| {
            let elapsed = request.job.sync().unwrap();
            done.send(FsyncDone {
                durable_end: request.job.durable_end(),
                elapsed,
                ready: request.ready,
                sync_acks: request.sync_acks,
                error: None,
                background: request.background,
            })
            .unwrap();
        };
        append(1);
        let first = requests.recv_timeout(Duration::from_secs(3)).unwrap();
        append(2);
        let deadline = Instant::now() + Duration::from_secs(3);
        while state.tail.load(Ordering::Acquire) != 2 {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        // Let the interval deadline expire well past its 1 ms horizon while
        // the only fsync slot is held.
        std::thread::sleep(Duration::from_millis(200));
        assert!(requests.try_recv().is_err());
        complete(first);
        let second = requests.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(second.job.durable_end(), 2);
        complete(second);
        for _ in 0..2 {
            notified.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        assert!(state.durable.load().covers(1));
        let (reply, finished) = tokio::sync::oneshot::channel();
        writes
            .send(WriterCmd::Shutdown { notify_tx: reply })
            .unwrap();
        finished.blocking_recv().unwrap();
        let timeouts = worker.join().unwrap();
        // A spinning writer times out hundreds of thousands of times here.
        // Batch and staging deadlines legitimately add a handful.
        assert!(
            timeouts < 100,
            "writer timed out {timeouts} times while waiting for a held fsync"
        );
    }
    // The worker below holds every fsync until the test completes it, so the
    // writer's background sync can be watched one request at a time.
    #[test]
    fn background_sync_keeps_one_outstanding_and_merges_waiting_acks() {
        let dir = crate::test_dir!("held_background_sync");
        let cfg = KeratinConfig {
            // Commits would wait a minute on their own, so only the background
            // sync can complete the AfterFsync append below.
            fsync_interval_ms: 60_000,
            min_fsync_interval_ms: 60_000,
            background_sync_interval: Duration::from_millis(5),
            tail_cache_bytes: 0,
            ..KeratinConfig::test_default()
        };
        let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
        let (mut log, _) = Log::open(
            &dir.root,
            crate::util::unix_millis(),
            cfg.segment_max_bytes,
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            0,
            0,
            false,
            false,
            state.clone(),
            None,
        )
        .unwrap();
        let (writes, input) = crossbeam_channel::bounded(8);
        let (notifications, notified) = crossbeam_channel::unbounded();
        let (jobs, requests) = crossbeam_channel::bounded::<FsyncReq>(8);
        let (done, completed) = crossbeam_channel::bounded(8);
        let writer_state = state.clone();
        let worker = std::thread::spawn(move || {
            writer_loop(
                &mut log,
                cfg,
                &input,
                writer_state,
                NotifyLink::fixed(notifications),
                FsyncLink::fixed(jobs, completed),
            );
        });
        let append = |id, durability| {
            let (reply, receipt) = tokio::sync::oneshot::channel();
            writes
                .send(WriterCmd::Append(AppendReq {
                    records: AppendPayload::One(Message {
                        flags: 0,
                        headers: vec![],
                        payload: vec![id],
                    }),
                    durability: Some(durability),
                    completion: AppendCompletionTarget::Oneshot(reply),
                    staged_offset_tx: None,
                }))
                .unwrap();
            receipt
        };
        let complete = |request: FsyncReq| {
            let elapsed = request.job.sync().unwrap();
            done.send(FsyncDone {
                durable_end: request.job.durable_end(),
                elapsed,
                ready: request.ready,
                sync_acks: request.sync_acks,
                error: None,
                background: request.background,
            })
            .unwrap();
        };
        let wait_for = |what: &str, check: &dyn Fn() -> bool| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !check() {
                assert!(Instant::now() < deadline, "timed out waiting for {what}");
                std::thread::sleep(Duration::from_millis(1));
            }
        };
        let deliver = |count: usize| {
            for _ in 0..count {
                match notified.recv_timeout(Duration::from_secs(5)).unwrap() {
                    NotifyMsg::One { item } => item.completion.complete(item.result),
                    NotifyMsg::Batch(items) => {
                        for item in items {
                            item.completion.complete(item.result);
                        }
                    }
                }
            }
        };

        // A waiting AfterFsync append is staged but not written. The AfterFlush
        // append behind it writes both to the OS.
        let mut fsync_ack = append(0, KDurability::AfterFsync);
        let _ = append(1, KDurability::AfterFlush);
        let first = requests.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(first.background);
        assert_eq!(first.job.durable_end(), 2);
        assert_eq!(
            first.ready.len(),
            1,
            "the waiting AfterFsync ack rides along"
        );
        assert_eq!(state.written.load(), 2);
        assert!(state.sync.status(2, 0).background_sync_pending);

        // More written records arrive while the first sync is held. Many
        // intervals pass, but no second sync starts and nothing turns durable
        // that a sync has not covered.
        let _ = append(2, KDurability::AfterFlush);
        wait_for("third record written", &|| state.written.load() == 3);
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            requests.try_recv().is_err(),
            "one background sync at a time"
        );
        assert_eq!(state.durable.load().first_non_durable(), 0);
        assert!(matches!(
            fsync_ack.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        // Completing it publishes only what it covered and releases the ack.
        complete(first);
        let second = requests.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(state.durable.load().covers(1));
        assert!(second.background);
        assert_eq!(second.job.durable_end(), 3);
        deliver(1);
        loop {
            match fsync_ack.try_recv() {
                Ok(result) => {
                    assert_eq!(result.unwrap().base_offset, 0);
                    break;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => deliver(1),
                Err(error) => panic!("{error}"),
            }
        }
        complete(second);
        wait_for("durable catches up", &|| {
            state.durable.load().first_non_durable() == 3
        });

        // Caught up and idle: missed ticks are not replayed.
        std::thread::sleep(Duration::from_millis(100));
        assert!(requests.try_recv().is_err(), "an idle log does not sync");
        let status = state.sync.status(3, 3);
        assert_eq!(status.background_syncs_started, 2);
        assert!(!status.background_sync_pending);

        let (reply, finished) = tokio::sync::oneshot::channel();
        writes
            .send(WriterCmd::Shutdown { notify_tx: reply })
            .unwrap();
        finished.blocking_recv().unwrap();
        worker.join().unwrap();
    }
}

#[cfg(test)]
mod batch_trace_tests {
    use super::*;
    use crate::DurableFrontier;

    fn check_fusion(fail: bool, across_roll: bool, enabled: bool) {
        let dir = crate::test_dir!("fsync_trace_parity");
        let cfg = KeratinConfig::test_default();
        let state = Arc::new(LogState::new(0, 0, DurableFrontier::from_exclusive(0)));
        let (mut log, _) = Log::open(
            &dir.root,
            0,
            if across_roll {
                1
            } else {
                cfg.segment_max_bytes
            },
            cfg.index_stride_bytes,
            cfg.flush_target_bytes,
            cfg.tail_cache_bytes,
            0,
            false,
            false,
            state,
            cfg.adaptive_staging,
        )
        .unwrap();
        #[cfg(feature = "writer-stage-trace")]
        let tracer = if enabled {
            WriterStageTracer::for_test(&dir.root.join("trace.jsonl"))
        } else {
            WriterStageTracer::default()
        };
        #[cfg(not(feature = "writer-stage-trace"))]
        let _ = enabled;
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (done_tx, done_rx) = crossbeam_channel::bounded(4);
        let mut replies = Vec::new();
        let mut sync_replies = Vec::new();
        for i in 0..3 {
            let (result, _) = log
                .stage_append(
                    &Message {
                        flags: 0,
                        headers: vec![],
                        payload: vec![i; 64],
                    },
                    1,
                )
                .unwrap();
            let job = log.prepare_fsync_job().unwrap();
            let (reply, receive) = tokio::sync::oneshot::channel();
            replies.push(receive);
            let (sync_reply, sync_receive) = tokio::sync::oneshot::channel();
            sync_replies.push(sync_receive);
            tx.send(FsyncReq {
                job,
                ready: vec![NotifyItem {
                    completion: AppendCompletionTarget::Oneshot(reply),
                    result: Ok(result),
                }],
                sync_acks: vec![sync_reply],
                background: false,
                #[cfg(feature = "writer-stage-trace")]
                work_id: tracer.next_work_id(),
            })
            .unwrap();
        }
        drop(tx);
        let mut calls = 0;
        fsync_loop_with(
            rx,
            done_tx,
            |job, _| {
                calls += 1;
                assert_eq!(job.durable_end(), 3);
                if fail {
                    Err(io::Error::other("injected sync failure"))
                } else {
                    job.sync()?;
                    Ok(Duration::from_millis(7))
                }
            },
            #[cfg(feature = "writer-stage-trace")]
            tracer,
        );
        assert_eq!(calls, 1, "tracing must preserve physical fsync fusion");
        let mut elapsed = Duration::ZERO;
        for (index, done) in done_rx.into_iter().enumerate() {
            assert_eq!(done.durable_end, index as u64 + 1);
            assert_eq!(done.error.is_some(), fail);
            elapsed += done.elapsed;
            answer_sync_acks(
                done.sync_acks,
                done.error.map_or(Ok(()), |e| Err(io::Error::other(e))),
            );
            for item in done.ready {
                item.completion.complete(item.result);
            }
        }
        assert_eq!(
            elapsed,
            if fail {
                Duration::ZERO
            } else {
                Duration::from_millis(7)
            }
        );
        for (index, mut reply) in replies.into_iter().enumerate() {
            let result = reply.try_recv().unwrap();
            assert_eq!(result.is_err(), fail);
            if let Ok(result) = result {
                assert_eq!(result.base_offset, index as u64);
            }
        }
        for mut reply in sync_replies {
            assert_eq!(reply.try_recv().unwrap().is_err(), fail);
        }
        #[cfg(feature = "writer-stage-trace")]
        if enabled {
            let rows: Vec<serde_json::Value> =
                std::fs::read_to_string(dir.root.join("trace.jsonl"))
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
            let group = rows.iter().find(|r| r["stage"] == "fsync").unwrap();
            let members: Vec<_> = rows
                .iter()
                .filter(|r| r["stage"] == "fsync_member")
                .collect();
            assert_eq!(members.len(), 3);
            assert!(members.iter().all(|r| r["parent"] == group["id"]));
            assert!(
                rows.iter()
                    .any(|r| r["stage"] == if fail { "fsync_error" } else { "fsync_success" })
            );
            assert_eq!(rows.last().unwrap()["closed"], true);
        }
    }

    #[test]
    fn queued_fsyncs_fuse_without_instrumentation() {
        check_fusion(false, false, false);
    }
    #[test]
    fn fused_failure_reaches_every_waiter() {
        check_fusion(true, false, false);
    }
    #[test]
    fn fusion_across_segment_roll_preserves_frontiers() {
        check_fusion(false, true, false);
    }
    #[cfg(feature = "writer-stage-trace")]
    #[test]
    fn traced_fsyncs_still_fuse() {
        check_fusion(false, false, true);
    }
    #[cfg(feature = "writer-stage-trace")]
    #[test]
    fn traced_fused_failure_reaches_every_waiter() {
        check_fusion(true, false, true);
    }
    #[cfg(feature = "writer-stage-trace")]
    #[test]
    fn traced_fusion_across_segment_roll() {
        check_fusion(false, true, true);
    }
}
