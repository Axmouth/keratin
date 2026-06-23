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
        let (records, bytes) = $weight;
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
        $durable_offset:expr,
        $last_fsync:expr,
        $fsync_interval:expr,
        $fsync_tx:expr,
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
            $durable_offset,
            $last_fsync,
            $fsync_interval,
            total_bytes,
            $notify_tx,
            $fsync_tx,
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
    msg: String,
}

impl IoError {
    pub fn new(msg: impl ToString) -> Self {
        Self {
            msg: msg.to_string(),
        }
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
            msg: value.to_string(),
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

#[inline]
fn pending_needs_fsync(pending: &VecDeque<PendingAck>) -> bool {
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
    #[cfg(feature = "writer-stage-trace")]
    work_id: u64,
}

struct FsyncDone {
    through_offset: u64,
    elapsed: Duration,
    ready: Vec<NotifyItem>,
    sync_acks: Vec<tokio::sync::oneshot::Sender<io::Result<()>>>,
    error: Option<String>,
    #[cfg(feature = "writer-stage-trace")]
    work_id: u64,
}

enum WriterEvent {
    Command(WriterCmd),
    FsyncDone(FsyncDone),
    Timeout,
    WritesDisconnected,
    FsyncDisconnected,
}

pub fn spawn_writer(mut log: Log, cfg: KeratinConfig, state: Arc<LogState>) -> WriterHandle {
    let (notify_tx, notify_rx) = crossbeam_channel::bounded::<NotifyMsg>(8192);

    #[cfg(feature = "writer-stage-trace")]
    let tracer = WriterStageTracer::from_env();
    #[cfg(feature = "writer-stage-trace")]
    let notifier_tracer = tracer.clone();
    #[cfg(feature = "writer-stage-trace")]
    let fsync_tracer = tracer.clone();

    std::thread::spawn(move || {
        #[cfg(feature = "writer-stage-trace")]
        notifier_loop(notify_rx, notifier_tracer);
        #[cfg(not(feature = "writer-stage-trace"))]
        notifier_loop(notify_rx);
        tracing::info!("Notifier loop exited");
    });

    let (fsync_tx, fsync_rx) = crossbeam_channel::bounded::<FsyncReq>(8);
    let (fsync_done_tx, fsync_done_rx) = crossbeam_channel::bounded::<FsyncDone>(8);

    std::thread::spawn(move || {
        #[cfg(feature = "writer-stage-trace")]
        fsync_loop(fsync_rx, fsync_done_tx, fsync_tracer);
        #[cfg(not(feature = "writer-stage-trace"))]
        fsync_loop(fsync_rx, fsync_done_tx);
        tracing::info!("Fsync loop exited");
    });

    let (tx, rx) = crossbeam_channel::bounded::<WriterCmd>(8192);

    std::thread::spawn(move || {
        #[cfg(feature = "writer-stage-trace")]
        writer_loop(
            &mut log,
            cfg,
            rx,
            state,
            notify_tx,
            fsync_tx,
            fsync_done_rx,
            tracer,
        );
        #[cfg(not(feature = "writer-stage-trace"))]
        writer_loop(&mut log, cfg, rx, state, notify_tx, fsync_tx, fsync_done_rx);
        tracing::info!("Writer loop exited")
    });

    WriterHandle { tx }
}

#[cfg(not(feature = "writer-stage-trace"))]
fn notifier_loop(rx: Receiver<NotifyMsg>) {
    while let Ok(msg) = rx.recv() {
        match msg {
            NotifyMsg::One { item } => {
                item.completion.complete(item.result);
            }
            NotifyMsg::Batch(items) => {
                for item in items {
                    item.completion.complete(item.result);
                }
            }
        }
    }
}

#[cfg(feature = "writer-stage-trace")]
fn notifier_loop(rx: Receiver<NotifyMsg>, tracer: WriterStageTracer) {
    while let Ok(msg) = rx.recv() {
        let work_id = tracer.next_work_id();
        match msg {
            NotifyMsg::One { item } => {
                trace_writer_stage!(tracer, work_id, "notify", (1, 0), {
                    item.completion.complete(item.result);
                });
            }
            NotifyMsg::Batch(items) => {
                let records = items.len();
                trace_writer_stage!(tracer, work_id, "notify", (records, 0), {
                    for item in items {
                        item.completion.complete(item.result);
                    }
                });
            }
        }
    }
}

#[cfg(not(feature = "writer-stage-trace"))]
fn fsync_loop(rx: Receiver<FsyncReq>, done_tx: Sender<FsyncDone>) {
    while let Ok(req) = rx.recv() {
        let through_offset = req.job.through_offset();
        let result = req.job.sync();
        let done = fsync_done_from_result(through_offset, req.ready, req.sync_acks, result);
        if done_tx.send(done).is_err() {
            break;
        }
    }
}

#[cfg(feature = "writer-stage-trace")]
fn fsync_loop(rx: Receiver<FsyncReq>, done_tx: Sender<FsyncDone>, tracer: WriterStageTracer) {
    while let Ok(req) = rx.recv() {
        let through_offset = req.job.through_offset();
        let result = trace_writer_stage!(tracer, req.work_id, "fsync", (0, 0), { req.job.sync() });
        let mut done = fsync_done_from_result(through_offset, req.ready, req.sync_acks, result);
        done.work_id = req.work_id;
        if done_tx.send(done).is_err() {
            break;
        }
    }
}

fn fsync_done_from_result(
    through_offset: u64,
    ready: Vec<NotifyItem>,
    sync_acks: Vec<tokio::sync::oneshot::Sender<io::Result<()>>>,
    result: io::Result<Duration>,
) -> FsyncDone {
    match result {
        Ok(elapsed) => FsyncDone {
            through_offset,
            elapsed,
            ready,
            sync_acks,
            error: None,
            #[cfg(feature = "writer-stage-trace")]
            work_id: 0,
        },
        Err(err) => {
            let msg = err.to_string();
            FsyncDone {
                through_offset,
                elapsed: Duration::ZERO,
                ready: ready
                    .into_iter()
                    .map(|item| NotifyItem {
                        completion: item.completion,
                        result: Err(IoError::new(&msg)),
                    })
                    .collect(),
                sync_acks,
                error: Some(msg),
                #[cfg(feature = "writer-stage-trace")]
                work_id: 0,
            }
        }
    }
}

#[cfg(not(feature = "writer-stage-trace"))]
fn writer_loop(
    log: &mut Log,
    cfg: KeratinConfig,
    writes_rx: Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: Sender<NotifyMsg>,
    fsync_tx: Sender<FsyncReq>,
    fsync_done_rx: Receiver<FsyncDone>,
) {
    writer_loop_inner(
        log,
        cfg,
        writes_rx,
        state,
        notify_tx,
        fsync_tx,
        fsync_done_rx,
    )
}

#[cfg(feature = "writer-stage-trace")]
fn writer_loop(
    log: &mut Log,
    cfg: KeratinConfig,
    writes_rx: Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: Sender<NotifyMsg>,
    fsync_tx: Sender<FsyncReq>,
    fsync_done_rx: Receiver<FsyncDone>,
    tracer: WriterStageTracer,
) {
    writer_loop_inner(
        log,
        cfg,
        writes_rx,
        state,
        notify_tx,
        fsync_tx,
        fsync_done_rx,
        tracer,
    )
}

fn writer_loop_inner(
    log: &mut Log,
    cfg: KeratinConfig,
    writes_rx: Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: Sender<NotifyMsg>,
    fsync_tx: Sender<FsyncReq>,
    fsync_done_rx: Receiver<FsyncDone>,
    #[cfg(feature = "writer-stage-trace")] tracer: WriterStageTracer,
) {
    let fsync_interval = Duration::from_millis(cfg.fsync_interval_ms.max(1));
    let mut last_fsync = Instant::now();

    let mut pending: VecDeque<PendingAck> = VecDeque::new();
    let mut durable_offset = log.durable_watermark();
    let mut inflight_fsyncs = 0usize;

    // Adaptive linger
    // TODO: Consider removing due to overlap with fsync window
    let linger_min = Duration::from_millis(0);
    let linger_max = Duration::from_millis(cfg.batch_linger_ms.max(1));
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

    loop {
        drain_fsync_done(
            log,
            &state,
            &notify_tx,
            &fsync_done_rx,
            &mut durable_offset,
            &mut inflight_fsyncs,
            #[cfg(feature = "writer-stage-trace")]
            &tracer,
        );

        // Keep linger synced with adaptive tuning.
        batcher.set_linger(linger);

        // (A) If a batch is due by idle timeout, flush it immediately (no I/O wait).
        if let Some((FlushReason::Timeout, reqs)) = batcher.flush_if_due(Instant::now())
            && !reqs.is_empty()
        {
            stage_reqs_then_post!(
                tracer,
                log,
                &cfg,
                &state,
                &mut pending,
                reqs,
                &notify_tx,
                &mut durable_offset,
                &mut last_fsync,
                fsync_interval,
                &fsync_tx,
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
            &mut durable_offset,
            &mut last_fsync,
            fsync_interval,
            &notify_tx,
            &fsync_tx,
            &mut inflight_fsyncs,
            #[cfg(feature = "writer-stage-trace")]
            &tracer,
        );

        // (C) Compute how long we may wait for the next cmd.
        let now = Instant::now();
        let mut wait = Duration::MAX;

        // Cap by fsync deadline when needed.
        if pending_needs_fsync(&pending) {
            // If checked_add overflows, treat as due now.
            let fs_deadline = match last_fsync.checked_add(fsync_interval) {
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

        // (D) Fetch next event (bounded).
        let event = recv_writer_event(&writes_rx, &fsync_done_rx, wait);

        let cmd = match event {
            WriterEvent::Command(cmd) => cmd,
            WriterEvent::FsyncDone(done) => {
                handle_fsync_done(
                    log,
                    &state,
                    &notify_tx,
                    done,
                    &mut durable_offset,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );
                continue;
            }
            WriterEvent::Timeout => {
                // Either batch deadline or fsync deadline is due; loop will handle.
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
                        &mut pending,
                        &mut last_fsync,
                        &notify_tx,
                        &fsync_tx,
                        &mut inflight_fsyncs,
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
                    &mut durable_offset,
                    &mut inflight_fsyncs,
                    #[cfg(feature = "writer-stage-trace")]
                    &tracer,
                );
                return;
            }
            WriterEvent::FsyncDisconnected => {
                fail_all_pending(&mut pending, "fsync worker disconnected", &notify_tx, true);
                return;
            }
        };

        // (E) Process command.
        match cmd {
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
                            &mut durable_offset,
                            &mut last_fsync,
                            fsync_interval,
                            &fsync_tx,
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
                            &mut durable_offset,
                            &mut last_fsync,
                            fsync_interval,
                            total_bytes,
                            &notify_tx,
                            &fsync_tx,
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
                        &mut durable_offset,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
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
                        )
                    }
                );
                if let Err(_err) = respond_to.send(res) {
                    tracing::info!("Error sending replicated append response");
                }
            }
            WriterCmd::Truncate { before, respond_to } => {
                tracing::info!("Truncate before {before}..");
                if let Err(e) = respond_to.send(log.truncate_before(before)).map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "could not notify truncate")
                }) {
                    tracing::info!("Internal Error in processing truncate command: {e}");
                } else {
                    tracing::info!("Truncate successful, before {before}");
                }
            }
            WriterCmd::ResetToCheckpoint {
                next_offset,
                respond_to,
            } => {
                shutdown_fail_reqs(batcher.flush(), "writer reset to checkpoint", &notify_tx);
                fail_all_pending(
                    &mut pending,
                    "writer reset to checkpoint",
                    &notify_tx,
                    false,
                );
                let res = log.reset_to_checkpoint(next_offset, crate::util::unix_millis());
                if res.is_ok() {
                    durable_offset = log.durable_watermark();
                    last_fsync = Instant::now();
                }
                if respond_to.send(res).is_err() {
                    tracing::info!("Error sending reset-to-checkpoint response");
                }
            }
            WriterCmd::AdvanceEpoch { epoch, respond_to } => {
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
                        &mut durable_offset,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
                        &mut inflight_fsyncs,
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }
                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                let res = commit(
                    log,
                    &mut pending,
                    &mut last_fsync,
                    &notify_tx,
                    &fsync_tx,
                    &mut inflight_fsyncs,
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
                        &mut durable_offset,
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
                            #[cfg(feature = "writer-stage-trace")]
                            work_id,
                        };
                        if let Err(err) = fsync_tx.send(req) {
                            let FsyncReq { sync_acks, .. } = err.into_inner();
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
                        &mut durable_offset,
                        &mut last_fsync,
                        fsync_interval,
                        &fsync_tx,
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
                        &mut pending,
                        &mut last_fsync,
                        &notify_tx,
                        &fsync_tx,
                        &mut inflight_fsyncs,
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
                    &mut durable_offset,
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
                return;
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
) -> Result<ReplicatedAppendOutcome, IoError> {
    let now_ms = crate::util::unix_millis();
    let (outcome, end_offset) =
        log.stage_replicated_append_batch(epoch, first_offset, records, mode, now_ms)?;
    state.epoch.store(log.current_epoch(), Ordering::Release);

    if let Some(end_offset) = end_offset {
        state.tail.store(end_offset + 1, Ordering::Release);

        match durability {
            KDurability::AfterWrite => {
                log.flush_buffers()?;
                log.flush()?;
            }
            KDurability::AfterFsync => {
                log.flush_buffers()?;
                log.flush()?;
                log.fsync()?;
                state
                    .durable
                    .store(log.durable_watermark(), Ordering::Release);
            }
        }
    }

    Ok(outcome)
}

/// Stage a flushed batch of AppendReqs.
/// Returns total bytes staged (for adaptive linger tuning).
fn stage_reqs(
    log: &mut Log,
    cfg: &KeratinConfig,
    state: &Arc<LogState>,
    pending: &mut VecDeque<PendingAck>,
    reqs: Vec<AppendReq>,
    notify_tx: &Sender<NotifyMsg>,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> usize {
    let now_ms = crate::util::unix_millis();

    let mut total_bytes: usize = 0;

    for r in reqs {
        total_bytes = total_bytes.saturating_add(r.records.bytes_len());

        let dur = r.durability.unwrap_or(cfg.default_durability);
        let completion = r.completion;
        let staged_offset_tx = r.staged_offset_tx;
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
                // Report the assigned offset as early as possible (staged, pre-ack).
                // A oneshot send is a non-blocking store plus a waker, safe on the
                // writer thread. The durability ack still flows through `completion`.
                if let Some(tx) = staged_offset_tx {
                    let _ = tx.send(ar.base_offset);
                }
                state.tail.store(end_offset + 1, Ordering::Release);
                if dur == KDurability::AfterWrite {
                    notify_tx
                        .send(NotifyMsg::One {
                            item: NotifyItem {
                                completion,
                                result: Ok(ar),
                            },
                        })
                        .ok();
                } else {
                    pending.push_back(PendingAck {
                        end_offset,
                        respond_to: completion,
                        result: ar,
                    });
                }
            }
            Err(e) => {
                let _ = notify_tx.send(NotifyMsg::One {
                    item: NotifyItem {
                        completion,
                        result: Err(e.into()),
                    },
                });
            }
        }
    }

    total_bytes
}

/// Commit if due, retry a few times and fail all pending waiters on repeated errors.
fn maybe_commit_due(
    log: &mut Log,
    _state: &Arc<LogState>,
    pending: &mut VecDeque<PendingAck>,
    _durable_offset: &mut u64,
    last_fsync: &mut Instant,
    fsync_interval: Duration,
    notify_tx: &Sender<NotifyMsg>,
    fsync_tx: &Sender<FsyncReq>,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    let needs_commit = pending_needs_fsync(pending);
    let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

    if !commit_due {
        return;
    }

    #[cfg(feature = "writer-stage-trace")]
    let work_id = tracer.next_work_id();

    let mut error_count = 0;
    while let Err(e) = commit(
        log,
        pending,
        last_fsync,
        notify_tx,
        fsync_tx,
        inflight_fsyncs,
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
    _state: &Arc<LogState>,
    pending: &mut VecDeque<PendingAck>,
    _durable_offset: &mut u64,
    last_fsync: &mut Instant,
    fsync_interval: Duration,
    total_bytes: usize,
    notify_tx: &Sender<NotifyMsg>,
    fsync_tx: &Sender<FsyncReq>,
    inflight_fsyncs: &mut usize,
    linger: &mut Duration,
    linger_min: Duration,
    linger_max: Duration,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) {
    // Commit scheduling (same as original, but factored)
    let needs_commit = pending_needs_fsync(pending);
    let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

    if commit_due {
        let _ = commit(
            log,
            pending,
            last_fsync,
            notify_tx,
            fsync_tx,
            inflight_fsyncs,
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
    notify_tx: &Sender<NotifyMsg>,
) {
    shutdown_fail_reqs(batcher.flush(), msg, notify_tx);
}

fn shutdown_fail_reqs(reqs: Vec<AppendReq>, msg: &str, notify_tx: &Sender<NotifyMsg>) {
    let mut items = Vec::new();
    for r in reqs {
        items.push(NotifyItem {
            completion: r.completion,
            result: Err(IoError {
                msg: msg.to_string(),
            }),
        });
    }
    if !items.is_empty() {
        let _ = notify_tx.send(NotifyMsg::Batch(items));
    }
}

fn recv_writer_event(
    writes_rx: &Receiver<WriterCmd>,
    fsync_done_rx: &Receiver<FsyncDone>,
    wait: Duration,
) -> WriterEvent {
    if wait == Duration::MAX {
        crossbeam_channel::select! {
            recv(writes_rx) -> msg => match msg {
                Ok(cmd) => WriterEvent::Command(cmd),
                Err(_) => WriterEvent::WritesDisconnected,
            },
            recv(fsync_done_rx) -> msg => match msg {
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
            recv(fsync_done_rx) -> msg => match msg {
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
    notify_tx: &Sender<NotifyMsg>,
    fsync_done_rx: &Receiver<FsyncDone>,
    durable_offset: &mut u64,
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
                durable_offset,
                inflight_fsyncs,
                #[cfg(feature = "writer-stage-trace")]
                tracer,
            ),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
}

fn wait_for_inflight_fsyncs(
    log: &mut Log,
    state: &Arc<LogState>,
    notify_tx: &Sender<NotifyMsg>,
    fsync_done_rx: &Receiver<FsyncDone>,
    durable_offset: &mut u64,
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
                durable_offset,
                inflight_fsyncs,
                #[cfg(feature = "writer-stage-trace")]
                tracer,
            ),
            Err(_) => break,
        }
    }
}

fn handle_fsync_done(
    log: &mut Log,
    state: &Arc<LogState>,
    notify_tx: &Sender<NotifyMsg>,
    done: FsyncDone,
    durable_offset: &mut u64,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
) {
    *inflight_fsyncs = inflight_fsyncs.saturating_sub(1);

    if let Some(error) = done.error {
        tracing::error!("fsync job failed: {error}");
        send_notify_items(notify_tx, done.ready);
        answer_sync_acks(done.sync_acks, Err(io::Error::other(error)));
        return;
    }

    #[cfg(feature = "writer-stage-trace")]
    let finish_result =
        log.finish_fsync_job_traced(done.through_offset, done.elapsed, tracer, done.work_id);
    #[cfg(not(feature = "writer-stage-trace"))]
    let finish_result = log.finish_fsync_job(done.through_offset, done.elapsed);

    match finish_result {
        Ok(()) => {
            *durable_offset = (*durable_offset).max(done.through_offset);
            state.durable.store(*durable_offset, Ordering::Release);
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

fn send_notify_items(notify_tx: &Sender<NotifyMsg>, mut items: Vec<NotifyItem>) {
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

fn commit(
    log: &mut Log,
    pending: &mut VecDeque<PendingAck>,
    last_fsync: &mut Instant,
    notify_tx: &Sender<NotifyMsg>,
    fsync_tx: &Sender<FsyncReq>,
    inflight_fsyncs: &mut usize,
    #[cfg(feature = "writer-stage-trace")] tracer: &WriterStageTracer,
    #[cfg(feature = "writer-stage-trace")] work_id: u64,
) -> Result<(), io::Error> {
    #[cfg(feature = "writer-stage-trace")]
    let job = log.prepare_fsync_job_traced(tracer, work_id)?;
    #[cfg(not(feature = "writer-stage-trace"))]
    let job = log.prepare_fsync_job()?;
    *last_fsync = Instant::now();

    let mut ready = Vec::new();
    let through_offset = job.through_offset();

    while let Some(front) = pending.front() {
        if front.end_offset <= through_offset {
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

    if ready.is_empty() {
        return Ok(());
    }

    let req = FsyncReq {
        job,
        ready,
        sync_acks: Vec::new(),
        #[cfg(feature = "writer-stage-trace")]
        work_id,
    };

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

fn fail_all_pending(
    pending: &mut VecDeque<PendingAck>,
    err_msg: impl AsRef<str>,
    notify_tx: &Sender<NotifyMsg>,
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
                msg: err_msg.as_ref().to_string(),
            }),
        });
    }

    notify_tx.send(NotifyMsg::Batch(items)).ok();
}
