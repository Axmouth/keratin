use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use crate::batcher::{BatcherConfig, BatcherCore, Deadline, FlushReason, PushResult};
use crate::durability::KDurability;
use crate::keratin::WriterCmd;
use crate::log::{AppendResult, Log, LogState, ReplicatedAppendMode, ReplicatedAppendOutcome};
use crate::record::Message;
use crate::{AppendCompletion, KeratinConfig};

#[cfg(feature = "writer-stage-trace")]
use crate::writer_stage_trace::WriterStageTracer;

#[cfg(feature = "writer-stage-trace")]
macro_rules! trace_writer_stage {
    ($tracer:ident, $id:expr, $stage:expr, $weight:expr, $body:block) => {{
        let (records, bytes) = $weight;
        let start = Instant::now();
        let result = $body;
        $tracer.record($id, $stage, start, Instant::now(), records, bytes);
        result
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
        $linger:expr,
        $linger_min:expr,
        $linger_max:expr $(,)?
    ) => {{
        let reqs = $reqs;
        #[cfg(feature = "writer-stage-trace")]
        let work_id = $tracer.next_work_id();

        let total_bytes =
            trace_writer_stage!($tracer, work_id, "stage_reqs", reqs_trace_weight(&reqs), {
                stage_reqs($log, $cfg, $state, $pending, reqs, $notify_tx)
            });

        trace_writer_stage!($tracer, work_id, "post_stage", (0, total_bytes), {
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
                $linger,
                $linger_min,
                $linger_max,
            );
        });
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
fn reqs_trace_weight(reqs: &[AppendReq]) -> (usize, usize) {
    reqs.iter().fold((0usize, 0usize), |(records, bytes), req| {
        (
            records.saturating_add(req.records.len()),
            bytes.saturating_add(req.records.bytes_len()),
        )
    })
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

pub fn spawn_writer(mut log: Log, cfg: KeratinConfig, state: Arc<LogState>) -> WriterHandle {
    let (notify_tx, notify_rx) = crossbeam_channel::bounded::<NotifyMsg>(8192);

    #[cfg(feature = "writer-stage-trace")]
    let tracer = WriterStageTracer::from_env();
    #[cfg(feature = "writer-stage-trace")]
    let notifier_tracer = tracer.clone();

    std::thread::spawn(move || {
        #[cfg(feature = "writer-stage-trace")]
        notifier_loop(notify_rx, notifier_tracer);
        #[cfg(not(feature = "writer-stage-trace"))]
        notifier_loop(notify_rx);
        tracing::info!("Notifier loop exited");
    });

    let (tx, rx) = crossbeam_channel::bounded::<WriterCmd>(8192);

    std::thread::spawn(move || {
        #[cfg(feature = "writer-stage-trace")]
        writer_loop(&mut log, cfg, rx, state, notify_tx, tracer);
        #[cfg(not(feature = "writer-stage-trace"))]
        writer_loop(&mut log, cfg, rx, state, notify_tx);
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
fn writer_loop(
    log: &mut Log,
    cfg: KeratinConfig,
    writes_rx: Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: Sender<NotifyMsg>,
) {
    writer_loop_inner(log, cfg, writes_rx, state, notify_tx)
}

#[cfg(feature = "writer-stage-trace")]
fn writer_loop(
    log: &mut Log,
    cfg: KeratinConfig,
    writes_rx: Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: Sender<NotifyMsg>,
    tracer: WriterStageTracer,
) {
    writer_loop_inner(log, cfg, writes_rx, state, notify_tx, tracer)
}

fn writer_loop_inner(
    log: &mut Log,
    cfg: KeratinConfig,
    writes_rx: Receiver<WriterCmd>,
    state: Arc<LogState>,
    notify_tx: Sender<NotifyMsg>,
    #[cfg(feature = "writer-stage-trace")] tracer: WriterStageTracer,
) {
    let fsync_interval = Duration::from_millis(cfg.fsync_interval_ms.max(1));
    let mut last_fsync = Instant::now();

    let mut pending: VecDeque<PendingAck> = VecDeque::new();
    let mut durable_offset = log.durable_watermark();

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

        // (D) Fetch next command (bounded).
        let cmd = if wait == Duration::MAX {
            match writes_rx.recv() {
                Ok(c) => c,
                Err(_) => {
                    shutdown_fail_unstaged(
                        &mut batcher,
                        "writer shutdown",
                        FlushReason::Shutdown,
                        &notify_tx,
                    );
                    // fail_all_pending(&mut pending, "writer shutdown");
                    return;
                }
            }
        } else {
            match writes_rx.recv_timeout(wait) {
                Ok(c) => c,
                Err(RecvTimeoutError::Timeout) => {
                    // Either batch deadline or fsync deadline is due; loop will handle.
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    shutdown_fail_unstaged(
                        &mut batcher,
                        "writer disconnected",
                        FlushReason::Shutdown,
                        &notify_tx,
                    );
                    // fail_all_pending(&mut pending, "writer disconnected");
                    return;
                }
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
                        let b1 = trace_writer_stage!(
                            tracer,
                            work_id1,
                            "stage_reqs",
                            reqs_trace_weight(&reqs1),
                            { stage_reqs(log, &cfg, &state, &mut pending, reqs1, &notify_tx) }
                        );

                        #[cfg(feature = "writer-stage-trace")]
                        let work_id2 = tracer.next_work_id();
                        let b2 = trace_writer_stage!(
                            tracer,
                            work_id2,
                            "stage_reqs",
                            reqs_trace_weight(&reqs2),
                            { stage_reqs(log, &cfg, &state, &mut pending, reqs2, &notify_tx) }
                        );

                        // Use combined bytes for tuning.
                        let total_bytes = b1.saturating_add(b2);

                        trace_writer_stage!(tracer, work_id2, "post_stage", (0, total_bytes), {
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
                                &mut linger,
                                linger_min,
                                linger_max,
                            );
                        });

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
                        &mut linger,
                        linger_min,
                        linger_max,
                    );
                }
                let res = commit(
                    log,
                    &mut pending,
                    &mut durable_offset,
                    &mut last_fsync,
                    state.clone(),
                    &notify_tx,
                )
                .and_then(|_| log.advance_epoch(epoch));
                if let Ok(epoch) = res {
                    state.epoch.store(epoch, Ordering::Release);
                }
                if respond_to.send(res).is_err() {
                    tracing::info!("Error sending advance-epoch response");
                }
            }
            WriterCmd::Shutdown { notify_tx } => {
                tracing::info!("Writer received shutdown command");
                #[cfg(feature = "writer-stage-trace")]
                let work_id = tracer.next_work_id();
                if let Err(e) = trace_writer_stage!(tracer, work_id, "shutdown_fsync", (0, 0), {
                    log.shutdown()
                }) {
                    tracing::error!("Error during writer shutdown fsync: {e}");
                } else {
                    tracing::info!("Writer shutdown fsync complete");
                }
                if notify_tx.send(()).is_err() {
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
) -> usize {
    let now_ms = crate::util::unix_millis();

    let mut total_bytes: usize = 0;

    for r in reqs {
        total_bytes = total_bytes.saturating_add(r.records.bytes_len());

        let dur = r.durability.unwrap_or(cfg.default_durability);
        match r.records {
            AppendPayload::One(message) => match log.stage_append(&message, now_ms) {
                Ok((ar, end_offset)) => {
                    state.tail.store(end_offset + 1, Ordering::Release);
                    if dur == KDurability::AfterWrite {
                        notify_tx
                            .send(NotifyMsg::One {
                                item: NotifyItem {
                                    completion: r.completion,
                                    result: Ok(ar),
                                },
                            })
                            .ok();
                    } else {
                        pending.push_back(PendingAck {
                            end_offset,
                            respond_to: r.completion,
                            result: ar,
                        });
                    }
                }
                Err(e) => {
                    let _ = notify_tx.send(NotifyMsg::One {
                        item: NotifyItem {
                            completion: r.completion,
                            result: Err(e.into()),
                        },
                    });
                }
            },
            AppendPayload::Many(messages) => match log.stage_append_batch(&messages, now_ms) {
                Ok((ar, end_offset)) => {
                    state.tail.store(end_offset + 1, Ordering::Release);
                    if dur == KDurability::AfterWrite {
                        notify_tx
                            .send(NotifyMsg::One {
                                item: NotifyItem {
                                    completion: r.completion,
                                    result: Ok(ar),
                                },
                            })
                            .ok();
                    } else {
                        pending.push_back(PendingAck {
                            end_offset,
                            respond_to: r.completion,
                            result: ar,
                        });
                    }
                }
                Err(e) => {
                    let _ = notify_tx.send(NotifyMsg::One {
                        item: NotifyItem {
                            completion: r.completion,
                            result: Err(e.into()),
                        },
                    });
                }
            },
        }
    }

    total_bytes
}

/// Commit if due, retry a few times and fail all pending waiters on repeated errors.
fn maybe_commit_due(
    log: &mut Log,
    state: &Arc<LogState>,
    pending: &mut VecDeque<PendingAck>,
    durable_offset: &mut u64,
    last_fsync: &mut Instant,
    fsync_interval: Duration,
    notify_tx: &Sender<NotifyMsg>,
) {
    let needs_commit = pending_needs_fsync(pending);
    let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

    if !commit_due {
        return;
    }

    let mut error_count = 0;
    while let Err(e) = commit(
        log,
        pending,
        durable_offset,
        last_fsync,
        state.clone(),
        notify_tx,
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
    state: &Arc<LogState>,
    pending: &mut VecDeque<PendingAck>,
    durable_offset: &mut u64,
    last_fsync: &mut Instant,
    fsync_interval: Duration,
    total_bytes: usize,
    notify_tx: &Sender<NotifyMsg>,
    linger: &mut Duration,
    linger_min: Duration,
    linger_max: Duration,
) {
    // Commit scheduling (same as original, but factored)
    let needs_commit = pending_needs_fsync(pending);
    let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

    if commit_due {
        let _ = commit(
            log,
            pending,
            durable_offset,
            last_fsync,
            state.clone(),
            notify_tx,
        );
    } else if log.should_flush() {
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

fn commit(
    log: &mut Log,
    pending: &mut VecDeque<PendingAck>,
    durable_offset: &mut u64,
    last_fsync: &mut Instant,
    state: Arc<LogState>,
    notify_tx: &Sender<NotifyMsg>,
) -> Result<(), io::Error> {
    log.flush_buffers()?;
    log.fsync()?;
    *durable_offset = log.durable_watermark();
    state.durable.store(*durable_offset, Ordering::Release);
    *last_fsync = Instant::now();

    let mut ready = Vec::new();

    while let Some(front) = pending.front() {
        if front.end_offset <= *durable_offset {
            let p = pending.pop_front().unwrap();
            ready.push(NotifyItem {
                completion: p.respond_to,
                result: Ok(p.result),
            });
        } else {
            break;
        }
    }

    if !ready.is_empty() {
        if ready.len() == 1 {
            if let Some(item) = ready.pop() {
                notify_tx.send(NotifyMsg::One { item }).ok();
            }
        } else {
            notify_tx.send(NotifyMsg::Batch(ready)).ok();
        }
    }

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
