use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::oneshot;

use crate::batcher::{BatcherConfig, BatcherCore, Deadline, FlushReason, PushResult};
use crate::durability::KDurability;
use crate::keratin::WriterCmd;
use crate::log::{AppendResult, Log, LogState};
use crate::record::Message;
use crate::{AppendCompletion, KeratinConfig};

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

pub enum AppendPayload {
    One(Message),
    Many(Vec<Message>),
}

pub struct AppendReq {
    pub records: AppendPayload,
    pub durability: Option<KDurability>,
    pub completion: Box<dyn AppendCompletion<IoError>>,
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

pub struct WriterHandle {
    pub tx: Sender<WriterCmd>,
}

struct PendingAck {
    end_offset: u64, // inclusive
    durability: KDurability,
    respond_to: Box<dyn AppendCompletion<IoError>>,
    result: AppendResult,
}

#[inline]
fn pending_needs_fsync(pending: &VecDeque<PendingAck>) -> bool {
    pending
        .iter()
        .any(|p| p.durability >= KDurability::AfterFsync)
}

pub fn spawn_writer(mut log: Log, cfg: KeratinConfig, state: Arc<LogState>) -> WriterHandle {
    let (tx, rx) = crossbeam_channel::bounded::<WriterCmd>(1024);
    std::thread::spawn(move || writer_loop(&mut log, cfg, rx, state));
    WriterHandle { tx }
}

fn writer_loop(log: &mut Log, cfg: KeratinConfig, rx: Receiver<WriterCmd>, state: Arc<LogState>) {
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
            let total_bytes = stage_reqs(log, &cfg, &state, &mut pending, reqs);
            post_stage_commit_and_tune(
                log,
                &cfg,
                &state,
                &mut pending,
                &mut durable_offset,
                &mut last_fsync,
                fsync_interval,
                total_bytes,
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
            match rx.recv() {
                Ok(c) => c,
                Err(_) => {
                    shutdown_fail_unstaged(&mut batcher, "writer shutdown", FlushReason::Shutdown);
                    // fail_all_pending(&mut pending, "writer shutdown");
                    return;
                }
            }
        } else {
            match rx.recv_timeout(wait) {
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
                        let total_bytes = stage_reqs(log, &cfg, &state, &mut pending, reqs);
                        post_stage_commit_and_tune(
                            log,
                            &cfg,
                            &state,
                            &mut pending,
                            &mut durable_offset,
                            &mut last_fsync,
                            fsync_interval,
                            total_bytes,
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
                        let b1 = stage_reqs(log, &cfg, &state, &mut pending, reqs1);
                        let b2 = stage_reqs(log, &cfg, &state, &mut pending, reqs2);

                        // Use combined bytes for tuning.
                        let total_bytes = b1.saturating_add(b2);

                        // Regardless of why, post-stage commit scheduling stays the same.
                        let _ = (why1, why2); // keep for tracing if you want
                        post_stage_commit_and_tune(
                            log,
                            &cfg,
                            &state,
                            &mut pending,
                            &mut durable_offset,
                            &mut last_fsync,
                            fsync_interval,
                            total_bytes,
                            &mut linger,
                            linger_min,
                            linger_max,
                        );
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
            WriterCmd::Truncate { before, respond_to } => {
                if let Err(e) = respond_to.send(log.truncate_before(before)).map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "could not notify truncate")
                }) {
                    tracing::info!("Internal Error in processing truncate command: {e:?}");
                } else {
                    tracing::info!("Truncate successfull");
                }
            }
            WriterCmd::Shutdown => {
                return;
            }
        }
    }
}

/// Stage a flushed batch of AppendReqs.
/// Returns total bytes staged (for adaptive linger tuning).
fn stage_reqs(
    log: &mut Log,
    cfg: &KeratinConfig,
    state: &Arc<LogState>,
    pending: &mut VecDeque<PendingAck>,
    reqs: Vec<AppendReq>,
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
                        r.completion.complete(Ok(ar));
                    } else {
                        pending.push_back(PendingAck {
                            end_offset,
                            durability: dur,
                            respond_to: r.completion,
                            result: ar,
                        });
                    }
                }
                Err(e) => {
                    r.completion.complete(Err(e.into()));
                }
            },
            AppendPayload::Many(messages) => match log.stage_append_batch(&messages, now_ms) {
                Ok((ar, end_offset)) => {
                    state.tail.store(end_offset + 1, Ordering::Release);
                    if dur == KDurability::AfterWrite {
                        r.completion.complete(Ok(ar));
                    } else {
                        pending.push_back(PendingAck {
                            end_offset,
                            durability: dur,
                            respond_to: r.completion,
                            result: ar,
                        });
                    }
                }
                Err(e) => {
                    r.completion.complete(Err(e.into()));
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
) {
    let needs_commit = pending_needs_fsync(pending);
    let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

    if !commit_due {
        return;
    }

    let mut error_count = 0;
    while let Err(e) = commit(log, pending, durable_offset, last_fsync, state.clone()) {
        fail_all_pending(pending, format!("Internal Error while commiting: {e}"));

        if error_count > 3 {
            fail_all_pending(
                pending,
                "Internal Error while commiting writes over 3 times",
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
    linger: &mut Duration,
    linger_min: Duration,
    linger_max: Duration,
) {
    // Commit scheduling (same as original, but factored)
    let needs_commit = pending_needs_fsync(pending);
    let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

    if commit_due {
        let _ = commit(log, pending, durable_offset, last_fsync, state.clone());
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
) {
    let reqs = batcher.flush();
    for r in reqs {
        r.completion.complete(Err(IoError {
            msg: msg.to_string(),
        }));
    }
}

fn commit(
    log: &mut Log,
    pending: &mut VecDeque<PendingAck>,
    durable_offset: &mut u64,
    last_fsync: &mut Instant,
    state: Arc<LogState>,
) -> Result<(), io::Error> {
    log.flush_buffers()?;
    log.fsync()?;
    *durable_offset = log.durable_watermark();
    state.durable.store(*durable_offset, Ordering::Release);
    *last_fsync = Instant::now();

    while let Some(front) = pending.front() {
        if front.end_offset <= *durable_offset {
            let p = pending.pop_front().ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "pending ack to commit should exist",
            ))?;
            p.respond_to.complete(Ok(p.result));
        } else {
            break;
        }
    }

    log.stats.batches += 1;

    Ok(())
}

fn fail_all_pending(pending: &mut VecDeque<PendingAck>, err_msg: impl AsRef<str>) {
    tracing::error!("{}", err_msg.as_ref());
    while let Some(p) = pending.pop_front() {
        p.respond_to.complete(Err(IoError {
            msg: err_msg.as_ref().to_string(),
        }));
    }
}
