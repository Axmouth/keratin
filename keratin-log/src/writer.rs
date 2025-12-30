use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use tokio::sync::oneshot;

use crate::durability::Durability;
use crate::keratin::WriterCmd;
use crate::log::{AppendResult, Log, LogState};
use crate::record::Message;
use crate::{KResult, KeratinConfig};

#[derive(Debug, Clone)]
pub struct IoError {
    msg: String,
}

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

pub struct AppendReq {
    pub records: Vec<Message>,
    pub durability: Option<Durability>,
    pub respond_to: oneshot::Sender<Result<AppendResult, IoError>>,
}

pub struct WriterHandle {
    pub tx: Sender<WriterCmd>,
}

struct PendingAck {
    end_offset: u64, // inclusive
    durability: Durability,
    respond_to: oneshot::Sender<Result<AppendResult, IoError>>,
    result: AppendResult,
}

#[inline]
fn pending_needs_fsync(pending: &VecDeque<PendingAck>) -> bool {
    pending
        .iter()
        .any(|p| p.durability >= Durability::AfterFsync)
}

pub fn spawn_writer(mut log: Log, cfg: KeratinConfig, state: Arc<LogState>) -> WriterHandle {
    let (tx, rx) = crossbeam_channel::bounded::<WriterCmd>(1024);
    std::thread::spawn(move || writer_loop(&mut log, cfg, rx, state));
    WriterHandle { tx }
}

fn handle_non_append(log: &mut Log, cmd: WriterCmd) -> KResult<()> {
    match cmd {
        WriterCmd::Append(_) => return Ok(()),
        WriterCmd::Truncate { before, respond_to } => {
            respond_to.send(log.truncate_before(before));
        }
    }
    Ok(())
}

fn writer_loop(log: &mut Log, cfg: KeratinConfig, rx: Receiver<WriterCmd>, state: Arc<LogState>) {
    let fsync_interval = Duration::from_millis(cfg.fsync_interval_ms.max(1));
    let mut last_fsync = Instant::now();

    let mut pending: VecDeque<PendingAck> = VecDeque::new();
    let mut durable_offset = log.durable_watermark();

    let linger_min = Duration::from_millis(0);
    let linger_max = Duration::from_millis(cfg.batch_linger_ms.max(1));
    let mut linger = Duration::from_millis(0);

    loop {
        // ===== 1) Wait for first request (or commit deadline) =====
        let first: AppendReq;

        loop {
            let needs_commit = pending_needs_fsync(&pending);
            let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

            if commit_due {
                let _ = commit(
                    log,
                    &mut pending,
                    &mut durable_offset,
                    &mut last_fsync,
                    state.clone(),
                );
            }

            // If we can block freely (no pending fsync-needed acks), block.
            if !pending_needs_fsync(&pending) {
                match rx.recv() {
                    Ok(WriterCmd::Append(r)) => {
                        first = r;
                        break;
                    }
                    Ok(cmd) => {
                        handle_non_append(log, cmd);
                    }
                    Err(_) => return,
                }
            }

            // Otherwise we must not block past commit deadline.
            let deadline = last_fsync + fsync_interval;

            // Try to grab work without blocking.
            match rx.try_recv() {
                Ok(WriterCmd::Append(r)) => {
                    first = r;
                    break;
                }
                Ok(cmd) => {
                    handle_non_append(log, cmd);
                }
                _ => (),
            }

            // If we reached the deadline, loop again to commit.
            if Instant::now() >= deadline {
                continue;
            }

            core::hint::spin_loop();
        }

        // ===== 2) Coalesce =====
        let start = Instant::now();
        let mut reqs = vec![first];
        let mut total_records = reqs[0].records.len();
        let mut total_bytes: usize = reqs[0].records.iter().map(|m| m.bytes_len()).sum();

        while reqs.len() < cfg.max_batch_records
            && total_records < cfg.max_batch_records
            && total_bytes < cfg.max_batch_bytes
        {
            match rx.try_recv() {
                Ok(WriterCmd::Append(r)) => {
                    total_records += r.records.len();
                    total_bytes += r.records.iter().map(|b| b.bytes_len()).sum::<usize>();
                    reqs.push(r);
                }
                Ok(cmd) => {
                    handle_non_append(log, cmd);
                }
                Err(_) => break,
            }
        }

        let substantial = total_bytes >= (cfg.max_batch_bytes / 4).max(64 * 1024);
        if !substantial && linger > Duration::from_millis(0) {
            while start.elapsed() < linger
                && reqs.len() < cfg.max_batch_records
                && total_records < cfg.max_batch_records
                && total_bytes < cfg.max_batch_bytes
            {
                match rx.try_recv() {
                    Ok(WriterCmd::Append(r)) => {
                        total_records += r.records.len();
                        total_bytes += r.records.iter().map(|m| m.bytes_len()).sum::<usize>();
                        reqs.push(r);
                    }
                    Ok(cmd) => {
                        handle_non_append(log, cmd);
                    }
                    Err(_) => core::hint::spin_loop(),
                }
            }
        }

        // ===== 3) Stage =====
        let now = crate::util::unix_millis();

        for r in reqs {
            let dur = r.durability.unwrap_or(cfg.default_durability);
            match log.stage_append_batch(&r.records, now) {
                Ok((ar, end_offset)) => {
                    state.tail.store(end_offset + 1, Ordering::Release);
                    if dur == Durability::AfterWrite {
                        let _ = r.respond_to.send(Ok(ar));
                    } else {
                        pending.push_back(PendingAck {
                            end_offset,
                            durability: dur,
                            respond_to: r.respond_to,
                            result: ar,
                        });
                    }
                }
                Err(e) => {
                    let _ = r.respond_to.send(Err(e.into()));
                }
            }
        }

        // ===== 4) Single commit scheduler =====
        let needs_commit = pending_needs_fsync(&pending);
        let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

        if commit_due {
            let _ = commit(
                log,
                &mut pending,
                &mut durable_offset,
                &mut last_fsync,
                state.clone(),
            );
        } else if log.should_flush() {
            let _ = log.flush_buffers();
        }

        // ===== 5) Adaptive linger tuning =====
        if total_bytes >= (cfg.max_batch_bytes / 2) {
            linger = (linger + Duration::from_millis(1)).min(linger_max);
        } else if total_bytes < 64 * 1024 && pending.is_empty() {
            linger = linger
                .saturating_sub(Duration::from_millis(1))
                .max(linger_min);
        }
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
            let p = pending.pop_front().unwrap();
            let _ = p.respond_to.send(Ok(p.result));
        } else {
            break;
        }
    }
    Ok(())
}

fn fail_all_pending(pending: &mut VecDeque<PendingAck>, msg: &str) {
    while let Some(p) = pending.pop_front() {
        let _ = p.respond_to.send(Err(IoError {
            msg: msg.to_string(),
        }));
    }
}
