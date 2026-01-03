use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use tokio::sync::oneshot;

use crate::durability::KDurability;
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
    pub durability: Option<KDurability>,
    pub respond_to: oneshot::Sender<Result<AppendResult, IoError>>,
}

pub struct WriterHandle {
    pub tx: Sender<WriterCmd>,
}

struct PendingAck {
    end_offset: u64, // inclusive
    durability: KDurability,
    respond_to: oneshot::Sender<Result<AppendResult, IoError>>,
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

fn handle_non_append(log: &mut Log, cmd: WriterCmd) -> KResult<()> {
    match cmd {
        WriterCmd::Append(_) => return Ok(()),
        WriterCmd::Truncate { before, respond_to } => {
            respond_to.send(log.truncate_before(before)).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "could not notify truncate")
            })?;
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

    let weight = |r: &AppendReq| -> (usize, usize) {
        let recs = r.records.len();
        let bytes = r.records.iter().map(|m| m.bytes_len()).sum::<usize>();
        (recs, bytes)
    };

    loop {
        // ===== 1) Wait for first request (or commit deadline) =====
        let first: AppendReq;

        loop {
            let needs_commit = pending_needs_fsync(&pending);
            let commit_due = needs_commit && last_fsync.elapsed() >= fsync_interval;

            if commit_due {
                // TODO: More sophisticarted back off/rewind and redo batching
                let mut error_count = 0;
                while let Err(e) = commit(
                    log,
                    &mut pending,
                    &mut durable_offset,
                    &mut last_fsync,
                    state.clone(),
                ) {
                    fail_all_pending(
                        &mut pending,
                        &format!("Internal Error while commiting: {e}"),
                    );

                    if error_count > 3 {
                        fail_all_pending(
                            &mut pending,
                            "Internal Error while commiting writes over 3 times",
                        );
                        std::thread::sleep(Duration::from_millis(1000));
                        break;
                    }

                    error_count += 1;
                    std::thread::sleep(Duration::from_millis(200));
                }
            }

            // If we can block freely (no pending fsync-needed acks), block.
            if !pending_needs_fsync(&pending) {
                match rx.recv() {
                    Ok(WriterCmd::Append(r)) => {
                        first = r;
                        break;
                    }
                    Ok(cmd) => {
                        if let Err(e) = handle_non_append(log, cmd) {
                            fail_all_pending(
                                &mut pending,
                                &format!("Internal Error in processing write command: {e:?}"),
                            );
                        }
                    }
                    Err(_) => {
                        // Assumed disconnected during cleanup
                        return;
                    }
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
                    if let Err(e) = handle_non_append(log, cmd) {
                        fail_all_pending(
                            &mut pending,
                            &format!("Internal Error in processing write command: {e:?}"),
                        );
                    }
                }
                Err(_) => {
                    // Assumed disconnected during cleanup
                }
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
                    if let Err(e) = handle_non_append(log, cmd) {
                        fail_all_pending(
                            &mut pending,
                            &format!("Internal Error in processing write command: {e:?}"),
                        );
                    }
                }
                Err(_) => {
                    // Assumed disconnected during cleanup
                    break;
                }
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
                        if let Err(e) = handle_non_append(log, cmd) {
                            fail_all_pending(
                                &mut pending,
                                &format!("Internal Error in processing write command: {e:?}"),
                            );
                        }
                    }
                    Err(e) => {
                        fail_all_pending(
                            &mut pending,
                            &format!("Internal Error receiving events: {e}"),
                        );
                        core::hint::spin_loop()
                    }
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
                    if dur == KDurability::AfterWrite {
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
            let p = pending.pop_front().ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "pending ack to commit should exist",
            ))?;
            p.respond_to.send(Ok(p.result)).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "could not notify commit")
            })?;
        } else {
            break;
        }
    }
    Ok(())
}

fn fail_all_pending(pending: &mut VecDeque<PendingAck>, msg: &str) {
    tracing::error!("{}", msg);
    while let Some(p) = pending.pop_front() {
        let _ = p.respond_to.send(Err(IoError {
            msg: msg.to_string(),
        }));
    }
}
