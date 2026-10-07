//! Queue payload identity is the containing event-log offset.
use super::*;
fn payload(record: keratin_log::OwnedRecord) -> Result<Option<(u64, Message)>> {
    match StromaEvent::decode(&record.payload).map_err(decode_err)? {
        StromaEvent::PayloadEnqueue {
            headers, payload, ..
        } => Ok(Some((
            record.offset,
            Message {
                flags: 0,
                headers,
                payload,
            },
        ))),
        _ => Ok(None),
    }
}
pub(super) fn fetch_payload(qh: &QueueHandleInner, off: u64) -> Result<Option<Message>> {
    let rs = qh.event_log().reader().scan_from(off, 1).map_err(io_err)?;
    match rs.into_iter().next() {
        Some(rec) if rec.offset == off => Ok(payload(rec)?.map(|(_, message)| message)),
        _ => Ok(None),
    }
}
pub(super) fn scan_payloads(
    qh: &QueueHandleInner,
    from: u64,
    max: usize,
) -> Result<Vec<(u64, Vec<u8>, MessageHeaders)>> {
    let reader = qh.event_log().reader();
    let mut next = from;
    let mut out = Vec::new();
    while out.len() < max {
        let records = reader
            .scan_from_with_byte_budget(next, 1024.min(max - out.len()), 1024 * 1024)
            .map_err(io_err)?;
        if records.is_empty() {
            break;
        }
        next = records.last().unwrap().offset + 1;
        for record in records {
            if let Some((off, message)) = payload(record)? {
                out.push((
                    off,
                    message.payload,
                    MessageHeaders::decode(&message.headers)?,
                ));
            }
        }
    }
    Ok(out)
}
pub(super) fn read_selected(
    qh: &QueueHandleInner,
    offsets: &[(u64, u32)],
) -> Result<Vec<(u64, MessageHeaders, Vec<u8>, u32)>> {
    // Experimental process-wide knob, sampled once rather than per record.
    // Preserve the existing 1 MiB default and the independent 4096-record cap.
    static SCAN_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let scan_bytes = *SCAN_BYTES.get_or_init(|| {
        std::env::var("STROMA_COMBINED_DELIVERY_SCAN_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| (1..=8 * 1024 * 1024).contains(v))
            .unwrap_or(1024 * 1024)
    });
    read_selected_with_budget(qh, offsets, scan_bytes)
}

/// Selected offsets at most this many records apart are read by scanning the
/// records between them. Wider gaps are skipped with a fresh positioned read.
/// Scanning a short run is cheaper than a positioned read, but scanning a long
/// retained range is not, for example when an old redelivered or newly
/// activated delayed message is polled with fresh ones. 64 had the lowest or
/// near-lowest total cost in a sweep over 64 B and 1 KiB records, from the tail
/// cache and from the file, with selections spaced 1 to 1024 records apart.
const SELECTED_SCAN_GAP: u64 = 64;

fn read_selected_with_budget(
    qh: &QueueHandleInner,
    offsets: &[(u64, u32)],
    scan_bytes: usize,
) -> Result<Vec<(u64, MessageHeaders, Vec<u8>, u32)>> {
    read_selected_with_limits(qh, offsets, scan_bytes, SELECTED_SCAN_GAP)
}

fn read_selected_with_limits(
    qh: &QueueHandleInner,
    offsets: &[(u64, u32)],
    scan_bytes: usize,
    scan_gap: u64,
) -> Result<Vec<(u64, MessageHeaders, Vec<u8>, u32)>> {
    if offsets.is_empty() {
        return Ok(Vec::new());
    }
    if offsets.windows(2).any(|v| v[0].0 >= v[1].0) {
        return Err(StromaError::InvalidArgument(
            "selected offsets must increase".into(),
        ));
    }
    offsets
        .last()
        .unwrap()
        .0
        .checked_add(1)
        .ok_or_else(|| io_err("offset exhausted"))?;
    let reader = qh.event_log().reader();
    let mut next = offsets[0].0;
    let mut i = 0;
    let mut out = Vec::with_capacity(offsets.len());
    while i < offsets.len() {
        let target = offsets[i].0;
        // A skip lands on the selected offset itself, so a missing or
        // misplaced record still fails the identity checks below.
        if target - next > scan_gap {
            next = target;
        }
        let mut run_end = target;
        for &(offset, _) in &offsets[i + 1..] {
            if offset - run_end - 1 > scan_gap {
                break;
            }
            run_end = offset;
        }
        let records = reader
            .scan_from_with_byte_budget(next, (run_end + 1 - next).min(4096) as usize, scan_bytes)
            .map_err(io_err)?;
        if records.is_empty() {
            return Err(StromaError::Corruption(
                "selected payload missing from combined log".into(),
            ));
        }
        next = records.last().unwrap().offset + 1;
        for record in records {
            if i < offsets.len() && record.offset == offsets[i].0 {
                let (_, m) = payload(record)?.ok_or_else(|| {
                    StromaError::Corruption("selected offset is not a payload enqueue".into())
                })?;
                out.push((
                    offsets[i].0,
                    MessageHeaders::decode(&m.headers)?,
                    m.payload,
                    offsets[i].1,
                ));
                i += 1;
            } else if i < offsets.len() && record.offset > offsets[i].0 {
                return Err(StromaError::Corruption("combined payload gap".into()));
            }
        }
    }
    if i != offsets.len() {
        return Err(StromaError::Corruption(
            "combined selection incomplete".into(),
        ));
    }
    Ok(out)
}

impl Stroma {
    pub(crate) async fn apply_combined_records(
        &self,
        events: Vec<StromaEvent>,
        base: u64,
        qh: &QueueHandleInner,
    ) -> Result<()> {
        let mut it = events.into_iter().enumerate().peekable();
        let mut skip = Vec::new();
        while let Some((index, event)) = it.next() {
            let off = base + index as u64;
            match event {
                StromaEvent::PayloadEnqueue {
                    not_before: None,
                    expire_at,
                    ..
                } => {
                    let mut reqs = vec![EnqueueEventMeta {
                        off,
                        retries: 0,
                        expire_at,
                    }];
                    while matches!(
                        it.peek(),
                        Some((
                            _,
                            StromaEvent::PayloadEnqueue {
                                not_before: None,
                                ..
                            }
                        ))
                    ) {
                        let (i, StromaEvent::PayloadEnqueue { expire_at, .. }) = it.next().unwrap()
                        else {
                            unreachable!()
                        };
                        reqs.push(EnqueueEventMeta {
                            off: base + i as u64,
                            retries: 0,
                            expire_at,
                        });
                    }
                    self.apply_event_inmem(StromaEvent::EnqueueMany { reqs }, qh)
                        .await?;
                }
                StromaEvent::PayloadEnqueue {
                    not_before: Some(not_before),
                    ..
                } => {
                    let mut reqs = vec![EnqueueDelayedEventMeta { off, not_before }];
                    while matches!(
                        it.peek(),
                        Some((
                            _,
                            StromaEvent::PayloadEnqueue {
                                not_before: Some(_),
                                ..
                            }
                        ))
                    ) {
                        let (
                            i,
                            StromaEvent::PayloadEnqueue {
                                not_before: Some(not_before),
                                ..
                            },
                        ) = it.next().unwrap()
                        else {
                            unreachable!()
                        };
                        reqs.push(EnqueueDelayedEventMeta {
                            off: base + i as u64,
                            not_before,
                        });
                    }
                    self.apply_event_inmem(StromaEvent::EnqueueDelayedMany { reqs }, qh)
                        .await?;
                }
                other => {
                    if other
                        .max_combined_referenced_offset()
                        .is_some_and(|target| target >= off)
                    {
                        return Err(StromaError::Corruption(
                            "combined state event has a forward payload reference".into(),
                        ));
                    }
                    self.apply_event_inmem(other, qh).await?;
                    skip.push(AckEventMeta { off });
                }
            }
        }
        // These positions are proven non-payload records in this applied range.
        // They cannot hold work and must not leave holes in the numeric frontier.
        if !skip.is_empty() {
            self.apply_event_inmem(StromaEvent::AckMany { reqs: skip }, qh)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn append_combined(
        &self,
        qh: QueueHandle,
        messages: Vec<Message>,
        items: Vec<CompletionItem>,
        observer: Option<QueuePublishObserver>,
        lease: OwnerOperationLease,
    ) -> Result<()> {
        let h = qh.resolve()?;
        let event_log = h.event_log();
        let scope = h.ordered_apply_scope();
        if scope.is_none() {
            return Err(StromaError::InvalidArgument(
                "combined queues require ordered application".into(),
            ));
        }
        let mut events = Vec::with_capacity(messages.len());
        let mut records = Vec::with_capacity(messages.len());
        for (message, item) in messages.into_iter().zip(&items) {
            let event = StromaEvent::PayloadEnqueue {
                not_before: item.meta.not_before,
                expire_at: item.meta.expire_at,
                headers: message.headers,
                payload: message.payload,
            };
            match event_msg(&event) {
                Ok(record) => records.push(record),
                Err(error) => {
                    for item in items {
                        item.completion
                            .complete(Err(IoError::new(error.to_string())));
                    }
                    return Ok(());
                }
            }
            // Retain only state metadata while the encoded record owns the body.
            events.push(StromaEvent::PayloadEnqueue {
                not_before: item.meta.not_before,
                expire_at: item.meta.expire_at,
                headers: Vec::new(),
                payload: Vec::new(),
            });
        }
        let bytes: usize = records.iter().map(Message::bytes_len).sum();
        let message_epoch = h.message_lane_epoch();
        let event_epoch = event_log.current_epoch();
        let receiver = event_log.append_batch_enqueue_receiver(
            records,
            Some(self.queue_event_log_config().default_durability),
        );
        let stroma = self.clone();
        self.task_group
            .spawn("combined publish completion", async move {
                let outcome = async {
                    let ar = receiver
                        .map_err(io_err)?
                        .await
                        .map_err(io_err)?
                        .map_err(io_err)?;
                    let h = qh.resolve()?;
                    #[cfg(test)]
                    {
                        let pause = stroma.publish_apply_pause.lock().unwrap().clone()
                            .filter(|p| p.topic == qh.topic() && p.base == ar.base_offset);
                        if let Some(pause) = pause {
                            pause.entered.notify_one();
                            pause.release.notified().await;
                        }
                    }
                    let turn = crate::ordered_apply::enter(&scope, ar.base_offset, ar.count as u64)
                        .await?;
                    stroma
                        .apply_combined_records(events, ar.base_offset, &h)
                        .await?;
                    h.applied_upto()
                        .fetch_max(ar.base_offset + ar.count as u64 - 1, Ordering::Release);
                    h.set_dirty_snapshot(true);
                    crate::ordered_apply::finish(turn, scope)?;
                    stroma
                        .metrics
                        .event_log_appends
                        .observe(ar.count as usize, bytes);
                    Ok::<_, StromaError>(ar)
                }
                .await;
                match outcome {
                    Ok(ar) => {
                        if let Some(observer) = observer {
                            observer(QueuePublishCommit {
                                message_next: ar.base_offset + ar.count as u64,
                                event_next: ar.base_offset + ar.count as u64,
                                message_epoch,
                                event_epoch,
                            });
                        }
                        for (i, item) in items.into_iter().enumerate() {
                            item.completion.complete(Ok(AppendResult {
                                base_offset: ar.base_offset + i as u64,
                                count: 1,
                            }));
                        }
                    }
                    Err(error) => {
                        for item in items {
                            item.completion
                                .complete(Err(IoError::new(error.to_string())));
                        }
                    }
                }
                drop(lease);
            });
        Ok(())
    }
}

pub(super) fn recover_events(
    log: Arc<Keratin>,
    mut from: u64,
    tail: u64,
    applied: Arc<AtomicU64>,
) -> Result<RecoveryScanOutcome> {
    let reader = log.reader();
    let mut events = Vec::new();
    let mut count = 0;
    while from < tail {
        let records = reader
            .scan_from_with_byte_budget(from, 1024, 1024 * 1024)
            .map_err(io_err)?;
        if records.is_empty() {
            return Err(StromaError::Corruption(
                "combined replay has missing records".into(),
            ));
        }
        for record in records {
            if record.offset != from {
                return Err(StromaError::Corruption("combined replay offset gap".into()));
            }
            let event = match StromaEvent::decode(&record.payload) {
                Ok(event) => event,
                Err(error) => return Ok(RecoveryScanOutcome {
                    events, events_count: count,
                    mismatch: Some(RecoveryMismatchFound { event_offset: record.offset,
                        kind: RecoveryMismatchKind::CorruptRecord { detail: error.to_string() } }),
                }),
            };
            let carries_payload = matches!(event, StromaEvent::PayloadEnqueue { .. });
            if event
                .max_combined_referenced_offset()
                .is_some_and(|off| off >= record.offset)
            {
                // A payload enqueue and its bytes occupy the same record. A
                // forward reference cannot be an ordinary lost second-log tail.
                // Preserve the valid prefix and use the existing corruption
                // policy, including its bound-history prohibition on repair.
                return Ok(RecoveryScanOutcome {
                    events, events_count: count,
                    mismatch: Some(RecoveryMismatchFound { event_offset: record.offset,
                        kind: RecoveryMismatchKind::CorruptRecord {
                            detail: "single-log replay has forward payload reference".into(),
                        } }),
                });
            }
            events.push(event.into_record_state(record.offset));
            if !carries_payload {
                events.push(StromaEvent::Ack { off: record.offset });
            }
            applied.store(record.offset, Ordering::Release);
            from = record.offset + 1;
            count += 1;
        }
    }
    Ok(RecoveryScanOutcome {
        events,
        events_count: count,
        mismatch: None,
    })
}

#[cfg(test)]
#[path = "combined_tests.rs"]
mod tests;

impl Stroma {
    pub(crate) async fn verify_combined_payload_coverage(&self, h: &QueueHandleInner, pending_backfill: bool) -> Result<()> {
        let captured = h.capture_exact_checkpoint(false).await?;
        let ranges = captured.state.recovery_live_ranges();
        let log = h.event_log();
        tokio::task::spawn_blocking(move || {
            let reader = log.reader();
            for range in ranges.iter() {
                let end = if pending_backfill { range.end.min(log.next_offset()) } else { range.end };
                let mut off = range.start;
                while off < end {
                    let records = reader.scan_from_disk(off, (end-off).min(1024) as usize).map_err(io_err)?;
                    if records.is_empty() { return Err(StromaError::Corruption("live combined payload was not retained".into())); }
                    for record in records {
                        if record.offset != off || !matches!(StromaEvent::decode(&record.payload).map_err(decode_err)?, StromaEvent::PayloadEnqueue{..}) {
                            return Err(StromaError::Corruption("live state does not identify a retained payload enqueue".into()));
                        }
                        off += 1;
                    }
                }
            }
            Ok::<_, StromaError>(())
        }).await.map_err(io_err)??;
        Ok(())
    }
}
