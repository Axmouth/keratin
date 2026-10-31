//! Bring a stream copy in line with the owner history before it follows or
//! owns under a newer assignment epoch.
//!
//! When a stream fails over, the new owner continues from its own tails. A
//! copy that followed an older owner can hold records and cursor events past
//! that point which were never confirmed, and the new owner writes different
//! entries at those offsets. The coordinator records where each epoch's owner
//! history starts, and this cuts both logs back to the first start the copy
//! has not seen yet.

use super::*;

/// Where the owner history of `epoch` starts: the promoted copy's record and
/// cursor event tails when it took over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEpochStart {
    pub epoch: u64,
    pub records: Offset,
    pub cursor_events: Offset,
}

/// What a rejoin changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamRejoin {
    /// Record log tail after the rejoin.
    pub record_next: Offset,
    /// Cursor event log tail after the rejoin.
    pub event_next: Offset,
    pub records_dropped: u64,
    pub events_dropped: u64,
    /// A recovery seal was released.
    pub released_seal: bool,
    /// The copy holds a seal it may not release, so nothing changed.
    pub still_sealed: bool,
    /// An open handle was closed. The next access reopens it from disk, in
    /// the default owner role.
    pub closed_handle: bool,
}

/// The lowest start of an epoch the copy has not followed yet, or `None` when
/// it is already on the history of `target`.
fn cut(
    starts: &[StreamEpochStart],
    local_epoch: u64,
    target: u64,
    pick: fn(&StreamEpochStart) -> Offset,
) -> Option<Offset> {
    starts
        .iter()
        .filter(|start| start.epoch > local_epoch && start.epoch <= target)
        .map(pick)
        .min()
}

impl Stroma {
    /// Cut a stream copy back to the owner history of assignment `epoch`
    /// before it follows or owns at that epoch. Records and cursor events past
    /// the first unseen epoch start are dropped, and on the next open the
    /// stream tail and any cursor above it come down to the record log end.
    /// Nothing at or below that start is touched.
    ///
    /// A copy sealed for recovery uses the epochs the seal replaced, since the
    /// seal advanced them without new data. With `release_seal` the seal is
    /// removed afterwards, once the assignment includes this copy at or after
    /// the fence epoch. Without it a sealed copy is left alone.
    ///
    /// Repeating the call is harmless: the cut is computed from epochs that
    /// only move once the caller advances them after this returns.
    pub async fn rejoin_stream_at_epoch(
        &self,
        topic: &str,
        part: u32,
        epoch: u64,
        starts: &[StreamEpochStart],
        release_seal: bool,
    ) -> Result<StreamRejoin> {
        if self.partition_kind_marker(topic, part, None) != Some(PartitionKind::Stream) {
            return Ok(StreamRejoin::default());
        }
        let sealed = self.sealed_stream_epochs(topic, part)?;
        if sealed.is_some_and(|seal| !release_seal || seal.fence_epoch > epoch) {
            return Ok(StreamRejoin {
                still_sealed: true,
                ..Default::default()
            });
        }
        if sealed.is_none()
            && let Some(rejoin) = self.stream_rejoin_unneeded(topic, part, epoch, starts)?
        {
            return Ok(rejoin);
        }
        // Logs are cut with no handle open, and the next open recovers the
        // stream state from them. Stream handles hold no inflight work.
        let mut closed_handle = false;
        let _lifecycle = loop {
            match self.evict(topic, part, None).await? {
                EvictOutcome::HasInflight => {
                    return Err(StromaError::InvalidArgument(
                        "stream rejoin found inflight work".into(),
                    ));
                }
                EvictOutcome::RaceLost => {
                    tokio::task::yield_now().await;
                    continue;
                }
                EvictOutcome::Evicted => closed_handle = true,
                _ => {}
            }
            let lifecycle = self.lock_partition_lifecycle(topic, part, None).await;
            let current = self.queue_handles.load();
            if slot_lookup_no_alloc(&current, topic, part, None)
                .is_some_and(|slot| slot.handle.get().is_some())
            {
                drop(lifecycle);
                tokio::task::yield_now().await;
                continue;
            }
            break lifecycle;
        };
        self.ensure_checkpoint_not_pending(topic, part, None)?;
        let sealed = self.sealed_stream_epochs(topic, part)?;
        let records = self.msg_log_init(topic, part, None).await?;
        let events = match self.event_log_init(topic, part, None).await {
            Ok(events) => events,
            Err(err) => {
                let _ = records.shutdown().await;
                return Err(err);
            }
        };
        let result = self
            .rejoin_open_stream_logs(topic, part, epoch, starts, sealed, &records, &events)
            .await;
        let (record_close, event_close) = tokio::join!(records.shutdown(), events.shutdown());
        let mut rejoin = result?;
        rejoin.closed_handle = closed_handle;
        record_close.map_err(io_err)?;
        event_close.map_err(io_err)?;
        if sealed.is_some() {
            let stroma = self.clone();
            let topic_owned = topic.to_owned();
            tokio::task::spawn_blocking(move || {
                stroma.remove_stream_recovery_seal(&topic_owned, part)
            })
            .await
            .map_err(io_err)??;
            rejoin.released_seal = true;
            tracing::info!(
                topic,
                partition = part,
                epoch,
                "released stream recovery seal"
            );
        }
        Ok(rejoin)
    }

    /// With the stream open and nothing to cut, report its tails without
    /// closing it. `None` means the full rejoin has to run.
    fn stream_rejoin_unneeded(
        &self,
        topic: &str,
        part: u32,
        epoch: u64,
        starts: &[StreamEpochStart],
    ) -> Result<Option<StreamRejoin>> {
        let current = self.queue_handles.load();
        let Some(handle) = slot_lookup_no_alloc(&current, topic, part, None)
            .and_then(|slot| slot.handle.get().cloned())
        else {
            return Ok(None);
        };
        let (Some(records), events) = (handle.optional_message_log(), handle.event_log()) else {
            return Ok(None);
        };
        let record_cut = cut(starts, records.current_epoch(), epoch, |s| s.records);
        let event_cut = cut(starts, events.current_epoch(), epoch, |s| s.cursor_events);
        if record_cut.is_some_and(|cut| cut < records.next_offset())
            || event_cut.is_some_and(|cut| cut < events.next_offset())
        {
            return Ok(None);
        }
        // A rejoin cut short before it lowered the snapshot finishes now.
        if self
            .read_queue_snapshot(&self.snap_file(topic, part, None))?
            .is_some_and(|(boundary, _)| snapshot_event_next(boundary) > events.next_offset())
        {
            return Ok(None);
        }
        Ok(Some(StreamRejoin {
            record_next: records.next_offset(),
            event_next: events.next_offset(),
            ..Default::default()
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn rejoin_open_stream_logs(
        &self,
        topic: &str,
        part: u32,
        epoch: u64,
        starts: &[StreamEpochStart],
        sealed: Option<recovery_seal::SealedStreamEpochs>,
        records: &Keratin,
        events: &Keratin,
    ) -> Result<StreamRejoin> {
        let (record_epoch, event_epoch) = match sealed {
            Some(seal) => (seal.records, seal.cursor_events),
            None => (records.current_epoch(), events.current_epoch()),
        };
        let record_cut = cut(starts, record_epoch, epoch, |s| s.records);
        let event_cut = cut(starts, event_epoch, epoch, |s| s.cursor_events);
        let records_dropped = truncate_log_to(records, record_cut, "record")?;
        let events_dropped = truncate_log_to(events, event_cut, "cursor event")?;
        if records_dropped > 0 {
            truncate_log(records, record_cut).await?;
        }
        if events_dropped > 0 {
            truncate_log(events, event_cut).await?;
        }
        // The snapshot may cover cursor events past the cut. Its cursors are
        // clamped to the record log on open, so keep its state but lower its
        // boundary, letting the new owner's events at those offsets replay.
        let event_next = events.next_offset();
        let snap = self.snap_file(topic, part, None);
        if let Some((boundary, blob)) = self.read_queue_snapshot(&snap)?
            && snapshot_event_next(boundary) > event_next
        {
            let stroma = self.clone();
            let topic_owned = topic.to_owned();
            tokio::task::spawn_blocking(move || match boundary {
                checkpoint_capture::SnapshotBoundary::ExactNext(_) => stroma
                    .write_queue_snapshot_envelope(&topic_owned, part, None, 2, event_next, &blob),
                // A legacy inclusive boundary of zero reads as empty, so a
                // stream cut to no events replays from its log head.
                checkpoint_capture::SnapshotBoundary::LegacyInclusive(_) => stroma
                    .write_queue_snapshot(
                        &topic_owned,
                        part,
                        None,
                        event_next.saturating_sub(1),
                        &blob,
                    ),
            })
            .await
            .map_err(io_err)??;
        }
        if records_dropped > 0 || events_dropped > 0 {
            tracing::warn!(
                topic,
                partition = part,
                epoch,
                records_dropped,
                events_dropped,
                record_next = records.next_offset(),
                event_next,
                "stream copy cut back to the new owner history before rejoining"
            );
        }
        Ok(StreamRejoin {
            record_next: records.next_offset(),
            event_next,
            records_dropped,
            events_dropped,
            ..Default::default()
        })
    }
}

/// First event a snapshot does not cover. A legacy boundary of zero also
/// stands for an empty snapshot, which covers nothing.
fn snapshot_event_next(boundary: checkpoint_capture::SnapshotBoundary) -> Offset {
    if boundary.ambiguous_zero() {
        0
    } else {
        boundary.event_next()
    }
}

/// How many entries a cut to `cut` drops. A cut below the retained head would
/// remove the whole log, which a rejoin never does on its own.
fn truncate_log_to(log: &Keratin, cut: Option<Offset>, lane: &str) -> Result<u64> {
    let Some(cut) = cut else { return Ok(0) };
    let next = log.next_offset();
    if cut >= next {
        return Ok(0);
    }
    if cut < log.head_offset() {
        return Err(StromaError::InvalidArgument(format!(
            "stream {lane} log retains nothing below the owner history start {cut}"
        )));
    }
    Ok(next - cut)
}

async fn truncate_log(log: &Keratin, cut: Option<Offset>) -> Result<()> {
    let cut = cut.expect("a dropping cut is set");
    log.become_follower();
    log.repair_suffix_at_epoch(cut, log.current_epoch())
        .await
        .map_err(io_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(epoch: u64, records: u64, cursor_events: u64) -> StreamEpochStart {
        StreamEpochStart {
            epoch,
            records,
            cursor_events,
        }
    }

    #[test]
    fn cut_uses_the_first_unseen_epoch_up_to_the_target() {
        let starts = [start(4, 100, 10), start(6, 90, 12), start(9, 200, 20)];
        let records = |s: &StreamEpochStart| s.records;
        // Already on epoch 6 history: only later starts count.
        assert_eq!(cut(&starts, 6, 8, records), None);
        assert_eq!(cut(&starts, 6, 9, records), Some(200));
        // Behind both: the lower start wins, even when it is the later one.
        assert_eq!(cut(&starts, 3, 8, records), Some(90));
        assert_eq!(cut(&starts, 5, 8, records), Some(90));
        assert_eq!(cut(&[], 1, 9, records), None);
    }
}
