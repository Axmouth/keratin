use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) enum SnapshotBoundary {
    LegacyInclusive(u64),
    ExactNext(u64),
}
impl SnapshotBoundary {
    pub(super) fn new(version: u16, offset: u64) -> Self {
        if version == 2 {
            Self::ExactNext(offset)
        } else {
            Self::LegacyInclusive(offset)
        }
    }
    pub(super) fn ambiguous_zero(self) -> bool {
        matches!(self, Self::LegacyInclusive(0))
    }
    pub(super) fn last_applied(self) -> u64 {
        match self {
            Self::LegacyInclusive(n) => n,
            Self::ExactNext(n) => n.saturating_sub(1),
        }
    }
    pub(super) fn event_next(self) -> u64 {
        match self {
            Self::LegacyInclusive(n) => n.saturating_add(1),
            Self::ExactNext(n) => n,
        }
    }
}

impl Stroma {
    /// Own the lifecycle/apply admission through persistence even if the caller
    /// cancels. Snapshot encoding and IO run after the actor releases its turn.
    pub(super) async fn write_exact_queue_checkpoint(
        &self,
        ticket: QueueHandle,
        compact: bool,
    ) -> Result<()> {
        let stroma = self.clone();
        tokio::spawn(async move {
            let qh = ticket.resolve()?;
            let apply = qh.follower_apply_state().await;
            if compact && qh.ensure_not_recovery_sealed().is_err() {
                return Ok(());
            }
            qh.ensure_not_recovery_sealed()?;
            if *apply {
                return Err(StromaError::Io(
                    "checkpoint blocked by incomplete follower apply".into(),
                ));
            }
            if compact && !qh.dirty_snapshot() {
                return Ok(());
            }
            if qh.role() == QueueRole::Follower {
                qh.align_follower_checkpoint_boundary()?;
            } else if qh.role() == QueueRole::Owner {
                qh.ensure_owner()?;
            }
            let generation = qh
                .recovery_gate
                .checkpoint_generation
                .load(Ordering::Acquire);
            let mut dirty_guard = RestoreDirty {
                handle: &qh,
                armed: true,
            };
            let captured = qh.capture_exact_checkpoint(true).await?;
            let event_next = captured.event_next;
            let message_floor = captured.message_floor;
            let timestamp = captured.state.last_snapshot_timestamp();
            let (topic, part, group) = (
                qh.topic().to_owned(),
                qh.partition(),
                qh.group().map(str::to_owned),
            );
            let gate = qh.recovery_gate.clone();
            let writer = stroma.clone();
            let persisted = tokio::task::spawn_blocking(move || {
                let bytes = captured.encode();
                gate.write_snapshot(generation, &topic, part, group.as_deref(), || {
                    writer.write_queue_snapshot_envelope(
                        &topic,
                        part,
                        group.as_deref(),
                        2,
                        event_next,
                        &bytes,
                    )
                })
            })
            .await
            .map_err(|err| StromaError::Io(err.to_string()))?;
            persisted?;
            dirty_guard.armed = false;
            qh.record_checkpoint_persisted(event_next, timestamp);
            if compact {
                let (message_floor, event_next) = stroma.checkpoint_retention_limits(&qh, message_floor, event_next).await?;
                qh.event_log()
                    .truncate_before(event_next)
                    .await
                    .map_err(io_err)?;
                qh.msg_log()
                    .truncate_before(message_floor)
                    .await
                    .map_err(io_err)?;
            }
            Ok(())
        })
        .await
        .map_err(|err| StromaError::Io(err.to_string()))?
    }
}

/// Failed persistence leaves the queue eligible for a fresh snapshot. Never
/// clear dirtiness here: an application after capture may already have set it.
struct RestoreDirty<'a> {
    handle: &'a QueueHandleInner,
    armed: bool,
}
impl Drop for RestoreDirty<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.handle.set_dirty_snapshot(true);
        }
    }
}

#[cfg(all(test, feature = "ordered-queue-apply"))]
#[path = "checkpoint_tests.rs"]
mod tests;
