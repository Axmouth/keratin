//! Disposable background-sync probe. No public durability policy is added.
use super::*;

impl Stroma {
    pub(super) fn periodic_afterflush_probe(&self, ticket: QueueHandle) {
        if std::env::var("KERATIN_EXPERIMENT_AFTERFLUSH").as_deref() != Ok("1") {
            return;
        }
        let Some(ms) = std::env::var("KERATIN_AFTERFLUSH_SYNC_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| (1..=60_000).contains(v))
        else {
            return;
        };
        self.task_group.spawn("AfterFlush sync probe", async move {
            if let Err(error) = run(ticket, Duration::from_millis(ms)).await {
                // Stop instead of silently claiming a bounded persistence delay.
                tracing::error!(%error, "AfterFlush background sync probe stopped");
            }
        });
    }
}

async fn run(ticket: QueueHandle, period: Duration) -> Result<()> {
    let cancellation = ticket.resolve()?.background_cancellation_token();
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // The first sync happens after one interval.
    let mut synced = None;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        let Ok(handle) = ticket.resolve() else {
            return Ok(());
        };
        // Queue logs only. Streams already have their own persistence policy.
        if handle.as_work_queue().is_none() || handle.ensure_not_recovery_sealed().is_err() {
            continue;
        }
        let log = handle.event_log();
        let observed = (log.current_epoch(), log.next_offset());
        if observed.1 == 0 || synced == Some(observed) {
            continue;
        }
        // One request at a time. Await completion before another tick, but the
        // fsync worker allows staging to continue. Never park a Tokio thread.
        // Retain only the pre-request boundary so concurrent appends stay dirty.
        // An admitted sync finishes even if this incarnation is being retired.
        log.sync().await.map_err(io_err)?;
        synced = Some(observed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn periodic_sync_covers_offset_zero_and_subsequent_writes() {
        let dir = keratin_log::test_dir!("afterflush_periodic");
        let st = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let handle = ticket.resolve().unwrap();
        let log = handle.event_log();
        let reader = log.reader();
        let event = StromaEvent::Ack { off: 0 };
        log.append_batch(
            vec![event_msg(&event).unwrap()],
            Some(KDurability::AfterFlush),
        )
        .await
        .unwrap();
        assert!(reader.scan_from(0, 1).unwrap().is_empty());
        let task = tokio::spawn(run(ticket.clone(), Duration::from_millis(20)));
        for offset in 0..3 {
            if offset > 0 {
                log.append_batch(
                    vec![event_msg(&event).unwrap()],
                    Some(KDurability::AfterFlush),
                )
                .await
                .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(3), async {
                while reader.scan_from(offset, 1).unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        handle.background_cancellation_token().cancel();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(log);
        drop(handle);
        st.shutdown().await.unwrap();
    }
}
