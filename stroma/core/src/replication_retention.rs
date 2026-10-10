//! Compaction constraints supplied by the replication coordinator.
use super::*;

/// Exclusive durable positions still needed by protected followers. This is
/// consulted only during compaction, never on the append path. Returning zero
/// conservatively retains everything. Errors must prevent compaction.
pub trait QueueReplicationRetention: std::fmt::Debug + Send + Sync {
    /// Exclusive position in the single queue log still needed by protected
    /// followers. Stroma consults only this method. The default reads the
    /// event half of `retained_from` for sources that implement only that.
    fn retained_next(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
        history: Option<&StorageHistoryBinding>,
    ) -> Result<u64> {
        self.retained_from(topic, partition, group, history)
            .map(|(_, event)| event)
    }

    /// Two-lane form kept so older sources still build. Only its event half
    /// was ever read. The default retains everything.
    fn retained_from(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
        history: Option<&StorageHistoryBinding>,
    ) -> Result<(u64, u64)> {
        let _ = (topic, partition, group, history);
        Ok((0, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Progress(std::sync::Mutex<u64>);
    impl QueueReplicationRetention for Progress {
        fn retained_next(
            &self,
            _: &str,
            _: u32,
            _: Option<&str>,
            _: Option<&StorageHistoryBinding>,
        ) -> Result<u64> {
            Ok(*self.0.lock().unwrap())
        }
    }

    /// A source that implements only the two-lane method keeps working: its
    /// event half becomes the single-log floor.
    #[derive(Debug)]
    struct TwoLaneProgress;
    impl QueueReplicationRetention for TwoLaneProgress {
        fn retained_from(
            &self,
            _: &str,
            _: u32,
            _: Option<&str>,
            _: Option<&StorageHistoryBinding>,
        ) -> Result<(u64, u64)> {
            Ok((3, 7))
        }
    }

    /// A source that implements neither method retains everything.
    #[derive(Debug)]
    struct Silent;
    impl QueueReplicationRetention for Silent {}

    #[test]
    fn single_log_retention_defaults_are_conservative() {
        assert_eq!(TwoLaneProgress.retained_next("q", 0, None, None).unwrap(), 7);
        assert_eq!(Silent.retained_next("q", 0, None, None).unwrap(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compaction_retains_lagging_replica_events_then_releases_reported_prefix() {
        let dir = keratin_log::test_dir!("replication_compaction_floor");
        let mut cfg = KeratinConfig::test_default();
        cfg.segment_max_bytes = 512;
        let st = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(cfg),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let progress = Arc::new(Progress(std::sync::Mutex::new(0)));
        st.set_queue_replication_retention(progress.clone());
        for _ in 0..32 {
            let (completion, rx) = KeratinAppendCompletion::pair();
            st.append_message_batch(
                "q",
                0,
                None,
                vec![PublishItem {
                    headers: MessageHeaders {
                        published: 0,
                        publish_received: 0,
                        content_type: None,
                        extra: Default::default(),
                    },
                    payload: vec![1; 1024],
                    completion,
                    not_before: None,
                    expire_at: None,
                }],
            )
            .await
            .unwrap();
            rx.await.unwrap().unwrap();
        }
        // Settle the prefix so payload retention no longer pins it. The
        // follower floor must still protect the same records until it advances.
        for off in 0..16 { st.ack_one("q", 0, None, off).await.unwrap(); }
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        Stroma::periodic_snapshot_step(&st, &ticket).await.unwrap();
        assert_eq!(
            h.event_log().head_offset(),
            0,
            "snapshot must not overrun a follower at zero"
        );
        assert!(matches!(
            st.read_owner_queue_records("q", 0, None, 0, 32, usize::MAX)
                .await
                .unwrap(),
            OwnerReplicationRead::Batch(_)
        ));
        *progress.0.lock().unwrap() = 16;
        h.set_dirty_snapshot(true);
        Stroma::periodic_snapshot_step(&st, &ticket).await.unwrap();
        assert!(
            h.event_log().head_offset() > 0,
            "old segments should now be reclaimed"
        );
        assert!(h.event_log().head_offset() <= 16);
        assert!(matches!(
            st.read_owner_queue_records("q", 0, None, 16, 32, usize::MAX)
                .await
                .unwrap(),
            OwnerReplicationRead::Batch(_)
        ));
        st.shutdown().await.unwrap();
    }
}

impl Stroma {
    pub fn set_queue_replication_retention(&self, source: Arc<dyn QueueReplicationRetention>) {
        *self.replication_retention.write().unwrap() = Some(source);
    }

    /// Exclusive queue log position protected followers still need.
    pub(super) fn replication_retention_limit(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
    ) -> Result<u64> {
        let history = self.storage_history_binding(topic, partition, group)?;
        let source = self.replication_retention.read().unwrap().clone();
        match source {
            Some(source) => source.retained_next(topic, partition, group, history.as_ref()),
            // After restart, history enrollment alone is not evidence that
            // remote replicas have reached this process's snapshot boundary.
            None if history.is_some() => Ok(0),
            None => Ok(u64::MAX),
        }
    }
}
