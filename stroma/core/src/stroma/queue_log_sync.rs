//! Written and durable progress of the logs behind a queue partition, and the
//! cadence of their background sync. Reporting and tuning only. Queue reads,
//! confirms and apply still follow the durable frontier.
use super::*;

/// Sync status of each log behind a queue partition. See
/// [`Stroma::queue_log_sync_status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueLogSyncStatus {
    /// The queue event log. In the single-log layout it holds the payloads too.
    pub event_log: keratin_log::SyncStatus,
    /// The separate payload log of the two-log layout.
    pub message_log: Option<keratin_log::SyncStatus>,
}

impl Stroma {
    /// Written and durable frontiers, durable lag and background sync state of
    /// every log behind a queue partition. Refuses stream partitions.
    pub async fn queue_log_sync_status(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueLogSyncStatus> {
        let qh = self.queue_handle(topic, part, group).await?;
        let h = qh.resolve()?;
        h.work_queue()?;
        Ok(QueueLogSyncStatus {
            event_log: h.event_log().sync_status(),
            message_log: h.optional_message_log().map(|log| log.sync_status()),
        })
    }

    /// Change the background sync interval of every log behind a queue
    /// partition through the live writer settings. Zero turns it off. A log
    /// that already uses the interval is left alone, otherwise the change
    /// syncs what that log has written first. The change lives only in the
    /// open logs, so a reopen goes back to the configured interval. Refuses
    /// stream partitions.
    pub async fn set_queue_background_sync_interval(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        interval: std::time::Duration,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, group).await?;
        let h = qh.resolve()?;
        h.work_queue()?;
        for log in h.physical_logs() {
            let current = log.writer_settings().await.map_err(io_err)?;
            if current.settings.background_sync_interval == interval {
                continue;
            }
            let mut next = current.settings;
            next.background_sync_interval = interval;
            log.replace_writer_settings(current.revision, next)
                .await
                .map_err(io_err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn queue_logs_report_frontiers_and_take_a_new_interval() {
        let dir = keratin_log::test_dir!("queue_log_sync");
        let message_log = KeratinConfig {
            background_sync_interval: Duration::from_millis(250),
            ..KeratinConfig::test_default()
        };
        let st = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(message_log),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let (completion, receipt) = KeratinAppendCompletion::pair();
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
                payload: vec![7; 64],
                completion,
                not_before: None,
                expire_at: None,
            }],
        )
        .await
        .unwrap();
        receipt.await.unwrap().unwrap();

        // Queue publishes still wait for fsync, so nothing lags.
        let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
        assert!(status.message_log.is_none());
        let log = status.event_log;
        assert_eq!(log.background_sync_interval, Duration::from_millis(250));
        assert!(log.durable_next > 0);
        assert_eq!(log.written_next, log.durable_next);
        assert_eq!(log.durable_lag_records, 0);

        for interval in [Duration::ZERO, Duration::from_millis(10)] {
            st.set_queue_background_sync_interval("q", 0, None, interval)
                .await
                .unwrap();
            let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
            assert_eq!(status.event_log.background_sync_interval, interval);
        }
        // Setting the same interval again changes no writer settings.
        let qh = st.queue_handle("q", 0, None).await.unwrap();
        let revision = qh
            .resolve()
            .unwrap()
            .event_log()
            .writer_settings()
            .await
            .unwrap()
            .revision;
        st.set_queue_background_sync_interval("q", 0, None, Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(
            qh.resolve()
                .unwrap()
                .event_log()
                .writer_settings()
                .await
                .unwrap()
                .revision,
            revision
        );
        drop(qh);

        st.create_stream("s", 0, None).await.unwrap();
        assert!(matches!(
            st.queue_log_sync_status("s", 0, None).await,
            Err(StromaError::WrongPartitionKind { .. })
        ));
        assert!(matches!(
            st.set_queue_background_sync_interval("s", 0, None, Duration::ZERO)
                .await,
            Err(StromaError::WrongPartitionKind { .. })
        ));
        st.shutdown().await.unwrap();
    }
}
