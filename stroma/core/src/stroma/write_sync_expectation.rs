//! Which logs declare that every owner write waits for an fsync. Keratin
//! warns when a background sync on such a log covers records no append waited
//! to sync, which means some write path skipped its sync.
//!
//! The declaration is set once per log, at open or through
//! `set_stream_writes_synced`. Keratin applies it only while the log is in
//! the owner role, and every Stroma role change (promotion, demotion, follower
//! apply, freeze) already moves the role of each physical log with the
//! partition. Followers, which write replicated records and sync them
//! separately, are therefore never checked, and no role change needs a writer
//! settings round trip.
use super::*;

impl Stroma {
    /// Whether every owner write on a queue log waits for an fsync. Publishes
    /// use the stronger of the message and event log defaults and settlements
    /// use the event log default, so the event log default decides.
    pub(crate) fn queue_writes_all_synced(&self) -> bool {
        self.keratin_cfg_event.default_durability == KDurability::AfterFsync
    }

    fn stream_declared_synced(&self, topic: &str, part: u32) -> bool {
        self.synced_streams
            .contains_key(&(Box::<str>::from(topic), part))
    }

    /// Whether the event log of a partition expects every owner write synced.
    /// A stream's cursor commits are left to the background sync on purpose,
    /// see `cursor_commit_durability`, so a stream event log never does.
    pub(crate) fn event_log_expects_synced(
        &self,
        kind: PartitionKind,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> bool {
        match kind {
            PartitionKind::Queue => {
                self.queue_writes_all_synced() && !self.queue_is_buffered(topic, part, group)
            }
            PartitionKind::Stream => false,
        }
    }

    /// Whether a stream record log expects every owner write synced.
    pub(crate) fn stream_record_log_expects_synced(&self, topic: &str, part: u32) -> bool {
        self.stream_declared_synced(topic, part)
    }

    /// Declare whether every record appended to a stream waits for an fsync,
    /// as on a durable tier stream. Tiers that append with `AfterWrite` or do
    /// not wait for the sync must leave this off. The declaration lasts for
    /// this Stroma instance and applies to logs opened later too. Open logs
    /// take it through their live writer settings, which syncs what they have
    /// written first. Refuses queue partitions.
    pub async fn set_stream_writes_synced(
        &self,
        topic: &str,
        part: u32,
        synced: bool,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, None).await?;
        let h = qh.resolve()?;
        h.stream()?;
        let key = (Box::<str>::from(topic), part);
        if synced {
            self.synced_streams.insert(key, ());
        } else {
            self.synced_streams.remove(&key);
        }
        let record_log = h.msg_log()?;
        // A flag set in the configs themselves always stays on.
        set_log_expects_synced(
            &record_log,
            self.keratin_cfg_msg.expect_every_write_synced
                || self.stream_record_log_expects_synced(topic, part),
        )
        .await?;
        set_log_expects_synced(
            &h.event_log(),
            self.keratin_cfg_event.expect_every_write_synced
                || self.event_log_expects_synced(PartitionKind::Stream, topic, part, None),
        )
        .await
    }
}

/// Change one log's declaration through its live writer settings, retrying
/// when another settings change lands first.
pub(super) async fn set_log_expects_synced(log: &Keratin, expect: bool) -> Result<()> {
    loop {
        let current = log.writer_settings().await.map_err(io_err)?;
        if current.settings.expect_every_write_synced == expect {
            return Ok(());
        }
        let mut next = current.settings;
        next.expect_every_write_synced = expect;
        match log.replace_writer_settings(current.revision, next).await {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(io_err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    type Receipt =
        tokio::sync::oneshot::Receiver<std::result::Result<keratin_log::AppendResult, IoError>>;

    fn publish_item(value: u8) -> (PublishItem, Receipt) {
        let (completion, receipt) = KeratinAppendCompletion::pair();
        (
            PublishItem {
                headers: MessageHeaders {
                    published: 0,
                    publish_received: 0,
                    content_type: None,
                    extra: Default::default(),
                },
                payload: vec![value; 64],
                completion,
                not_before: None,
                expire_at: None,
            },
            receipt,
        )
    }

    async fn open(dir: &std::path::Path, message_log: KeratinConfig) -> Stroma {
        Stroma::open(
            dir,
            StromaKeratinConfig::from_message_log(message_log),
            SnapshotConfig::default(),
        )
        .await
        .unwrap()
    }

    // A busy background sync timer gives every chance to catch a write that
    // skipped its sync.
    fn config() -> KeratinConfig {
        KeratinConfig {
            background_sync_interval: Duration::from_millis(2),
            ..KeratinConfig::test_default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_queue_owner_traffic_never_skips_a_sync() {
        let dir = keratin_log::test_dir!("write_sync_queue_owner");
        let st = open(&dir.root, config()).await;
        let mut offsets = Vec::new();
        for round in 0..20u8 {
            let mut receipts = Vec::new();
            let mut items = Vec::new();
            for value in 0..5u8 {
                let (item, receipt) = publish_item(round.wrapping_mul(5).wrapping_add(value));
                items.push(item);
                receipts.push(receipt);
            }
            st.append_message_batch("q", 0, None, items).await.unwrap();
            for receipt in receipts {
                receipt.await.unwrap().unwrap();
            }
            let ready = st
                .poll_ready("q", 0, None, 5, u64::MAX, u64::MAX)
                .await
                .unwrap();
            for (offset, ..) in ready {
                offsets.push(offset);
            }
            // Settle by ack, nack with requeue and nack to the dead letters.
            while let Some(offset) = offsets.pop() {
                match offset % 3 {
                    0 => st.ack_one("q", 0, None, offset).await.unwrap(),
                    1 => st.nack_one("q", 0, None, offset, true).await.unwrap(),
                    _ => st.nack_one("q", 0, None, offset, false).await.unwrap(),
                }
            }
            // Idle stretches let the next write start a background sync.
            if round % 4 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
        let log = status.event_log;
        assert!(log.every_write_sync_expected, "{log:?}");
        assert_eq!(log.unexpected_background_syncs, 0, "{log:?}");

        // A follower log is not checked.
        st.demote_queue_owner_to_follower("q", 0, None)
            .await
            .unwrap();
        let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
        assert!(!status.event_log.every_write_sync_expected);
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn learner_queue_opens_unchecked_and_weak_queues_are_never_flagged() {
        let dir = keratin_log::test_dir!("write_sync_queue_kinds");
        let st = open(&dir.root, config()).await;
        st.become_queue_follower("q", 0, None).await.unwrap();
        let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
        assert!(!status.event_log.every_write_sync_expected);
        st.become_queue_owner("q", 0, None).await.unwrap();
        let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
        assert!(status.event_log.every_write_sync_expected);
        st.shutdown().await.unwrap();
        drop(st);

        let dir = keratin_log::test_dir!("write_sync_queue_weak");
        let mut weak = config();
        weak.default_durability = KDurability::AfterWrite;
        let st = Stroma::open(
            &dir.root,
            StromaKeratinConfig {
                message_log: weak,
                event_log: weak,
            },
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let status = st.queue_log_sync_status("q", 0, None).await.unwrap();
        assert!(!status.event_log.every_write_sync_expected);
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn streams_are_checked_only_once_declared_synced() {
        let dir = keratin_log::test_dir!("write_sync_stream");
        let st = open(&dir.root, config()).await;
        st.create_stream("s", 0, None).await.unwrap();
        let statuses = |st: &Stroma| {
            let st = st.clone();
            async move {
                let qh = st.queue_handle("s", 0, None).await.unwrap();
                let h = qh.resolve().unwrap();
                (
                    h.msg_log().unwrap().sync_status(),
                    h.event_log().sync_status(),
                )
            }
        };
        let (records, cursors) = statuses(&st).await;
        assert!(!records.every_write_sync_expected);
        assert!(!cursors.every_write_sync_expected);

        st.set_stream_writes_synced("s", 0, true).await.unwrap();
        let (records, cursors) = statuses(&st).await;
        assert!(records.every_write_sync_expected);
        // Cursor commits are left to the background sync.
        assert!(!cursors.every_write_sync_expected);
        for value in 0..10u8 {
            let headers = MessageHeaders {
                published: 0,
                publish_received: 0,
                content_type: None,
                extra: Default::default(),
            };
            st.append_stream_record("s", 0, &headers, vec![value; 32])
                .await
                .unwrap();
        }
        st.commit_stream_cursor("s", 0, "reader", 5).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (records, cursors) = statuses(&st).await;
        assert_eq!(records.unexpected_background_syncs, 0);
        assert_eq!(cursors.unexpected_background_syncs, 0);

        // The declaration also covers logs opened after it.
        st.evict("s", 0, None).await.unwrap();
        let (records, _) = statuses(&st).await;
        assert!(records.every_write_sync_expected);

        st.set_stream_writes_synced("s", 0, false).await.unwrap();
        let (records, cursors) = statuses(&st).await;
        assert!(!records.every_write_sync_expected);
        assert!(!cursors.every_write_sync_expected);

        st.queue_log_sync_status("q", 0, None).await.unwrap();
        assert!(matches!(
            st.set_stream_writes_synced("q", 0, true).await,
            Err(StromaError::WrongPartitionKind { .. })
        ));
        st.shutdown().await.unwrap();
    }
}
