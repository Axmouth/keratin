//! Removal of a queue partition that never held a record.
//!
//! An older broker could open a stream partition with the default queue kind
//! before the stream itself was created. The partition then refuses the
//! stream for good. This lets the caller drop such a partition, but only when
//! the queue provably holds nothing.
use super::*;

/// Result of [`Stroma::remove_empty_queue_partition`]. `path` is the
/// partition directory, for operator logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmptyQueueRemoval {
    /// The partition was empty and is gone from the registry and from disk.
    Removed { path: PathBuf },
    /// No partition exists at this identity.
    Absent,
    /// Nothing was removed. The reason says which check failed.
    Kept { path: PathBuf, reason: String },
}

impl Stroma {
    /// Remove a queue partition only if it never held a record.
    ///
    /// Empty means every log of the partition has next offset 0, so no
    /// message, ack, declaration or other event was ever appended. Log
    /// offsets never move back, even when retention deletes segments, so a
    /// queue that ever held anything is always kept. Fencing epochs live in
    /// the log manifest, not in records, and do not count.
    ///
    /// Also kept: stream partitions, partitions that are sealed, quarantined,
    /// waiting on a checkpoint, bound to a replicated storage history or carrying
    /// a recovery installation, and partitions this process still has open
    /// after an eviction attempt.
    ///
    /// The check and the removal run under the partition lifecycle lock, so no
    /// open can slip in between. Removal reuses [`Self::destroy_partition`],
    /// which renames the partition into trash before deleting it, so a crash
    /// leaves either the old partition or nothing.
    pub async fn remove_empty_queue_partition(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<EmptyQueueRemoval> {
        let group = normalize_group(group);
        let path = self.partition_dir(topic, part, group);
        let kept = |reason: &dyn std::fmt::Display| EmptyQueueRemoval::Kept {
            path: path.clone(),
            reason: reason.to_string(),
        };

        // Close an open handle first. Eviction takes the lifecycle lock, so it
        // has to happen before this function takes it.
        if self.is_materialized(topic, part, group) {
            match self.evict(topic, part, group).await? {
                EvictOutcome::HasInflight => return Ok(kept(&"the queue has leased messages")),
                EvictOutcome::RaceLost => return Ok(kept(&"the partition is busy")),
                EvictOutcome::Evicted
                | EvictOutcome::NotMaterialized
                | EvictOutcome::NotPresent => {}
            }
        }

        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;

        match self.partition_kind_marker(topic, part, group) {
            None => {
                return Ok(if path.try_exists().map_err(io_err)? {
                    kept(&"the partition has no readable kind marker")
                } else {
                    EmptyQueueRemoval::Absent
                });
            }
            Some(PartitionKind::Stream) => return Ok(kept(&"the partition is a stream")),
            Some(PartitionKind::Queue) => {}
        }
        if self.is_materialized(topic, part, group) {
            return Ok(kept(&"the partition is open in this process"));
        }
        let key = (Box::<str>::from(topic), part, group.map(Box::<str>::from));
        if self.quarantined.contains_key(&key) {
            return Ok(kept(&"the partition is quarantined"));
        }
        if let Err(err) = self
            .ensure_partition_not_sealed(topic, part, group)
            .and_then(|()| self.ensure_checkpoint_not_pending(topic, part, group))
            .and_then(|()| self.require_unbound_storage_history(topic, part, group))
        {
            return Ok(kept(&err));
        }
        // Queues keep everything in one log. A payload-only directory would
        // mean storage this check does not understand.
        if self
            .msg_tp_part_dir(topic, part, group)
            .try_exists()
            .map_err(io_err)?
        {
            return Ok(kept(&"the partition has a separate message log"));
        }

        // Open only the log, as repair_partition does, to read its true end
        // after recovery rather than a possibly stale manifest.
        let log = self.event_log_init(topic, part, group).await?;
        let next = log.next_offset();
        log.shutdown().await.map_err(io_err)?;
        if next != 0 {
            return Ok(kept(&format_args!(
                "the queue log holds records (next offset {next})"
            )));
        }

        match self.destroy_partition_locked(topic, part, group).await? {
            DestroyOutcome::Destroyed => Ok(EmptyQueueRemoval::Removed { path: path.clone() }),
            DestroyOutcome::HasInflight => Ok(kept(&"the queue has leased messages")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keratin_log::test_dir;

    async fn open(root: &Path) -> Stroma {
        Stroma::open(
            root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig { every_events: 1 },
        )
        .await
        .unwrap()
    }

    fn headers() -> MessageHeaders {
        MessageHeaders {
            published: 0,
            publish_received: 0,
            content_type: None,
            extra: Default::default(),
        }
    }

    async fn publish(st: &Stroma, topic: &str) -> u64 {
        let (completion, rx) = KeratinAppendCompletion::pair();
        st.append_message_batch(
            topic,
            0,
            None,
            vec![PublishItem {
                headers: headers(),
                payload: b"m".to_vec(),
                completion,
                not_before: None,
                expire_at: None,
            }],
        )
        .await
        .unwrap();
        rx.await.unwrap().unwrap().base_offset
    }

    async fn ack(st: &Stroma, topic: &str, off: u64) {
        let (completion, rx) = KeratinAppendCompletion::pair();
        st.ack_enqueue(topic, 0, None, off, completion)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    /// What an accidental open left behind: a queue partition that was opened,
    /// fenced at an epoch and made owner, with no record appended.
    async fn stray_queue(st: &Stroma, topic: &str) {
        st.materialize(topic, 0, None).await.unwrap();
        st.advance_stream_epoch(topic, 0, 3).await.unwrap();
        st.become_queue_owner(topic, 0, None).await.unwrap();
        st.ensure_queue_owner_epoch(topic, 0, None, Some(4))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn removes_a_queue_partition_that_never_held_a_record() {
        let dir = test_dir!("empty_queue_removal_removes");
        let st = open(&dir.root).await;
        stray_queue(&st, "events").await;
        assert_eq!(
            st.partition_kind_marker("events", 0, None),
            Some(PartitionKind::Queue)
        );

        // While open in this process.
        assert!(matches!(
            st.remove_empty_queue_partition("events", 0, None).await.unwrap(),
            EmptyQueueRemoval::Removed { path } if path == st.partition_dir("events", 0, None)
        ));
        assert!(!st.partition_dir("events", 0, None).exists());
        assert_eq!(st.partition_kind_marker("events", 0, None), None);
        assert_eq!(
            st.remove_empty_queue_partition("events", 0, None)
                .await
                .unwrap(),
            EmptyQueueRemoval::Absent
        );

        // The stream can be created in its place.
        st.create_stream("events", 0, None).await.unwrap();
        assert_eq!(st.partition_kind("events", 0, None), PartitionKind::Stream);
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn removes_a_cold_empty_queue_partition_after_restart() {
        let dir = test_dir!("empty_queue_removal_cold");
        let st = open(&dir.root).await;
        stray_queue(&st, "events").await;
        st.shutdown().await.unwrap();

        let st = open(&dir.root).await;
        assert!(!st.is_materialized("events", 0, None));
        assert!(matches!(
            st.remove_empty_queue_partition("events", 0, None).await.unwrap(),
            EmptyQueueRemoval::Removed { path } if path == st.partition_dir("events", 0, None)
        ));
        assert!(!st.partition_dir("events", 0, None).exists());
        st.shutdown().await.unwrap();

        // Nothing comes back at the next open.
        let st = open(&dir.root).await;
        assert_eq!(st.partition_kind_marker("events", 0, None), None);
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn keeps_a_queue_that_ever_held_a_message() {
        let dir = test_dir!("empty_queue_removal_keeps_message");
        let st = open(&dir.root).await;
        stray_queue(&st, "events").await;
        let off = publish(&st, "events").await;
        ack(&st, "events", off).await;
        st.shutdown().await.unwrap();

        let st = open(&dir.root).await;
        let outcome = st
            .remove_empty_queue_partition("events", 0, None)
            .await
            .unwrap();
        assert!(
            matches!(outcome, EmptyQueueRemoval::Kept { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            st.partition_kind_marker("events", 0, None),
            Some(PartitionKind::Queue)
        );
        assert!(st.partition_dir("events", 0, None).exists());
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn keeps_a_declared_queue_without_messages() {
        let dir = test_dir!("empty_queue_removal_keeps_declared");
        let st = open(&dir.root).await;
        st.declare("orders", 0, None, DeclareMeta::default())
            .await
            .unwrap();
        let outcome = st
            .remove_empty_queue_partition("orders", 0, None)
            .await
            .unwrap();
        assert!(
            matches!(outcome, EmptyQueueRemoval::Kept { .. }),
            "{outcome:?}"
        );
        assert!(st.partition_dir("orders", 0, None).exists());
        st.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn keeps_a_stream_partition() {
        let dir = test_dir!("empty_queue_removal_keeps_stream");
        let st = open(&dir.root).await;
        st.create_stream("events", 0, None).await.unwrap();
        let outcome = st
            .remove_empty_queue_partition("events", 0, None)
            .await
            .unwrap();
        assert!(
            matches!(outcome, EmptyQueueRemoval::Kept { .. }),
            "{outcome:?}"
        );
        assert_eq!(st.partition_kind("events", 0, None), PartitionKind::Stream);
        st.shutdown().await.unwrap();
    }
}
