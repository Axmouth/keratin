//! Buffered queues confirm, apply and deliver once a record reaches the OS on
//! the owner, without waiting for its fsync. Background sync makes those
//! records durable later. Power loss or a failed sync can lose records written
//! since the last completed sync, confirmed or not, along with their settles.
//!
//! The mode is declared per partition by the embedder. Stroma keeps it in a
//! `buffered` marker file in the partition directory and loads it when the
//! queue log opens, so it survives a restart.
//!
//! Every owner write of a buffered queue uses at most `AfterFlush`: publishes,
//! acks, nacks, releases, discards and dead-letter markers. A cut back to the
//! last sync always leaves a prefix of the log, so a lost settle only moves the
//! queue back to an earlier consistent state and the message is delivered
//! again. Writes that ask for `AfterFsync` explicitly keep it. A dead-letter
//! copy lands in the target queue under the target's own mode.
use super::write_sync_expectation::set_log_expects_synced;
use super::*;

impl Stroma {
    /// Declare whether a queue partition is buffered. Refuses stream
    /// partitions and queues on the two-log layout. The declaration is stored
    /// with the partition and lasts until changed.
    ///
    /// Turning it off pauses owner operations and waits for those in flight,
    /// syncs the log, and only then clears the flag. Every applied record is
    /// durable by the time reads and writes move back to the durable frontier,
    /// and no write picks its durability in between.
    pub async fn set_queue_buffered(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        buffered: bool,
    ) -> Result<()> {
        let qh = self.queue_handle(topic, part, group).await?;
        let h = qh.resolve()?;
        h.work_queue()?;
        if !h.has_combined_log() {
            return Err(StromaError::InvalidArgument(format!(
                "{topic}/{part} uses the two-log queue layout, which cannot be buffered"
            )));
        }
        let key = buffered_key(topic, part, group);
        let event_log = h.event_log();
        let part_dir = self.partition_dir(topic, part, group);
        if buffered {
            layout::write_buffered(&part_dir, true)?;
            self.buffered_queues.insert(key, ());
        } else if self.buffered_queues.contains_key(&key) {
            // Owner operations hold their lease until they have applied, so
            // the pause waits out every write that chose AfterFlush.
            let _pause = match h.role() {
                QueueRole::Owner => Some(h.pause_owner_operations_and_wait().await?),
                _ => None,
            };
            event_log.sync().await.map_err(io_err)?;
            layout::write_buffered(&part_dir, false)?;
            self.buffered_queues.remove(&key);
        }
        set_log_expects_synced(
            &event_log,
            self.keratin_cfg_msg.expect_every_write_synced
                || self.event_log_expects_synced(PartitionKind::Queue, topic, part, group),
        )
        .await
    }

    /// Load the stored declaration of a queue partition whose log is opening.
    pub(crate) fn load_queue_buffered(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        if layout::read_buffered(&self.partition_dir(topic, part, group))? {
            self.buffered_queues
                .insert(buffered_key(topic, part, group), ());
        }
        Ok(())
    }

    pub(crate) fn forget_queue_buffered(&self, topic: &str, part: u32, group: Option<&str>) {
        self.buffered_queues
            .remove(&buffered_key(topic, part, group));
    }

    /// Whether a queue partition is declared buffered. Known once its log has
    /// opened.
    pub fn queue_is_buffered(&self, topic: &str, part: u32, group: Option<&str>) -> bool {
        !self.buffered_queues.is_empty()
            && self
                .buffered_queues
                .contains_key(&buffered_key(topic, part, group))
    }

    pub(crate) fn handle_is_buffered(&self, qh: &QueueHandleInner) -> bool {
        self.queue_is_buffered(qh.topic(), qh.partition(), qh.group())
    }

    /// The durability for an owner write that would otherwise use `default`.
    pub(crate) fn queue_write_durability(
        &self,
        qh: &QueueHandleInner,
        default: KDurability,
    ) -> KDurability {
        if self.handle_is_buffered(qh) {
            default.min(KDurability::AfterFlush)
        } else {
            default
        }
    }

    /// Default durability of queue publishes and acks.
    pub(crate) fn queue_publish_durability(&self, qh: &QueueHandleInner) -> KDurability {
        self.queue_write_durability(
            qh,
            self.keratin_cfg_msg
                .default_durability
                .max(self.keratin_cfg_event.default_durability),
        )
    }

    /// Default durability of nacks, releases and dead-letter records.
    pub(crate) fn queue_settle_durability(&self, qh: &QueueHandleInner) -> KDurability {
        self.queue_write_durability(qh, self.keratin_cfg_event.default_durability)
    }
}

fn buffered_key(topic: &str, part: u32, group: Option<&str>) -> (Box<str>, u32, Option<Box<str>>) {
    (
        Box::from(topic),
        part,
        normalize_group(group).map(Box::from),
    )
}

#[cfg(test)]
#[path = "buffered_queue_tests.rs"]
mod tests;
