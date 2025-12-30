use crate::state::{Offset, UnixMillis};

pub trait StromaStore: Send + Sync + 'static {
    // ACK frontier
    fn lowest_unacked_offset(&self, tp: &str, part: u32, group: &str) -> Offset;

    fn is_inflight_or_acked(&self, tp: &str, part: u32, group: &str, off: Offset) -> bool;

    // ACKs
    fn ack_batch(&self, tp: &str, part: u32, group: &str, offs: &[Offset]);

    // inflight leases
    fn mark_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        group: &str,
        entries: &[(Offset, UnixMillis)],
    );

    fn clear_inflight(&self, tp: &str, part: u32, group: &str, off: Offset);

    fn list_expired(&self, now: UnixMillis, max: usize) -> Vec<(String, u32, String, Offset)>;

    fn next_expiry_hint(&self) -> Option<UnixMillis>;
}
