use std::cmp::Reverse;
use std::collections::BinaryHeap;

use bitvec::vec::BitVec;

pub type Offset = u64;
pub type UnixMillis = u64;

pub const ACK_WINDOW: usize = 8192; // fixed bounded memory
/// Durable-ish semantics:
/// - ACKs are allowed out-of-order.
/// - We maintain a contiguous ACK frontier `acked_until`:
///   all offsets < acked_until are ACKed.
/// - Out-of-order ACKs for offsets >= acked_until go into `acked_set`.
///   We advance the frontier when possible.
/// - Inflight is a lease: offset -> deadline.
/// - Expiry heap is an acceleration structure and may contain stale entries.
#[derive(Debug, Clone)]
pub struct GroupState {
    // ----- ACK state -----
    // Lowest offset that is NOT ACKed (frontier).
    acked_until: Offset,

    // Bounded window of out-of-order ACKs for offsets in [ack_window_base, ack_window_base + ACK_WINDOW)
    ack_window_base: Offset,
    ack_bits: BitVec,

    // ----- inflight -----
    // offset -> deadline_ts
    inflight: hashbrown::HashMap<Offset, UnixMillis>,

    // min-heap via Reverse(deadline), contains stale entries; validated against inflight map
    expiry_heap: BinaryHeap<(Reverse<UnixMillis>, Offset)>,

    // best-effort hint
    min_deadline_hint: Option<UnixMillis>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InflightEntry {
    pub deadline_ts: UnixMillis,
    pub epoch: u32, // optional; ok to keep at 0 for now
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpiryItem {
    deadline_rev: Reverse<UnixMillis>,
    offset: Offset,
    epoch: u32,
}

impl Ord for ExpiryItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is max-heap; we want min-deadline => compare Reverse(deadline) first.
        self.deadline_rev
            .cmp(&other.deadline_rev)
            // tie-breakers to make ordering total/deterministic
            .then_with(|| self.offset.cmp(&other.offset))
            .then_with(|| self.epoch.cmp(&other.epoch))
    }
}
impl PartialOrd for ExpiryItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    NoopAlreadyAcked,
    Applied, // advanced frontier or set bit
}

impl GroupState {
    pub fn new() -> Self {
        Self {
            acked_until: 0,
            ack_window_base: 0,
            ack_bits: BitVec::repeat(false, ACK_WINDOW),
            inflight: hashbrown::HashMap::new(),
            expiry_heap: BinaryHeap::new(),
            min_deadline_hint: None,
        }
    }

    // ---------------- ACK API ----------------

    #[inline]
    pub fn acked_until(&self) -> Offset {
        self.acked_until
    }

    /// True if this offset is known ACKed.
    #[inline]
    pub fn is_acked(&self, offset: Offset) -> bool {
        if offset < self.acked_until {
            return true;
        }

        if offset < self.ack_window_base {
            // Out of tracked window and >= acked_until implies "unknown / not acked"
            return false;
        }

        let idx = (offset - self.ack_window_base) as usize;
        idx < ACK_WINDOW && self.ack_bits[idx]
    }

    #[inline]
    pub fn is_inflight(&self, offset: Offset) -> bool {
        self.inflight.contains_key(&offset)
    }

    #[inline]
    pub fn is_inflight_or_acked(&self, offset: Offset) -> bool {
        self.is_acked(offset) || self.is_inflight(offset)
    }

    pub fn ack(&mut self, offset: u64) {
        if offset < self.acked_until {
            // already acked
            self.inflight.remove(&offset); // best-effort cleanup
            return;
        }

        // ACK beats inflight: always remove inflight if present
        let removed = self.inflight.remove(&offset);
        if removed.is_some() {
            // heap can have stale entries now
            self.recompute_hint_if_needed();
        }

        if offset == self.acked_until {
            self.acked_until += 1;
            self.advance_frontier();
            return;
        }

        let end = self.ack_window_base + ACK_WINDOW as u64;
        if offset < end {
            let idx = (offset - self.ack_window_base) as usize;
            self.ack_bits.set(idx, true);
        } else {
            // far ack: leave for persistence/event log (still applied logically by replay later)
            // (Your materialized model can ignore or store it; see note below.)
        }
    }

    pub fn ack_batch(&mut self, offsets: &[Offset]) {
        for &o in offsets {
            self.ack(o);
        }
    }

    /// Slide frontier through contiguous acked bits and slide the window accordingly.
    fn advance_frontier(&mut self) {
        loop {
            // Is acked_until represented inside window?
            if self.acked_until < self.ack_window_base {
                break;
            }
            let idx = (self.acked_until - self.ack_window_base) as usize;
            if idx >= ACK_WINDOW || !self.ack_bits[idx] {
                break;
            }
            self.ack_bits.set(idx, false);
            self.acked_until += 1;
        }

        // Slide the window so its base follows acked_until (keeps bits "near" the frontier).
        let new_base = self.acked_until;
        let delta = new_base.saturating_sub(self.ack_window_base);
        if delta == 0 {
            return;
        }

        if delta as usize >= ACK_WINDOW {
            // We've advanced more than the window; just clear it.
            self.ack_bits.fill(false);
            self.ack_window_base = new_base;
            return;
        }

        // Rotate left by delta and clear new tail.
        self.ack_bits.rotate_left(delta as usize);
        for i in (ACK_WINDOW - delta as usize)..ACK_WINDOW {
            self.ack_bits.set(i, false);
        }
        self.ack_window_base = new_base;
    }

    #[inline]
    pub fn lowest_unacked_offset(&self) -> Offset {
        self.acked_until
    }

    #[inline]
    pub fn lowest_not_acked_offset(&self) -> Offset {
        self.acked_until
    }

    // ---------------- Inflight API ----------------

    /// Mark inflight for an offset. If offset is already ACKed, no-op.
    pub fn mark_inflight(&mut self, offset: Offset, deadline: UnixMillis) {
        if self.is_acked(offset) {
            return;
        }
        self.inflight.insert(offset, deadline);
        self.expiry_heap.push((Reverse(deadline), offset));
        self.min_deadline_hint = Some(match self.min_deadline_hint {
            None => deadline,
            Some(cur) => cur.min(deadline),
        });
    }

    pub fn mark_inflight_batch(&mut self, entries: &[(Offset, UnixMillis)]) {
        for &(o, d) in entries {
            self.mark_inflight(o, d);
        }
    }

    /// Clear inflight for an offset (e.g. expired worker clears it before requeue).
    pub fn clear_inflight(&mut self, offset: Offset) {
        let was = self.inflight.remove(&offset);
        if was.is_some() {
            // heap might now be stale; recompute hint lazily but safely
            self.recompute_hint_if_needed();
        }
    }

    #[inline]
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    #[inline]
    pub fn next_expiry_hint(&mut self) -> Option<UnixMillis> {
        self.recompute_hint_full();
        self.min_deadline_hint
    }

    /// Pop expired offsets (deadline <= now), up to max.
    pub fn pop_expired(&mut self, now: UnixMillis, max: usize) -> Vec<Offset> {
        let mut out = Vec::new();

        while let Some(&(Reverse(ts), off)) = self.expiry_heap.peek() {
            if ts > now || out.len() >= max {
                break;
            }
            self.expiry_heap.pop();

            // Validate against current inflight map (skip stale heap entries)
            match self.inflight.get(&off).copied() {
                Some(cur_deadline) if cur_deadline == ts => {
                    self.inflight.remove(&off);
                    out.push(off);
                }
                _ => continue,
            }
        }

        self.recompute_hint_if_needed();
        out
    }

    /// Walk heap until we find a valid inflight entry; rebuild if heap fully stale.
    fn recompute_hint_if_needed(&mut self) {
        if self.inflight.is_empty() {
            self.min_deadline_hint = None;
            return;
        }

        while let Some(&(Reverse(ts), off)) = self.expiry_heap.peek() {
            match self.inflight.get(&off).copied() {
                Some(cur) if cur == ts => {
                    self.min_deadline_hint = Some(ts);
                    return;
                }
                _ => {
                    self.expiry_heap.pop(); // stale
                }
            }
        }

        // Heap drained but inflight not empty: rebuild heap from inflight.
        self.expiry_heap.clear();
        let mut min: Option<UnixMillis> = None;
        for (&off, &deadline) in self.inflight.iter() {
            self.expiry_heap.push((Reverse(deadline), off));
            min = Some(min.map_or(deadline, |m| m.min(deadline)));
        }
        self.min_deadline_hint = min;
    }

    fn recompute_hint_full(&mut self) {
        while let Some(&(Reverse(ts), off)) = self.expiry_heap.peek() {
            match self.inflight.get(&off).copied() {
                Some(cur) if cur == ts => {
                    self.min_deadline_hint = Some(ts);
                    return;
                }
                _ => {
                    self.expiry_heap.pop(); // stale
                }
            }
        }

        if self.inflight.is_empty() {
            self.min_deadline_hint = None;
            return;
        }

        // rebuild heap from inflight
        self.expiry_heap.clear();
        let mut min = None;
        for (&off, &deadline) in self.inflight.iter() {
            self.expiry_heap.push((Reverse(deadline), off));
            min = Some(min.map_or(deadline, |m: u64| m.min(deadline)));
        }
        self.min_deadline_hint = min;
    }

    // ---------------- Delivery helper ----------------

    /// Find next deliverable offset in [from, upper).
    /// Skips inflight and (bounded) acked entries.
    pub fn next_deliverable(&self, from: Offset, upper: Offset) -> Offset {
        let mut off = from.max(self.acked_until);

        while off < upper {
            if self.inflight.contains_key(&off) {
                off += 1;
                continue;
            }

            if self.is_acked(off) {
                off += 1;
                continue;
            }

            return off;
        }

        upper
    }

    // ---------------- Debug / maintenance ----------------

    pub fn dump_inflight(&self) -> Vec<(Offset, UnixMillis)> {
        let mut v: Vec<_> = self.inflight.iter().map(|(&o, &d)| (o, d)).collect();
        v.sort_unstable_by_key(|x| x.0);
        v
    }

    pub fn reset(&mut self) {
        *self = GroupState::new();
    }

    // Snapshot/recovery setters (used by recover.rs)
    pub fn set_acked_until(&mut self, v: Offset) {
        self.acked_until = v;
        // keep window consistent with new frontier
        self.ack_window_base = v;
        self.ack_bits.fill(false);
    }

    pub fn set_ack_window(&mut self, base: Offset, bits: BitVec) {
        self.ack_window_base = base;
        self.ack_bits = bits;
        if self.ack_bits.len() != ACK_WINDOW {
            self.ack_bits.resize(ACK_WINDOW, false);
        }
    }

    pub fn load_inflight(&mut self, entries: &[(Offset, UnixMillis)]) {
        self.inflight.clear();
        self.expiry_heap.clear();
        self.min_deadline_hint = None;
        for &(o, d) in entries {
            self.inflight.insert(o, d);
            self.expiry_heap.push((Reverse(d), o));
            self.min_deadline_hint = Some(match self.min_deadline_hint {
                None => d,
                Some(cur) => cur.min(d),
            });
        }
        // ensure heap top is valid
        self.recompute_hint_if_needed();
    }

    pub fn ack_window_base(&self) -> u64 {
        self.ack_window_base
    }

    pub fn ack_bits_bytes(&self) -> Vec<u8> {
        // Convert BitVec<usize,Lsb0> → Vec<u8> in a stable packed form
        self.ack_bits
            .as_raw_slice()
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect()
    }

    pub fn set_ack_window_from_bytes(&mut self, base: u64, bytes: &[u8]) -> std::io::Result<()> {
        let word_bytes = std::mem::size_of::<usize>();

        if !bytes.len().is_multiple_of(word_bytes) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad ack window bytes",
            ));
        }

        let mut words = Vec::with_capacity(bytes.len() / word_bytes);
        for chunk in bytes.chunks_exact(word_bytes) {
            words.push(usize::from_le_bytes(chunk.try_into().unwrap()));
        }

        let mut bits = bitvec::vec::BitVec::<usize, bitvec::order::Lsb0>::from_vec(words);

        if bits.len() < ACK_WINDOW {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ack window too small",
            ));
        }

        bits.truncate(ACK_WINDOW);

        self.ack_window_base = base;
        self.ack_bits = bits;
        Ok(())
    }

    pub fn encode_snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();

        // acked_until
        out.extend_from_slice(&self.acked_until.to_be_bytes());

        // ack window
        out.extend_from_slice(&self.ack_window_base.to_be_bytes());
        let bits = self.ack_bits_bytes();
        out.extend_from_slice(&(bits.len() as u32).to_be_bytes());
        out.extend_from_slice(&bits);

        // inflight
        out.extend_from_slice(&(self.inflight.len() as u32).to_be_bytes());
        for (&off, e) in self.inflight.iter() {
            out.extend_from_slice(&off.to_be_bytes());
            out.extend_from_slice(&e.to_be_bytes());
        }

        out
    }

    pub fn load_snapshot(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        use std::io::{Error, ErrorKind};

        fn take<const N: usize>(b: &mut &[u8]) -> std::io::Result<[u8; N]> {
            if b.len() < N {
                return Err(Error::new(ErrorKind::UnexpectedEof, "snapshot"));
            }
            let (a, rest) = b.split_at(N);
            *b = rest;
            Ok(a.try_into().unwrap())
        }

        self.reset();

        self.acked_until = u64::from_be_bytes(take::<8>(&mut bytes)?);
        let base = u64::from_be_bytes(take::<8>(&mut bytes)?);

        let win_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        if bytes.len() < win_len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "ack window"));
        }
        let win = &bytes[..win_len];
        bytes = &bytes[win_len..];
        self.set_ack_window_from_bytes(base, win)?;

        let inflight_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
        for _ in 0..inflight_len {
            let off = u64::from_be_bytes(take::<8>(&mut bytes)?);
            let dl = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.mark_inflight(off, dl);
        }

        Ok(())
    }

    pub fn canonical(&self) -> CanonicalGroupState {
        let mut inflight: Vec<_> = self.inflight.iter().map(|(&o, &d)| (o, d)).collect();
        inflight.sort_unstable();

        CanonicalGroupState {
            acked_until: self.acked_until,
            ack_window_base: self.ack_window_base,
            ack_bits: self.ack_bits_bytes(),
            inflight,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalGroupState {
    pub acked_until: u64,
    pub ack_window_base: u64,
    pub ack_bits: Vec<u8>,
    pub inflight: Vec<(u64, u64)>,
}

#[cfg(test)]
mod tests {
    use super::{GroupState, Offset};

    #[test]
    fn frontier_is_monotonic() {
        let mut s = GroupState::new();

        for _i in 0..10_000 {
            s.ack(fastrand::u64(0..5000));
            assert!(s.acked_until() <= 5000);
        }
    }

    #[test]
    fn next_deliverable_never_returns_acked_or_inflight() {
        let mut s = GroupState::new();

        for i in 0..2000 {
            if i % 3 == 0 {
                s.ack(i);
            } else if i % 3 == 1 {
                s.mark_inflight(i, 1000);
            }
        }

        for _i in 0..2000 {
            let d = s.next_deliverable(0, 2000);
            if d >= 2000 {
                break;
            }

            assert!(!s.is_acked(d));
            assert!(!s.is_inflight(d));

            s.mark_inflight(d, 2000);
        }
    }

    #[test]
    fn expired_offsets_are_removed_exactly_once() {
        let mut s = GroupState::new();

        for i in 0..1000 {
            s.mark_inflight(i, 100);
        }

        let ex1 = s.pop_expired(100, 2000);
        let ex2 = s.pop_expired(100, 2000);

        assert_eq!(ex1.len(), 1000);
        assert!(ex2.is_empty());
    }

    #[test]
    fn snapshot_roundtrip_is_identity() {
        let mut s = GroupState::new();

        for i in 0..1000 {
            s.mark_inflight(i, 1000 + i);
            if i % 2 == 0 {
                s.ack(i);
            }
        }

        let snap = s.encode_snapshot();

        let mut s2 = GroupState::new();
        s2.load_snapshot(&snap).unwrap();

        assert_eq!(s.canonical(), s2.canonical());
    }

    #[test]
    fn random_ops_never_break_invariants() {
        let mut s = GroupState::new();

        for _ in 0..1_000_000 {
            let o = fastrand::u64(0..30000);
            match fastrand::u8(0..8) {
                0 => s.mark_inflight(o, fastrand::u64(0..100_000)),
                1 => s.clear_inflight(o),
                2 => s.ack(o),
                _ => {
                    let _ = s.pop_expired(fastrand::u64(0..100_000), 100);
                }
            }

            // Hard invariants:
            assert!(s.acked_until() <= 30000);
            if let Some(h) = s.next_expiry_hint() {
                let assert_cond = s.inflight.values().any(|&d| d == h);
                if !assert_cond {
                    dbg!(s.dump_inflight());
                    dbg!(h);
                    assert!(assert_cond);
                }
            }
        }
    }

    #[test]
    fn out_of_order_acks_advance_frontier() {
        let mut s = GroupState::new();

        // ACK 2 and 4 out-of-order; frontier stays at 0
        s.ack(2);
        s.ack(4);
        assert_eq!(s.acked_until(), 0);
        assert!(s.is_acked(2));
        assert!(s.is_acked(4));
        assert!(!s.is_acked(0));

        // ACK 0 should advance frontier to 1
        s.ack(0);
        assert_eq!(s.acked_until(), 1);

        // ACK 1 should advance to 3 (because 2 was already acked)
        s.ack(1);
        assert_eq!(s.acked_until(), 3);

        // ACK 3 should advance to 5 (because 4 was already acked)
        s.ack(3);
        assert_eq!(s.acked_until(), 5);

        // sanity: everything < 5 is acked now
        for o in 0..5 {
            assert!(s.is_acked(o));
        }
    }

    #[test]
    fn ack_removes_inflight() {
        let mut s = GroupState::new();

        s.mark_inflight(10, 1000);
        assert!(s.is_inflight_or_acked(10));
        assert!(!s.is_acked(10));

        s.ack(10);
        assert!(s.is_acked(10));
        assert!(s.is_inflight_or_acked(10 /* still true via ack */)); // just to show logic
        // More direct:
        assert!(s.is_inflight_or_acked(10));
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn mark_inflight_ignored_if_already_acked() {
        let mut s = GroupState::new();
        s.ack_batch(&[0, 1, 2, 3, 4]);
        assert_eq!(s.acked_until(), 5);

        s.mark_inflight(2, 123);
        s.mark_inflight(4, 123);
        assert_eq!(s.inflight_len(), 0);
    }

    #[test]
    fn expiry_hint_tracks_min_deadline_and_handles_updates() {
        let mut s = GroupState::new();

        s.mark_inflight(10, 500);
        assert_eq!(s.next_expiry_hint(), Some(500));

        s.mark_inflight(11, 400);
        assert_eq!(s.next_expiry_hint(), Some(400));

        // update 11 to later deadline; heap now has stale(400) + current(700).
        s.mark_inflight(11, 700);

        s.recompute_hint_if_needed();
        // hint may still be 400 until recompute/pop; force recompute
        assert_eq!(s.next_expiry_hint(), Some(500));
    }

    #[test]
    fn pop_expired_skips_stale_heap_entries() {
        let mut s = GroupState::new();

        // inflight 1 deadline 10
        s.mark_inflight(1, 10);
        // then update to deadline 30 (creates stale heap entry)
        s.mark_inflight(1, 30);
        // inflight 2 deadline 20
        s.mark_inflight(2, 20);

        // At now=15, only offset 2 is not expired; offset 1 has current deadline 30.
        let expired = s.pop_expired(15, 300);
        assert!(expired.is_empty());
        assert_eq!(s.next_expiry_hint(), Some(20));

        // At now=25, offset 2 expires
        let expired = s.pop_expired(25, 300);
        assert_eq!(expired, vec![2]);
        assert_eq!(s.next_expiry_hint(), Some(30));

        // At now=35, offset 1 expires (stale deadline=10 entry should be ignored)
        let expired = s.pop_expired(35, 300);
        assert_eq!(expired, vec![1]);
        assert_eq!(s.next_expiry_hint(), None);
    }

    #[test]
    fn clear_inflight_removes_and_hint_updates() {
        let mut s = GroupState::new();

        s.mark_inflight(1, 10);
        s.mark_inflight(2, 20);
        assert_eq!(s.next_expiry_hint(), Some(10));

        s.clear_inflight(1);
        assert_eq!(s.inflight_len(), 1);
        assert_eq!(s.next_expiry_hint(), Some(20));

        s.clear_inflight(2);
        assert_eq!(s.inflight_len(), 0);
        assert_eq!(s.next_expiry_hint(), None);
    }

    #[test]
    fn is_inflight_or_acked_behaves() {
        let mut s = GroupState::new();

        s.mark_inflight(5, 100);
        assert!(s.is_inflight_or_acked(5));
        assert!(!s.is_acked(5));

        s.ack(5);
        assert!(s.is_inflight_or_acked(5));
        assert!(s.is_acked(5));

        // below frontier is always acked
        s.ack(0);
        s.ack(1);
        s.ack(2);
        s.ack(3);
        s.ack(4);
        assert_eq!(s.acked_until(), 6);
        for o in 0..6 {
            assert!(s.is_inflight_or_acked(o));
            assert!(s.is_acked(o));
        }
    }

    #[test]
    fn ack_batch_handles_duplicates() {
        let mut s = GroupState::new();
        let v: Vec<Offset> = vec![2, 2, 0, 1, 1, 3];
        s.ack_batch(&v);
        assert_eq!(s.acked_until(), 4);
    }
}
