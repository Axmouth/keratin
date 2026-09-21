//! Canonical logical queue state for offline recovery evidence. This is not a
//! snapshot format or a live actor's applied-boundary certificate.

use super::*;

struct Digest(blake3::Hasher);
impl Digest {
    fn number(&mut self, n: u64) {
        self.0.update(&n.to_be_bytes());
    }
    fn text(&mut self, s: &str) {
        self.number(s.len() as u64);
        self.0.update(s.as_bytes());
    }
    fn optional_text(&mut self, s: Option<&str>) {
        self.number(u64::from(s.is_some()));
        if let Some(s) = s {
            self.text(s);
        }
    }
    fn optional_number(&mut self, n: Option<u64>) {
        self.number(u64::from(n.is_some()));
        if let Some(n) = n {
            self.number(n);
        }
    }
    fn ranges(&mut self, ranges: &RangeSet<u64>) {
        self.number(ranges.iter().count() as u64);
        for r in ranges.iter() {
            self.number(r.start);
            self.number(r.end);
        }
    }
    fn heap(&mut self, heap: &BinaryHeap<(Reverse<u64>, u64)>) {
        let mut entries: Vec<_> = heap.iter().map(|(Reverse(d), o)| (*d, *o)).collect();
        entries.sort_unstable();
        self.number(entries.len() as u64);
        // Multiplicity matters: delayed work is consumed one entry at a time.
        for (deadline, off) in entries {
            self.number(deadline);
            self.number(off);
        }
    }
    fn target(&mut self, tp: &str, part: u32, group: Option<&str>) {
        self.text(tp);
        self.number(part as u64);
        self.optional_text(group);
    }
}

impl QueueInternalState {
    pub(crate) fn recovery_resource(&self) -> (String, u32) {
        (self.topic.clone(), self.partition)
    }

    /// Projection for a new owner's delivery attempts. Leases are local to an
    /// owner and ordinary delivery does not write MarkInflight to the event log.
    /// Retry counts, delayed work, TTL and DLQ state remain significant.
    fn release_recovery_leases(&mut self) {
        for &off in self.inflight.keys() {
            if !self.is_settled(off) && !self.is_pending_dlq(off) {
                self.ready.insert(off..off + 1);
            }
        }
        self.inflight.clear();
        self.expiry_heap.clear();
        self.min_deadline_hint = None;
    }

    pub(crate) fn recovery_lease_normalized_digest(&self) -> [u8; 32] {
        let mut state = self.clone();
        state.release_recovery_leases();
        state.recovery_state_digest()
    }

    /// Stable bytes for a selected recovery plan. Only local delivery leases and
    /// capture time are reset; retry, delayed, TTL, terminal and DLQ state remain.
    pub(crate) fn into_recovery_snapshot(mut self, event_next: u64) -> Vec<u8> {
        self.release_recovery_leases();
        self.last_snapshot_timestamp = 0;
        let mut delayed_enqueues: Vec<_> = self
            .delayed_enqueue_heap
            .iter()
            .map(|(Reverse(d), off)| (*d, *off))
            .collect();
        let mut delayed_retries: Vec<_> = self
            .delayed_retry_heap
            .iter()
            .map(|(Reverse(d), off)| (*d, *off))
            .collect();
        let mut retries: Vec<_> = self.retries.iter().map(|(off, n)| (*off, *n)).collect();
        delayed_enqueues.sort_unstable();
        delayed_retries.sort_unstable();
        retries.sort_unstable();
        self.encode_snapshot_with_entries(
            event_next.saturating_sub(1),
            delayed_enqueues.into_iter(),
            delayed_retries.into_iter(),
            retries.into_iter(),
        )
    }

    pub(crate) fn recovery_live_ranges(&self) -> RangeSet<u64> {
        let mut live = self.ready.clone();
        for off in self.inflight.keys().chain(self.pending_dlq.keys()).copied()
            .chain(self.delayed_enqueue_heap.iter().map(|(_, off)| *off))
            .chain(self.delayed_retry_heap.iter().map(|(_, off)| *off))
        {
            if !self.is_settled(off) { live.insert(off..off + 1); }
        }
        live
    }

    pub(crate) fn recovery_state_digest(&self) -> [u8; 32] {
        let mut h = Digest(blake3::Hasher::new());
        h.0.update(b"fibril-queue-state-v1\0");
        h.text(&self.topic);
        h.number(self.partition as u64);
        h.ranges(&self.settled);
        h.number(self.inflight.len() as u64);
        for (&off, &deadline) in &self.inflight {
            h.number(off);
            h.number(deadline);
        }
        h.heap(&self.delayed_enqueue_heap);
        h.heap(&self.delayed_retry_heap);
        let mut retries: Vec<_> = self.retries.iter().collect();
        retries.sort_unstable_by_key(|(off, _)| **off);
        h.number(retries.len() as u64);
        for (&off, &retry) in retries {
            h.number(off);
            h.number(retry as u64);
        }
        h.ranges(&self.ready);
        h.number(self.ttl_deadlines.iter().count() as u64);
        for (range, &deadline) in self.ttl_deadlines.iter() {
            h.number(range.start);
            h.number(range.end);
            h.number(deadline);
        }
        h.number(self.pending_dlq.len() as u64);
        for (&off, target) in &self.pending_dlq {
            h.number(off);
            h.number(u64::from(target.is_some()));
            if let Some(target) = target {
                h.target(&target.tp, target.part, target.group.as_deref());
            }
        }
        match &self.dlq_policy {
            DLQDiscardPolicy::Discard => h.number(0),
            DLQDiscardPolicy::GlobalDQL => h.number(1),
            DLQDiscardPolicy::CustomDQL(t) => {
                h.number(2);
                h.target(&t.tp, t.part, t.group.as_deref());
            }
        }
        h.number(self.dlq_discard_max_retries as u64);
        h.optional_number(self.default_message_ttl_ms);
        // Snapshot timestamp/frontier are envelope metadata. The expiry heap,
        // min-deadline hint and wakeup handle are derivative/local machinery.
        *h.0.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint_fixture() -> QueueInternalState {
        let mut state = QueueInternalState::new("q".into(), 0);
        state.enqueue(0, 2, Some(100));
        state.enqueue(1, 0, None);
        state.mark_inflight(1, 50);
        state.ack(3);
        state.enqueue_delayed(4, 200);
        state.enqueue(5, 1, Some(500));
        state.nack_at(5, true, Some(300));
        state.pending_dlq.insert(
            6,
            Some(ResolvedDlqTarget {
                tp: "dead".into(),
                part: 2,
                group: Some("workers".into()),
            }),
        );
        state.dlq_policy = DLQDiscardPolicy::CustomDQL(CustomDLQ {
            tp: "dead".into(),
            part: 2,
            group: Some("workers".into()),
        });
        state.default_message_ttl_ms = Some(900);
        state
    }

    #[test]
    fn recovery_snapshot_bytes_are_canonical_without_changing_semantic_state() {
        let mut a = QueueInternalState::new("q".into(), 0);
        let mut b = a.clone();
        for off in 0..50 {
            a.enqueue(off, 1 + off as u32, Some(900));
            a.enqueue_delayed(off + 100, 200 + off % 7);
            a.delayed_retry_heap.push((Reverse(300 + off % 5), off));
        }
        for off in (0..50).rev() {
            b.enqueue(off, 1 + off as u32, Some(900));
            b.enqueue_delayed(off + 100, 200 + off % 7);
            b.delayed_retry_heap.push((Reverse(300 + off % 5), off));
        }
        a.mark_inflight(0, 1000);
        b.mark_inflight(1, 2000);
        a.last_snapshot_timestamp = 50;
        b.last_snapshot_timestamp = 99;
        assert_eq!(
            a.recovery_lease_normalized_digest(),
            b.recovery_lease_normalized_digest()
        );
        let expected = a.recovery_lease_normalized_digest();
        let bytes = a.into_recovery_snapshot(80);
        assert_eq!(bytes, b.into_recovery_snapshot(80));
        let mut restored = QueueInternalState::new("q".into(), 0);
        assert_eq!(
            restored
                .load_snapshot(&bytes)
                .unwrap()
                .last_snapshot_event_offset,
            79
        );
        assert_eq!(restored.recovery_state_digest(), expected);
        assert_eq!(restored.last_snapshot_timestamp, 0);
    }

    #[test]
    fn delayed_retry_projection_preserves_deadline_without_a_local_lease() {
        let mut owner = QueueInternalState::new("q".into(), 0);
        owner.enqueue(0, 0, Some(900));
        let mut follower = owner.clone();
        owner.mark_inflight(0, 300);
        for state in [&mut owner, &mut follower] {
            state.nack_at(0, true, Some(500));
            assert!(!state.is_ready(0));
            assert_eq!(state.get_retries(0), 1);
        }
        assert_eq!(
            owner.recovery_state_digest(),
            follower.recovery_state_digest()
        );
        assert_eq!(owner.activate_delayed(499, 10), 0);
        assert_eq!(follower.activate_delayed(499, 10), 0);
        owner.activate_delayed(500, 10);
        follower.activate_delayed(500, 10);
        assert_eq!(
            owner.recovery_state_digest(),
            follower.recovery_state_digest()
        );
        assert!(owner.is_ready(0));
        assert_eq!(owner.get_retries(0), 1);
        assert_eq!(owner.collect_ttl_expired(900, 10), vec![0]);
    }

    #[test]
    fn delayed_activation_is_bounded_and_cannot_resurrect_terminal_state() {
        let mut state = QueueInternalState::new("q".into(), 0);
        for off in 0..4 {
            state.enqueue_delayed(off, 100);
        }
        state.ack(3);
        state.pending_dlq.insert(2, None);
        assert_eq!(state.activate_delayed(100, 2), 2);
        assert_eq!(state.ready.iter().count(), 0);
        assert!(state.has_due_delayed(100));
        assert_eq!(state.activate_delayed(100, 2), 2);
        assert!(state.is_ready(0));
        assert!(state.is_ready(1));
        assert!(!state.is_ready(2));
        assert!(!state.is_ready(3));
        assert!(!state.has_due_delayed(100));
    }

    #[test]
    fn checkpoint_truncation_never_panics_or_partially_replaces_state() {
        let fixture = checkpoint_fixture();
        let bytes = fixture.encode_snapshot(19);
        let mut existing = QueueInternalState::new("q".into(), 0);
        existing.enqueue(90, 7, Some(123));
        existing.last_snapshot_event_offset = 44;
        let original = existing.recovery_state_digest();
        for cut in 0..bytes.len() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                existing.load_snapshot(&bytes[..cut])
            }));
            assert!(result.is_ok(), "snapshot prefix {cut} panicked");
            assert!(result.unwrap().is_err(), "snapshot prefix {cut} accepted");
            assert_eq!(
                existing.recovery_state_digest(),
                original,
                "snapshot prefix {cut} changed live state"
            );
            assert_eq!(existing.last_snapshot_event_offset, 44);
        }
        let meta = existing.load_snapshot(&bytes).unwrap();
        assert_eq!(meta.last_snapshot_event_offset, 19);
        assert_eq!(
            existing.recovery_state_digest(),
            fixture.recovery_state_digest()
        );
    }

    #[test]
    fn checkpoint_malformed_ranges_counts_and_offsets_are_errors() {
        let fixture = checkpoint_fixture();
        let bytes = fixture.encode_snapshot(19);
        // Exercise all field boundaries, including range ends, count fields,
        // DLQ string lengths/tags, terminal offsets and presence tags.
        for at in 0..bytes.len() {
            for byte in [0, 255] {
                let mut malformed = bytes.clone();
                malformed[at] = byte;
                let mut target = QueueInternalState::new("q".into(), 0);
                target.enqueue(90, 3, None);
                let original = target.recovery_state_digest();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    target.load_snapshot(&malformed)
                }));
                assert!(result.is_ok(), "snapshot mutation {at}={byte} panicked");
                if result.unwrap().is_err() {
                    assert_eq!(target.recovery_state_digest(), original);
                }
            }
        }
        let mut overflow = bytes.clone();
        overflow[24..32].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(
            QueueInternalState::new("q".into(), 0)
                .load_snapshot(&overflow)
                .is_err()
        );
        // First settled range is [3,4). Empty/inverted ranges must not silently
        // remove terminal history or reach RangeSet's assertions.
        overflow = bytes;
        overflow[40..48].copy_from_slice(&3u64.to_be_bytes());
        assert!(
            QueueInternalState::new("q".into(), 0)
                .load_snapshot(&overflow)
                .is_err()
        );
    }

    #[test]
    fn recovery_digest_canonicalizes_order_and_excludes_only_local_metadata() {
        let mut a = QueueInternalState::new("q".into(), 3);
        let mut b = a.clone();
        for off in 0..40 {
            a.enqueue(off * 2, 1 + off as u32, Some(50 + off));
            a.enqueue_delayed(off * 2 + 1, 100 + off % 3);
            a.delayed_retry_heap.push((Reverse(400 + off % 7), off));
        }
        for off in (0..40).rev() {
            b.enqueue(off * 2, 1 + off as u32, Some(50 + off));
            b.enqueue_delayed(off * 2 + 1, 100 + off % 3);
            b.delayed_retry_heap.push((Reverse(400 + off % 7), off));
        }
        b.last_snapshot_timestamp = 900;
        b.last_snapshot_event_offset = 456;
        b.expiry_heap.push((Reverse(10), 999));
        b.min_deadline_hint = Some(10);
        assert_eq!(a.recovery_state_digest(), b.recovery_state_digest());
        b.delayed_enqueue_heap.push((Reverse(100), 1));
        assert_ne!(a.recovery_state_digest(), b.recovery_state_digest());
    }

    #[test]
    fn recovery_digest_covers_every_semantic_field() {
        let base = QueueInternalState::new("q".into(), 0);
        let mutations: Vec<Box<dyn Fn(&mut QueueInternalState)>> = vec![
            Box::new(|s| s.topic.push('x')),
            Box::new(|s| s.partition = 1),
            Box::new(|s| s.settled.insert(1..2)),
            Box::new(|s| {
                s.inflight.insert(1, 10);
            }),
            Box::new(|s| s.delayed_enqueue_heap.push((Reverse(10), 1))),
            Box::new(|s| s.delayed_retry_heap.push((Reverse(10), 1))),
            Box::new(|s| {
                s.retries.insert(1, 2);
            }),
            Box::new(|s| s.ready.insert(1..2)),
            Box::new(|s| s.ttl_deadlines.insert(1..2, 10)),
            Box::new(|s| {
                s.pending_dlq.insert(1, None);
            }),
            Box::new(|s| {
                s.dlq_policy = DLQDiscardPolicy::CustomDQL(CustomDLQ {
                    tp: "dlq".into(),
                    part: 2,
                    group: Some("g".into()),
                })
            }),
            Box::new(|s| s.dlq_discard_max_retries += 1),
            Box::new(|s| s.default_message_ttl_ms = Some(100)),
        ];
        for (i, mutate) in mutations.into_iter().enumerate() {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert_ne!(
                base.recovery_state_digest(),
                changed.recovery_state_digest(),
                "field {i}"
            );
        }
        let mut a = base;
        a.pending_dlq.insert(
            1,
            Some(ResolvedDlqTarget {
                tp: "dlq".into(),
                part: 2,
                group: None,
            }),
        );
        let mut b = a.clone();
        b.pending_dlq.get_mut(&1).unwrap().as_mut().unwrap().group = Some("".into());
        assert_ne!(a.recovery_state_digest(), b.recovery_state_digest());
        b = a.clone();
        b.pending_dlq.get_mut(&1).unwrap().as_mut().unwrap().part += 1;
        assert_ne!(a.recovery_state_digest(), b.recovery_state_digest());
    }
}
