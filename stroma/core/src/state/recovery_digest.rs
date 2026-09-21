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
