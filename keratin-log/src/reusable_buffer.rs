//! Reusable staging allocations with caller-driven maintenance.
//!
//! Capacity is measured in elements. Growth follows `Vec`; retention is a soft
//! policy, never a bound on accepted data. No timer, task or allocator is owned
//! here. Callers include `maintenance_deadline` in their existing wait loop.

use std::{
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

/// Empty-buffer resizing is allocator-dependent. Reallocation can retain mapped
/// pages; replacement avoids copying unused bytes but may incur fresh page faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyBufferResize {
    Reallocate,
    Replace,
}

#[derive(Debug)]
struct Retention {
    minimum: usize,
    interval: Duration,
    release_after: Duration,
    deadline: Option<Instant>,
    last_use: Option<Instant>,
    peak: usize,
    empty_resize: EmptyBufferResize,
}

/// A reusable vector that can shed unused capacity when it is empty.
///
/// Use `edit` to fill a batch and `clear` after consuming it. Retention observes
/// length at edit boundaries, so clear completed batches through this wrapper,
/// rather than inside an edit. All times must use the same monotonic clock.
#[derive(Debug)]
pub struct ReusableBuffer<T> {
    data: Vec<T>,
    retention: Option<Retention>,
}

impl<T> ReusableBuffer<T> {
    /// Eager allocation with ordinary `Vec` lifetime retention.
    pub fn retained(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            retention: None,
        }
    }

    /// Lazy allocation, at most one halving per interval, and full idle release.
    ///
    /// `minimum` is the floor for reservations and decay, not a maximum. Decay
    /// keeps at least twice the largest observed batch since the previous check.
    /// A busy buffer is never shrunk. Idle release only drops an empty buffer.
    /// Panics for a zero minimum/interval or a release delay below the interval.
    pub fn adaptive(minimum: usize, interval: Duration, release_after: Duration) -> Self {
        assert!(minimum > 0);
        assert!(!interval.is_zero());
        assert!(release_after >= interval);
        if std::mem::size_of::<T>() == 0 {
            return Self::retained(0);
        }
        Self {
            data: Vec::new(),
            retention: Some(Retention {
                minimum,
                interval,
                release_after,
                deadline: None,
                last_use: None,
                peak: 0,
                empty_resize: EmptyBufferResize::Reallocate,
            }),
        }
    }

    /// Select how adaptive buffers resize while empty. Defaults to ordinary
    /// Vec reallocation; select replacement only after testing your allocator.
    /// Retained buffers keep their ordinary Vec behavior.
    pub fn with_empty_resize(mut self, strategy: EmptyBufferResize) -> Self {
        if let Some(r) = &mut self.retention {
            r.empty_resize = strategy;
        }
        self
    }

    /// Reserve additional elements immediately, including the adaptive floor.
    pub fn reserve(&mut self, additional: usize, now: Instant) {
        if additional == 0 {
            return;
        }
        let minimum = self.retention.as_ref().map_or(0, |r| r.minimum);
        let additional = additional.max(minimum.saturating_sub(self.data.len()));
        if self
            .retention
            .as_ref()
            .is_some_and(|r| r.empty_resize == EmptyBufferResize::Replace)
            && self.data.is_empty()
            && additional > self.data.capacity()
        {
            let capacity = additional.max(self.data.capacity().saturating_mul(2));
            // Vec reallocation may copy the old capacity even when len is zero.
            // An empty batch has no elements to preserve; release it first.
            self.data = Vec::new();
            self.data = Vec::with_capacity(capacity);
        } else {
            self.data.reserve(additional);
        }
        // A reservation without a subsequent edit must also eventually expire.
        if let Some(r) = &mut self.retention
            && r.deadline.is_none()
        {
            r.last_use = Some(now);
            r.deadline = now.checked_add(r.interval);
        }
    }

    /// Borrow the vector for one batch. Retention is recorded on guard drop,
    /// including early error returns. `now` allows deterministic policy tests.
    pub fn edit(&mut self, now: Instant) -> BufferEdit<'_, T> {
        let starting_len = self.data.len();
        BufferEdit {
            buffer: self,
            now,
            starting_len,
        }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.data
    }
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn maintenance_deadline(&self) -> Option<Instant> {
        self.retention.as_ref().and_then(|r| r.deadline)
    }

    /// Perform due maintenance. Nonempty data, including unflushed staging,
    /// always stays intact. Late calls perform one decay step, not a catch-up
    /// series of allocations. Returns whether capacity was reduced.
    pub fn maintain(&mut self, now: Instant) -> bool {
        let Some(r) = &mut self.retention else {
            return false;
        };
        let Some(deadline) = r.deadline else {
            return false;
        };
        if now < deadline {
            return false;
        }
        let before = self.data.capacity();
        if self.data.is_empty() {
            if r.last_use
                .is_some_and(|last| now.saturating_duration_since(last) >= r.release_after)
            {
                // Dropping avoids depending on allocator-specific shrink-to-zero.
                self.data = Vec::new();
            } else {
                let target = (before / 2).max(r.minimum).max(r.peak.saturating_mul(2));
                if target < before {
                    match r.empty_resize {
                        EmptyBufferResize::Replace => {
                            self.data = Vec::new();
                            self.data = Vec::with_capacity(target);
                        }
                        EmptyBufferResize::Reallocate => self.data.shrink_to(target),
                    }
                }
            }
        }
        r.peak = self.data.len();
        r.deadline = if self.data.capacity() == 0 {
            None
        } else {
            now.checked_add(r.interval)
        };
        self.data.capacity() < before
    }
}

impl<T> Deref for ReusableBuffer<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

/// Scoped vector access that records the batch's retention demand on return.
pub struct BufferEdit<'a, T> {
    buffer: &'a mut ReusableBuffer<T>,
    now: Instant,
    starting_len: usize,
}

impl<T> Deref for BufferEdit<'_, T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Vec<T> {
        &self.buffer.data
    }
}

impl<T> DerefMut for BufferEdit<'_, T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.buffer.data
    }
}

impl<T> Drop for BufferEdit<'_, T> {
    fn drop(&mut self) {
        if let Some(r) = &mut self.buffer.retention {
            let used = self.starting_len.max(self.buffer.data.len());
            r.peak = r.peak.max(used);
            if used > 0 || r.last_use.is_none() && self.buffer.data.capacity() > 0 {
                r.last_use = Some(self.now);
            }
            if r.deadline.is_none() && self.buffer.data.capacity() > 0 {
                r.deadline = self.now.checked_add(r.interval);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const INTERVAL: Duration = Duration::from_secs(10);
    fn buffer() -> ReusableBuffer<u8> {
        ReusableBuffer::adaptive(64, INTERVAL, Duration::from_secs(60))
    }
    fn fill(b: &mut ReusableBuffer<u8>, now: Instant, bytes: usize) {
        b.edit(now).resize(bytes, 42);
    }

    #[test]
    fn lazy_then_grows_to_fit_without_a_hard_ceiling() {
        let mut b = buffer();
        assert_eq!(b.capacity(), 0);
        assert_eq!(b.maintenance_deadline(), None);
        b.reserve(1, Instant::now());
        assert!(b.capacity() >= 64);
        fill(&mut b, Instant::now(), 32 * 1024 * 1024);
        assert_eq!(b.len(), 32 * 1024 * 1024);
        assert!(b.iter().all(|v| *v == 42));
    }

    #[test]
    fn repeated_large_batches_keep_allocation_and_low_demand_decays() {
        let start = Instant::now();
        let mut b = buffer();
        for second in 0..30 {
            fill(&mut b, start + Duration::from_secs(second), 4096);
            b.clear();
            assert!(!b.maintain(start + Duration::from_secs(second)));
        }
        let large = b.capacity();
        // Last high demand is retained across the next maintenance boundary.
        fill(&mut b, start + Duration::from_secs(30), 8);
        b.clear();
        assert!(!b.maintain(start + Duration::from_secs(30)));
        for second in [40, 50, 60, 70, 80, 90, 100] {
            fill(&mut b, start + Duration::from_secs(second), 8);
            b.clear();
            b.maintain(start + Duration::from_secs(second));
        }
        assert!(b.capacity() < large);
        assert_eq!(b.capacity(), 64);
    }

    #[test]
    fn unflushed_data_survives_shrink_and_idle_release_deadlines() {
        let start = Instant::now();
        let mut b = buffer();
        fill(&mut b, start, 4096);
        let capacity = b.capacity();
        assert!(!b.maintain(start + Duration::from_secs(120)));
        assert_eq!(b.capacity(), capacity);
        assert_eq!(b.as_slice(), vec![42; 4096]);
        b.clear();
        assert!(b.maintain(start + Duration::from_secs(130)));
        assert_eq!(b.capacity(), 0);
        assert_eq!(b.maintenance_deadline(), None);
        fill(&mut b, start + Duration::from_secs(131), 8192);
        assert_eq!(b.len(), 8192);
        assert!(b.maintenance_deadline().is_some());
    }

    #[test]
    fn late_maintenance_does_only_one_halving_and_recent_burst_resets_demand() {
        let start = Instant::now();
        let mut b = buffer();
        fill(&mut b, start, 4096);
        b.clear();
        assert!(!b.maintain(start + INTERVAL));
        let before = b.capacity();
        fill(&mut b, start + Duration::from_secs(35), 1);
        b.clear();
        assert!(b.maintain(start + Duration::from_secs(35)));
        assert_eq!(b.capacity(), before / 2);
        assert!(!b.maintain(start + Duration::from_secs(35)));
        fill(&mut b, start + Duration::from_secs(44), 4096);
        b.clear();
        assert!(!b.maintain(start + Duration::from_secs(45)));
    }

    #[test]
    fn error_return_still_records_partially_encoded_batch() {
        let start = Instant::now();
        let mut b = buffer();
        let failed = (|| -> Result<(), ()> {
            let mut edit = b.edit(start);
            edit.extend_from_slice(&[1, 2, 3]);
            Err(())
        })();
        assert!(failed.is_err());
        assert_eq!(b.as_slice(), &[1, 2, 3]);
        assert!(!b.maintain(start + Duration::from_secs(60)));
        assert_eq!(b.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn retained_mode_never_arms_or_shrinks() {
        let start = Instant::now();
        let mut b = ReusableBuffer::retained(4096);
        fill(&mut b, start, 1);
        b.clear();
        assert!(!b.maintain(start + Duration::from_secs(120)));
        assert_eq!(b.capacity(), 4096);
        assert_eq!(b.maintenance_deadline(), None);
    }

    #[test]
    fn idle_release_stops_maintenance_even_after_tiny_batches() {
        let start = Instant::now();
        let mut b = buffer();
        fill(&mut b, start, 64);
        b.clear();
        assert!(!b.maintain(start + Duration::from_secs(59)));
        // Late checks reschedule from now; release occurs on the next due check.
        assert!(b.maintain(start + Duration::from_secs(69)));
        assert_eq!(b.capacity(), 0);
        assert_eq!(b.maintenance_deadline(), None);
    }

    #[test]
    fn unused_reservation_is_released_and_zero_sized_elements_need_no_timer() {
        let start = Instant::now();
        let mut b = buffer();
        b.reserve(1024, start);
        assert_eq!(b.len(), 0);
        assert!(b.maintenance_deadline().is_some());
        assert!(b.maintain(start + Duration::from_secs(60)));
        assert_eq!(b.capacity(), 0);
        let mut zst = ReusableBuffer::<()>::adaptive(64, INTERVAL, INTERVAL);
        zst.edit(start).resize(100, ());
        zst.clear();
        assert_eq!(zst.maintenance_deadline(), None);
    }

    #[test]
    fn burst_just_before_deadline_keeps_headroom_for_next_interval() {
        let start = Instant::now();
        let mut b = buffer();
        fill(&mut b, start, 4096);
        b.clear();
        b.maintain(start + INTERVAL);
        fill(&mut b, start + Duration::from_secs(19), 4096);
        b.clear();
        assert!(!b.maintain(start + Duration::from_secs(20)));
        assert!(!b.maintain(start + Duration::from_secs(29)));
        assert!(b.maintain(start + Duration::from_secs(30)));
        assert_eq!(b.capacity(), 2048);
    }

    #[test]
    fn growth_preserves_live_elements_and_empty_growth_keeps_geometric_headroom() {
        for strategy in [EmptyBufferResize::Reallocate, EmptyBufferResize::Replace] {
            let start = Instant::now();
            let mut b = buffer().with_empty_resize(strategy);
            b.reserve(64, start);
            b.edit(start).extend_from_slice(&[1, 2, 3]);
            b.reserve(4096, start);
            assert_eq!(b.as_slice(), &[1, 2, 3]);
            let capacity = b.capacity();
            b.clear();
            b.reserve(capacity + 1, start);
            assert!(b.is_empty());
            assert!(b.capacity() >= capacity * 2);
            b.edit(start).extend_from_slice(&[4, 5, 6]);
            assert_eq!(b.as_slice(), &[4, 5, 6]);
        }
    }
}
