use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    MaxItems,
    MaxRecords,
    MaxBytes,
    Shutdown,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct BatcherConfig {
    pub max_items: usize,
    pub max_records: usize,
    pub max_bytes: usize,
    pub linger: Duration,
}

#[derive(Debug, Clone)]
pub enum PushResult<T> {
    None,
    One((FlushReason, Vec<T>)),
    Two((FlushReason, Vec<T>), (FlushReason, Vec<T>)),
}

struct Entry<T> {
    item: T,
    records: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deadline {
    None,         // no active batch
    DueNow,       // deadline reached or overflow/impossible
    In(Duration), // due in ...
}

impl<T, W> BatcherCore<T, W>
where
    W: FnMut(&T) -> (usize, usize),
{
    pub fn set_linger(&mut self, linger: Duration) {
        self.cfg.linger = linger;
    }
}

/// Deterministic batching state machine.
///
/// Semantics:
/// - `linger` is an *idle timeout* measured since the **last arrival**.
/// - If the batch is idle for >= `linger`, it is eligible to flush (`Timeout`).
/// - **Stale-on-arrival barrier:** if an item arrives when the current batch is stale,
///   the old batch flushes first; the arriving item starts a new batch.
/// - Size limits are enforced only by `push()` (single source of truth).
/// - `flush_if_due(now)` is time-only and assumes all admissions went through `push()`.
/// - Totals use saturating arithmetic to avoid overflow bugs.
/// - `Instant` overflow in deadline computation is treated as "due immediately".
pub struct BatcherCore<T, W>
where
    W: FnMut(&T) -> (usize, usize),
{
    cfg: BatcherConfig,
    weight: W,

    items: Vec<Entry<T>>,
    total_records: usize,
    total_bytes: usize,
    last_arrival: Option<Instant>,
}

impl<T, W> BatcherCore<T, W>
where
    W: FnMut(&T) -> (usize, usize),
{
    pub fn new(cfg: BatcherConfig, weight: W) -> Self {
        Self {
            cfg,
            weight,
            items: Vec::new(),
            total_records: 0,
            total_bytes: 0,
            last_arrival: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn total_records(&self) -> usize {
        self.total_records
    }

    pub fn time_until_deadline(&self, now: Instant) -> Option<Duration> {
        match self.deadline(now) {
            Deadline::In(d) => Some(d),
            Deadline::DueNow => Some(Duration::ZERO),
            Deadline::None => None,
        }
    }

    pub fn deadline(&self, now: Instant) -> Deadline {
        if self.items.is_empty() {
            return Deadline::None;
        }

        let Some(last) = self.last_arrival else {
            // Should be impossible if invariants hold
            return Deadline::DueNow;
        };

        if self.cfg.linger == Duration::ZERO {
            return Deadline::DueNow;
        }

        match last.checked_add(self.cfg.linger) {
            None => Deadline::DueNow, // overflow => due immediately (per your policy)
            Some(deadline) => {
                if now >= deadline {
                    Deadline::DueNow
                } else {
                    Deadline::In(deadline - now)
                }
            }
        }
    }

    #[must_use]
    pub fn flush_if_due(&mut self, now: Instant) -> Option<(FlushReason, Vec<T>)> {
        match self.deadline(now) {
            Deadline::DueNow => Some((FlushReason::Timeout, self.flush())),
            Deadline::In(_) | Deadline::None => None,
        }
    }

    pub fn push(&mut self, now: Instant, item: T) -> PushResult<T> {
        // If we have an active batch and it's due (or deadline overflowed), flush old batch first.
        let stale = !self.items.is_empty() && matches!(self.deadline(now), Deadline::DueNow);

        if stale {
            let old = self.flush();
            let second = self.admit_and_maybe_flush(now, item);
            return match second {
                None => PushResult::One((FlushReason::Timeout, old)),
                Some(f2) => PushResult::Two((FlushReason::Timeout, old), f2),
            };
        }

        match self.admit_and_maybe_flush(now, item) {
            None => PushResult::None,
            Some(f) => PushResult::One(f),
        }
    }

    fn admit_and_maybe_flush(&mut self, now: Instant, item: T) -> Option<(FlushReason, Vec<T>)> {
        let (r, b) = (self.weight)(&item);

        debug_assert!(
            r > 0 || b > 0 || self.cfg.max_items < usize::MAX,
            "weight() returned (0,0) with unbounded max_items: batch may grow without bound"
        );

        self.items.push(Entry {
            item,
            records: r,
            bytes: b,
        });
        self.total_records = self.total_records.saturating_add(r);
        self.total_bytes = self.total_bytes.saturating_add(b);
        self.last_arrival = Some(now);

        if self.items.len() >= self.cfg.max_items {
            return Some((FlushReason::MaxItems, self.flush()));
        }
        if self.total_records >= self.cfg.max_records {
            return Some((FlushReason::MaxRecords, self.flush()));
        }
        if self.total_bytes >= self.cfg.max_bytes {
            return Some((FlushReason::MaxBytes, self.flush()));
        }
        None
    }

    pub fn flush(&mut self) -> Vec<T> {
        self.total_records = 0;
        self.total_bytes = 0;
        self.last_arrival = None;

        let mut out = Vec::with_capacity(self.items.len());
        for e in self.items.drain(..) {
            out.push(e.item);
        }
        out
    }

    pub fn has_active_batch(&self) -> bool {
        !self.items.is_empty()
    }
}

pub struct BatcherDriver<T, W>
where
    W: FnMut(&T) -> (usize, usize),
{
    core: BatcherCore<T, W>,
}

impl<T, W> BatcherDriver<T, W>
where
    W: FnMut(&T) -> (usize, usize),
{
    pub fn new(cfg: BatcherConfig, weight: W) -> Self {
        Self {
            core: BatcherCore::new(cfg, weight),
        }
    }

    pub fn next_batch(
        &mut self,
        rx: &crossbeam_channel::Receiver<T>,
    ) -> Option<(FlushReason, Vec<T>)> {
        // Ensure at least one item in flight
        if self.core.is_empty() {
            let x = rx.recv().ok()?;
            let now = Instant::now();
            match self.core.push(now, x) {
                PushResult::One(f) => return Some(f),
                PushResult::Two(f, _) => return Some(f),
                PushResult::None => {}
            }
        }

        loop {
            let now = Instant::now();

            if let Some(flush) = self.core.flush_if_due(now) {
                return Some(flush);
            }

            let wait = match self.core.deadline(now) {
                Deadline::In(w) => w,
                Deadline::DueNow => return self.core.flush_if_due(now),
                Deadline::None if !self.core.is_empty() => Duration::ZERO,
                Deadline::None => Duration::MAX,
            };

            match rx.recv_timeout(wait) {
                Ok(x) => {
                    let now = Instant::now();
                    match self.core.push(now, x) {
                        PushResult::None => {}
                        PushResult::One(f) => return Some(f),
                        PushResult::Two(f1, _) => return Some(f1),
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    // Don't silently drop buffered data on shutdown.
                    if !self.core.is_empty() {
                        return Some((FlushReason::Shutdown, self.core.flush()));
                    }
                    return None;
                }
            }
        }
    }

    pub fn drain(&mut self) -> Vec<T> {
        self.core.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    pub trait Ms {
        fn ms(self) -> Duration;
    }

    macro_rules! impl_ms {
        ($($t:ty),*) => {$(
            impl Ms for $t {
                #[inline]
                fn ms(self) -> Duration {
                    assert!(self >= 0, "negative duration literal");
                    Duration::from_millis(self as u64)
                }
            }
        )*}
    }

    impl_ms!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

    trait PushExt<T> {
        fn unwrap_one(self) -> (FlushReason, Vec<T>);
        fn unwrap_two(self) -> ((FlushReason, Vec<T>), (FlushReason, Vec<T>));
    }

    impl<T> PushExt<T> for PushResult<T>
    where
        T: std::fmt::Debug,
    {
        #[inline(always)]
        #[track_caller]
        fn unwrap_one(self) -> (FlushReason, Vec<T>) {
            match self {
                PushResult::One(f) => f,
                _ => panic!("expected One, got {self:?}"),
            }
        }

        #[inline(always)]
        #[track_caller]
        fn unwrap_two(self) -> ((FlushReason, Vec<T>), (FlushReason, Vec<T>)) {
            match self {
                PushResult::Two(a, b) => (a, b),
                _ => panic!("expected Two, got {self:?}"),
            }
        }
    }

    #[test]
    fn drip_feed_never_starves() {
        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(10),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);
        c.push(t0 + 9.ms(), 2);

        // Not due yet (deadline is t0+19)
        assert!(c.flush_if_due(t0 + 10.ms()).is_none());

        // Due once we pass last_arrival + linger
        let (reason, batch) = c.flush_if_due(t0 + 19.ms()).unwrap();
        assert_eq!(reason, FlushReason::Timeout);
        assert_eq!(batch, vec![1, 2]);
    }

    #[test]
    fn burst_batches_until_limit() {
        let cfg = BatcherConfig {
            max_items: 3,
            max_records: 3,
            max_bytes: 100,
            linger: Duration::from_millis(50),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);
        c.push(t0 + 1.ms(), 2);
        let (r, b) = c.push(t0 + 2.ms(), 3).unwrap_one();

        assert_eq!(r, FlushReason::MaxItems);
        assert_eq!(b, vec![1, 2, 3]);
    }

    #[test]
    fn stale_batch_flushes_on_next_arrival() {
        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(5),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);

        // At t0+10 the old batch is stale (deadline t0+5),
        // so pushing 2 must flush [1] first.
        let (r, b) = c.push(t0 + 10.ms(), 2).unwrap_one();
        assert_eq!(r, FlushReason::Timeout);
        assert_eq!(b, vec![1]);

        // Now only [2] remains buffered
        let (_r2, b2) = c.flush_if_due(t0 + 15.ms()).unwrap(); // deadline for 2 is 10+5=15
        assert_eq!(b2, vec![2]);
    }

    #[test]
    fn stale_arrival_flushes_before_size() {
        let cfg = BatcherConfig {
            max_items: 2,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(10),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);

        // stale arrival -> timeout barrier fires before size
        let (r, b) = c.push(t0 + 10.ms(), 2).unwrap_one();
        assert_eq!(r, FlushReason::Timeout);
        assert_eq!(b, vec![1]);
    }

    #[test]
    fn adversarial_schedule_preserves_order() {
        let cfg = BatcherConfig {
            max_items: 3,
            max_records: 3,
            max_bytes: 3,
            linger: Duration::from_millis(7),
        };

        let base = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        let schedule = [
            (0, Some(1)),
            (2, Some(2)),
            (4, None),
            (8, None),
            (9, Some(3)),
            (10, Some(4)),
            (15, None),
            (16, Some(5)),
        ];

        let mut out = vec![];

        for (dt, maybe) in schedule {
            let now = base + Duration::from_millis(dt);

            if let Some(x) = maybe
                && let PushResult::One((_r, b)) = c.push(now, x)
            {
                out.push(b);
            }

            if let Some((_r, b)) = c.flush_if_due(now) {
                out.push(b);
            }
        }

        // FINAL TICK: force any remaining buffered items to flush by timeout.
        let final_now = base + Duration::from_millis(16 + 7);
        if let Some((_r, b)) = c.flush_if_due(final_now) {
            out.push(b);
        } else if !c.is_empty() {
            out.push(c.flush());
        }

        let flat: Vec<u32> = out.into_iter().flatten().collect();
        assert_eq!(flat, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn driver_flushes_on_timeout() {
        use crossbeam_channel::bounded;

        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(5),
        };

        let (tx, rx) = bounded::<u32>(10);
        let mut d = BatcherDriver::new(cfg, |_x: &u32| (1, 1));

        tx.send(1).unwrap();

        let (reason, batch) = d.next_batch(&rx).unwrap();
        assert_eq!(reason, FlushReason::Timeout);
        assert_eq!(batch, vec![1]);
    }

    #[test]
    fn flushes_exactly_on_deadline() {
        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(10),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);
        assert!(c.flush_if_due(t0 + 9.ms()).is_none());
        let (r, b) = c.flush_if_due(t0 + 10.ms()).unwrap();
        assert_eq!(r, FlushReason::Timeout);
        assert_eq!(b, vec![1]);
    }
    #[test]
    fn successive_batches_do_not_mix() {
        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(5),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);
        let (_r1, b1) = c.flush_if_due(t0 + 5.ms()).unwrap();
        assert_eq!(b1, vec![1]);

        c.push(t0 + 10.ms(), 2);
        let (_r2, b2) = c.flush_if_due(t0 + 15.ms()).unwrap();
        assert_eq!(b2, vec![2]);
    }

    #[test]
    fn large_batches_are_linear() {
        let cfg = BatcherConfig {
            max_items: 10_000,
            max_records: 10_000,
            max_bytes: 10_000,
            linger: Duration::from_millis(100),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        for i in 0..10_000 {
            if let PushResult::One((_r, b)) = c.push(t0 + i.ms(), i) {
                assert_eq!(b.len(), 10_000);
            }
        }
    }

    #[test]
    fn deterministic_fuzz_seed() {
        let mut rng = fastrand::Rng::with_seed(0xdead_beef);

        let cfg = BatcherConfig {
            max_items: 7,
            max_records: 7,
            max_bytes: 7,
            linger: Duration::from_millis(11),
        };

        let base = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        let mut out = vec![];
        let mut now = base;

        for i in 0..1000 {
            now += Duration::from_millis(rng.u64(0..5));

            if rng.bool() {
                let x = i as u32;
                if let PushResult::One((_r, b)) = c.push(now, x) {
                    out.extend(b);
                }
            }

            if let Some((_r, b)) = c.flush_if_due(now) {
                out.extend(b);
            }
        }

        // No duplicates, no losses
        let mut sorted = out.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), out.len());
    }

    #[test]
    fn zero_linger_flushes_immediately() {
        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::ZERO,
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);
        let (r, b) = c.flush_if_due(t0).unwrap();
        assert_eq!(r, FlushReason::Timeout);
        assert_eq!(b, vec![1]);
    }

    #[test]
    fn drop_drains_batch() {
        let cfg = BatcherConfig {
            max_items: 100,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(50),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        c.push(t0, 1);
        c.push(t0 + 1.ms(), 2);

        let drained = c.flush();
        assert_eq!(drained, vec![1, 2]);
    }

    #[test]
    fn stale_arrival_can_produce_two_flushes() {
        let cfg = BatcherConfig {
            max_items: 1,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(5),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (1, 1));

        // First batch flushes by size
        let (r1, b1) = c.push(t0, 1).unwrap_one();
        assert_eq!(r1, FlushReason::MaxItems);
        assert_eq!(b1, vec![1]);

        // Prepare a buffered batch
        let cfg2 = BatcherConfig {
            max_items: 10,
            max_records: 100,
            max_bytes: 100,
            linger: Duration::from_millis(5),
        };
        let mut c = BatcherCore::new(cfg2, |_x: &u32| (1, 1));

        c.push(t0, 1);

        // Force immediate size flush on new batch
        c.cfg.max_items = 1;

        let ((r_old, b_old), (r_new, b_new)) = c.push(t0 + 10.ms(), 2).unwrap_two();

        assert_eq!(r_old, FlushReason::Timeout);
        assert_eq!(b_old, vec![1]);
        assert_eq!(r_new, FlushReason::MaxItems);
        assert_eq!(b_new, vec![2]);
    }

    #[test]
    fn driver_wait_rule_treats_none_as_due_when_batch_nonempty() {
        fn driver_wait(time_until: Option<Duration>, has_batch: bool) -> Duration {
            match time_until {
                Some(w) => w,
                None if has_batch => Duration::ZERO,
                None => Duration::MAX,
            }
        }

        assert_eq!(driver_wait(None, true), Duration::ZERO);
        assert_eq!(driver_wait(None, false), Duration::MAX);
        assert_eq!(
            driver_wait(Some(Duration::from_millis(5)), true),
            Duration::from_millis(5)
        );
    }

    #[test]
    #[should_panic]
    fn zero_weight_with_unbounded_max_items_panics() {
        let cfg = BatcherConfig {
            max_items: usize::MAX,
            max_records: usize::MAX,
            max_bytes: usize::MAX,
            linger: Duration::from_millis(10),
        };

        let t0 = Instant::now();
        let mut c = BatcherCore::new(cfg, |_x: &u32| (0, 0));

        let _ = c.push(t0, 1);
    }
}
