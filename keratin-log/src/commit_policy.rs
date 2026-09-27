//! Pure scheduling rules for the isolated immediate-staging experiment.
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Trigger {
    Drain,
    Size,
    Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Decision {
    pub trigger: Option<Trigger>,
    pub wait: Option<Duration>,
}
pub(crate) struct Input {
    pub now: Instant,
    pub oldest: Option<Instant>,
    pub last_submit: Instant,
    pub max_delay: Duration,
    pub min_interval: Duration,
    pub input_empty: bool,
    pub bytes: usize,
    pub records: usize,
    pub max_bytes: usize,
    pub max_records: usize,
    pub inflight: usize,
    pub hard_cap: usize,
    pub pipeline: bool,
    pub startup_deadline: Option<Instant>,
}
impl Input {
    pub fn decide(&self) -> Decision {
        let idle = Decision {
            trigger: None,
            wait: None,
        };
        let Some(oldest) = self.oldest else {
            return idle;
        };
        let hard_cap = self.hard_cap.max(1);
        if self.inflight >= hard_cap {
            return idle;
        }
        let soft_cap = if self.pipeline { hard_cap } else { 1 };
        let floor = self
            .last_submit
            .checked_add(self.min_interval)
            .unwrap_or(self.now);
        let deadline = oldest.checked_add(self.max_delay).unwrap_or(self.now);
        let size = self.bytes >= self.max_bytes || self.records >= self.max_records;
        let early = self.inflight < soft_cap && (self.input_empty || size);
        let wake = if early { floor } else { deadline.max(floor) };
        // Only the first commit after a real idle period gets this window.
        // Size and the oldest-ack deadline still bound it. The caller keeps
        // staging and flushing writes while this durability cutoff waits.
        let wake = if size {
            wake
        } else {
            wake.max(self.startup_deadline.unwrap_or(self.now).min(deadline))
        };
        if self.now < wake {
            return Decision {
                trigger: None,
                wait: Some(wake - self.now),
            };
        }
        let trigger = if self.now >= deadline {
            Trigger::Deadline
        } else if size {
            Trigger::Size
        } else {
            Trigger::Drain
        };
        Decision {
            trigger: Some(trigger),
            wait: Some(Duration::ZERO),
        }
    }
}
/// Per-writer startup state. A brief empty channel during active work does
/// not arm this window. Both outstanding durability and queued input must be
/// absent for the idle threshold. New arrivals never renew an armed deadline.
pub(crate) struct StartupWindow {
    delay: Duration,
    idle_threshold: Duration,
    idle_since: Option<Instant>,
    deadline: Option<Instant>,
}
impl StartupWindow {
    pub fn new(delay: Duration, idle_threshold: Duration) -> Self {
        Self {
            delay,
            idle_threshold,
            idle_since: None,
            deadline: None,
        }
    }
    pub fn observe(&mut self, now: Instant, pending: bool, inflight: usize, input_empty: bool) {
        if !pending {
            self.deadline = None;
        }
        if !pending && inflight == 0 && input_empty {
            self.idle_since.get_or_insert(now);
        } else {
            self.idle_since = None;
        }
    }
    pub fn append(&mut self, now: Instant) {
        if let Some(idle_since) = self.idle_since.take() {
            if !self.delay.is_zero()
                && now.saturating_duration_since(idle_since) >= self.idle_threshold
            {
                self.deadline = now.checked_add(self.delay);
            }
        }
    }
    pub fn reset(&mut self) {
        self.idle_since = None;
        self.deadline = None;
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input(now: Instant) -> Input {
        Input {
            now,
            oldest: Some(now),
            last_submit: now,
            max_delay: Duration::from_millis(5),
            min_interval: Duration::ZERO,
            input_empty: false,
            bytes: 1,
            records: 1,
            max_bytes: 1024,
            max_records: 64,
            inflight: 0,
            hard_cap: 8,
            pipeline: true,
            startup_deadline: None,
        }
    }
    #[test]
    fn drain_starts_commit_without_waiting_for_deadline() {
        let mut x = input(Instant::now());
        x.input_empty = true;
        assert_eq!(x.decide().trigger, Some(Trigger::Drain));
    }
    #[test]
    fn continuous_input_commits_at_oldest_deadline() {
        let mut x = input(Instant::now());
        assert_eq!(x.decide().wait, Some(Duration::from_millis(5)));
        x.now += Duration::from_millis(3);
        x.records += 1;
        assert_eq!(x.decide().wait, Some(Duration::from_millis(2)));
        x.now += Duration::from_millis(2);
        assert_eq!(x.decide().trigger, Some(Trigger::Deadline));
    }
    #[test]
    fn bytes_and_record_limits_commit_with_queued_input() {
        let mut x = input(Instant::now());
        x.bytes = x.max_bytes;
        assert_eq!(x.decide().trigger, Some(Trigger::Size));
        x.bytes = 1;
        x.records = x.max_records;
        assert_eq!(x.decide().trigger, Some(Trigger::Size));
    }
    #[test]
    fn full_capacity_has_no_expired_wakeup_until_completion() {
        let mut x = input(Instant::now());
        x.now += Duration::from_secs(1);
        x.inflight = 8;
        x.input_empty = true;
        assert_eq!(
            x.decide(),
            Decision {
                trigger: None,
                wait: None
            }
        );
        x.inflight = 7;
        assert_eq!(x.decide().trigger, Some(Trigger::Deadline));
    }
    #[test]
    fn minimum_interval_gates_even_overdue_work() {
        let mut x = input(Instant::now());
        x.now += Duration::from_millis(6);
        x.min_interval = Duration::from_millis(10);
        x.input_empty = true;
        assert_eq!(x.decide().wait, Some(Duration::from_millis(4)));
        assert_eq!(x.decide().trigger, None);
    }
    #[test]
    fn soft_cap_waits_for_deadline_but_not_beyond_hard_cap() {
        let mut x = input(Instant::now());
        x.pipeline = false;
        x.inflight = 1;
        x.input_empty = true;
        assert_eq!(x.decide().wait, Some(Duration::from_millis(5)));
        x.now += Duration::from_millis(5);
        assert_eq!(x.decide().trigger, Some(Trigger::Deadline));
    }
    #[test]
    fn submitted_work_alone_never_creates_another_commit() {
        let mut x = input(Instant::now());
        x.oldest = None;
        x.inflight = 1;
        x.input_empty = true;
        assert_eq!(
            x.decide(),
            Decision {
                trigger: None,
                wait: None
            }
        );
    }
    #[test]
    fn startup_requires_full_idle_and_does_not_renew() {
        let now = Instant::now();
        let mut window = StartupWindow::new(Duration::from_millis(2), Duration::from_millis(20));
        window.observe(now, false, 0, true);
        window.append(now + Duration::from_millis(10));
        assert_eq!(window.deadline(), None);
        window.observe(now + Duration::from_millis(11), false, 0, true);
        window.append(now + Duration::from_millis(32));
        assert_eq!(window.deadline(), Some(now + Duration::from_millis(34)));
        window.observe(now + Duration::from_millis(33), true, 0, true);
        window.append(now + Duration::from_millis(33));
        assert_eq!(window.deadline(), Some(now + Duration::from_millis(34)));
        window.observe(now + Duration::from_millis(35), false, 1, true);
        assert_eq!(window.deadline(), None);
        window.append(now + Duration::from_millis(90));
        assert_eq!(
            window.deadline(),
            None,
            "inflight sync never qualifies as idle"
        );
    }

    #[test]
    fn pending_or_queued_work_cannot_arm_startup() {
        let now = Instant::now();
        for (pending, inflight, empty) in [(true, 0, true), (false, 1, true), (false, 0, false)] {
            let mut window =
                StartupWindow::new(Duration::from_millis(2), Duration::from_millis(20));
            window.observe(now, false, 0, true);
            window.observe(now + Duration::from_millis(21), pending, inflight, empty);
            window.append(now + Duration::from_millis(22));
            assert_eq!(window.deadline(), None);
        }
    }

    #[test]
    fn startup_delays_only_first_early_commit_and_respects_bounds() {
        let now = Instant::now();
        let mut x = input(now);
        x.input_empty = true;
        x.startup_deadline = Some(now + Duration::from_millis(2));
        assert_eq!(x.decide().wait, Some(Duration::from_millis(2)));
        assert_eq!(x.decide().trigger, None);
        x.now += Duration::from_millis(1);
        assert_eq!(x.decide().wait, Some(Duration::from_millis(1)));
        x.now += Duration::from_millis(1);
        assert_eq!(x.decide().trigger, Some(Trigger::Drain));
        x.now = now;
        x.records = x.max_records;
        assert_eq!(x.decide().trigger, Some(Trigger::Size));
        x.records = 1;
        x.bytes = x.max_bytes;
        assert_eq!(x.decide().trigger, Some(Trigger::Size));
        x.bytes = 1;
        x.startup_deadline = Some(now + Duration::from_secs(1));
        x.now = now + Duration::from_millis(5);
        assert_eq!(x.decide().trigger, Some(Trigger::Deadline));
        x.inflight = x.hard_cap;
        assert_eq!(x.decide().wait, None);
        assert_eq!(x.decide().trigger, None);
    }

    #[test]
    fn startup_reset_and_disabled_delay_preserve_original_policy() {
        let now = Instant::now();
        let mut window = StartupWindow::new(Duration::ZERO, Duration::from_millis(20));
        window.observe(now, false, 0, true);
        window.append(now + Duration::from_secs(1));
        assert_eq!(window.deadline(), None);
        let mut window = StartupWindow::new(Duration::from_millis(2), Duration::from_millis(20));
        window.observe(now, false, 0, true);
        window.append(now + Duration::from_secs(1));
        assert!(window.deadline().is_some());
        window.reset();
        assert_eq!(
            window.deadline(),
            None,
            "explicit barriers bypass the startup wait"
        );
        window.append(now + Duration::from_secs(2));
        assert_eq!(window.deadline(), None);
    }
}
