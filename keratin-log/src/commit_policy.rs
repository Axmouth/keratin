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
}
