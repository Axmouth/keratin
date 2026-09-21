//! Bounded control-plane breadcrumbs. No message data and no append hot-path work.
use std::{
    collections::VecDeque,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

pub const CONTROL_HISTORY_LIMIT: usize = 32;
const REPORT_INTERVAL: Duration = Duration::from_secs(30);
static NEXT_REPORT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogControlKind {
    SuffixTruncated,
    Opened,
    Owner,
    Follower,
    Frozen,
    DeclareRequested,
    DeclareCommitted,
    EpochAdvanced,
    CheckpointReset,
    Truncated,
}

#[derive(Debug, Clone, Copy)]
pub struct LogControlEvent {
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub kind: LogControlKind,
    pub epoch: u64,
    /// Observed log boundary, not the identity of an appended record.
    /// Exact record offsets are reported in the event windows.
    pub offset: u64,
}

/// Only internal coordinates and comparison results: never payloads, headers,
/// their hashes, or declaration settings. Offset ranges are end-exclusive.
#[derive(Debug, Clone)]
pub struct ReplicationOverlapDiagnostic {
    pub report_id: u64,
    pub offset: u64,
    pub existing_offset: u64,
    pub epoch: u64,
    pub local_head: u64,
    pub local_next: u64,
    pub local_durable_next: u64,
    pub compared_first: u64,
    pub compared_count: usize,
    pub flags_differ: bool,
    pub headers_differ: bool,
    pub payloads_differ: bool,
    pub emit_details: bool,
    pub suppressed_reports: u64,
    pub control_history: Vec<LogControlEvent>,
}

impl fmt::Display for ReplicationOverlapDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "replicated overlap mismatch at offset {} (diagnostic_id={}, details={})",
            self.offset, self.report_id, self.emit_details
        )
    }
}
impl std::error::Error for ReplicationOverlapDiagnostic {}

#[derive(Debug, Default)]
pub(crate) struct LogDiagnostics {
    history: VecDeque<LogControlEvent>,
    sequence: u64,
    last_report: Option<(Instant, u64)>,
    suppressed: u64,
}

impl LogDiagnostics {
    pub fn record(&mut self, kind: LogControlKind, epoch: u64, offset: u64) {
        self.sequence = self.sequence.saturating_add(1);
        if self.history.len() == CONTROL_HISTORY_LIMIT {
            self.history.pop_front();
        }
        self.history.push_back(LogControlEvent {
            sequence: self.sequence,
            observed_at_ms: crate::util::unix_millis(),
            kind,
            epoch,
            offset,
        });
    }

    pub fn capture(&mut self, now: Instant) -> (u64, bool, u64, Vec<LogControlEvent>) {
        if let Some((last, id)) = self.last_report {
            if now.duration_since(last) < REPORT_INTERVAL {
                self.suppressed = self.suppressed.saturating_add(1);
                return (id, false, self.suppressed, Vec::new());
            }
        }
        let id = NEXT_REPORT_ID.fetch_add(1, Ordering::Relaxed);
        self.last_report = Some((now, id));
        let suppressed = std::mem::take(&mut self.suppressed);
        (id, true, suppressed, self.history.iter().copied().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_is_bounded_ordered_and_reports_are_rate_limited() {
        let mut diagnostics = LogDiagnostics::default();
        for offset in 0..100 {
            diagnostics.record(LogControlKind::EpochAdvanced, offset, offset);
        }
        let now = Instant::now();
        let (id, detailed, suppressed, history) = diagnostics.capture(now);
        assert!(detailed);
        assert_eq!(suppressed, 0);
        assert_eq!(history.len(), CONTROL_HISTORY_LIMIT);
        assert_eq!(history.first().unwrap().offset, 68);
        assert_eq!(history.last().unwrap().offset, 99);
        let (again, detailed, suppressed, history) =
            diagnostics.capture(now + Duration::from_secs(1));
        assert_eq!(again, id);
        assert!(!detailed);
        assert_eq!(suppressed, 1);
        assert!(history.is_empty());
        let (next, detailed, suppressed, _) = diagnostics.capture(now + REPORT_INTERVAL);
        assert_ne!(next, id);
        assert!(detailed);
        assert_eq!(suppressed, 1);
    }
}
