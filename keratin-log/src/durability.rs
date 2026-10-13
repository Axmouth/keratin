use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KDurability {
    /// Local appends may complete while still staged in Keratin's buffers.
    /// Replicated appends write to the OS before completing. Pair with
    /// `sync()` for persistence.
    AfterWrite,
    /// Opt-in. Completes once the batch is written through Keratin's buffers
    /// to the OS, without waiting for fsync. The record survives a process
    /// crash but not power loss or an OS crash until a later sync. It does not
    /// advance the durable frontier, so ordinary scans do not see it yet.
    /// A failed write stops the log like any other storage failure.
    AfterFlush,
    /// Completes after the record crosses the persistence boundary (fsync).
    AfterFsync,
}

/// The exclusive durable frontier: the number of durable records, i.e. the first
/// offset that is NOT yet durable. `0` means nothing is durable (an empty log).
///
/// This is a distinct type from a plain offset on purpose. A bare inclusive
/// watermark cannot tell an empty log apart from "offset 0 is durable" (both are
/// `0`), and mixing an inclusive watermark with an exclusive bound is an easy
/// off-by-one. Encoding the frontier as an exclusive count removes the ambiguity,
/// and wrapping it in a newtype makes `covers` the only way to ask "is this offset
/// durable?" so the inclusive-vs-exclusive confusion cannot recur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurableFrontier(u64);

impl DurableFrontier {
    /// Wrap an exclusive durable-record count (`0` = nothing durable).
    pub fn from_exclusive(end: u64) -> Self {
        Self(end)
    }

    /// Whether `offset` is durable, i.e. strictly below the frontier.
    pub fn covers(self, offset: u64) -> bool {
        offset < self.0
    }

    /// The first offset that is NOT durable (the exclusive upper read bound).
    pub fn first_non_durable(self) -> u64 {
        self.0
    }
}

/// A shared, atomic publication of the [`DurableFrontier`]. Cloned handles point
/// at the same cell (the writer publishes, readers observe), so the frontier
/// encoding lives in exactly one place and no raw `u64` ever escapes: the only way
/// to touch the cell is through the typed `load`/`advance`/`reset` below. That also
/// pins the memory ordering (release on publish, acquire on observe) instead of
/// leaving it to each call site.
#[derive(Debug, Clone)]
pub struct DurableWatermark(Arc<AtomicU64>);

impl DurableWatermark {
    pub fn new(frontier: DurableFrontier) -> Self {
        Self(Arc::new(AtomicU64::new(frontier.0)))
    }

    pub fn load(&self) -> DurableFrontier {
        DurableFrontier(self.0.load(Ordering::Acquire))
    }

    /// Publish forward progress after an fsync. Monotonic by construction: a
    /// `fetch_max`, so a stale or out-of-order frontier can never move the
    /// watermark backward. This is the steady-state publish path.
    pub fn advance(&self, frontier: DurableFrontier) {
        self.0.fetch_max(frontier.0, Ordering::Release);
    }

    /// Authoritatively set the frontier, including moving it *backward*. Only for
    /// recovery baselines and checkpoint/truncation resets, where the durable set
    /// legitimately shrinks. Never use this on the steady-state path (use
    /// `advance`), which must never rewind.
    pub fn reset(&self, frontier: DurableFrontier) {
        self.0.store(frontier.0, Ordering::Release);
    }
}

/// A shared publication of the exclusive written frontier: every offset below
/// it has been written through Keratin's buffers to the OS (the point where an
/// `AfterFlush` append completes), whether or not an fsync has covered it yet.
/// The writer publishes and readers observe, like `DurableWatermark`. It never
/// falls below the durable frontier and never passes the next offset to assign.
///
/// Changes are also sent on a watch channel so a reader can wait for progress
/// without polling. The channel carries the latest frontier, so a slow reader
/// only sees the newest value.
#[derive(Debug, Clone)]
pub struct WrittenWatermark {
    next: Arc<AtomicU64>,
    changed: Arc<tokio::sync::watch::Sender<u64>>,
}

impl WrittenWatermark {
    pub fn new(next: u64) -> Self {
        Self {
            next: Arc::new(AtomicU64::new(next)),
            changed: Arc::new(tokio::sync::watch::channel(next).0),
        }
    }

    /// The first offset not yet written to the OS.
    pub fn load(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }

    /// Watch the frontier. The receiver starts at the current value.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// Publish forward progress after a write. Monotonic, like
    /// `DurableWatermark::advance`.
    pub(crate) fn advance(&self, next: u64) {
        if self.next.fetch_max(next, Ordering::Release) < next {
            self.changed.send_if_modified(|current| {
                if *current < next {
                    *current = next;
                    true
                } else {
                    false
                }
            });
        }
    }

    /// Set the frontier, including moving it backward. Only for open and for
    /// resets that rewrite the log tail.
    pub(crate) fn reset(&self, next: u64) {
        self.next.store(next, Ordering::Release);
        self.changed.send_if_modified(|current| {
            let modified = *current != next;
            *current = next;
            modified
        });
    }
}
