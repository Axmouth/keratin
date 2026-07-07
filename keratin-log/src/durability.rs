use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KDurability {
    AfterWrite,
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
