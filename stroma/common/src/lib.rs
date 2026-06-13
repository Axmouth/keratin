//! Shared value types originating at the stroma/storage layer.
//!
//! Deliberately dependency-light (just `serde`) so any consumer — including
//! network clients several layers above the engine — can use these types
//! without pulling in the engine machinery. The newtypes serialize transparently
//! (as their inner integer), so they are zero-cost on the wire and on disk.

use serde::{Deserialize, Serialize};

/// A topic name. (Alias for now; a candidate for an `Arc<str>`-backed newtype to
/// cut clone/allocation churn — kept an alias until that representation lands.)
pub type Topic = String;
/// An optional queue group: a namespace under a topic, part of queue identity.
pub type Group = String;

/// A partition index within a logical queue.
///
/// A distinct newtype (not a bare `u32`) so it cannot be silently confused with
/// an [`Offset`], an epoch, or any other integer. Arithmetic is intentionally
/// not derived — reinterpreting a partition as a number is explicit via
/// [`Partition::id`].
#[derive(
    Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Partition {
    id: u32,
}

impl Partition {
    /// The conventional first/only partition.
    pub const ZERO: Partition = Partition { id: 0 };

    /// Construct from a raw index.
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self { id }
    }

    /// The raw partition index.
    #[inline]
    pub const fn id(self) -> u32 {
        self.id
    }
}

impl From<u32> for Partition {
    fn from(id: u32) -> Self {
        Partition { id }
    }
}

impl From<Partition> for u32 {
    fn from(partition: Partition) -> Self {
        partition.id
    }
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// A message offset within a single partition log.
///
/// A distinct newtype so it cannot be confused with a partition or other
/// integer. `next` is the only built-in arithmetic (the common "advance one"
/// step); anything else goes through [`Offset::value`] explicitly.
#[derive(
    Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Offset {
    inner: u64,
}

impl Offset {
    /// The first offset in a log.
    pub const ZERO: Offset = Offset { inner: 0 };

    /// Construct from a raw offset.
    #[inline]
    pub const fn new(offset: u64) -> Self {
        Self { inner: offset }
    }

    /// The raw offset value.
    #[inline]
    pub const fn value(self) -> u64 {
        self.inner
    }

    /// The next offset.
    #[inline]
    pub const fn next(self) -> Self {
        Self {
            inner: self.inner + 1,
        }
    }
}

impl From<u64> for Offset {
    fn from(offset: u64) -> Self {
        Offset { inner: offset }
    }
}

impl From<Offset> for u64 {
    fn from(offset: Offset) -> Self {
        offset.inner
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
