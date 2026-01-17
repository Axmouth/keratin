mod event;
mod state;
mod sequencer;
mod stroma;

use keratin_log::KDurability;
use thiserror::Error;

pub use keratin_log::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KeratinAppendCompletion,
    KeratinConfig, Message, ReceivedMessage,
};
pub use state::QueueState;
pub use stroma::{SnapshotConfig, Stroma};

pub type Offset = u64;
pub type UnixMillis = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    AfterWrite,
    AfterFsync,
    AfterReplicated, // for later; can map to AfterFsync initially
}

impl From<Durability> for KDurability {
    fn from(value: Durability) -> Self {
        match value {
            Durability::AfterWrite => KDurability::AfterWrite,
            Durability::AfterFsync => KDurability::AfterFsync,
            Durability::AfterReplicated => KDurability::AfterFsync,
        }
    }
}

#[derive(Debug, Error)]
pub enum StromaError {
    #[error("decode: {0}")]
    Decode(String),

    #[error("io: {0}")]
    Io(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("not found")]
    NotFound,

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, StromaError>;

pub mod topic {
    use thiserror::Error;

    #[derive(Clone, Hash, Eq, PartialEq)]
    pub struct Topic {
        inner: Box<str>,
    }

    impl Topic {
        pub fn parse(s: &str) -> std::result::Result<Self, TopicError> {
            let b = s.as_bytes();
            let len = b.len();

            if len == 0 || len > 128 {
                return Err(TopicError::InvalidLength);
            }

            if b[0] == b'.' || b[len - 1] == b'.' {
                return Err(TopicError::DotEdge);
            }

            let mut prev_dot = false;

            for &c in b {
                let ok = c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || c == b'.'
                    || c == b'_'
                    || c == b'-';

                if !ok {
                    return Err(TopicError::InvalidChar);
                }

                if c == b'.' {
                    if prev_dot {
                        return Err(TopicError::DoubleDot);
                    }
                    prev_dot = true;
                } else {
                    prev_dot = false;
                }
            }

            Ok(Self { inner: s.into() })
        }

        pub fn as_str(&self) -> &str {
            &self.inner
        }
    }

    impl std::borrow::Borrow<str> for Topic {
        fn borrow(&self) -> &str {
            &self.inner
        }
    }

    #[derive(Debug, Error)]
    pub enum TopicError {
        #[error("topic must be 1–128 bytes")]
        InvalidLength,
        #[error("topic cannot start or end with '.'")]
        DotEdge,
        #[error("topic contains invalid characters")]
        InvalidChar,
        #[error("topic cannot contain consecutive dots")]
        DoubleDot,
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn ok(s: &str) {
            Topic::parse(s).unwrap();
        }

        fn err(s: &str) {
            assert!(Topic::parse(s).is_err(), "{s:?} should be invalid");
        }

        #[test]
        fn valid_topics() {
            ok("orders");
            ok("orders.processing");
            ok("orders-v2");
            ok("_dlq.orders");
            ok("metrics_2026");
            ok("a");
            ok("a1_b-2.c");
        }

        #[test]
        fn empty_and_length() {
            err("");
            err(&"a".repeat(129));
            ok(&"a".repeat(128));
        }

        #[test]
        fn case_and_ascii() {
            err("Orders");
            err("Ωmega");
            err("hello💥");
        }

        #[test]
        fn dot_rules() {
            err(".orders");
            err("orders.");
            err("orders..processing");
            ok("orders.processing");
        }

        #[test]
        fn invalid_chars() {
            err("orders!");
            err("hello@world");
            err("foo/bar");
            err("foo bar");
        }

        #[test]
        fn borrow_works() {
            let t = Topic::parse("orders").unwrap();
            let mut map = std::collections::HashMap::new();
            map.insert(t.clone(), 1);

            assert_eq!(map.get("orders"), Some(&1));
        }
    }
}

pub mod partition {
    #[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Debug)]
    pub struct Partition {
        id: u32,
    }

    impl Partition {
        #[inline]
        pub const fn new(id: u32) -> Self {
            Self { id }
        }

        #[inline]
        pub const fn id(self) -> u32 {
            self.id
        }
    }
}

pub mod offset {
    #[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Debug)]
    pub struct Offset {
        inner: u64,
    }

    impl Offset {
        #[inline]
        pub fn new(offset: u64) -> Self {
            Self { inner: offset }
        }

        #[inline]
        pub fn value(&self) -> u64 {
            self.inner
        }

        #[inline]
        pub const fn next(self) -> Self {
            Self {
                inner: self.inner + 1,
            }
        }
    }
}

pub mod unix_millis {
    use std::{sync::atomic::{AtomicU64, Ordering}, time::{SystemTime, UNIX_EPOCH}};

    /// Wall-clock timestamp in milliseconds since the Unix epoch.
    ///
    /// ------------------------------------
    /// 
    /// #### Monotonicity:
    ///
    /// `UnixMillis::now()` is guaranteed to be **monotonic within a process**.
    /// The returned value will never be less than a previously returned value,
    /// even if the system wall clock is adjusted backwards (NTP, VM migration,
    /// suspend/resume, leap seconds, etc).
    ///
    /// This protects ordering, TTL, and retention logic from clock rollback.
    ///
    /// Note:
    /// This monotonicity is local to the current process and does **not**
    /// imply synchronization with other machines or processes.
    #[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Debug)]
    pub struct UnixMillis {
        inner: u64,
    }

    impl UnixMillis {
        #[inline]
        pub fn new(millis: u64) -> Self {
            Self { inner: millis }
        }

        /// Wall-clock timestamp in milliseconds since the Unix epoch.
        ///
        /// ------------------------------------
        /// 
        /// #### Monotonicity"
        ///
        /// `UnixMillis::now()` is guaranteed to be **monotonic within a process**.
        /// The returned value will never be less than a previously returned value,
        /// even if the system wall clock is adjusted backwards (NTP, VM migration,
        /// suspend/resume, leap seconds, etc).
        ///
        /// This protects ordering, TTL, and retention logic from clock rollback.
        ///
        /// Note:
        /// This monotonicity is local to the current process and does **not**
        /// imply synchronization with other machines or processes.
        #[inline]
        pub fn now() -> Self {
            static LAST: AtomicU64 = AtomicU64::new(0);

            let real = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let prev = LAST.fetch_max(real, Ordering::Relaxed);
            Self { inner: prev.max(real) }
        }

        #[inline]
        pub fn value(&self) -> u64 {
            self.inner
        }
    }
}
