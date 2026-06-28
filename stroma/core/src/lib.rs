mod engine;
mod event;
mod global;
mod metrics;
mod replication;
mod state;
// Some stream-engine helpers (cursor introspection, retention axes) are wired in
// by later Plexus steps (fibril stream actor, retention worker), so they are
// unused for now.
#[allow(dead_code)]
mod stream_state;
mod stroma;

use thiserror::Error;

pub use engine::PartitionKind;
pub use event::{
    AckEventMeta, DLQDiscardPolicyWire, DeadLetterMeta, DeadLetterReason, DeclareMeta,
    EnqueueEventMeta, MarkInflightEventMeta, NackEventMeta, StromaEvent,
};
pub use global::{GlobalKey, GlobalStore, GlobalValue, PutOutcome};
pub use keratin_log::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KDurability, KeratinAppendCompletion,
    KeratinConfig, Message, ReceivedMessage, ReplicatedAppendOutcome, test_dir, util::TempDir,
};
pub use metrics::StromaMetrics;
pub use state::{
    AckOutcome, CustomDLQ, DLQDiscardPolicy, DLQDiscardSettings, InspectMode,
    MessageInspectionStatus, NackBatchOutcome, NackOutcome, QueueHandle, QueueHandleError,
    QueueHandleInner, QueueInspectionSnapshot, QueueInspectionState, QueueInternalState, QueueRole,
    Resolved, SnapshotMeta, StreamHandle, StromaDebugSnapshot, WorkQueueHandle,
};
pub use stream_state::RetentionConfig;
pub use stroma::{
    DestroyOutcome, EvictOutcome, FollowerStateCheckpointInstall,
    EnqueuedStreamAppend, FollowerStateCheckpointInstallOutcome, GlobalDLQ, GlobalDlqSnapshot,
    GlobalDlqUpdateOutcome, MessageContentType, MessageHeaders, MessageInspectionItem,
    MessageInspectionPage,
    OwnerReplicationBatch, OwnerReplicationRead, OwnerStateCheckpoint, PublishItem, QuarantineInfo,
    QueueDemotionOutcome, QueuePromotionOutcome, ReplicatedEventBatch, ReplicatedMessageBatch,
    ReplicatedQueueApplyOutcome, SnapshotConfig, Stroma, StromaKeratinConfig, StromaOptions,
    TaskGroup,
};

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

    #[error("encode: {0}")]
    Encode(String),

    #[error("io: {0}")]
    Io(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("not found")]
    NotFound(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("queue actor is gone")]
    QueueActorGone,

    #[error("queue role mismatch: expected {expected:?}, current role is {actual:?}")]
    WrongQueueRole {
        expected: QueueRole,
        actual: QueueRole,
    },

    #[error("partition kind mismatch: expected {expected:?}, this partition is {actual:?}")]
    WrongPartitionKind {
        expected: PartitionKind,
        actual: PartitionKind,
    },

    #[error("recovery mismatch for {topic}/{partition}/{group:?}: {reason}")]
    RecoveryMismatch {
        topic: String,
        partition: u32,
        group: Option<String>,
        reason: String,
    },

    #[error("queue {topic}/{partition}/{group:?} is quarantined: {reason}")]
    QueueQuarantined {
        topic: String,
        partition: u32,
        group: Option<String>,
        reason: String,
    },

    #[error("internal: {0}")]
    Internal(String),
}

/// What to do when recovery finds the event log references a message the message
/// log has not durably accepted (a dangling event->message reference).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryMismatchPolicy {
    /// Default: park the affected partition (it does not serve) and keep the rest
    /// of the broker running. The partition is surfaced as quarantined for an
    /// operator to repair. Blast radius is the one queue.
    #[default]
    Quarantine,
    /// Louder: treat the mismatch as fatal. Stroma still quarantines the
    /// partition and returns the error; the caller (broker) escalates - e.g.
    /// refuses to serve / exits - so the failure cannot be missed.
    Refuse,
    /// "Screw it, continue": auto-apply the truncate-to-valid repair (drop the
    /// dangling event-log suffix) and carry on, with a loud warning. Never the
    /// default; for operators who accept the potential data loss.
    Ignore,
}

impl RecoveryMismatchPolicy {
    pub fn as_u8(self) -> u8 {
        match self {
            RecoveryMismatchPolicy::Quarantine => 0,
            RecoveryMismatchPolicy::Refuse => 1,
            RecoveryMismatchPolicy::Ignore => 2,
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => RecoveryMismatchPolicy::Refuse,
            2 => RecoveryMismatchPolicy::Ignore,
            _ => RecoveryMismatchPolicy::Quarantine,
        }
    }
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

pub mod group {
    use thiserror::Error;

    #[derive(Clone, Hash, Eq, PartialEq)]
    pub struct Group {
        inner: Box<str>,
    }

    impl Group {
        pub fn parse(s: &str) -> std::result::Result<Self, GroupError> {
            let b = s.as_bytes();
            let len = b.len();

            if len == 0 || len > 128 {
                return Err(GroupError::InvalidLength);
            }

            if b[0] == b'.' || b[len - 1] == b'.' {
                return Err(GroupError::DotEdge);
            }

            let mut prev_dot = false;

            for &c in b {
                let ok = c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || c == b'.'
                    || c == b'_'
                    || c == b'-';

                if !ok {
                    return Err(GroupError::InvalidChar);
                }

                if c == b'.' {
                    if prev_dot {
                        return Err(GroupError::DoubleDot);
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

    impl std::borrow::Borrow<str> for Group {
        fn borrow(&self) -> &str {
            &self.inner
        }
    }

    #[derive(Debug, Error)]
    pub enum GroupError {
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
            Group::parse(s).unwrap();
        }

        fn err(s: &str) {
            assert!(Group::parse(s).is_err(), "{s:?} should be invalid");
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
            let t = Group::parse("orders").unwrap();
            let mut map = std::collections::HashMap::new();
            map.insert(t.clone(), 1);

            assert_eq!(map.get("orders"), Some(&1));
        }
    }
}

// Partition/Offset now live in the dependency-light `stroma-common` crate so
// they can be shared upward (e.g. by network clients) without the engine.
// Re-exported through these module paths so existing `partition::Partition` /
// `offset::Offset` references keep working.
pub mod partition {
    pub use stroma_common::Partition;
}

pub mod offset {
    pub use stroma_common::Offset;
}

pub mod unix_millis {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

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
            Self {
                inner: prev.max(real),
            }
        }

        #[inline]
        pub fn value(&self) -> u64 {
            self.inner
        }
    }
}
