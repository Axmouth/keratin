//! Stream engine in-memory state (Plexus fan-out channels).
//!
//! Counterpart to the work queue's `QueueInternalState`, but far smaller: a
//! stream never leases, acks, requeues, or dead-letters. A record is appended
//! once and stays until retention drops it, and every consumer reads the same log
//! at its own position. So the only durable state the stream engine keeps is a map
//! of named cursors (durable consumer positions), a retention policy, and the
//! head/tail watermarks of the retained window.
//!
//! Like the work queue, this state lives inside a per-partition control actor and
//! is driven by commands and by replayed events. It rides the same substrate
//! (message log, event log, durable append, replication, recovery, snapshot file
//! IO) through the `PartitionEngine` seam.

use std::collections::HashMap;
use std::io::{Error, ErrorKind};

use tokio::sync::{mpsc, oneshot};

use crate::Offset;
use crate::engine::{PartitionEngine, PartitionKind};
use crate::state::SnapshotMeta;

/// Snapshot format version for the stream engine. Independent of the work queue's
/// `FORMAT_VERSION` because the two engines serialize different state. Strict: a
/// reader rejects any other version (pre-alpha, no back-compat).
const STREAM_FORMAT_VERSION: u64 = 1;

/// How long records are kept before retention may drop them. Each axis is
/// independent and optional. A record may be dropped once it exceeds any
/// configured axis. `None` everywhere means keep forever (bounded only by disk).
///
/// The stream engine only stores this. The retention worker reads it and decides
/// what to truncate, then reports the new head back through `apply_truncation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetentionConfig {
    /// Drop records older than this age in milliseconds.
    pub max_age_ms: Option<u64>,
    /// Drop oldest records once the retained byte size exceeds this.
    pub max_bytes: Option<u64>,
    /// Drop oldest records once the retained record count exceeds this.
    pub max_records: Option<u64>,
}

/// In-memory state for a stream (Plexus) partition.
#[derive(Debug)]
pub struct StreamState {
    topic: String,
    partition: u32,

    last_snapshot_timestamp: u64,
    last_snapshot_event_offset: u64,

    /// Durable named cursors: consumer durable-name -> committed offset. The
    /// offset is the next record the consumer has not yet settled, so resuming
    /// reads from here. Naming and ownership (the optional single-active lease)
    /// live in fibril; the engine treats a name as an opaque bookmark key.
    cursors: HashMap<String, Offset>,

    retention: RetentionConfig,

    /// Lowest offset still retained. Advanced by retention truncation. Cursors
    /// below it are clamped up to it (retention wins over a lagging consumer).
    head: Offset,
    /// One past the highest appended offset (the next offset to be written).
    tail: Offset,
}

impl StreamState {
    pub fn new(topic: String, partition: u32) -> Self {
        StreamState {
            topic,
            partition,
            last_snapshot_timestamp: 0,
            last_snapshot_event_offset: 0,
            cursors: HashMap::new(),
            retention: RetentionConfig::default(),
            head: 0,
            tail: 0,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// Record a durable cursor position for `name`. Last-writer-wins: the caller
    /// (the control actor) enforces advance-on-ack for the durable default and
    /// allows an explicit backward seek for replay. The engine just stores it,
    /// clamped into the retained window so a commit can never point below head or
    /// past tail.
    pub fn commit_cursor(&mut self, name: impl Into<String>, offset: Offset) {
        let clamped = offset.clamp(self.head, self.tail);
        self.cursors.insert(name.into(), clamped);
    }

    pub fn cursor(&self, name: &str) -> Option<Offset> {
        self.cursors.get(name).copied()
    }

    /// Forget a durable cursor (consumer retired or explicit delete).
    pub fn remove_cursor(&mut self, name: &str) -> Option<Offset> {
        self.cursors.remove(name)
    }

    /// All durable cursors as (name, offset) pairs, name-sorted - the
    /// admin's per-stream subscriber view reads this.
    pub fn cursors_snapshot(&self) -> Vec<(String, Offset)> {
        let mut out: Vec<(String, Offset)> =
            self.cursors.iter().map(|(k, v)| (k.clone(), *v)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn cursor_count(&self) -> usize {
        self.cursors.len()
    }

    pub fn set_retention(&mut self, retention: RetentionConfig) {
        self.retention = retention;
    }

    pub fn retention(&self) -> RetentionConfig {
        self.retention
    }

    pub fn head(&self) -> Offset {
        self.head
    }

    pub fn tail(&self) -> Offset {
        self.tail
    }

    /// Advance the tail to reflect newly appended records. Monotonic: a stale or
    /// out-of-order call never moves the tail backward.
    pub fn advance_tail(&mut self, next_offset: Offset) {
        if next_offset > self.tail {
            self.tail = next_offset;
        }
    }

    /// Apply a retention truncation that dropped everything below `new_head`.
    /// Advances head (monotonic) and clamps any cursor that fell behind up to the
    /// new head. Returns the names of clamped cursors so the actor can flag those
    /// consumers as lagged (retention won over their position).
    pub fn apply_truncation(&mut self, new_head: Offset) -> Vec<String> {
        if new_head <= self.head {
            return Vec::new();
        }
        self.head = new_head;
        if self.tail < self.head {
            self.tail = self.head;
        }
        let mut lagged = Vec::new();
        for (name, off) in self.cursors.iter_mut() {
            if *off < new_head {
                *off = new_head;
                lagged.push(name.clone());
            }
        }
        lagged
    }

    pub fn set_snapshot_meta(&mut self, timestamp: u64, event_offset: u64) {
        self.last_snapshot_timestamp = timestamp;
        self.last_snapshot_event_offset = event_offset;
    }

    fn reset_to_empty(&mut self) {
        self.last_snapshot_timestamp = 0;
        self.last_snapshot_event_offset = 0;
        self.cursors.clear();
        self.retention = RetentionConfig::default();
        self.head = 0;
        self.tail = 0;
    }

    /// Serialize the durable state. Format (big endian):
    /// version u64, last_snapshot_timestamp u64, last_snapshot_event_offset u64,
    /// head u64, tail u64, retention (3 x optional u64 as presence byte + value),
    /// cursor count u64, then per cursor: name_len u32, name bytes, offset u64.
    fn encode(&self, last_event_offset: u64) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&STREAM_FORMAT_VERSION.to_be_bytes());
        out.extend_from_slice(&self.last_snapshot_timestamp.to_be_bytes());
        out.extend_from_slice(&last_event_offset.to_be_bytes());
        out.extend_from_slice(&self.head.to_be_bytes());
        out.extend_from_slice(&self.tail.to_be_bytes());

        for axis in [
            self.retention.max_age_ms,
            self.retention.max_bytes,
            self.retention.max_records,
        ] {
            match axis {
                Some(v) => {
                    out.push(1);
                    out.extend_from_slice(&v.to_be_bytes());
                }
                None => out.push(0),
            }
        }

        out.extend_from_slice(&(self.cursors.len() as u64).to_be_bytes());
        for (name, &offset) in &self.cursors {
            let name_bytes = name.as_bytes();
            out.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(name_bytes);
            out.extend_from_slice(&offset.to_be_bytes());
        }

        out
    }

    fn decode(&mut self, mut bytes: &[u8]) -> std::io::Result<SnapshotMeta> {
        fn take<const N: usize>(b: &mut &[u8]) -> std::io::Result<[u8; N]> {
            if b.len() < N {
                return Err(Error::new(ErrorKind::UnexpectedEof, "stream snapshot"));
            }
            let (a, rest) = b.split_at(N);
            *b = rest;
            Ok(a.try_into().expect("exact-length slice"))
        }

        fn take_optional_u64(b: &mut &[u8]) -> std::io::Result<Option<u64>> {
            let present = u8::from_be_bytes(take::<1>(b)?);
            match present {
                0 => Ok(None),
                1 => Ok(Some(u64::from_be_bytes(take::<8>(b)?))),
                _ => Err(Error::new(ErrorKind::InvalidData, "bad presence byte")),
            }
        }

        let version = u64::from_be_bytes(take::<8>(&mut bytes)?);
        if version != STREAM_FORMAT_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "unsupported stream snapshot version {version}, expected {STREAM_FORMAT_VERSION}"
                ),
            ));
        }

        self.reset_to_empty();

        self.last_snapshot_timestamp = u64::from_be_bytes(take::<8>(&mut bytes)?);
        self.last_snapshot_event_offset = u64::from_be_bytes(take::<8>(&mut bytes)?);
        self.head = u64::from_be_bytes(take::<8>(&mut bytes)?);
        self.tail = u64::from_be_bytes(take::<8>(&mut bytes)?);

        self.retention = RetentionConfig {
            max_age_ms: take_optional_u64(&mut bytes)?,
            max_bytes: take_optional_u64(&mut bytes)?,
            max_records: take_optional_u64(&mut bytes)?,
        };

        let cursor_count = u64::from_be_bytes(take::<8>(&mut bytes)?);
        for _ in 0..cursor_count {
            let name_len = u32::from_be_bytes(take::<4>(&mut bytes)?) as usize;
            if bytes.len() < name_len {
                return Err(Error::new(ErrorKind::UnexpectedEof, "cursor name"));
            }
            let name = String::from_utf8(bytes[..name_len].to_vec())
                .map_err(|_| Error::new(ErrorKind::InvalidData, "cursor name not utf8"))?;
            bytes = &bytes[name_len..];
            let offset = u64::from_be_bytes(take::<8>(&mut bytes)?);
            self.cursors.insert(name, offset);
        }

        Ok(SnapshotMeta {
            last_snapshot_event_offset: self.last_snapshot_event_offset,
            last_snapshot_timestamp: self.last_snapshot_timestamp,
            default_message_ttl_ms: None,
        })
    }
}

impl PartitionEngine for StreamState {
    fn kind(&self) -> PartitionKind {
        PartitionKind::Stream
    }

    fn encode_snapshot(&self, last_event_offset: u64) -> Vec<u8> {
        self.encode(last_event_offset)
    }

    fn load_snapshot(&mut self, bytes: &[u8]) -> std::io::Result<SnapshotMeta> {
        self.decode(bytes)
    }

    fn reset(&mut self) {
        self.reset_to_empty();
    }
}

/// Commands the stream control actor processes against its `StreamState`. The
/// stream engine has its own vocabulary, separate from the work queue's
/// `QueueCommand`, because the two engines share only the substrate (logs,
/// durable append, replication, recovery, snapshot IO), not their semantics. A
/// stream never leases, acks, requeues, or dead-letters, so none of those appear
/// here.
///
/// Each variant that mutates carries an optional response sender so a caller can
/// await the apply (used when an apply must be ordered before the next step, for
/// example replaying a durable event before continuing recovery).
#[derive(Debug)]
pub enum StreamCommand {
    /// Record a durable cursor position. The actor applies the advance-on-ack
    /// policy for the durable default; the engine itself clamps into the retained
    /// window. See [`StreamState::commit_cursor`].
    CommitCursor {
        name: String,
        offset: Offset,
        response: Option<oneshot::Sender<()>>,
    },
    /// Record a coalesced batch of cursor positions in one actor message. The
    /// broker microbatches a window of acks into this so high-fan-out auto-ack
    /// does not send one command per record. Each entry applies exactly like
    /// [`StreamCommand::CommitCursor`].
    CommitCursors {
        commits: Vec<(String, Offset)>,
        response: Option<oneshot::Sender<()>>,
    },
    /// Read a durable cursor position.
    GetCursor {
        name: String,
        response: oneshot::Sender<Option<Offset>>,
    },
    /// List every durable cursor as (name, offset), name-sorted.
    ListCursors {
        response: oneshot::Sender<Vec<(String, Offset)>>,
    },
    /// Forget a durable cursor (consumer retired or explicit delete).
    RemoveCursor {
        name: String,
        response: Option<oneshot::Sender<Option<Offset>>>,
    },
    /// Replace the retention policy.
    SetRetention {
        config: RetentionConfig,
        response: Option<oneshot::Sender<()>>,
    },
    /// Move the tail forward after records were appended to the message log.
    AdvanceTail {
        next_offset: Offset,
        response: Option<oneshot::Sender<()>>,
    },
    /// Apply a retention truncation that dropped everything below `new_head`.
    /// Returns the names of cursors that fell behind and were clamped up (lagged).
    ApplyTruncation {
        new_head: Offset,
        response: Option<oneshot::Sender<Vec<String>>>,
    },
    /// Read the current head/tail watermarks.
    GetHeadTail {
        response: oneshot::Sender<(Offset, Offset)>,
    },
    /// Read the retention policy plus the head/tail watermarks, for the retention
    /// worker (one round trip instead of two).
    GetRetentionState {
        response: oneshot::Sender<(RetentionConfig, Offset, Offset)>,
    },
    /// Serialize the current state for a snapshot.
    EncodeSnapshot {
        last_event_offset: u64,
        response: oneshot::Sender<Vec<u8>>,
    },
    /// Restore state from a snapshot blob.
    LoadSnapshot {
        bytes: Vec<u8>,
        response: oneshot::Sender<std::io::Result<SnapshotMeta>>,
    },
    /// Drop all in-memory state back to empty.
    Reset {
        response: Option<oneshot::Sender<()>>,
    },
    /// Stop the actor.
    Shutdown {
        response: Option<oneshot::Sender<()>>,
    },
}

/// The per-partition stream control actor loop. Owns the `StreamState` and
/// processes commands sequentially on a single task, so the state needs no
/// locking, exactly like the work queue's control loop. Returns when the channel
/// closes (all senders dropped) or a `Shutdown` is received, so when the
/// substrate drops the partition the sender drops and the loop exits on its own.
pub async fn run_stream_control(mut state: StreamState, mut rx: mpsc::Receiver<StreamCommand>) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            StreamCommand::CommitCursor {
                name,
                offset,
                response,
            } => {
                state.commit_cursor(name, offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
            }
            StreamCommand::CommitCursors { commits, response } => {
                for (name, offset) in commits {
                    state.commit_cursor(name, offset);
                }
                if let Some(r) = response {
                    let _ = r.send(());
                }
            }
            StreamCommand::GetCursor { name, response } => {
                let _ = response.send(state.cursor(&name));
            }
            StreamCommand::ListCursors { response } => {
                let _ = response.send(state.cursors_snapshot());
            }
            StreamCommand::RemoveCursor { name, response } => {
                let removed = state.remove_cursor(&name);
                if let Some(r) = response {
                    let _ = r.send(removed);
                }
            }
            StreamCommand::SetRetention { config, response } => {
                state.set_retention(config);
                if let Some(r) = response {
                    let _ = r.send(());
                }
            }
            StreamCommand::AdvanceTail {
                next_offset,
                response,
            } => {
                state.advance_tail(next_offset);
                if let Some(r) = response {
                    let _ = r.send(());
                }
            }
            StreamCommand::ApplyTruncation { new_head, response } => {
                let lagged = state.apply_truncation(new_head);
                if let Some(r) = response {
                    let _ = r.send(lagged);
                }
            }
            StreamCommand::GetHeadTail { response } => {
                let _ = response.send((state.head(), state.tail()));
            }
            StreamCommand::GetRetentionState { response } => {
                let _ = response.send((state.retention(), state.head(), state.tail()));
            }
            StreamCommand::EncodeSnapshot {
                last_event_offset,
                response,
            } => {
                let _ = response.send(state.encode_snapshot(last_event_offset));
            }
            StreamCommand::LoadSnapshot { bytes, response } => {
                let _ = response.send(state.load_snapshot(&bytes));
            }
            StreamCommand::Reset { response } => {
                StreamState::reset(&mut state);
                if let Some(r) = response {
                    let _ = r.send(());
                }
            }
            StreamCommand::Shutdown { response } => {
                if let Some(r) = response {
                    let _ = r.send(());
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> StreamState {
        let mut s = StreamState::new("sensors".into(), 0);
        s.advance_tail(1000);
        s
    }

    #[test]
    fn commit_and_read_cursor() {
        let mut s = state();
        s.commit_cursor("group-a", 42);
        assert_eq!(s.cursor("group-a"), Some(42));
        assert_eq!(s.cursor("group-b"), None);
    }

    #[test]
    fn commit_clamps_into_retained_window() {
        let mut s = state();
        s.apply_truncation(100);
        // below head clamps up, past tail clamps down
        s.commit_cursor("low", 10);
        s.commit_cursor("high", 99_999);
        assert_eq!(s.cursor("low"), Some(100));
        assert_eq!(s.cursor("high"), Some(1000));
    }

    #[test]
    fn truncation_advances_head_and_flags_lagged_cursors() {
        let mut s = state();
        s.commit_cursor("behind", 50);
        s.commit_cursor("ahead", 500);
        let lagged = s.apply_truncation(200);
        assert_eq!(lagged, vec!["behind".to_string()]);
        assert_eq!(s.cursor("behind"), Some(200));
        assert_eq!(s.cursor("ahead"), Some(500));
        assert_eq!(s.head(), 200);
    }

    #[test]
    fn truncation_is_monotonic() {
        let mut s = state();
        s.apply_truncation(300);
        let lagged = s.apply_truncation(100);
        assert!(lagged.is_empty());
        assert_eq!(s.head(), 300);
    }

    #[test]
    fn tail_advance_is_monotonic() {
        let mut s = StreamState::new("t".into(), 0);
        s.advance_tail(10);
        s.advance_tail(5);
        assert_eq!(s.tail(), 10);
    }

    #[test]
    fn snapshot_round_trip() {
        let mut s = state();
        s.set_retention(RetentionConfig {
            max_age_ms: Some(3_600_000),
            max_bytes: None,
            max_records: Some(1_000_000),
        });
        s.apply_truncation(100);
        s.commit_cursor("group-a", 150);
        s.commit_cursor("group-b", 900);
        s.set_snapshot_meta(12_345, 678);

        let blob = s.encode_snapshot(678);

        let mut restored = StreamState::new("sensors".into(), 0);
        let meta = restored.load_snapshot(&blob).expect("load");

        assert_eq!(meta.last_snapshot_event_offset, 678);
        assert_eq!(meta.last_snapshot_timestamp, 12_345);
        assert_eq!(meta.default_message_ttl_ms, None);
        assert_eq!(restored.head(), 100);
        assert_eq!(restored.tail(), 1000);
        assert_eq!(restored.retention().max_age_ms, Some(3_600_000));
        assert_eq!(restored.retention().max_records, Some(1_000_000));
        assert_eq!(restored.retention().max_bytes, None);
        assert_eq!(restored.cursor("group-a"), Some(150));
        assert_eq!(restored.cursor("group-b"), Some(900));
    }

    #[test]
    fn load_rejects_unknown_version() {
        let mut bad = 99u64.to_be_bytes().to_vec();
        bad.extend_from_slice(&[0u8; 32]);
        let mut s = StreamState::new("t".into(), 0);
        assert!(s.load_snapshot(&bad).is_err());
    }

    #[test]
    fn reset_clears_everything() {
        let mut s = state();
        s.commit_cursor("g", 10);
        s.apply_truncation(5);
        PartitionEngine::reset(&mut s);
        assert_eq!(s.cursor_count(), 0);
        assert_eq!(s.head(), 0);
        assert_eq!(s.tail(), 0);
        assert_eq!(s.kind(), PartitionKind::Stream);
    }

    async fn ask<T>(
        tx: &mpsc::Sender<StreamCommand>,
        make: impl FnOnce(oneshot::Sender<T>) -> StreamCommand,
    ) -> T {
        let (rtx, rrx) = oneshot::channel();
        tx.send(make(rtx)).await.expect("send");
        rrx.await.expect("reply")
    }

    #[tokio::test]
    async fn actor_commits_reads_and_snapshots() {
        let mut s = StreamState::new("sensors".into(), 0);
        s.advance_tail(1000);
        let (tx, rx) = mpsc::channel(16);
        let actor = tokio::spawn(run_stream_control(s, rx));

        tx.send(StreamCommand::CommitCursor {
            name: "group-a".into(),
            offset: 250,
            response: None,
        })
        .await
        .expect("send");

        let got = ask(&tx, |response| StreamCommand::GetCursor {
            name: "group-a".into(),
            response,
        })
        .await;
        assert_eq!(got, Some(250));

        // Truncating past a cursor clamps it up and flags it lagged.
        let lagged = ask(&tx, |response| StreamCommand::ApplyTruncation {
            new_head: 300,
            response: Some(response),
        })
        .await;
        assert_eq!(lagged, vec!["group-a".to_string()]);

        let blob = ask(&tx, |response| StreamCommand::EncodeSnapshot {
            last_event_offset: 42,
            response,
        })
        .await;

        // A fresh actor restores the same state from the snapshot blob.
        let (tx2, rx2) = mpsc::channel(16);
        let actor2 = tokio::spawn(run_stream_control(
            StreamState::new("sensors".into(), 0),
            rx2,
        ));
        let meta = ask(&tx2, |response| StreamCommand::LoadSnapshot {
            bytes: blob,
            response,
        })
        .await
        .expect("load");
        assert_eq!(meta.last_snapshot_event_offset, 42);
        let restored = ask(&tx2, |response| StreamCommand::GetCursor {
            name: "group-a".into(),
            response,
        })
        .await;
        assert_eq!(restored, Some(300));
        let (head, _tail) = ask(&tx2, |response| StreamCommand::GetHeadTail { response }).await;
        assert_eq!(head, 300);

        drop(tx);
        drop(tx2);
        actor.await.expect("actor join");
        actor2.await.expect("actor2 join");
    }

    #[tokio::test]
    async fn actor_exits_when_sender_dropped() {
        let (tx, rx) = mpsc::channel(4);
        let actor = tokio::spawn(run_stream_control(StreamState::new("t".into(), 0), rx));
        drop(tx);
        actor.await.expect("actor should exit when channel closes");
    }
}
