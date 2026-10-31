//! A stream copy that falls behind a failover: the old owner or a copy sealed
//! for recovery rejoins under the new owner's history.

use std::sync::Arc;

use keratin_log::{KDurability, KeratinConfig, Message, test_dir};
use stroma_core::{
    MessageHeaders, RecoverySealRequest, ReplicatedEventBatch, ReplicatedMessageBatch,
    SnapshotConfig, StreamEpochStart, Stroma, StromaEvent, StromaKeratinConfig,
};

async fn open_on(dir: &std::path::Path) -> Arc<Stroma> {
    Arc::new(
        Stroma::open(
            dir,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    )
}

fn headers() -> MessageHeaders {
    MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        content_type: None,
        extra: Default::default(),
    }
}

fn record(payload: &[u8]) -> Message {
    Message {
        flags: 0,
        headers: headers().encode().unwrap(),
        payload: payload.to_vec(),
    }
}

async fn payloads(st: &Stroma, tp: &str) -> Vec<Vec<u8>> {
    st.read_stream_records(tp, 0, 0, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, payload, _)| payload)
        .collect()
}

fn start(epoch: u64, records: u64, cursor_events: u64) -> StreamEpochStart {
    StreamEpochStart {
        epoch,
        records,
        cursor_events,
    }
}

/// The old owner wrote past the promoted copy's tails. It drops that suffix,
/// clamps its cursors to the new tail and then takes the new owner's
/// different entries at the same offsets.
#[tokio::test]
async fn old_owner_cuts_its_unconfirmed_suffix_and_clamps_cursors() {
    let dir = test_dir!("stream_rejoin_old_owner");
    let st = open_on(&dir.root).await;
    st.create_stream("s", 0, None).await.unwrap();
    st.become_stream_owner_with_epoch("s", 0, 1).await.unwrap();
    for payload in [b"a", b"b", b"c", b"x", b"y"] {
        st.append_stream_record("s", 0, &headers(), payload.to_vec())
            .await
            .unwrap();
    }
    // Events 0 and 1 survive the failover, events 2 and 3 do not. The
    // snapshot covers events 0 to 2.
    st.commit_stream_cursor("s", 0, "early", 2).await.unwrap();
    st.commit_stream_cursor("s", 0, "reader", 5).await.unwrap();
    st.commit_stream_cursor("s", 0, "other", 1).await.unwrap();
    st.snapshot_partition("s", 0, None).await.unwrap();
    st.commit_stream_cursor("s", 0, "late", 4).await.unwrap();
    st.sync_stream("s", 0).await.unwrap();

    // The new owner took over at epoch 2 with 3 records and 2 cursor events.
    let starts = [start(2, 3, 2)];
    let rejoin = st
        .rejoin_stream_at_epoch("s", 0, 2, &starts, true)
        .await
        .unwrap();
    assert_eq!((rejoin.record_next, rejoin.event_next), (3, 2));
    assert_eq!((rejoin.records_dropped, rejoin.events_dropped), (2, 2));
    assert!(!rejoin.released_seal && !rejoin.still_sealed);
    st.become_stream_follower_with_epoch("s", 0, 2)
        .await
        .unwrap();

    assert_eq!(st.stream_head_tail("s", 0).await.unwrap(), (0, 3));
    assert_eq!(
        payloads(&st, "s").await,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(st.stream_cursor("s", 0, "early").await.unwrap(), Some(2));
    // Pulled down to the new tail.
    assert_eq!(st.stream_cursor("s", 0, "reader").await.unwrap(), Some(3));
    // Its only commit came after the cut. A snapshot written while the
    // handle closed can still carry it, never above the new tail.
    let late = st.stream_cursor("s", 0, "late").await.unwrap();
    assert!(late.is_none_or(|offset| offset <= 3), "{late:?}");

    st.apply_replicated_stream_batch(
        "s",
        0,
        Some(ReplicatedMessageBatch {
            epoch: 2,
            first_offset: 3,
            records: vec![record(b"new-3")],
            durability: Some(KDurability::AfterFsync),
        }),
        Some(ReplicatedEventBatch {
            epoch: 2,
            first_offset: 2,
            events: vec![StromaEvent::CursorCommit {
                name: "reader".into(),
                offset: 4,
            }],
            durability: Some(KDurability::AfterFsync),
            encoded: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(st.stream_cursor("s", 0, "reader").await.unwrap(), Some(4));

    // Already on the epoch 2 history: a repeat changes nothing.
    let again = st
        .rejoin_stream_at_epoch("s", 0, 2, &starts, true)
        .await
        .unwrap();
    assert_eq!((again.records_dropped, again.events_dropped), (0, 0));

    st.sync_stream("s", 0).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);
    let st = open_on(&dir.root).await;
    let expected = vec![
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
        b"new-3".to_vec(),
    ];
    assert_eq!(payloads(&st, "s").await, expected);
    assert_eq!(st.stream_head_tail("s", 0).await.unwrap(), (0, 4));
    assert_eq!(st.stream_cursor("s", 0, "reader").await.unwrap(), Some(4));
    assert_eq!(st.stream_cursor("s", 0, "early").await.unwrap(), Some(2));
    st.shutdown().await.unwrap();
}

/// The promoted copy holds exactly the new history start. Its seal is kept
/// until it may be released, then released without dropping anything.
#[tokio::test]
async fn sealed_copy_is_released_whole_and_can_take_over() {
    let dir = test_dir!("stream_rejoin_sealed");
    let st = open_on(&dir.root).await;
    st.create_stream("s", 0, None).await.unwrap();
    st.become_stream_owner_with_epoch("s", 0, 1).await.unwrap();
    for payload in [b"a", b"b"] {
        st.append_stream_record("s", 0, &headers(), payload.to_vec())
            .await
            .unwrap();
    }
    st.commit_stream_cursor("s", 0, "reader", 1).await.unwrap();
    st.sync_stream("s", 0).await.unwrap();
    let request = RecoverySealRequest {
        transition: [7; 32],
        fence_epoch: 2,
    };
    let sealed = st
        .seal_replica_for_recovery("s", 0, None, request.clone())
        .await
        .unwrap();
    assert!(sealed.history.logs.is_stream());
    let starts = [start(2, 2, 1)];

    // Not released when the caller may not release it, or for an older epoch.
    for (epoch, release) in [(2, false), (1, true)] {
        let kept = st
            .rejoin_stream_at_epoch("s", 0, epoch, &starts, release)
            .await
            .unwrap();
        assert!(kept.still_sealed);
        assert!(st.stream_head_tail("s", 0).await.is_err(), "still sealed");
    }

    let released = st
        .rejoin_stream_at_epoch("s", 0, 2, &starts, true)
        .await
        .unwrap();
    assert!(released.released_seal);
    assert_eq!((released.records_dropped, released.events_dropped), (0, 0));
    st.become_stream_follower_with_epoch("s", 0, 2)
        .await
        .unwrap();
    st.promote_stream_follower_to_local_tail("s", 0, 2)
        .await
        .unwrap();
    assert_eq!(st.stream_cursor("s", 0, "reader").await.unwrap(), Some(1));
    let next = st
        .append_stream_record("s", 0, &headers(), b"c".to_vec())
        .await
        .unwrap();
    assert_eq!(next, 2);
    st.shutdown().await.unwrap();
}

/// A withdrawn stream recovery leaves its seal. A newer transition at the
/// same fence epoch takes the seal over, a different fence does not.
#[tokio::test]
async fn stream_seal_takes_a_newer_transition_at_the_same_fence() {
    let dir = test_dir!("stream_rejoin_restamp");
    let st = open_on(&dir.root).await;
    st.create_stream("s", 0, None).await.unwrap();
    st.become_stream_owner_with_epoch("s", 0, 1).await.unwrap();
    st.append_stream_record("s", 0, &headers(), b"a".to_vec())
        .await
        .unwrap();
    st.sync_stream("s", 0).await.unwrap();
    let request = |byte: u8, fence_epoch: u64| RecoverySealRequest {
        transition: [byte; 32],
        fence_epoch,
    };
    let first = st
        .seal_replica_for_recovery("s", 0, None, request(1, 2))
        .await
        .unwrap();
    let second = st
        .seal_replica_for_recovery("s", 0, None, request(2, 2))
        .await
        .unwrap();
    assert_eq!(second.request, request(2, 2));
    assert_eq!(second.history.logs, first.history.logs);
    assert!(
        st.seal_replica_for_recovery("s", 0, None, request(3, 3))
            .await
            .is_err()
    );
    st.shutdown().await.unwrap();
    drop(st);
    let st = open_on(&dir.root).await;
    assert_eq!(
        st.seal_replica_for_recovery("s", 0, None, request(2, 2))
            .await
            .unwrap(),
        second
    );
    st.shutdown().await.unwrap();
}
