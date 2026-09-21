use std::sync::Arc;

use keratin_log::{KeratinConfig, test_dir};
use similar_asserts::assert_eq;
use stroma_core::*;

#[tokio::test]
async fn delayed_publish_nack_preserves_retry_count_on_log_replay() {
    let dir = test_dir!("delayed_nack_replay");
    let cfg = StromaKeratinConfig::from_message_log(KeratinConfig::test_default());
    let st = Stroma::open(&dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    let (completion, done) = KeratinAppendCompletion::pair();
    st.append_message_batch(
        "q",
        0,
        None,
        vec![PublishItem {
            headers: MessageHeaders {
                published: 0,
                publish_received: 0,
                content_type: None,
                extra: Default::default(),
            },
            payload: b"delayed".to_vec(),
            not_before: Some(100),
            expire_at: None,
            completion,
        }],
    )
    .await
    .unwrap();
    done.await.unwrap().unwrap();
    st.collect_expired(100, 10).await.unwrap();
    // Delivery leases are normally actor-local. A consumer NACK is logged.
    {
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let handle = ticket.resolve().unwrap();
        let q = handle.work_queue().unwrap();
        q.mark_inflight(0, 1000).await.unwrap();
    }
    st.nack_one("q", 0, None, 0, true).await.unwrap();
    let before = {
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        ticket
            .resolve()
            .unwrap()
            .work_queue()
            .unwrap()
            .retries(0)
            .await
    };
    assert_eq!(before, 1);

    // A follower does not run owner timers or inherit local delivery leases.
    let follower_dir = test_dir!("delayed_nack_follower");
    let follower = Stroma::open(&follower_dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    follower.become_queue_follower("q", 0, None).await.unwrap();
    let OwnerReplicationRead::Batch(messages) = st
        .read_owner_message_records("q", 0, None, 0, 100)
        .await
        .unwrap()
    else {
        panic!("message batch")
    };
    let OwnerReplicationRead::Batch(events) = st
        .read_owner_event_records("q", 0, None, 0, 100)
        .await
        .unwrap()
    else {
        panic!("event batch")
    };
    assert!(
        events
            .records
            .iter()
            .any(|(_, event)| matches!(event, StromaEvent::ActivateDelayed { .. }))
    );
    follower
        .apply_replicated_queue_batch(
            "q",
            0,
            None,
            Some(ReplicatedMessageBatch {
                epoch: messages.epoch,
                first_offset: 0,
                records: messages
                    .records
                    .into_iter()
                    .map(|(_, record)| record)
                    .collect(),
                durability: None,
            }),
            Some(ReplicatedEventBatch {
                epoch: events.epoch,
                first_offset: 0,
                events: events.records.into_iter().map(|(_, event)| event).collect(),
                durability: None,
            }),
        )
        .await
        .unwrap();
    {
        let ticket = follower.queue_handle("q", 0, None).await.unwrap();
        assert_eq!(
            ticket
                .resolve()
                .unwrap()
                .work_queue()
                .unwrap()
                .retries(0)
                .await,
            before
        );
    }
    follower.shutdown().await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);
    let reopened = Stroma::open(&dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    let ticket = reopened.queue_handle("q", 0, None).await.unwrap();
    let handle = ticket.resolve().unwrap();
    assert_eq!(handle.work_queue().unwrap().retries(0).await, before);
    drop(handle);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn delayed_activation_has_bounded_batches_and_no_idle_writes() {
    let dir = test_dir!("delayed_activation_bounds");
    let cfg = StromaKeratinConfig::from_message_log(KeratinConfig::test_default());
    let st = Stroma::open(&dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    for _ in 0..3 {
        let (completion, done) = KeratinAppendCompletion::pair();
        st.append_message_batch(
            "q",
            0,
            None,
            vec![PublishItem {
                headers: MessageHeaders {
                    published: 0,
                    publish_received: 0,
                    content_type: None,
                    extra: Default::default(),
                },
                payload: vec![1],
                not_before: Some(100),
                expire_at: None,
                completion,
            }],
        )
        .await
        .unwrap();
        done.await.unwrap().unwrap();
    }
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let handle = ticket.resolve().unwrap();
    let q = handle.work_queue().unwrap();
    let initial = handle.event_log().next_offset();
    st.collect_expired(99, 100).await.unwrap();
    st.collect_expired(100, 0).await.unwrap();
    assert_eq!(handle.event_log().next_offset(), initial);
    st.collect_expired(100, 1).await.unwrap();
    assert_eq!(q.status_report().await.unwrap().ready_count, 1);
    assert_eq!(handle.event_log().next_offset(), initial + 1);
    st.collect_expired(100, 1).await.unwrap();
    st.collect_expired(100, 1).await.unwrap();
    assert_eq!(q.status_report().await.unwrap().ready_count, 3);
    let complete = handle.event_log().next_offset();
    st.collect_expired(u64::MAX, 10).await.unwrap();
    assert_eq!(handle.event_log().next_offset(), complete);
    drop(handle);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshot_delta_replay_is_deterministic() {
    let dir = test_dir!("stroma_replay");
    let kcfg = StromaKeratinConfig::from_message_log(KeratinConfig::test_default());
    let scfg = SnapshotConfig::default();

    let st = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    for i in 0..500 {
        st.mark_inflight_one("t", 0, None, i, 1000000000 + i)
            .await
            .unwrap();
        if i.is_multiple_of(3) {
            st.ack_one("t", 0, None, i).await.unwrap();
        }
    }

    // snapshot logical state BEFORE drop
    let before = st.debug_dump_queue("t", 0, None).await.unwrap();

    // force persistence so restart is deterministic
    st.snapshot_partition("t", 0, None).await.unwrap();
    st.shutdown().await.unwrap();

    drop(st);

    let st2 = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    let after = st2.debug_dump_queue("t", 0, None).await.unwrap();

    assert_eq!(before, after);
}

#[tokio::test]
async fn expired_messages_survive_restart() {
    let dir = test_dir!("expiry_restart");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    let (c, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        content_type: None,
        extra: Default::default(),
    };
    st.append_message("t", 0, None, &headers, b"x".to_vec(), c)
        .await
        .unwrap();
    st.mark_inflight_one("t", 0, None, 0, 10).await.unwrap();
    let offset = rx.await.unwrap().unwrap().base_offset;

    st.collect_expired(100, 10).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    assert!(st2.is_ready("t", 0, None, offset).await.unwrap());
}

#[tokio::test]
async fn discover_partitions_handles_encoded_names() {
    // create dirs like:
    // events/group%2Fa/topic%2Fb/0000000001
    let dir = test_dir!("discover_partitions_handles_encoded_names");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    st.queue_handle("a", 1, Some("b")).await.unwrap();
    st.queue_handle("c", 2, Some("d")).await.unwrap();
    st.queue_handle("topic+\\/i", 3, Some("group+\\/j"))
        .await
        .unwrap();
    st.queue_handle("topic/e", 4, Some("group/f"))
        .await
        .unwrap();
    st.queue_handle("topic g", 5, Some("group h"))
        .await
        .unwrap();

    let mut parts = st.discover_partitions().unwrap();
    parts.sort();

    assert_eq!(
        parts,
        vec![
            (Some("b".to_string()), "a".to_string(), 1),
            (Some("d".to_string()), "c".to_string(), 2),
            (Some("group h".to_string()), "topic g".to_string(), 5),
            (Some("group+\\/j".to_string()), "topic+\\/i".to_string(), 3),
            (Some("group/f".to_string()), "topic/e".to_string(), 4),
        ]
    );
}
