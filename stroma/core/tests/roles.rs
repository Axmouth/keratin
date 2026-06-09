use std::time::Duration;

use keratin_log::{
    CompletionPair, KDurability, KeratinAppendCompletion, KeratinConfig, Message,
    ReplicatedAppendOutcome, test_dir, util::TempDir,
};
use stroma_core::{
    DeadLetterMeta, DeadLetterReason, EnqueueEventMeta, MessageHeaders, QueueDemotionOutcome,
    QueueHandle, QueueHandleError, QueuePromotionOutcome, QueueRole, ReplicatedEventBatch,
    ReplicatedMessageBatch, SnapshotConfig, Stroma, StromaError, StromaEvent, StromaKeratinConfig,
};
use tokio::time::{Instant, timeout};

fn delayed_fsync_config() -> StromaKeratinConfig {
    let log = KeratinConfig {
        default_durability: KDurability::AfterFsync,
        batch_linger_ms: 0,
        fsync_interval_ms: 250,
        ..KeratinConfig::test_default()
    };
    StromaKeratinConfig::from_message_log(log)
}

async fn open_test_stroma(name: &str) -> (Stroma, TempDir) {
    let dir = test_dir!(name);
    let stroma = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    (stroma, dir)
}

async fn open_delayed_fsync_stroma(name: &str) -> (Stroma, TempDir) {
    let dir = test_dir!(name);
    let stroma = Stroma::open(&dir.root, delayed_fsync_config(), SnapshotConfig::default())
        .await
        .unwrap();
    (stroma, dir)
}

async fn wait_for_active_owner_operation(qh: &QueueHandle) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(1) {
        if qh.active_owner_operations() > 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("owner operation did not become active");
}

fn assert_wrong_role<T>(result: Result<T, QueueHandleError>, actual: QueueRole) {
    assert!(matches!(
        result,
        Err(QueueHandleError::WrongRole {
            expected: QueueRole::Owner,
            actual: got,
        }) if got == actual
    ));
}

fn assert_stroma_wrong_role<T>(result: Result<T, StromaError>, actual: QueueRole) {
    assert!(matches!(
        result,
        Err(StromaError::WrongQueueRole {
            expected: QueueRole::Owner,
            actual: got,
        }) if got == actual
    ));
}

#[tokio::test]
async fn follower_handle_rejects_owner_operations() {
    let (st, _dir) = open_test_stroma("stroma_roles_follower_handle_rejects_owner_ops").await;
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();

    qh.enqueue(0, 0).await.unwrap();
    qh.become_follower();

    assert_wrong_role(qh.enqueue(1, 0).await, QueueRole::Follower);
    assert_wrong_role(qh.ack(0).await, QueueRole::Follower);
    assert_wrong_role(qh.poll_ready_and_mark(1, 1_000).await, QueueRole::Follower);
}

#[tokio::test]
async fn public_event_append_rejects_follower_before_log_append() {
    let (st, _dir) = open_test_stroma("stroma_roles_reject_before_log_append").await;
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    let before = qh.event_log().next_offset();

    qh.become_follower();
    let result = st.ack_batch("topic-a".into(), 0, None, &[0]).await;

    assert_stroma_wrong_role(result, QueueRole::Follower);
    assert_eq!(qh.event_log().next_offset(), before);
}

#[tokio::test]
async fn expiry_scan_skips_follower_queues() {
    let (st, _dir) = open_test_stroma("stroma_roles_expiry_skips_followers").await;
    let owner = st.queue_handle("owner", 0, None).await.unwrap();
    let follower = st.queue_handle("follower", 0, None).await.unwrap();

    owner.enqueue(0, 0).await.unwrap();
    owner.mark_inflight(0, 10).await.unwrap();
    follower.enqueue(0, 0).await.unwrap();
    follower.mark_inflight(0, 10).await.unwrap();
    follower.become_follower();

    let expired = st.collect_expired(20, 10).await.unwrap();
    assert_eq!(expired, vec![("owner".to_string(), 0, None, 0)]);
}

#[tokio::test]
async fn freeze_waits_for_active_owner_operation_before_role_swap() {
    let (st, _dir) = open_test_stroma("stroma_roles_freeze_waits_for_owner_operation").await;
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    let owner_operation = qh.begin_owner_operation().unwrap();

    let freezer = {
        let st = st.clone();
        tokio::spawn(async move {
            st.freeze_queue_for_transition("topic-a", 0, None)
                .await
                .unwrap();
        })
    };

    tokio::task::yield_now().await;
    assert_eq!(qh.role(), QueueRole::Frozen);
    assert_eq!(qh.active_owner_operations(), 1);
    assert_wrong_role(qh.enqueue(1, 0).await, QueueRole::Frozen);

    drop(owner_operation);
    freezer.await.unwrap();
    assert_eq!(qh.active_owner_operations(), 0);

    st.become_queue_follower("topic-a", 0, None).await.unwrap();
    assert_eq!(qh.role(), QueueRole::Follower);
    assert_wrong_role(qh.enqueue(2, 0).await, QueueRole::Follower);

    st.become_queue_owner("topic-a", 0, None).await.unwrap();
    qh.enqueue(3, 0).await.unwrap();
}

#[tokio::test]
async fn demotion_freezes_drains_and_switches_owner_to_follower() {
    let (st, _dir) = open_delayed_fsync_stroma("stroma_roles_demote_owner_to_follower").await;
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    let headers = MessageHeaders {
        published: 1,
        publish_received: 2,
        content_type: None,
        extra: Default::default(),
    };
    let (completion, rx) = KeratinAppendCompletion::pair();

    st.append_message(
        "topic-a",
        0,
        None,
        &headers,
        b"accepted".to_vec(),
        completion,
    )
    .await
    .unwrap();
    wait_for_active_owner_operation(&qh).await;

    let demoter = {
        let st = st.clone();
        tokio::spawn(async move {
            st.demote_queue_owner_to_follower("topic-a", 0, None)
                .await
                .unwrap()
        })
    };

    tokio::task::yield_now().await;
    assert_eq!(qh.role(), QueueRole::Frozen);
    assert_wrong_role(qh.enqueue(1, 0).await, QueueRole::Frozen);

    let appended = timeout(Duration::from_secs(2), rx)
        .await
        .expect("owner publish before demotion did not complete")
        .unwrap()
        .unwrap();
    assert_eq!(appended.base_offset, 0);

    let outcome = timeout(Duration::from_secs(2), demoter)
        .await
        .expect("demotion did not finish")
        .unwrap();
    assert_eq!(
        outcome,
        QueueDemotionOutcome {
            message_next_offset: 1,
            event_next_offset: 1,
            applied_event_offset: Some(0),
        }
    );
    assert_eq!(qh.role(), QueueRole::Follower);
    assert_wrong_role(qh.enqueue(2, 0).await, QueueRole::Follower);

    st.apply_replicated_queue_batch(
        "topic-a",
        0,
        None,
        Some(ReplicatedMessageBatch {
            epoch: 0,
            first_offset: 1,
            records: vec![Message {
                flags: 0,
                headers: vec![],
                payload: b"replicated".to_vec(),
            }],
            durability: Some(KDurability::AfterFsync),
        }),
        Some(ReplicatedEventBatch {
            epoch: 0,
            first_offset: 1,
            events: vec![StromaEvent::Enqueue { off: 1, retries: 0 }],
            durability: Some(KDurability::AfterFsync),
        }),
    )
    .await
    .unwrap();
    assert!(qh.is_ready(1).await);
}

#[tokio::test]
async fn demotion_refuses_non_owner_without_changing_role() {
    let (st, _dir) = open_test_stroma("stroma_roles_demote_refuses_non_owner").await;
    st.become_queue_follower("topic-a", 0, None).await.unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();

    let result = st.demote_queue_owner_to_follower("topic-a", 0, None).await;

    assert_stroma_wrong_role(result, QueueRole::Follower);
    assert_eq!(qh.role(), QueueRole::Follower);
}

#[tokio::test]
async fn freeze_allows_started_publish_to_finish_but_rejects_new_publish() {
    let (st, _dir) = open_delayed_fsync_stroma("stroma_roles_freeze_started_publish").await;
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    let headers = MessageHeaders {
        published: 1,
        publish_received: 2,
        content_type: None,
        extra: Default::default(),
    };
    let (completion, rx) = KeratinAppendCompletion::pair();

    st.append_message(
        "topic-a",
        0,
        None,
        &headers,
        b"accepted".to_vec(),
        completion,
    )
    .await
    .unwrap();
    wait_for_active_owner_operation(&qh).await;

    let freezer = {
        let st = st.clone();
        tokio::spawn(async move {
            st.freeze_queue_for_transition("topic-a", 0, None)
                .await
                .unwrap();
        })
    };

    tokio::task::yield_now().await;
    assert_eq!(qh.role(), QueueRole::Frozen);
    let (blocked_completion, _blocked_rx) = KeratinAppendCompletion::pair();
    let blocked = st
        .append_message(
            "topic-a",
            0,
            None,
            &headers,
            b"blocked".to_vec(),
            blocked_completion,
        )
        .await;
    assert_stroma_wrong_role(blocked, QueueRole::Frozen);

    let appended = timeout(Duration::from_secs(2), rx)
        .await
        .expect("accepted publish did not complete")
        .unwrap()
        .unwrap();
    assert_eq!(appended.base_offset, 0);
    freezer.await.unwrap();
    assert_eq!(qh.active_owner_operations(), 0);
    assert!(qh.is_ready(0).await);
}

#[tokio::test]
async fn freeze_allows_started_ack_to_finish_but_rejects_new_ack() {
    let (st, _dir) = open_delayed_fsync_stroma("stroma_roles_freeze_started_ack").await;
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    qh.enqueue(0, 0).await.unwrap();
    qh.mark_inflight(0, 60_000).await.unwrap();

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.ack_enqueue("topic-a", 0, None, 0, completion)
        .await
        .unwrap();
    wait_for_active_owner_operation(&qh).await;

    let freezer = {
        let st = st.clone();
        tokio::spawn(async move {
            st.freeze_queue_for_transition("topic-a", 0, None)
                .await
                .unwrap();
        })
    };

    tokio::task::yield_now().await;
    assert_eq!(qh.role(), QueueRole::Frozen);
    let (blocked_completion, _blocked_rx) = KeratinAppendCompletion::pair();
    let blocked = st
        .ack_enqueue("topic-a", 0, None, 1, blocked_completion)
        .await;
    assert_stroma_wrong_role(blocked, QueueRole::Frozen);

    timeout(Duration::from_secs(2), rx)
        .await
        .expect("accepted ack did not complete")
        .unwrap()
        .unwrap();
    freezer.await.unwrap();
    assert_eq!(qh.active_owner_operations(), 0);
    assert!(st.is_acked("topic-a", 0, None, 0).await.unwrap());
}

#[tokio::test]
async fn follower_ingest_applies_replicated_messages_and_events() {
    let (st, _dir) = open_test_stroma("stroma_roles_follower_ingest").await;
    st.become_queue_follower("topic-a", 0, None).await.unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    let headers = MessageHeaders {
        published: 1,
        publish_received: 2,
        content_type: None,
        extra: Default::default(),
    };

    let outcome = st
        .apply_replicated_queue_batch(
            "topic-a",
            0,
            None,
            Some(ReplicatedMessageBatch {
                epoch: 0,
                first_offset: 0,
                records: vec![
                    Message {
                        flags: 0,
                        headers: headers.encode().unwrap(),
                        payload: b"one".to_vec(),
                    },
                    Message {
                        flags: 0,
                        headers: headers.encode().unwrap(),
                        payload: b"two".to_vec(),
                    },
                ],
                durability: Some(KDurability::AfterFsync),
            }),
            Some(ReplicatedEventBatch {
                epoch: 0,
                first_offset: 0,
                events: vec![StromaEvent::EnqueueMany {
                    reqs: vec![
                        EnqueueEventMeta { off: 0, retries: 0 },
                        EnqueueEventMeta { off: 1, retries: 0 },
                    ],
                }],
                durability: Some(KDurability::AfterFsync),
            }),
        )
        .await
        .unwrap();

    assert!(matches!(
        outcome.message_log,
        Some(ReplicatedAppendOutcome::Applied(result))
            if result.base_offset == 0 && result.count == 2
    ));
    assert!(matches!(
        outcome.event_log,
        Some(ReplicatedAppendOutcome::Applied(result))
            if result.base_offset == 0 && result.count == 1
    ));
    assert_eq!(qh.role(), QueueRole::Follower);
    assert!(qh.is_ready(0).await);
    assert!(qh.is_ready(1).await);
    let fetched = st.fetch_message_by_offset(&qh, 1).await.unwrap().unwrap();
    assert_eq!(fetched.payload, b"two");
    assert_wrong_role(qh.enqueue(2, 0).await, QueueRole::Follower);
}

#[tokio::test]
async fn replicated_ingest_rejects_owner_queue() {
    let (st, _dir) = open_test_stroma("stroma_roles_replicated_ingest_rejects_owner").await;

    let result = st
        .apply_replicated_queue_batch(
            "topic-a",
            0,
            None,
            None,
            Some(ReplicatedEventBatch {
                epoch: 0,
                first_offset: 0,
                events: vec![StromaEvent::Enqueue { off: 0, retries: 0 }],
                durability: Some(KDurability::AfterFsync),
            }),
        )
        .await;

    assert!(matches!(
        result,
        Err(StromaError::WrongQueueRole {
            expected: QueueRole::Follower,
            actual: QueueRole::Owner,
        })
    ));
}

#[tokio::test]
async fn source_follower_ingests_dlq_events_without_writing_target_queue() {
    let (st, _dir) = open_test_stroma("stroma_roles_follower_dlq_boundary").await;
    st.become_queue_follower("src", 0, None).await.unwrap();
    let src = st.queue_handle("src", 0, None).await.unwrap();

    st.apply_replicated_queue_batch(
        "src",
        0,
        None,
        Some(ReplicatedMessageBatch {
            epoch: 0,
            first_offset: 0,
            records: vec![Message {
                flags: 0,
                headers: vec![],
                payload: b"source".to_vec(),
            }],
            durability: Some(KDurability::AfterFsync),
        }),
        Some(ReplicatedEventBatch {
            epoch: 0,
            first_offset: 0,
            events: vec![
                StromaEvent::Enqueue { off: 0, retries: 0 },
                StromaEvent::DeadLetter {
                    reqs: vec![DeadLetterMeta {
                        off: 0,
                        retry_count: 1,
                        reason: DeadLetterReason::RetriesExhausted,
                        target_tp: "dlq".into(),
                        target_part: 0,
                        target_group: None,
                    }],
                },
            ],
            durability: Some(KDurability::AfterFsync),
        }),
    )
    .await
    .unwrap();

    assert_eq!(src.pending_dlq().await.unwrap().len(), 1);
    assert!(
        !st.is_materialized("dlq", 0, None),
        "source follower must not materialize or write the DLQ target queue"
    );
}

#[tokio::test]
async fn caught_up_follower_can_promote_and_accept_owner_writes() {
    let (st, _dir) = open_test_stroma("stroma_roles_promote_caught_up_follower").await;
    st.become_queue_follower("topic-a", 0, None).await.unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();

    st.apply_replicated_queue_batch(
        "topic-a",
        0,
        None,
        Some(ReplicatedMessageBatch {
            epoch: 0,
            first_offset: 0,
            records: vec![Message {
                flags: 0,
                headers: vec![],
                payload: b"replicated".to_vec(),
            }],
            durability: Some(KDurability::AfterFsync),
        }),
        Some(ReplicatedEventBatch {
            epoch: 0,
            first_offset: 0,
            events: vec![StromaEvent::Enqueue { off: 0, retries: 0 }],
            durability: Some(KDurability::AfterFsync),
        }),
    )
    .await
    .unwrap();

    let promoted = st
        .promote_queue_follower_if_caught_up("topic-a", 0, None, 1, 1)
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 1,
            event_next_offset: 1,
            applied_event_offset: Some(0),
        }
    );
    assert_eq!(qh.role(), QueueRole::Owner);

    let headers = MessageHeaders {
        published: 3,
        publish_received: 4,
        content_type: None,
        extra: Default::default(),
    };
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("topic-a", 0, None, &headers, b"owner".to_vec(), completion)
        .await
        .unwrap();
    let appended = timeout(Duration::from_secs(2), rx)
        .await
        .expect("owner publish after promotion did not complete")
        .unwrap()
        .unwrap();
    assert_eq!(appended.base_offset, 1);
    assert!(qh.is_ready(1).await);
}

#[tokio::test]
async fn promotion_reports_follower_lag_without_changing_role() {
    let (st, _dir) = open_test_stroma("stroma_roles_promote_reports_lag").await;
    st.become_queue_follower("topic-a", 0, None).await.unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();

    let outcome = st
        .promote_queue_follower_if_caught_up("topic-a", 0, None, 1, 1)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        QueuePromotionOutcome::MessageLogBehind {
            local_next_offset: 0,
            expected_next_offset: 1,
        }
    );
    assert_eq!(qh.role(), QueueRole::Follower);
    assert_wrong_role(qh.enqueue(0, 0).await, QueueRole::Follower);
}

#[tokio::test]
async fn promotion_refuses_unexpected_local_tail_without_changing_role() {
    let (st, _dir) = open_test_stroma("stroma_roles_promote_reports_ahead").await;
    st.become_queue_follower("topic-a", 0, None).await.unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();

    st.apply_replicated_queue_batch(
        "topic-a",
        0,
        None,
        Some(ReplicatedMessageBatch {
            epoch: 0,
            first_offset: 0,
            records: vec![Message {
                flags: 0,
                headers: vec![],
                payload: b"replicated".to_vec(),
            }],
            durability: Some(KDurability::AfterFsync),
        }),
        Some(ReplicatedEventBatch {
            epoch: 0,
            first_offset: 0,
            events: vec![StromaEvent::Enqueue { off: 0, retries: 0 }],
            durability: Some(KDurability::AfterFsync),
        }),
    )
    .await
    .unwrap();

    let outcome = st
        .promote_queue_follower_if_caught_up("topic-a", 0, None, 0, 1)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        QueuePromotionOutcome::MessageLogAhead {
            local_next_offset: 1,
            expected_next_offset: 0,
        }
    );
    assert_eq!(qh.role(), QueueRole::Follower);
    assert_wrong_role(qh.enqueue(0, 0).await, QueueRole::Follower);
}
