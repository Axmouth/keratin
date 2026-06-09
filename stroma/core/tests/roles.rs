use keratin_log::{KeratinConfig, test_dir};
use stroma_core::{
    QueueHandleError, QueueRole, SnapshotConfig, Stroma, StromaError, StromaKeratinConfig,
};

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
    let dir = test_dir!("stroma_roles_follower_handle_rejects_owner_ops");
    let st = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();

    qh.enqueue(0, 0).await.unwrap();
    qh.become_follower();

    assert_wrong_role(qh.enqueue(1, 0).await, QueueRole::Follower);
    assert_wrong_role(qh.ack(0).await, QueueRole::Follower);
    assert_wrong_role(qh.poll_ready_and_mark(1, 1_000).await, QueueRole::Follower);
}

#[tokio::test]
async fn public_event_append_rejects_follower_before_log_append() {
    let dir = test_dir!("stroma_roles_reject_before_log_append");
    let st = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let qh = st.queue_handle("topic-a", 0, None).await.unwrap();
    let before = qh.event_log().next_offset();

    qh.become_follower();
    let result = st.ack_batch("topic-a".into(), 0, None, &[0]).await;

    assert_stroma_wrong_role(result, QueueRole::Follower);
    assert_eq!(qh.event_log().next_offset(), before);
}

#[tokio::test]
async fn expiry_scan_skips_follower_queues() {
    let dir = test_dir!("stroma_roles_expiry_skips_followers");
    let st = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
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
