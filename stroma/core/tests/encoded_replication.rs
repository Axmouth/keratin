use keratin_log::{KDurability, KeratinAppendCompletion, KeratinConfig, test_dir};
use stroma_core::*;

fn headers() -> MessageHeaders {
    MessageHeaders {
        published: 1,
        publish_received: 2,
        content_type: None,
        extra: Default::default(),
    }
}

async fn owner_records(st: &Stroma) -> OwnerReplicationBatch<EncodedEvent> {
    let OwnerReplicationRead::Batch(batch) = st
        .read_owner_queue_encoded("q", 0, None, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("queue batch")
    };
    batch
}

/// Owner records travel as their stored bytes, and a follower that appends
/// those bytes ends up with the same log and the same queue state.
#[tokio::test]
async fn follower_appends_owner_bytes_unchanged() {
    let cfg = StromaKeratinConfig::from_message_log(KeratinConfig::test_default());
    let owner_dir = test_dir!("encoded_replication_owner");
    let owner = Stroma::open(&owner_dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    for body in [&b"one"[..], b"two", b"three"] {
        let (completion, done) = KeratinAppendCompletion::pair();
        owner
            .append_message("q", 0, None, &headers(), body.to_vec(), completion)
            .await
            .unwrap();
        done.await.unwrap().unwrap();
    }
    owner.ack_one("q", 0, None, 0).await.unwrap();
    let sent = owner_records(&owner).await;
    assert!(sent.records.len() >= 4);

    let follower_dir = test_dir!("encoded_replication_follower");
    let follower = Stroma::open(&follower_dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    follower.become_queue_follower("q", 0, None).await.unwrap();
    let batch = ReplicatedEventBatch::from_encoded(
        sent.epoch,
        0,
        sent.records.iter().map(|(_, record)| record.clone()).collect(),
        Some(KDurability::AfterFsync),
    )
    .unwrap();
    follower
        .apply_replicated_queue_batch("q", 0, None, batch)
        .await
        .unwrap();

    let next = sent.next_offset;
    assert_eq!(
        follower
            .promote_queue_follower_if_caught_up("q", 0, None, next)
            .await
            .unwrap(),
        QueuePromotionOutcome::Promoted { next }
    );
    assert_eq!(owner_records(&follower).await.records, sent.records);
    let ticket = follower.queue_handle("q", 0, None).await.unwrap();
    let handle = ticket.resolve().unwrap();
    let queue = handle.work_queue().unwrap();
    assert!(!queue.is_ready(0).await);
    assert!(queue.is_ready(1).await);
    let fetched = follower.fetch_message_by_offset(&handle, 2).await.unwrap().unwrap();
    assert_eq!(fetched.payload, b"three");
    drop(handle);
    follower.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn follower_refuses_bytes_that_do_not_match_the_events() {
    let cfg = StromaKeratinConfig::from_message_log(KeratinConfig::test_default());
    let dir = test_dir!("encoded_replication_mismatch");
    let follower = Stroma::open(&dir.root, cfg, SnapshotConfig::default())
        .await
        .unwrap();
    follower.become_queue_follower("q", 0, None).await.unwrap();
    let event = StromaEvent::PayloadEnqueue {
        not_before: None,
        expire_at: None,
        headers: headers().encode().unwrap(),
        payload: b"one".to_vec(),
    };
    let mut batch = ReplicatedEventBatch::from_encoded(
        0,
        0,
        vec![EncodedEvent::encode(&event).unwrap()],
        Some(KDurability::AfterFsync),
    )
    .unwrap();
    batch.events.push(event);
    assert!(matches!(
        follower.apply_replicated_queue_batch("q", 0, None, batch).await,
        Err(StromaError::InvalidArgument(_))
    ));
    follower.shutdown().await.unwrap();
}
