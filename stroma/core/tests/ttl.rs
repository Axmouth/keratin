use std::sync::Arc;

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{MessageHeaders, Offset, SnapshotConfig, Stroma, StromaKeratinConfig};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    (
        Arc::new(
            Stroma::open(
                &test_dir.root,
                StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                SnapshotConfig::default(),
            )
            .await
            .unwrap(),
        ),
        test_dir,
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

async fn append_with_ttl(
    st: &Arc<Stroma>,
    tp: &str,
    expire_at: Option<u64>,
) -> Offset {
    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message_with_ttl(tp, 0, None, &headers(), b"x".to_vec(), expire_at, c)
        .await
        .unwrap();
    // Awaiting the completion means the enqueue event is durable and applied.
    rx.await.unwrap().unwrap().base_offset
}

#[tokio::test]
async fn ttl_expired_message_is_dropped_and_settled() {
    let (st, _td) = open_test_stroma().await;
    let off = append_with_ttl(&st, "t", Some(1000)).await;

    // Ready before the deadline.
    assert!(!st.is_inflight_or_acked("t", 0, None, off).await.unwrap());

    // Drop after the deadline. No DLQ configured -> discard (settle).
    let dropped = st.drop_ttl_expired(2000, 100).await.unwrap();
    assert_eq!(dropped.len(), 1);
    assert!(dropped.contains(&("t".to_string(), 0, None, off)));

    // Settled now, so it is no longer deliverable.
    assert!(st.is_inflight_or_acked("t", 0, None, off).await.unwrap());

    // Idempotent: a second sweep drops nothing.
    assert!(st.drop_ttl_expired(2000, 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn ttl_drop_skips_messages_before_deadline_and_without_ttl() {
    let (st, _td) = open_test_stroma().await;
    let with_ttl = append_with_ttl(&st, "t", Some(5000)).await;
    let _no_ttl = append_with_ttl(&st, "t", None).await;

    // now < deadline -> nothing dropped.
    assert!(st.drop_ttl_expired(1000, 100).await.unwrap().is_empty());

    // Past the deadline -> only the TTL message drops.
    let dropped = st.drop_ttl_expired(9000, 100).await.unwrap();
    assert_eq!(dropped.len(), 1);
    assert!(dropped.contains(&("t".to_string(), 0, None, with_ttl)));
}

#[tokio::test]
async fn ttl_drop_never_touches_inflight() {
    let (st, _td) = open_test_stroma().await;
    let off = append_with_ttl(&st, "t", Some(1000)).await;

    // Lease it well past the TTL deadline.
    st.mark_inflight_one("t", 0, None, off, 100_000)
        .await
        .unwrap();

    // Past its deadline but in flight -> must not be dropped.
    assert!(st.drop_ttl_expired(2000, 100).await.unwrap().is_empty());
    assert!(st.is_inflight_or_acked("t", 0, None, off).await.unwrap());
}
