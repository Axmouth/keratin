use std::sync::Arc;

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{
    DeclareMeta, MessageHeaders, Offset, PublishItem, SnapshotConfig, Stroma, StromaKeratinConfig,
};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

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

// Publish through the batch path (the real broker publish path), where the
// per-queue default TTL is resolved.
async fn append_batch(st: &Arc<Stroma>, tp: &str, expire_at: Option<u64>) -> Offset {
    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message_batch(
        tp,
        0,
        None,
        vec![PublishItem {
            headers: headers(),
            payload: b"x".to_vec(),
            not_before: None,
            expire_at,
            completion: c,
        }],
    )
    .await
    .unwrap();
    rx.await.unwrap().unwrap().base_offset
}

async fn declare_default_ttl(st: &Arc<Stroma>, tp: &str, default_ttl_ms: u64) {
    st.declare(
        tp,
        0,
        None,
        DeclareMeta {
            dlq_policy: None,
            dlq_max_retries: None,
            default_message_ttl_ms: Some(default_ttl_ms),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn queue_default_ttl_applies_when_message_has_no_ttl() {
    let (st, _td) = open_test_stroma().await;
    declare_default_ttl(&st, "t", 1_000).await;
    let t0 = now_ms();
    let off = append_batch(&st, "t", None).await;

    // Before the default deadline (~t0+1000): not dropped.
    assert!(st.drop_ttl_expired(t0, 100).await.unwrap().is_empty());
    // After it: dropped.
    let dropped = st.drop_ttl_expired(t0 + 5_000, 100).await.unwrap();
    assert!(dropped.contains(&("t".to_string(), 0, None, off)));
}

#[tokio::test]
async fn explicit_message_ttl_overrides_queue_default() {
    let (st, _td) = open_test_stroma().await;
    declare_default_ttl(&st, "t", 1_000).await;
    let t0 = now_ms();
    // Explicit far-future deadline beats the 1s queue default.
    let _off = append_batch(&st, "t", Some(t0 + 1_000_000)).await;

    // The default alone would have dropped it by now; the explicit deadline holds.
    assert!(st.drop_ttl_expired(t0 + 5_000, 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn no_queue_default_means_no_expiry() {
    let (st, _td) = open_test_stroma().await;
    let t0 = now_ms();
    let _off = append_batch(&st, "t", None).await;
    // No default declared, no per-message ttl: never expires.
    assert!(
        st.drop_ttl_expired(t0 + 1_000_000, 100)
            .await
            .unwrap()
            .is_empty()
    );
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
