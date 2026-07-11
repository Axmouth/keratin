use std::sync::Arc;

use keratin_log::{KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir};
use stroma_core::{
    CompletionPair, MessageHeaders, SnapshotConfig, Stroma, StromaKeratinConfig,
};

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

async fn append(st: &Arc<Stroma>, tp: &str) {
    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message_with_ttl(tp, 0, None, &headers(), b"payload".to_vec(), None, c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

/// An evicted queue's bytes stay in the disk breakdown: unloaded partitions
/// are measured from their directories rather than vanishing from the total.
#[tokio::test]
async fn disk_breakdown_covers_evicted_queues() {
    let (st, _dir) = open_test_stroma().await;
    append(&st, "kept").await;
    append(&st, "evicted").await;

    let breakdown = st.estimate_disk_used_breakdown().await.unwrap();
    let bytes_of = |name: &str, entries: &[stroma_core::DiskUsedBreakdownEntry]| {
        entries
            .iter()
            .find(|e| e.topic == name)
            .map(|e| e.message_bytes + e.event_bytes)
            .unwrap_or(0)
    };
    let evicted_before = bytes_of("evicted", &breakdown);
    assert!(evicted_before > 0, "{breakdown:?}");

    st.evict("evicted", 0, None).await.unwrap();

    let breakdown = st.estimate_disk_used_breakdown().await.unwrap();
    assert!(bytes_of("kept", &breakdown) > 0, "{breakdown:?}");
    let evicted_after = bytes_of("evicted", &breakdown);
    assert!(
        evicted_after > 0,
        "evicted queue's bytes vanished: {breakdown:?}"
    );
}
