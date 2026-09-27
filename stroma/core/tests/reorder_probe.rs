//! Concurrent publishes must bind each confirmation to its own durable payload.

use std::sync::Arc;

use keratin_log::{CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir};
use stroma_core::{
    MessageHeaders, PublishItem, SnapshotConfig, Stroma, StromaEvent, StromaKeratinConfig,
};

fn headers() -> MessageHeaders {
    MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        content_type: None,
        extra: Default::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_publish_confirms_identify_exact_durable_payloads() {
    let dir = test_dir!("reorder_probe");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    const N: usize = 5000;
    // Unique payloads detect completion mix-ups as well as missing records.
    let mut handles = Vec::with_capacity(N);
    for id in 0..N {
        let st = st.clone();
        handles.push(tokio::spawn(async move {
            let (c, rx) = KeratinAppendCompletion::pair();
            let item = PublishItem {
                headers: headers(),
                payload: (id as u64).to_be_bytes().to_vec(),
                completion: c,
                not_before: None,
                expire_at: None,
            };
            st.append_message_batch("t", 0, None, vec![item])
                .await
                .unwrap();
            (rx.await.unwrap().unwrap().base_offset, id as u64)
        }));
    }
    let mut confirmed = std::collections::BTreeMap::new();
    for h in handles {
        let (offset, id) = h.await.unwrap();
        assert!(
            confirmed.insert(offset, id).is_none(),
            "duplicate confirmation offset"
        );
    }
    let qh = st.queue_handle("t", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let records = qh.event_log().reader().scan_from(0, N + 1).unwrap();
    assert_eq!(records.len(), N);
    for (expected, record) in records.into_iter().enumerate() {
        assert_eq!(record.offset, expected as u64);
        let StromaEvent::PayloadEnqueue { payload, .. } =
            StromaEvent::decode(&record.payload).unwrap()
        else {
            panic!("payload record")
        };
        assert_eq!(
            payload,
            confirmed.remove(&record.offset).unwrap().to_be_bytes()
        );
    }
    assert!(confirmed.is_empty());
    st.shutdown().await.unwrap();
}
