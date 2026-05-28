use std::sync::Arc;

use hashbrown::HashSet;
use keratin_log::KeratinConfig;
use stroma_core::{SnapshotConfig, Stroma, TempDir, test_dir};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    let res = Arc::new(
        Stroma::open(
            &test_dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );
    (res, test_dir)
}

#[tokio::test]
async fn random_operations_never_break_invariants() {
    fastrand::seed(0xC0FFEE);

    let (st, _test_dir) = open_test_stroma().await;
    let q = st.queue_handle("test", 0, None).await.unwrap();

    for _ in 0..50_000 {
        let o = fastrand::u64(0..2000);
        match fastrand::u8(0..10) {
            0 | 4 | 8 => q.mark_inflight(o, fastrand::u64(0..100_000)).await,
            1 | 5 | 7 => q.ack(o).await,
            i => {
                q.nack(o, i.is_multiple_of(3)).await;
            }
        }

        // Since we never mention offsets >= 2000, frontier must never exceed 2000.
        assert!(q.settled_until().await <= 2000);
    }
}

#[tokio::test]
async fn many_new_topics_congestion() {
    let (st, _test_dir) = open_test_stroma().await;
    let topics_target = 200;

    let mut topics = (0..topics_target)
        .map(|i| format!("topic-{i}"))
        .collect::<HashSet<_>>();
    let mut handles = Vec::new();
    println!(
        "Spawning tasks to create queue handles for {} new topics...",
        topics_target
    );
    for topic in topics.iter() {
        let st = st.clone();
        let topic = topic.clone();
        let handle = tokio::spawn(async move { st.queue_handle(&topic, 0, None).await.unwrap() });
        handles.push(handle);
    }

    println!("All tasks spawned, waiting for them to complete...");
    let mut queue_handles = Vec::new();
    for handle in handles {
        println!("Waiting for a task to complete...");
        let qh = handle.await.unwrap();
        queue_handles.push(qh);
    }

    let added_topics = st.list_topics();
    println!("Topics after adding: {:?}", added_topics);

    assert_eq!(added_topics.len(), topics_target);
    assert_eq!(
        added_topics
            .into_iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>(),
        topics
    );

    for qh in queue_handles {
        topics.remove(qh.topic());
    }

    assert!(topics.is_empty());
}
