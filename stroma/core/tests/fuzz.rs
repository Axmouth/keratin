use std::sync::Arc;

use hashbrown::HashSet;
use keratin_log::KeratinConfig;
use stroma_core::{SnapshotConfig, Stroma, StromaKeratinConfig, TempDir, test_dir};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    let res = Arc::new(
        Stroma::open(
            &test_dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
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
    let q = q.resolve().unwrap();

    for _ in 0..50_000 {
        let o = fastrand::u64(0..2000);
        match fastrand::u8(0..10) {
            0 | 4 | 8 => q.mark_inflight(o, fastrand::u64(0..100_000)).await.unwrap(),
            1 | 5 | 7 => q.ack(o).await.unwrap(),
            i => {
                q.nack(o, i.is_multiple_of(3)).await.unwrap();
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

    #[cfg(target_os = "linux")]
    if let Ok(limits) = std::fs::read_to_string("/proc/self/limits") {
        if let Some(open_files) = limits
            .lines()
            .find(|line| line.starts_with("Max open files"))
        {
            eprintln!(
                "many_new_topics_congestion materializes {topics_target} queues concurrently; \
                 if this fails with `Too many open files`, rerun with `ulimit -n 4096` or higher. \
                 Current limit: {open_files}"
            );
        }
    }

    let mut topics = (0..topics_target)
        .map(|i| format!("topic-{i}"))
        .collect::<HashSet<_>>();
    let mut handles = Vec::new();
    for topic in topics.iter() {
        let st = st.clone();
        let topic = topic.clone();
        let handle = tokio::spawn(async move { st.queue_handle(&topic, 0, None).await.unwrap() });
        handles.push(handle);
    }

    let mut queue_handles = Vec::new();
    for handle in handles {
        let qh = handle.await.unwrap();
        queue_handles.push(qh);
    }

    let added_topics = st.list_topics();

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
    // Keep the congestion assertion focused on queue creation; explicit async
    // shutdown avoids hundreds of log handles falling back to blocking Drop.
    st.shutdown().await.unwrap();
}
