use std::{sync::Arc, time::Duration};

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{MessageHeaders, Offset, SnapshotConfig, Stroma};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    (
        Arc::new(
            Stroma::open(
                &test_dir.root,
                KeratinConfig::test_default(),
                SnapshotConfig::default(),
            )
            .await
            .unwrap(),
        ),
        test_dir,
    )
}

pub async fn append_one(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    payload: &[u8],
) -> Offset {
    let (c, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        extra: Default::default(),
    };
    st.append_message(tp, part, group, &headers, payload.to_vec(), c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap().base_offset
}

#[tokio::test]
async fn truncated_delta_does_not_corrupt_state() {
    let dir = test_dir!("test_data");
    let kcfg = KeratinConfig::test_default();
    let scfg = SnapshotConfig::default();

    let st = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    for i in 0..1000 {
        st.mark_inflight_one("t", 0, None, i, 1000).await.unwrap();
        if i % 4 == 0 {
            st.ack_one("t", 0, None, i).await.unwrap();
        }
    }

    let qh = st.queue_handle("t", 0, None).await.unwrap();
    st.truncate_partition_log(qh, 123).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);

    let st2 = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();
    st2.validate().await.unwrap();
}

#[tokio::test]
async fn enqueue_is_durable_and_replayed() {
    let dir = test_dir!("enqueue_replay");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    let (completion, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        extra: Default::default(),
    };
    st.append_message("t", 0, None, &headers, b"hello".to_vec(), completion)
        .await
        .unwrap();
    let _append_result = rx.await.unwrap().unwrap();

    // Force snapshot & restart
    st.snapshot_partition("t", 0, None).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();

    assert!(st2.is_ready("t", 0, None, 0).await.unwrap());
}

#[tokio::test]
async fn enqueue_is_durable_and_replayed_no_snap() {
    let dir = test_dir!("enqueue_replay");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    let (completion, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        extra: Default::default(),
    };
    st.append_message("t", 0, None, &headers, b"hello".to_vec(), completion)
        .await
        .unwrap();
    let _append_result = rx.await.unwrap().unwrap();
    assert!(st.is_ready("t", 0, None, 0).await.unwrap());
    st.shutdown().await.unwrap();

    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();

    assert!(st2.is_ready("t", 0, None, 0).await.unwrap());
}

// #[tokio::test]
// async fn crash_between_message_and_enqueue_event_is_safe() {
//     let test_dir = test_dir!("test_data");
//     let st = Arc::new(
//         Stroma::open(
//             &test_dir.root,
//             KeratinConfig::test_default(),
//             SnapshotConfig::default(),
//         )
//         .await
//         .unwrap(),
//     );

//     let (c, rx) = KeratinAppendCompletion::pair();
//     st.append_message("t", 0,  None, b"x", c).await.unwrap();
//     let offset = rx.await.unwrap().unwrap().base_offset;

//     drop(st);

//     let st2 = Stroma::open(
//         &test_dir.root,
//         KeratinConfig::test_default(),
//         SnapshotConfig::default(),
//     )
//     .await
//     .unwrap();
//     st2.validate().unwrap();

//     assert!(st2.is_ready("t", 0,  None, offset).unwrap());
// }

#[tokio::test]
async fn expiry_is_durable_across_restart() {
    let (st, dir) = open_test_stroma().await;

    let off = append_one(&st, "t", 0, None, b"x").await;
    dbg!(off);
    st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();
    assert!(!st.is_ready("t", 0, None, off).await.unwrap());
    st.requeue_expired(10, 10).await.unwrap();
    assert!(st.is_ready("t", 0, None, off).await.unwrap());

    st.shutdown().await.unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;

    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();

    assert!(st2.is_ready("t", 0, None, off).await.unwrap());
}
