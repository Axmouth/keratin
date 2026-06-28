use std::sync::Arc;
use std::time::Duration;

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{
    DLQDiscardPolicyWire, DeclareMeta, MessageHeaders, NackEventMeta, Offset, SnapshotConfig,
    Stroma, StromaKeratinConfig,
};
use tokio::time::Instant;

// ---------- helpers ----------

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    let s = Arc::new(
        Stroma::open(
            &test_dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );
    (s, test_dir)
}

async fn reopen_test_stroma(dir: &TempDir) -> Arc<Stroma> {
    Arc::new(
        Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    )
}

async fn append_one(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    payload: &[u8],
) -> Offset {
    let (c, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: 0,
        publish_received: 0,
        content_type: None,
        extra: Default::default(),
    };
    st.append_message(tp, part, group, &headers, payload.to_vec(), c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap().base_offset
}

/// Drives `nack_enqueue` and waits for durability + apply.
async fn nack_one(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    off: Offset,
    requeue: bool,
) {
    let (c, rx) = KeratinAppendCompletion::pair();
    st.nack_enqueue(tp, part, group, off, requeue, c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

async fn nack_many(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    reqs: Vec<NackEventMeta>,
) {
    let (c, rx) = KeratinAppendCompletion::pair();
    st.nack_enqueue_many(tp, part, group, reqs, c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn declare_dlq_custom(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    target_tp: &str,
    target_part: u32,
    target_group: Option<&str>,
    max_retries: u32,
) {
    st.declare(
        tp,
        part,
        group,
        DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::CustomDQL {
                tp: target_tp.into(),
                part: target_part,
                group: target_group.map(Into::into),
            }),
            dlq_max_retries: Some(max_retries),
            default_message_ttl_ms: None,
        },
    )
    .await
    .unwrap();
}

async fn declare_discard(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    max_retries: u32,
) {
    let qh = st.queue_handle(tp, part, group).await.unwrap();
    let qh = qh.resolve().unwrap();
    let qh = qh.work_queue().unwrap();
    qh.declare(DeclareMeta {
        dlq_policy: Some(DLQDiscardPolicyWire::Discard),
        dlq_max_retries: Some(max_retries),
        default_message_ttl_ms: None,
    })
    .await
    .unwrap();
}

async fn wait_until<F, Fut>(timeout: Duration, label: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("wait_until({label}) timed out after {:?}", timeout);
}

// ---------- tests ----------

#[tokio::test]
async fn nack_under_max_requeues_no_dlq() {
    let (st, _d) = open_test_stroma().await;
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 3).await;

    let off = append_one(&st, "src", 0, None, b"hi").await;
    st.mark_inflight_one("src", 0, None, off, 60_000)
        .await
        .unwrap();
    nack_one(&st, "src", 0, None, off, true).await;

    let q = st.queue_handle("src", 0, None).await.unwrap();
    let q = q.resolve().unwrap();
    let q = q.work_queue().unwrap();
    assert_eq!(q.retries(off).await, 1);
    assert!(q.is_ready(off).await);
    assert!(!st.is_settled("src", 0, None, off).await.unwrap());
    // No DLQ message produced.
    assert_eq!(
        st.current_next_offset("dlq", 0, None).await.unwrap(),
        0,
        "DLQ should be empty"
    );
}

#[tokio::test]
async fn nack_at_max_dead_letters_to_custom_target() {
    let (st, _d) = open_test_stroma().await;
    // max_retries=0: any nack-with-requeue exhausts immediately
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 0).await;

    let off = append_one(&st, "src", 0, None, b"payload").await;
    st.mark_inflight_one("src", 0, None, off, 60_000)
        .await
        .unwrap();
    nack_one(&st, "src", 0, None, off, true).await;

    wait_until(
        Duration::from_secs(5),
        "source acked after dlq copy",
        || async { st.is_settled("src", 0, None, off).await.unwrap() },
    )
    .await;

    let next = st.current_next_offset("dlq", 0, None).await.unwrap();
    assert_eq!(
        next, 1,
        "DLQ should have exactly 1 message, got next_offset={next}"
    );

    let qh = st.queue_handle("dlq", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let qh = qh.work_queue().unwrap();
    let m = st
        .fetch_message_by_offset(&qh, 0)
        .await
        .unwrap()
        .expect("DLQ message present");
    assert_eq!(m.payload, b"payload");

    let headers = MessageHeaders::decode(&m.headers).unwrap();
    assert!(!headers.extra.contains_key("x-dlq-source-tp"));
    assert!(!headers.extra.contains_key("x-dlq-source-part"));
    assert!(!headers.extra.contains_key("x-dlq-source-group"));
    assert!(!headers.extra.contains_key("x-dlq-source-offset"));

    let q = st.queue_handle("src", 0, None).await.unwrap();
    let q = q.resolve().unwrap();
    let q = q.work_queue().unwrap();
    assert!(!q.is_ready(off).await);
    assert!(!q.is_inflight(off).await);
    assert!(
        q.pending_dlq().await.unwrap().is_empty(),
        "pending_dlq should be drained"
    );
}

#[tokio::test]
async fn nack_no_requeue_with_discard_acks_locally() {
    let (st, _d) = open_test_stroma().await;
    declare_discard(&st, "src", 0, None, 5).await;

    let off = append_one(&st, "src", 0, None, b"x").await;
    st.mark_inflight_one("src", 0, None, off, 60_000)
        .await
        .unwrap();
    nack_one(&st, "src", 0, None, off, false).await;

    wait_until(Duration::from_secs(2), "discard acked", || async {
        st.is_settled("src", 0, None, off).await.unwrap()
    })
    .await;

    assert_eq!(
        st.current_next_offset("dlq", 0, None).await.unwrap(),
        0,
        "DLQ should not have been written"
    );
}

#[tokio::test]
async fn nack_no_requeue_with_custom_dlq_routes_to_dlq() {
    // Reject-with-requeue=false is policy-driven: same path as retries-exhausted.
    let (st, _d) = open_test_stroma().await;
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 99).await;

    let off = append_one(&st, "src", 0, None, b"reject-me").await;
    st.mark_inflight_one("src", 0, None, off, 60_000)
        .await
        .unwrap();
    nack_one(&st, "src", 0, None, off, false).await;

    wait_until(Duration::from_secs(5), "source acked", || async {
        st.is_settled("src", 0, None, off).await.unwrap()
    })
    .await;
    assert_eq!(st.current_next_offset("dlq", 0, None).await.unwrap(), 1);
}

#[tokio::test]
async fn batched_nacks_split_requeue_and_dlq() {
    let (st, _d) = open_test_stroma().await;
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 0).await;

    let mut offs = Vec::new();
    for _ in 0..4 {
        offs.push(append_one(&st, "src", 0, None, b"x").await);
    }
    for &o in &offs {
        st.mark_inflight_one("src", 0, None, o, 60_000)
            .await
            .unwrap();
    }

    let reqs = vec![
        NackEventMeta {
            off: offs[0],
            requeue: false,
            not_before: None,
        },
        NackEventMeta {
            off: offs[1],
            requeue: false,
            not_before: None,
        },
        NackEventMeta {
            off: offs[2],
            requeue: true,
            not_before: None,
        },
        NackEventMeta {
            off: offs[3],
            requeue: true,
            not_before: None,
        },
    ];
    nack_many(&st, "src", 0, None, reqs).await;

    wait_until(Duration::from_secs(5), "all 4 acked on source", || async {
        for &o in &offs {
            if !st.is_settled("src", 0, None, o).await.unwrap() {
                return false;
            }
        }
        true
    })
    .await;

    assert_eq!(
        st.current_next_offset("dlq", 0, None).await.unwrap(),
        4,
        "all four should land in DLQ once"
    );
}

#[tokio::test]
async fn dlq_routing_preserves_distinct_payloads() {
    let (st, _d) = open_test_stroma().await;
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 0).await;

    let payloads: Vec<Vec<u8>> = (0..8).map(|i| format!("p{i}").into_bytes()).collect();
    let mut offs = Vec::new();
    for p in &payloads {
        offs.push(append_one(&st, "src", 0, None, p).await);
    }
    for &o in &offs {
        st.mark_inflight_one("src", 0, None, o, 60_000)
            .await
            .unwrap();
    }
    let reqs: Vec<_> = offs
        .iter()
        .map(|&off| NackEventMeta {
            off,
            requeue: false,
            not_before: None,
        })
        .collect();
    nack_many(&st, "src", 0, None, reqs).await;

    wait_until(Duration::from_secs(5), "all dlq'd", || async {
        st.current_next_offset("dlq", 0, None).await.unwrap() == offs.len() as u64
    })
    .await;

    let mut dlq_payloads: Vec<Vec<u8>> = Vec::new();
    let qh = st.queue_handle("dlq", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let qh = qh.work_queue().unwrap();
    for i in 0..offs.len() as u64 {
        let m = st.fetch_message_by_offset(&qh, i).await.unwrap().unwrap();
        dlq_payloads.push(m.payload);
    }
    dlq_payloads.sort();
    let mut expected = payloads.clone();
    expected.sort();
    assert_eq!(dlq_payloads, expected);
}

#[tokio::test]
async fn dlq_routing_survives_restart_no_duplicates() {
    let (st, dir) = open_test_stroma().await;
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 0).await;

    let off = append_one(&st, "src", 0, None, b"persist").await;
    st.mark_inflight_one("src", 0, None, off, 60_000)
        .await
        .unwrap();
    nack_one(&st, "src", 0, None, off, false).await;

    wait_until(Duration::from_secs(5), "pre-restart acked", || async {
        st.is_settled("src", 0, None, off).await.unwrap()
    })
    .await;
    assert_eq!(st.current_next_offset("dlq", 0, None).await.unwrap(), 1);

    st.shutdown().await.unwrap();
    drop(st);

    let st2 = reopen_test_stroma(&dir).await;

    assert!(st2.is_settled("src", 0, None, off).await.unwrap());
    assert_eq!(
        st2.current_next_offset("dlq", 0, None).await.unwrap(),
        1,
        "no duplicate DLQ messages after restart"
    );

    let qh = st2.queue_handle("dlq", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let qh = qh.work_queue().unwrap();
    let m = st2.fetch_message_by_offset(&qh, 0).await.unwrap().unwrap();
    assert_eq!(m.payload, b"persist");
}

#[tokio::test]
async fn declare_settings_survive_restart() {
    let (st, dir) = open_test_stroma().await;
    declare_dlq_custom(&st, "t", 0, None, "audit", 5, None, 42).await;
    // Force queue dirs to exist after shutdown.
    let _ = append_one(&st, "t", 0, None, b"x").await;
    st.shutdown().await.unwrap();
    drop(st);

    let st2 = reopen_test_stroma(&dir).await;
    let qh = st2.queue_handle("t", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let qh = qh.work_queue().unwrap();
    let dbg = qh.debug_info().await;
    assert_eq!(dbg.dlq_max_retries, 42);
    assert!(
        dbg.dlq_policy.contains("audit"),
        "expected dlq_policy debug repr to include target topic, got: {}",
        dbg.dlq_policy
    );
}

#[tokio::test]
async fn pending_dlq_blocks_msg_truncation_watermark() {
    // Weak test: it can race with the background DLQ-copy task. The strong
    // claim is the *invariant*: at no observable moment is a pending-DLQ offset
    // below the truncation watermark. A deterministic stall-test would need a
    // dedicated test hook that does not exist yet.
    let (st, _d) = open_test_stroma().await;
    declare_dlq_custom(&st, "src", 0, None, "dlq", 0, None, 0).await;

    let off = append_one(&st, "src", 0, None, b"hold").await;
    st.mark_inflight_one("src", 0, None, off, 60_000)
        .await
        .unwrap();
    nack_one(&st, "src", 0, None, off, false).await;

    let q = st.queue_handle("src", 0, None).await.unwrap();
    let q = q.resolve().unwrap();
    let q = q.work_queue().unwrap();
    let pending = q.pending_dlq().await.unwrap();
    let watermark = q.lowest_not_settled_offset().await;

    if pending.iter().any(|(o, _)| o == &off) {
        assert!(
            watermark <= off,
            "watermark {watermark} must not pass pending-DLQ offset {off}"
        );
    } else {
        // Background task already finished; offset is acked. Nothing pathological.
        assert!(st.is_settled("src", 0, None, off).await.unwrap());
    }
}

#[tokio::test]
async fn expiry_path_dead_letters_at_max_retries() {
    // Exercises the expiry-driven retry loop ending in DLQ via real path.
    // Requires that nack_enqueue with requeue=true (which is what requeue_expired
    // emits internally) is what runs here.
    let (st, _d) = open_test_stroma().await;
    declare_dlq_custom(&st, "t", 0, None, "dlq", 0, None, 3).await;

    let off = append_one(&st, "t", 0, None, b"flaky").await;

    // Burn through retries via expiry.
    for _ in 0..4 {
        st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();
        st.requeue_expired(20, 10).await.unwrap();
    }

    wait_until(Duration::from_secs(5), "dlq routed via expiry", || async {
        st.is_settled("t", 0, None, off).await.unwrap()
            && st.current_next_offset("dlq", 0, None).await.unwrap() >= 1
    })
    .await;

    assert_eq!(
        st.current_next_offset("dlq", 0, None).await.unwrap(),
        1,
        "exactly one DLQ message via expiry path"
    );
    let q = st.queue_handle("t", 0, None).await.unwrap();
    let q = q.resolve().unwrap();
    let q = q.work_queue().unwrap();
    assert!(!q.is_ready(off).await);
}
