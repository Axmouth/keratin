//! Measurement probe (not a pass/fail gate): does the parallel durable-publish
//! path append EnqueueMany events to the event log OUT OF msg-offset order under
//! concurrent publishing? The recovery fold assumes event-log order == msg-offset
//! order; if that does not hold, a crash that strands a middle publish can truncate
//! a confirmed one. This prints the observed reorder rate.

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
async fn measure_event_log_reorder_under_concurrent_publish() {
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
    // Fire N single-message publishes concurrently. Each assigns one msg offset
    // (serial msg writer) and appends one EnqueueMany from a racing spawned task.
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let st = st.clone();
        handles.push(tokio::spawn(async move {
            let (c, rx) = KeratinAppendCompletion::pair();
            let item = PublishItem {
                headers: headers(),
                payload: vec![0u8; 16],
                completion: c,
                not_before: None,
                expire_at: None,
            };
            st.append_message_batch("t", 0, None, vec![item])
                .await
                .unwrap();
            rx.await.unwrap().unwrap().base_offset
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Scan the event log in PHYSICAL order and pull each EnqueueMany's msg offset.
    let qh = st.queue_handle("t", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let reader = qh.event_log().reader();
    let mut cur = 0u64;
    let mut seq: Vec<u64> = Vec::new();
    loop {
        let batch = reader.scan_from(cur, 10_000).unwrap();
        if batch.is_empty() {
            break;
        }
        for rec in &batch {
            if let Ok(StromaEvent::EnqueueMany { reqs }) = StromaEvent::decode(&rec.payload)
                && let Some(r) = reqs.first()
            {
                seq.push(r.off);
            }
        }
        cur = batch.last().unwrap().offset + 1;
    }

    // Adjacent inversions = a later physical event carries a lower msg offset.
    let inversions = seq.windows(2).filter(|w| w[1] < w[0]).count();
    let mut sorted = seq.clone();
    sorted.sort_unstable();
    let out_of_place = seq.iter().zip(&sorted).filter(|(a, b)| a != b).count();
    let pct = out_of_place as f64 * 100.0 / seq.len().max(1) as f64;

    eprintln!(
        "REORDER PROBE (worker_threads=8, N={N}): {} EnqueueMany events, {inversions} adjacent inversions, {out_of_place} out-of-place ({pct:.1}%)",
        seq.len()
    );

    assert_eq!(seq.len(), N, "every publish produced exactly one EnqueueMany");
    // With the per-partition publish-order lock, the event log must be in strict
    // msg-offset order: zero reorder regardless of concurrency.
    assert_eq!(
        out_of_place, 0,
        "event log must be in msg-offset order (no reorder)"
    );
}
