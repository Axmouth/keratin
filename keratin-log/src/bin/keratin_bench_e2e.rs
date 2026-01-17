use keratin_log::*;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const PAYLOAD_SIZE: usize = 256;
const BATCH: usize = 4096;
const PRODUCERS: u32 = 8;
const CONSUMERS: u32 = PRODUCERS;
const RUN_SECS: u64 = 10;

pub const FLAG_EOS: u16 = 0x0001;

fn eos_record(pid: u32) -> Vec<u8> {
    let mut v = make_record(pid, u64::MAX);
    let p = b"KERATIN_EOS";
    v[12..12 + p.len()].copy_from_slice(b"KERATIN_EOS");
    v
}

fn make_record(pid: u32, seq: u64) -> Vec<u8> {
    let mut v = vec![32u8; 16 + PAYLOAD_SIZE];
    v[..4].copy_from_slice(&pid.to_be_bytes());
    v[4..12].copy_from_slice(&seq.to_be_bytes());
    let p = b"Le String";
    v[12..12 + p.len()].copy_from_slice(p);
    v
}

fn decode_record(v: &[u8]) -> (u32, u64) {
    let pid = u32::from_be_bytes(v[..4].try_into().unwrap());
    let seq = u64::from_be_bytes(v[4..12].try_into().unwrap());
    let p = String::from_utf8_lossy(&v[12..]);
    if p.trim().as_bytes() != b"Le String" {
        assert_eq!(p, "Le String");
        println!("{}", p);
    }
    (pid, seq)
}

#[tokio::main]
async fn main() {
    let root = util::test_dir("keratin-stream");
    let cfg = KeratinConfig {
        segment_max_bytes: 256 * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 4 * 1024 * 1024,
        max_batch_records: 2048,
        batch_linger_ms: 5,
        default_durability: KDurability::AfterFsync,
        fsync_interval_ms: 2,
        flush_target_bytes: 32 * 1024 * 1024,
    };

    let k = Arc::new(Keratin::open(&root.root, cfg).await.unwrap());

    let produced = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let seen_eos = Arc::new(AtomicU64::new(0));

    let now = Instant::now();

    let mut handles_p = vec![];
    // -------- producers --------
    for pid in 0..PRODUCERS {
        let k = k.clone();
        let produced = produced.clone();
        let handle = tokio::spawn(async move {
            let mut seq = 0u64;
            loop {
                let mut batch = Vec::with_capacity(BATCH);
                for _ in 0..BATCH {
                    let msg = Message {
                        flags: 0b1110,
                        headers: vec![],
                        payload: make_record(pid, seq),
                    };
                    batch.push(msg);
                    seq += 1;
                }
                let batch_len = batch.len();
                k.append_batch(batch, None).await.unwrap();
                produced.fetch_add(batch_len as u64, Ordering::Relaxed);
                if now.elapsed() >= Duration::from_secs(RUN_SECS) {
                    // let msg = Message {
                    //     flags: FLAG_EOS,
                    //     headers: vec![],
                    //     payload: eos_record(pid).to_vec(),
                    // };
                    // k.append_batch(vec![msg], Some(Durability::AfterFsync))
                    //     .await
                    //     .unwrap();
                    // println!("Sent EOS");
                    break;
                }
            }
        });
        handles_p.push(handle);
    }

    // -------- consumers --------
    let consumed_ref = consumed.clone();
    let produced_ref = produced.clone();
    let handle_c = tokio::spawn(async move {
        let mut next_offset = 0u64;
        let mut seen = HashSet::new();
        let reader = k.reader();

        // while seen_eos.load(Ordering::SeqCst) < PRODUCERS as u64 {
        while {
            let c = consumed_ref.load(Ordering::SeqCst);
            c == 0 || c < produced_ref.load(Ordering::SeqCst)
        } {
            let msgs = reader.scan_from(next_offset, 4096).unwrap();

            if msgs.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }

            for m in &msgs {
                assert_eq!(m.offset, next_offset);

                let (pid, seq) = decode_record(&m.payload);

                // if seq == u64::MAX {
                //     println!("found eos!");
                //     seen_eos.fetch_add(1, Ordering::SeqCst);
                //     next_offset = m.offset + 1;
                //     continue;
                // }

                assert!(
                    seen.insert((pid, seq)),
                    "duplicate message {:?}",
                    (pid, seq)
                );
                next_offset = m.offset + 1;
            }

            consumed_ref.fetch_add(msgs.len() as u64, Ordering::Relaxed);
        }
    });

    // -------- run --------
    let start = Instant::now();

    for handle in handles_p {
        handle.await.unwrap();
    }

    handle_c.await.unwrap();

    // drain
    tokio::time::sleep(Duration::from_secs(2)).await;

    let secs = start.elapsed().as_secs_f64();
    let p = produced.load(Ordering::Relaxed);
    let c = consumed.load(Ordering::Relaxed);

    println!("Produced: {}", p);
    println!("Consumed: {}", c);
    println!("Throughput p: {:.0} msgs/sec", p as f64 / secs);
    println!("Throughput c: {:.0} msgs/sec", c as f64 / secs);

    assert!(c >= p, "consumer fell behind: {c} < {p}");
}
