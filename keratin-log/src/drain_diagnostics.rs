//! Optional experiment counters. Disabled unless a persistent output path is set.
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

#[derive(Default)]
struct Counters {
    staged_batches: AtomicU64,
    staged_records: AtomicU64,
    staged_bytes: AtomicU64,
    worker_fsync_jobs: AtomicU64,
    direct_fsync_jobs: AtomicU64,
}
static COUNTERS: OnceLock<Counters> = OnceLock::new();
static STARTED: OnceLock<()> = OnceLock::new();
pub(crate) fn init() {
    STARTED.get_or_init(|| {
        let Ok(path) = std::env::var("KERATIN_DRAIN_COUNTERS_PATH") else {
            return;
        };
        let c = COUNTERS.get_or_init(Counters::default);
        std::thread::Builder::new()
            .name("drain-counters".into())
            .spawn(move || {
                loop {
                    let fields = [
                        ("staged_batches", &c.staged_batches),
                        ("staged_records", &c.staged_records),
                        ("staged_bytes", &c.staged_bytes),
                        ("worker_fsync_jobs", &c.worker_fsync_jobs),
                        ("direct_fsync_jobs", &c.direct_fsync_jobs),
                    ];
                    let values =
                        fields.map(|(name, v)| format!("\"{name}\":{}", v.load(Ordering::Relaxed)));
                    let temp = format!("{path}.pending");
                    if let Err(error) = std::fs::write(&temp, format!("{{{}}}\n", values.join(",")))
                        .and_then(|_| std::fs::rename(&temp, &path))
                    {
                        tracing::error!(%error, "writing drain experiment counters");
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
            })
            .expect("start optional drain counters");
    });
}
pub(crate) fn stage(records: u64, bytes: u64) {
    if let Some(c) = COUNTERS.get() {
        c.staged_batches.fetch_add(1, Ordering::Relaxed);
        c.staged_records.fetch_add(records, Ordering::Relaxed);
        c.staged_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}
pub(crate) fn fsync(direct: bool) {
    if let Some(c) = COUNTERS.get() {
        let counter = if direct {
            &c.direct_fsync_jobs
        } else {
            &c.worker_fsync_jobs
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

