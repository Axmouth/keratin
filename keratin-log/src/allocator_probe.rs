//! Local diagnostic only. A central call purges shared arenas but does not
//! collect every thread heap; writer collection is requested and logged separately.
use std::{
    cell::Cell,
    path::Path,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
static EPOCH: AtomicU64 = AtomicU64::new(0);
thread_local! { static LAST_EPOCH: Cell<u64> = const { Cell::new(0) }; }

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED
        .get_or_init(|| std::env::var("FIBRIL_ALLOCATOR_COLLECTION_PROBE").as_deref() == Ok("1"))
}

pub fn collect_current_thread(label: &str) {
    // Ensure the collecting thread has an initialized default mimalloc heap.
    drop(std::hint::black_box(vec![0u8; 64]));
    let start = Instant::now();
    // SAFETY: collection targets this thread's allocator-owned heap. There are
    // no external pointers or ownership transfers. The public allocator API
    // synchronizes its shared-arena purge internally.
    unsafe {
        libmimalloc_sys::mi_collect(true);
    }
    tracing::info!(target: "allocator_probe", "ALLOCATOR_COLLECT label={} elapsed_us={}", label, start.elapsed().as_micros());
}

pub fn request_writer_collection() -> u64 {
    let epoch = EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
    tracing::info!(target: "allocator_probe", "ALLOCATOR_REQUEST epoch={}", epoch);
    epoch
}

pub(crate) fn collect_requested(root: &Path) {
    if !enabled() {
        return;
    }
    let epoch = EPOCH.load(Ordering::SeqCst);
    LAST_EPOCH.with(|last| {
        if last.get() == epoch { return; }
        last.set(epoch);
        let start = Instant::now();
        // SAFETY: called by the log's owning writer thread, between operations.
        unsafe { libmimalloc_sys::mi_collect(true); }
        tracing::info!(target: "allocator_probe", "ALLOCATOR_WRITER_COLLECT epoch={} root={} elapsed_us={}", epoch, root.display(), start.elapsed().as_micros());
    });
}
