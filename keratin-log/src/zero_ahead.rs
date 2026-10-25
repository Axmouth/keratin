//! Writes zeros ahead of a fresh segment's write cursor and syncs them in the
//! background. On ext4 a data sync after an extending write, or a write into
//! fallocated (unwritten) space, waits for a journal commit, and with several
//! logs syncing on one filesystem that commit is nearly always in progress.
//! Overwriting blocks that already hold synced zeros changes no metadata, so
//! the writer's sync only has to flush data.
//!
//! One worker thread serves every segment. A segment asks for more zeros when
//! its zeroed lead drops below half the window, so an idle log costs nothing.
//! Zeroing shares one bandwidth cap. A writer that overtakes its zeros stops
//! zeroing for that segment, because at that rate the extra writes would only
//! double the disk traffic. Fast logs mostly reuse freed segments instead.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Bytes zeroed per request.
const CHUNK: u64 = 1 << 20;
/// Cap on zeroing across all segments.
const MAX_BYTES_PER_SEC: u64 = 64 << 20;

pub(crate) struct ZeroAhead {
    file: File,
    /// Everything below this is zeros the worker wrote, or data.
    zeroed_end: AtomicU64,
    /// The writer's cursor.
    written: AtomicU64,
    /// How far ahead of the cursor zeros are kept.
    window: u64,
    /// Set once the worker has written zeros. A writer that passes them
    /// after that is outrunning the worker.
    filled: AtomicBool,
    requested: AtomicBool,
    stopped: AtomicBool,
    /// Held while the worker writes zeros, and by the writer when it writes
    /// past `zeroed_end`, so zeros never land on data.
    lock: Mutex<()>,
}

impl ZeroAhead {
    /// `ready_end` is where space still needs zeros: the cursor for a new
    /// file, the file's end for a reused one whose blocks are already written.
    pub(crate) fn new(file: File, cursor: u64, ready_end: u64, window: u64) -> Arc<Self> {
        let zero = Arc::new(Self {
            file,
            zeroed_end: ready_end.max(cursor).into(),
            written: cursor.into(),
            window: window.max(CHUNK),
            filled: false.into(),
            requested: false.into(),
            stopped: false.into(),
            lock: Mutex::new(()),
        });
        zero.request();
        zero
    }

    /// Write `data` at `start`. Inside zeroed space this takes no lock.
    pub(crate) fn write(self: &Arc<Self>, write: impl FnOnce() -> std::io::Result<()>, start: u64, len: u64) -> std::io::Result<()> {
        let need = start + len;
        if need <= self.zeroed_end.load(Ordering::Acquire) {
            write()?;
        } else {
            let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
            write()?;
            self.zeroed_end.fetch_max(need, Ordering::AcqRel);
            // Zeroing cannot keep up at this rate. Stop rather than double
            // the writes.
            if self.filled.load(Ordering::Acquire) {
                self.stopped.store(true, Ordering::Release);
            }
        }
        self.written.store(need, Ordering::Release);
        self.request();
        Ok(())
    }

    fn request(self: &Arc<Self>) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        let lead = self.zeroed_end.load(Ordering::Acquire).saturating_sub(self.written.load(Ordering::Acquire));
        if lead < self.window / 2 && !self.requested.swap(true, Ordering::AcqRel) {
            worker().send(self.clone());
        }
    }

    /// Stop zeroing and return the end of the zeroed range. Waits out a write
    /// in progress, so no zeros land after this returns.
    pub(crate) fn stop(&self) -> u64 {
        self.stopped.store(true, Ordering::Release);
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.zeroed_end.load(Ordering::Acquire)
    }

    /// One worker turn: zero up to the window, a chunk at a time.
    fn fill(&self, pace: &mut Pace) {
        loop {
            let chunk_started = Instant::now();
            {
                let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
                if self.stopped.load(Ordering::Acquire) {
                    break;
                }
                let zeroed = self.zeroed_end.load(Ordering::Acquire);
                let written = self.written.load(Ordering::Acquire);
                if zeroed >= written + self.window {
                    break;
                }
                if let Err(error) = self.file.write_all_at(&ZEROS, zeroed) {
                    tracing::warn!(%error, "zeroing ahead of the writer failed, segment grows by plain writes");
                    self.stopped.store(true, Ordering::Release);
                    break;
                }
                self.zeroed_end.store(zeroed + CHUNK, Ordering::Release);
                self.filled.store(true, Ordering::Release);
            }
            if let Err(error) = crate::util::sync_file_data(&self.file) {
                tracing::warn!(%error, "syncing zeros ahead of the writer failed");
                self.stopped.store(true, Ordering::Release);
                break;
            }
            pace.wait(chunk_started);
        }
        self.requested.store(false, Ordering::Release);
    }
}

static ZEROS: [u8; CHUNK as usize] = [0; CHUNK as usize];

struct Pace;

impl Pace {
    fn wait(&mut self, chunk_started: Instant) {
        let min = Duration::from_secs_f64(CHUNK as f64 / MAX_BYTES_PER_SEC as f64);
        if let Some(rest) = min.checked_sub(chunk_started.elapsed()) {
            std::thread::sleep(rest);
        }
    }
}

struct Worker {
    tx: crossbeam_channel::Sender<Arc<ZeroAhead>>,
}

impl Worker {
    fn send(&self, segment: Arc<ZeroAhead>) {
        if self.tx.send(segment.clone()).is_err() {
            segment.requested.store(false, Ordering::Release);
        }
    }
}

fn worker() -> &'static Worker {
    static WORKER: OnceLock<Worker> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = crossbeam_channel::unbounded::<Arc<ZeroAhead>>();
        let started = std::thread::Builder::new().name("keratin-zero".into()).spawn(move || {
            let mut pace = Pace;
            while let Ok(segment) = rx.recv() {
                segment.fill(&mut pace);
            }
        });
        if let Err(error) = started {
            tracing::warn!(%error, "starting the zeroing worker failed");
        }
        Worker { tx }
    })
}
