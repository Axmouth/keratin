//! Bounded background persistence of periodic, dirty manifests, and the
//! truncation work that follows a manifest store. Critical metadata stores
//! drain the outstanding job before writing. Periodic stores run here unless
//! `KERATIN_BACKGROUND_MANIFEST=0`.
use crate::manifest::Manifest;
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};
use std::{
    io,
    path::Path,
    sync::OnceLock,
    time::{Duration, Instant},
};

type Job = Box<dyn FnOnce() + Send + 'static>;

fn pool() -> &'static Sender<Job> {
    static POOL: OnceLock<Sender<Job>> = OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = bounded::<Job>(64);
        for i in 0..2 {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("keratin-manifest-{i}"))
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        // Disconnect the result channel on panic, without losing a worker.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                })
                .expect("start manifest worker");
        }
        tx
    })
}

pub(crate) struct ManifestWorker {
    pub(crate) enabled: bool,
    pub(crate) pending: Option<Receiver<io::Result<Duration>>>,
}

impl Default for ManifestWorker {
    fn default() -> Self {
        Self {
            // Diagnostic off switch for comparing against inline stores.
            enabled: std::env::var("KERATIN_BACKGROUND_MANIFEST").as_deref() != Ok("0"),
            pending: None,
        }
    }
}

impl ManifestWorker {
    pub(crate) fn poll(&mut self) -> io::Result<Option<Duration>> {
        let Some(rx) = &self.pending else {
            return Ok(None);
        };
        match rx.try_recv() {
            Ok(result) => {
                self.pending = None;
                result.map(Some)
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                self.pending = None;
                Err(io::Error::other("manifest worker disconnected"))
            }
        }
    }

    pub(crate) fn drain(&mut self) -> io::Result<Option<Duration>> {
        self.pending
            .take()
            .map(|rx| {
                rx.recv()
                    .map_err(|_| io::Error::other("manifest worker disconnected"))?
                    .map(Some)
            })
            .unwrap_or(Ok(None))
    }

    /// At most one snapshot per log. A full pool defers this optional checkpoint.
    pub(crate) fn submit(&mut self, root: &Path, manifest: &Manifest) -> io::Result<bool> {
        if self.pending.is_some() {
            return Ok(false);
        }
        assert!(
            !manifest.clean_shutdown,
            "clean metadata requires a blocking barrier"
        );
        let root = root.to_path_buf();
        let snapshot = manifest.clone();
        let (tx, rx) = bounded(1);
        let job = Box::new(move || {
            let start = Instant::now();
            let result = snapshot.store_atomic(&root).map(|()| start.elapsed());
            if let Err(error) = &result {
                tracing::error!(log_root = %root.display(), %error, "background manifest persistence failed");
            }
            let _ = tx.send(result);
        });
        match pool().try_send(job) {
            Ok(()) => {
                self.pending = Some(rx);
                Ok(true)
            }
            Err(TrySendError::Full(_)) => Ok(false),
            Err(TrySendError::Disconnected(_)) => {
                Err(io::Error::other("manifest pool disconnected"))
            }
        }
    }
}

impl ManifestWorker {
    /// Run `job` on the pool after this log's outstanding snapshot, and track
    /// it as the outstanding work, so metadata writes for one log never
    /// overlap. Runs inline when the pool is full. The snapshot's result is
    /// passed on, since the job takes its place.
    pub(crate) fn submit_after(
        &mut self,
        job: impl FnOnce() + Send + 'static,
    ) -> io::Result<()> {
        let previous = self.pending.take();
        let (tx, rx) = bounded(1);
        let chained: Job = Box::new(move || {
            let start = Instant::now();
            let earlier = match previous {
                Some(rx) => rx
                    .recv()
                    .unwrap_or_else(|_| Err(io::Error::other("manifest worker disconnected"))),
                None => Ok(Duration::ZERO),
            };
            job();
            let _ = tx.send(earlier.map(|d| d + start.elapsed()));
        });
        match pool().try_send(chained) {
            Ok(()) => {
                self.pending = Some(rx);
                Ok(())
            }
            Err(TrySendError::Full(chained)) => {
                chained();
                rx.recv()
                    .map_err(|_| io::Error::other("manifest worker disconnected"))?
                    .map(|_| ())
            }
            Err(TrySendError::Disconnected(_)) => {
                Err(io::Error::other("manifest pool disconnected"))
            }
        }
    }
}

impl Drop for ManifestWorker {
    fn drop(&mut self) {
        // No file writer may outlive ownership of its Log, even on error paths.
        if let Err(error) = self.drain() {
            tracing::error!(%error, "draining manifest worker failed");
        }
    }
}
