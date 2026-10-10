//! Isolated experiment support. Inject at the file operation boundary while
//! retaining the real staging, fsync worker, receipts, cache and recovery paths.
//! This models returned I/O errors, not a device failure or power loss.
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Write},
    os::unix::fs::MetadataExt,
    path::Path,
    sync::{
        Arc, Condvar, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Debug)]
pub enum Fault {
    Write,
    PartialWrite,
    Fsync,
    FsyncAfterSuccess,
    /// Test-only gate that performs a normal sync after release.
    FsyncPause,
    /// Test-only gate that performs a normal write after release.
    WritePause,
}
struct Entry {
    fault: Fault,
    hits: Arc<AtomicUsize>,
    remaining: usize,
    gate: Option<Arc<Gate>>,
}
static FAULTS: LazyLock<Mutex<HashMap<(u64, u64), Entry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
fn key(file: &File) -> io::Result<(u64, u64)> {
    let m = file.metadata()?;
    Ok((m.dev(), m.ino()))
}
struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let mut open = self.released.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
}
pub struct ArmedFault {
    key: (u64, u64),
    hits: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
}
impl ArmedFault {
    pub fn release(&self) {
        if let Some(gate) = &self.gate {
            gate.release();
        }
    }
    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}
impl Drop for ArmedFault {
    fn drop(&mut self) {
        self.release();
        FAULTS.lock().unwrap().remove(&self.key);
    }
}
pub fn arm(path: &Path, fault: Fault) -> io::Result<ArmedFault> {
    arm_repeated(path, fault, 1)
}
pub fn arm_repeated(path: &Path, fault: Fault, remaining: usize) -> io::Result<ArmedFault> {
    assert!(remaining > 0);
    let key = key(&File::open(path)?)?;
    let hits = Arc::new(AtomicUsize::new(0));
    assert!(
        FAULTS
            .lock()
            .unwrap()
            .insert(
                key,
                Entry {
                    fault,
                    hits: hits.clone(),
                    remaining,
                    gate: None
                }
            )
            .is_none()
    );
    Ok(ArmedFault {
        key,
        hits,
        gate: None,
    })
}
/// Hold the real file operation until the test observes follower persistence.
/// Dropping the guard releases the worker even when the test unwinds.
pub fn arm_gated(path: &Path, fault: Fault) -> io::Result<ArmedFault> {
    let mut armed = arm(path, fault)?;
    let gate = Arc::new(Gate {
        released: Mutex::new(false),
        changed: Condvar::new(),
    });
    FAULTS.lock().unwrap().get_mut(&armed.key).unwrap().gate = Some(gate.clone());
    armed.gate = Some(gate);
    Ok(armed)
}
fn take(file: &File, write: bool) -> io::Result<Option<Fault>> {
    let key = key(file)?;
    let mut faults = FAULTS.lock().unwrap();
    if let Some(entry) = faults.get_mut(&key) {
        if entry.remaining > 0 && matches!(entry.fault, Fault::Write | Fault::PartialWrite | Fault::WritePause) == write
        {
            entry.hits.fetch_add(1, Ordering::SeqCst);
            entry.remaining -= 1;
            let fault = entry.fault;
            let gate = entry.gate.clone();
            drop(faults);
            if let Some(gate) = gate {
                gate.wait();
            }
            return Ok(Some(fault));
        }
    }
    Ok(None)
}
fn failure() -> io::Error {
    io::Error::other("injected file-operation failure")
}
pub(crate) fn write_all(file: &mut File, bytes: &[u8]) -> io::Result<()> {
    match take(file, true)? {
        Some(Fault::Write) => Err(failure()),
        Some(Fault::PartialWrite) => {
            file.write_all(&bytes[..bytes.len() / 2])?;
            Err(failure())
        }
        _ => file.write_all(bytes),
    }
}
pub(crate) fn sync_data(file: &File) -> io::Result<()> {
    match take(file, false)? {
        Some(Fault::Fsync) => Err(failure()),
        Some(Fault::FsyncAfterSuccess) => {
            file.sync_data()?;
            Err(failure())
        }
        _ => file.sync_data(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};

    #[test]
    fn dropping_gated_fault_releases_worker() {
        let dir = crate::test_dir!("gated_fault");
        let path = dir.root.join("segment");
        let file = File::create(&path).unwrap();
        let fault = arm_gated(&path, Fault::Fsync).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || sender.send(sync_data(&file)).unwrap());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while fault.hits() == 0 {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(receiver.try_recv().is_err());
        drop(fault);
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .is_err()
        );
        worker.join().unwrap();
    }

    #[test]
    fn partial_fault_writes_real_prefix_and_retry_can_replace_it() {
        let dir = crate::test_dir!("file_fault");
        let path = dir.root.join("segment");
        let mut file = File::options()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let armed = arm(&path, Fault::PartialWrite).unwrap();
        assert!(write_all(&mut file, b"abcdefgh").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"abcd");
        assert_eq!(armed.hits(), 1);
        file.seek(SeekFrom::Start(0)).unwrap();
        write_all(&mut file, b"abcdefgh").unwrap();
        let mut bytes = Vec::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"abcdefgh");
        assert_eq!(armed.hits(), 1);
    }
}
