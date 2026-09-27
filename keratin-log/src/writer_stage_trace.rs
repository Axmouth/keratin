//! Optional observation only. One bounded collector and clock per process.
use crossbeam_channel::{Receiver, Sender};
use serde::Serialize;
use std::{
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    path::Path,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CAPACITY: usize = 65_536;
static COLLECTOR: OnceLock<Mutex<Option<Arc<Collector>>>> = OnceLock::new();

#[derive(Serialize)]
struct Event {
    writer: u64,
    id: u64,
    stage: &'static str,
    start_ns: u64,
    end_ns: u64,
    records: usize,
    bytes: usize,
    parent: Option<u64>,
    first: Option<u64>,
    next: Option<u64>,
}

enum Row {
    Event(Event),
    Writer {
        writer: u64,
        path: String,
        epoch: u64,
    },
    Flush(Sender<io::Result<()>>),
}

#[derive(Default)]
struct Counters {
    emitted: AtomicU64,
    dropped: AtomicU64,
    emission_ns: AtomicU64,
    max_emission_ns: AtomicU64,
}

struct Collector {
    base: Instant,
    next_id: AtomicU64,
    tx: Option<Sender<Row>>,
    worker: Option<JoinHandle<io::Result<()>>>,
    counters: Arc<Counters>,
    measure: bool,
}

fn json_line(writer: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")
}

fn summary(
    writer: &mut impl Write,
    counters: &Counters,
    measure: bool,
    closed: bool,
) -> io::Result<()> {
    json_line(
        writer,
        &serde_json::json!({
            "type": "summary", "closed": closed,
            "emitted": counters.emitted.load(Ordering::Relaxed),
            "dropped": counters.dropped.load(Ordering::Relaxed),
            "measure_emission": measure,
            "emission_ns": counters.emission_ns.load(Ordering::Relaxed),
            "max_emission_ns": counters.max_emission_ns.load(Ordering::Relaxed),
        }),
    )
}

fn collect(
    mut output: impl Write,
    rx: Receiver<Row>,
    counters: Arc<Counters>,
    measure: bool,
) -> io::Result<()> {
    loop {
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(Row::Event(event)) => json_line(&mut output, &event)?,
            Ok(Row::Writer {
                writer,
                path,
                epoch,
            }) => json_line(
                &mut output,
                &serde_json::json!({"type":"writer", "writer":writer, "path":path, "epoch":epoch}),
            )?,
            Ok(Row::Flush(reply)) => {
                let result =
                    summary(&mut output, &counters, measure, false).and_then(|_| output.flush());
                let failed = result.is_err();
                let _ = reply.send(result);
                if failed {
                    return Err(io::Error::other("trace flush failed"));
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                summary(&mut output, &counters, measure, false)?;
                output.flush()?;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }
    summary(&mut output, &counters, measure, true)?;
    output.flush()
}

impl Collector {
    fn open(path: &Path, measure: bool) -> io::Result<Arc<Self>> {
        // Never truncate another process's trace or overwrite a previous run.
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let base = Instant::now();
        let unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut output = BufWriter::new(file);
        json_line(
            &mut output,
            &serde_json::json!({
                "type":"process", "version":1, "pid":std::process::id(),
                "unix_ns":unix_ns.to_string(), "capacity":CAPACITY,
                "clock":"process monotonic nanoseconds", "offsets":"half-open",
            }),
        )?;
        output.flush()?;
        let (tx, rx) = crossbeam_channel::bounded(CAPACITY);
        let counters = Arc::new(Counters::default());
        let worker_counters = counters.clone();
        let worker = std::thread::Builder::new()
            .name("keratin-trace".into())
            .spawn(move || {
                let result = collect(output, rx, worker_counters, measure);
                if let Err(error) = &result {
                    eprintln!("Keratin batch trace is incomplete: {error}");
                }
                result
            })?;
        Ok(Arc::new(Self {
            base,
            next_id: AtomicU64::new(1),
            tx: Some(tx),
            worker: Some(worker),
            counters,
            measure,
        }))
    }

    fn finish(&mut self) -> io::Result<()> {
        self.tx.take();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| io::Error::other("trace collector panicked"))??;
        }
        Ok(())
    }

    fn emit(&self, row: Row) {
        let before = self.measure.then(Instant::now);
        if self
            .tx
            .as_ref()
            .expect("live collector")
            .try_send(row)
            .is_err()
        {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.emitted.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(before) = before {
            let ns = before.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            self.counters.emission_ns.fetch_add(ns, Ordering::Relaxed);
            self.counters
                .max_emission_ns
                .fetch_max(ns, Ordering::Relaxed);
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        let _ = self.finish();
        let dropped = self.counters.dropped.load(Ordering::Relaxed);
        if dropped != 0 {
            eprintln!("Keratin batch trace is incomplete: dropped {dropped} events");
        }
    }
}

#[derive(Clone, Default)]
pub struct WriterStageTracer {
    collector: Option<Arc<Collector>>,
    writer: u64,
}

impl WriterStageTracer {
    pub fn from_env(path: &Path, epoch: u64) -> Self {
        let Some(destination) = std::env::var_os("KERATIN_WRITER_STAGE_TRACE") else {
            return Self::default();
        };
        let slot = COLLECTOR.get_or_init(|| Mutex::new(None));
        let mut shared = slot.lock().unwrap_or_else(|e| e.into_inner());
        let collector = if let Some(existing) = shared.as_ref() {
            existing.clone()
        } else {
            let destination = destination
                .to_string_lossy()
                .replace("%p", &std::process::id().to_string());
            match Collector::open(
                Path::new(&destination),
                std::env::var_os("KERATIN_TRACE_MEASURE_EMISSION").as_deref()
                    == Some(std::ffi::OsStr::new("1")),
            ) {
                Ok(collector) => {
                    *shared = Some(collector.clone());
                    collector
                }
                Err(error) => {
                    eprintln!("Could not start Keratin batch trace: {error}");
                    return Self::default();
                }
            }
        };
        Self::register(collector, path, epoch)
    }

    fn register(collector: Arc<Collector>, path: &Path, epoch: u64) -> Self {
        let writer = collector.next_id.fetch_add(1, Ordering::Relaxed);
        collector.emit(Row::Writer {
            writer,
            path: path.display().to_string(),
            epoch,
        });
        Self {
            collector: Some(collector),
            writer,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(path: &Path) -> Self {
        Self::register(
            Collector::open(path, true).unwrap(),
            Path::new("test-log"),
            0,
        )
    }

    pub fn enabled(&self) -> bool {
        self.collector.is_some()
    }
    pub fn next_work_id(&self) -> u64 {
        self.collector
            .as_ref()
            .map_or(0, |c| c.next_id.fetch_add(1, Ordering::Relaxed))
    }
    pub fn start(&self) -> Option<Instant> {
        self.enabled().then(Instant::now)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn event(
        &self,
        id: u64,
        stage: &'static str,
        start: Option<Instant>,
        records: usize,
        bytes: usize,
        parent: Option<u64>,
        range: Option<(u64, u64)>,
    ) {
        if !self.enabled() {
            return;
        }
        let end = Instant::now();
        self.record_event(
            id,
            stage,
            start.unwrap_or(end),
            end,
            records,
            bytes,
            parent,
            range,
        );
    }

    pub fn record(
        &self,
        id: u64,
        stage: &'static str,
        start: Instant,
        end: Instant,
        records: usize,
        bytes: usize,
    ) {
        self.record_event(id, stage, start, end, records, bytes, None, None);
    }

    #[allow(clippy::too_many_arguments)]
    fn record_event(
        &self,
        id: u64,
        stage: &'static str,
        start: Instant,
        end: Instant,
        records: usize,
        bytes: usize,
        parent: Option<u64>,
        range: Option<(u64, u64)>,
    ) {
        let Some(c) = &self.collector else {
            return;
        };
        c.emit(Row::Event(Event {
            writer: self.writer,
            id,
            stage,
            start_ns: start
                .saturating_duration_since(c.base)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            end_ns: end
                .saturating_duration_since(c.base)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            records,
            bytes,
            parent,
            first: range.map(|r| r.0),
            next: range.map(|r| r.1),
        }));
    }

    pub fn trace<T>(
        &self,
        id: u64,
        stage: &'static str,
        records: usize,
        bytes: usize,
        f: impl FnOnce() -> T,
    ) -> T {
        let Some(start) = self.start() else {
            return f();
        };
        let result = f();
        self.record(id, stage, start, Instant::now(), records, bytes);
        result
    }
}

/// Flush buffered diagnostics from a control thread after a workload. Never call
/// this from a storage hot path. This flush does not fsync the diagnostic file.
pub fn flush_writer_stage_trace() -> io::Result<()> {
    let collector = COLLECTOR
        .get()
        .and_then(|slot| slot.lock().unwrap_or_else(|e| e.into_inner()).clone());
    if let Some(collector) = collector {
        let (tx, rx) = crossbeam_channel::bounded(1);
        collector
            .tx
            .as_ref()
            .expect("live collector")
            .send(Row::Flush(tx))
            .map_err(|_| io::Error::other("trace collector stopped"))?;
        rx.recv()
            .map_err(|_| io::Error::other("trace collector stopped"))??;
    }
    Ok(())
}

/// Finish a capture after all instrumented writers have stopped. Returns
/// WouldBlock while a writer still owns the collector. Call from a control thread.
pub fn finish_writer_stage_trace() -> io::Result<()> {
    let Some(slot) = COLLECTOR.get() else {
        return Ok(());
    };
    let mut shared = slot.lock().unwrap_or_else(|e| e.into_inner());
    if shared
        .as_ref()
        .is_some_and(|collector| Arc::strong_count(collector) > 1)
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "trace writers are still active",
        ));
    }
    // Holding the registry lock prevents reopening the destination during drain.
    if let Some(collector) = shared.take() {
        match Arc::try_unwrap(collector) {
            Ok(mut collector) => collector.finish(),
            Err(collector) => {
                *shared = Some(collector);
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "trace writers are still active",
                ))
            }
        }
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writers_share_clock_and_ids_and_preserve_metadata() {
        let dir = crate::test_dir!("trace_identity");
        let output = dir.root.join("trace.jsonl");
        let collector = Collector::open(&output, true).unwrap();
        let a =
            WriterStageTracer::register(collector.clone(), Path::new("messages/quoted\"path"), 7);
        let b = WriterStageTracer::register(collector.clone(), Path::new("events"), 9);
        let start = Instant::now();
        let a_id = a.next_work_id();
        let b_id = b.next_work_id();
        assert_ne!(a_id, b_id);
        assert_ne!(a.writer, b.writer);
        a.record(a_id, "stage", start, start, 2, 128);
        b.record(b_id, "stage", start, start, 1, 64);
        drop((a, b, collector));
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let stages: Vec<_> = rows.iter().filter(|r| r["stage"] == "stage").collect();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0]["start_ns"], stages[1]["start_ns"]);
        assert_eq!(rows[1]["epoch"], 7);
        assert_eq!(rows[1]["path"], "messages/quoted\"path");
        let summary = rows.last().unwrap();
        assert_eq!(summary["closed"], true);
        assert_eq!(summary["emitted"], 4);
        assert_eq!(summary["dropped"], 0);
        assert!(summary["emission_ns"].as_u64().unwrap() > 0);
        assert!(
            Collector::open(&output, false).is_err(),
            "never truncate a prior capture"
        );
    }

    #[test]
    fn full_collector_drops_instead_of_blocking_storage() {
        let (tx, _rx) = crossbeam_channel::bounded(1);
        let collector = Arc::new(Collector {
            base: Instant::now(),
            next_id: AtomicU64::new(1),
            tx: Some(tx),
            worker: None,
            counters: Arc::new(Counters::default()),
            measure: false,
        });
        let tracer = WriterStageTracer::register(collector.clone(), Path::new("log"), 0);
        for _ in 0..100 {
            tracer.event(2, "stage", None, 1, 16, None, Some((0, 1)));
        }
        assert_eq!(collector.counters.emitted.load(Ordering::Relaxed), 1);
        assert_eq!(collector.counters.dropped.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn disabled_instrumentation_runs_body_once_without_ids() {
        let tracer = WriterStageTracer::default();
        assert!(tracer.start().is_none());
        assert_eq!(tracer.next_work_id(), 0);
        let mut calls = 0;
        let result = tracer.trace(0, "stage", 0, 0, || {
            calls += 1;
            42
        });
        assert_eq!(result, 42);
        assert_eq!(calls, 1);
    }
}
