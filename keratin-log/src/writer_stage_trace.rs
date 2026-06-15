use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    time::Instant,
};

use crossbeam_channel::Sender;

struct WriterStageEvent {
    id: u64,
    stage: &'static str,
    start_ns: u128,
    end_ns: u128,
    records: usize,
    bytes: usize,
}

pub struct WriterStageTracer {
    base: Instant,
    tx: Option<Sender<WriterStageEvent>>,
    next_id: u64,
    dropped: u64,
}

impl WriterStageTracer {
    pub fn from_env() -> Self {
        let Some(path) = std::env::var_os("KERATIN_WRITER_STAGE_TRACE") else {
            return Self::disabled();
        };

        let path = PathBuf::from(path);
        let file = match File::create(&path) {
            Ok(file) => file,
            Err(err) => {
                eprintln!(
                    "could not create Keratin writer stage trace at {}: {err}",
                    path.display()
                );
                return Self::disabled();
            }
        };

        let (tx, rx) = crossbeam_channel::bounded::<WriterStageEvent>(65_536);
        std::thread::spawn(move || {
            let mut writer = BufWriter::new(file);
            let _ = writeln!(writer, "id,stage,start_ns,end_ns,duration_ns,records,bytes");
            while let Ok(event) = rx.recv() {
                let duration_ns = event.end_ns.saturating_sub(event.start_ns);
                let _ = writeln!(
                    writer,
                    "{},{},{},{},{},{},{}",
                    event.id,
                    event.stage,
                    event.start_ns,
                    event.end_ns,
                    duration_ns,
                    event.records,
                    event.bytes
                );
            }
        });

        Self {
            base: Instant::now(),
            tx: Some(tx),
            next_id: 1,
            dropped: 0,
        }
    }

    fn disabled() -> Self {
        Self {
            base: Instant::now(),
            tx: None,
            next_id: 1,
            dropped: 0,
        }
    }

    pub fn next_work_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        id
    }

    pub fn record(
        &mut self,
        id: u64,
        stage: &'static str,
        start: Instant,
        end: Instant,
        records: usize,
        bytes: usize,
    ) {
        let Some(tx) = &self.tx else {
            return;
        };

        let event = WriterStageEvent {
            id,
            stage,
            start_ns: start.saturating_duration_since(self.base).as_nanos(),
            end_ns: end.saturating_duration_since(self.base).as_nanos(),
            records,
            bytes,
        };

        if tx.try_send(event).is_err() {
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}

impl Drop for WriterStageTracer {
    fn drop(&mut self) {
        if self.dropped > 0 {
            eprintln!("dropped {} Keratin writer stage trace events", self.dropped);
        }
    }
}
