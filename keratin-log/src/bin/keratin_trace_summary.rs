use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Debug, Clone)]
struct Event {
    id: u64,
    stage: String,
    start_ns: u128,
    end_ns: u128,
    duration_ns: u128,
    records: usize,
    bytes: usize,
}

#[derive(Debug, Default)]
struct StageStats {
    events: usize,
    total_duration_ns: u128,
    total_records: u128,
    total_bytes: u128,
    durations_ns: Vec<u128>,
    intervals: Vec<(u128, u128)>,
}

#[derive(Debug)]
struct WorkStats {
    events: usize,
    start_ns: u128,
    end_ns: u128,
    total_duration_ns: u128,
}

impl WorkStats {
    fn new(event: &Event) -> Self {
        Self {
            events: 1,
            start_ns: event.start_ns,
            end_ns: event.end_ns,
            total_duration_ns: event.duration_ns,
        }
    }

    fn push(&mut self, event: &Event) {
        self.events = self.events.saturating_add(1);
        self.start_ns = self.start_ns.min(event.start_ns);
        self.end_ns = self.end_ns.max(event.end_ns);
        self.total_duration_ns = self.total_duration_ns.saturating_add(event.duration_ns);
    }

    fn span_ns(&self) -> u128 {
        self.end_ns.saturating_sub(self.start_ns)
    }
}

#[derive(Debug)]
struct Summary {
    events: usize,
    min_start_ns: u128,
    max_end_ns: u128,
    stages: BTreeMap<String, StageStats>,
    works: HashMap<u64, WorkStats>,
}

impl Summary {
    fn new() -> Self {
        Self {
            events: 0,
            min_start_ns: u128::MAX,
            max_end_ns: 0,
            stages: BTreeMap::new(),
            works: HashMap::new(),
        }
    }

    fn push(&mut self, event: Event) {
        self.events = self.events.saturating_add(1);
        self.min_start_ns = self.min_start_ns.min(event.start_ns);
        self.max_end_ns = self.max_end_ns.max(event.end_ns);

        let stage = self.stages.entry(event.stage.clone()).or_default();
        stage.events = stage.events.saturating_add(1);
        stage.total_duration_ns = stage.total_duration_ns.saturating_add(event.duration_ns);
        stage.total_records = stage.total_records.saturating_add(event.records as u128);
        stage.total_bytes = stage.total_bytes.saturating_add(event.bytes as u128);
        stage.durations_ns.push(event.duration_ns);
        stage.intervals.push((event.start_ns, event.end_ns));

        self.works
            .entry(event.id)
            .and_modify(|work| work.push(&event))
            .or_insert_with(|| WorkStats::new(&event));
    }

    fn span_ns(&self) -> u128 {
        if self.events == 0 {
            0
        } else {
            self.max_end_ns.saturating_sub(self.min_start_ns)
        }
    }
}

fn main() -> Result<(), String> {
    let options = Options::parse(env::args().skip(1))?;
    if options.help {
        print_usage();
        return Ok(());
    }

    let Some(path) = options.path else {
        print_usage();
        return Err("missing trace CSV path".to_string());
    };

    let summary = read_summary(&path)?;
    print_summary(&path, &summary, options.top);
    Ok(())
}

#[derive(Debug)]
struct Options {
    path: Option<String>,
    top: usize,
    help: bool,
}

impl Options {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut path = None;
        let mut top = 10usize;
        let mut help = false;
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    help = true;
                }
                "--top" => {
                    let Some(value) = args.next() else {
                        return Err("--top requires a value".to_string());
                    };
                    top = value
                        .parse::<usize>()
                        .map_err(|err| format!("invalid --top value {value:?}: {err}"))?;
                }
                _ if path.is_none() => {
                    path = Some(arg);
                }
                _ => {
                    return Err(format!("unexpected argument {arg:?}"));
                }
            }
        }

        Ok(Self { path, top, help })
    }
}

fn print_usage() {
    eprintln!(
        "usage: keratin_trace_summary [--top N] TRACE.csv\n\
         \n\
         Summarizes Keratin writer-stage traces generated with\n\
         KERATIN_WRITER_STAGE_TRACE=/path/to/trace.csv."
    );
}

fn read_summary(path: &str) -> Result<Summary, String> {
    let file = File::open(path).map_err(|err| format!("could not open {path:?}: {err}"))?;
    let reader = BufReader::new(file);
    let mut summary = Summary::new();

    for (idx, line) in reader.lines().enumerate() {
        let line_no = idx + 1;
        let line = line.map_err(|err| format!("could not read line {line_no}: {err}"))?;
        let Some(event) =
            parse_event_line(&line).map_err(|err| format!("line {line_no}: {err}"))?
        else {
            continue;
        };
        summary.push(event);
    }

    Ok(summary)
}

fn parse_event_line(line: &str) -> Result<Option<Event>, String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with("id,stage,") {
        return Ok(None);
    }

    let fields: Vec<&str> = line.split(',').collect();
    if fields.len() != 7 {
        return Err(format!("expected 7 CSV fields, got {}", fields.len()));
    }

    let id = parse_field::<u64>(fields[0], "id")?;
    let stage = fields[1].to_string();
    if stage.is_empty() {
        return Err("stage is empty".to_string());
    }
    let start_ns = parse_field::<u128>(fields[2], "start_ns")?;
    let end_ns = parse_field::<u128>(fields[3], "end_ns")?;
    let reported_duration_ns = parse_field::<u128>(fields[4], "duration_ns")?;
    let records = parse_field::<usize>(fields[5], "records")?;
    let bytes = parse_field::<usize>(fields[6], "bytes")?;
    let duration_ns = end_ns.saturating_sub(start_ns);

    if end_ns < start_ns {
        return Err(format!("end_ns {end_ns} is before start_ns {start_ns}"));
    }
    if reported_duration_ns != duration_ns {
        return Err(format!(
            "duration_ns {reported_duration_ns} does not match end-start {duration_ns}"
        ));
    }

    Ok(Some(Event {
        id,
        stage,
        start_ns,
        end_ns,
        duration_ns,
        records,
        bytes,
    }))
}

fn parse_field<T>(field: &str, name: &'static str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    field
        .parse::<T>()
        .map_err(|err| format!("invalid {name} {field:?}: {err}"))
}

fn print_summary(path: &str, summary: &Summary, top: usize) {
    let span_ns = summary.span_ns();
    println!("Trace: {path}");
    println!("Events: {}", summary.events);
    println!("Work ids: {}", summary.works.len());
    println!("Stages: {}", summary.stages.len());
    println!("Span: {}", fmt_ms(span_ns));

    println!();
    print_stage_table(summary, span_ns);

    println!();
    print_overlap_table(summary, span_ns);

    println!();
    print_work_table(summary, top);
}

fn print_stage_table(summary: &Summary, span_ns: u128) {
    println!("Per-stage summary:");
    println!(
        "{:<22} {:>10} {:>10} {:>10} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12}",
        "stage",
        "events",
        "sum",
        "active",
        "util",
        "avg",
        "p50",
        "p95",
        "p99",
        "max",
        "records",
        "MiB"
    );

    for (stage, stats) in &summary.stages {
        let mut durations = stats.durations_ns.clone();
        durations.sort_unstable();
        let avg = stats.total_duration_ns / stats.events.max(1) as u128;
        let active_ns = merged_active_ns(&stats.intervals);
        let util = percent(active_ns, span_ns);
        let mib = stats.total_bytes as f64 / 1024.0 / 1024.0;

        println!(
            "{:<22} {:>10} {:>10} {:>10} {:>7.2}% {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12.2}",
            stage,
            stats.events,
            fmt_ms(stats.total_duration_ns),
            fmt_ms(active_ns),
            util,
            fmt_us(avg),
            fmt_us(percentile(&durations, 50)),
            fmt_us(percentile(&durations, 95)),
            fmt_us(percentile(&durations, 99)),
            fmt_us(percentile(&durations, 100)),
            stats.total_records,
            mib
        );
    }
}

fn print_overlap_table(summary: &Summary, span_ns: u128) {
    println!("Stage overlap:");
    let merged: BTreeMap<&str, Vec<(u128, u128)>> = summary
        .stages
        .iter()
        .map(|(stage, stats)| (stage.as_str(), merged_intervals(&stats.intervals)))
        .collect();
    let stages: Vec<&str> = merged.keys().copied().collect();
    let mut printed = false;

    for i in 0..stages.len() {
        for j in (i + 1)..stages.len() {
            let a = stages[i];
            let b = stages[j];
            let overlap_ns = intersection_ns(&merged[a], &merged[b]);
            if overlap_ns == 0 {
                continue;
            }
            printed = true;
            println!(
                "{:<22} {:<22} {:>10} {:>7.2}%",
                a,
                b,
                fmt_ms(overlap_ns),
                percent(overlap_ns, span_ns)
            );
        }
    }

    if !printed {
        println!("no overlapping stage intervals");
    }
}

fn print_work_table(summary: &Summary, top: usize) {
    let mut spans: Vec<u128> = summary.works.values().map(WorkStats::span_ns).collect();
    spans.sort_unstable();

    println!("Work-id span summary:");
    println!(
        "count={} p50={} p95={} p99={} max={}",
        spans.len(),
        fmt_ms(percentile(&spans, 50)),
        fmt_ms(percentile(&spans, 95)),
        fmt_ms(percentile(&spans, 99)),
        fmt_ms(percentile(&spans, 100))
    );

    if top == 0 {
        return;
    }

    let mut slowest: Vec<(&u64, &WorkStats)> = summary.works.iter().collect();
    slowest.sort_by(|a, b| b.1.span_ns().cmp(&a.1.span_ns()).then_with(|| a.0.cmp(b.0)));

    println!();
    println!("Slowest work ids:");
    println!("{:>12} {:>8} {:>10} {:>10}", "id", "events", "span", "sum");
    for (id, work) in slowest.into_iter().take(top) {
        println!(
            "{:>12} {:>8} {:>10} {:>10}",
            id,
            work.events,
            fmt_ms(work.span_ns()),
            fmt_ms(work.total_duration_ns)
        );
    }
}

fn merged_intervals(intervals: &[(u128, u128)]) -> Vec<(u128, u128)> {
    let mut intervals = intervals.to_vec();
    intervals.sort_unstable_by_key(|(start, end)| (*start, *end));
    let mut merged: Vec<(u128, u128)> = Vec::new();

    for (start, end) in intervals {
        let Some((_, last_end)) = merged.last_mut() else {
            merged.push((start, end));
            continue;
        };
        if start <= *last_end {
            *last_end = (*last_end).max(end);
        } else {
            merged.push((start, end));
        }
    }

    merged
}

fn merged_active_ns(intervals: &[(u128, u128)]) -> u128 {
    merged_intervals(intervals)
        .into_iter()
        .map(|(start, end)| end.saturating_sub(start))
        .sum()
}

fn intersection_ns(a: &[(u128, u128)], b: &[(u128, u128)]) -> u128 {
    let mut i = 0usize;
    let mut j = 0usize;
    let mut total = 0u128;

    while i < a.len() && j < b.len() {
        let start = a[i].0.max(b[j].0);
        let end = a[i].1.min(b[j].1);
        total = total.saturating_add(end.saturating_sub(start));

        if a[i].1 <= b[j].1 {
            i += 1;
        } else {
            j += 1;
        }
    }

    total
}

fn percentile(sorted_values: &[u128], percentile: usize) -> u128 {
    if sorted_values.is_empty() {
        return 0;
    }
    let percentile = percentile.min(100);
    let idx = ((sorted_values.len() - 1) * percentile).div_ceil(100);
    sorted_values[idx]
}

fn percent(value: u128, total: u128) -> f64 {
    if total == 0 {
        0.0
    } else {
        value as f64 * 100.0 / total as f64
    }
}

fn fmt_ms(ns: u128) -> String {
    format!("{:.3}ms", ns as f64 / 1_000_000.0)
}

fn fmt_us(ns: u128) -> String {
    format!("{:.3}us", ns as f64 / 1_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_event_line() {
        let event = parse_event_line("7,stage_reqs,10,25,15,2,512")
            .unwrap()
            .unwrap();
        assert_eq!(event.id, 7);
        assert_eq!(event.stage, "stage_reqs");
        assert_eq!(event.duration_ns, 15);
        assert_eq!(event.records, 2);
        assert_eq!(event.bytes, 512);
    }

    #[test]
    fn rejects_duration_mismatch() {
        let err = parse_event_line("7,stage_reqs,10,25,14,2,512").unwrap_err();
        assert!(err.contains("does not match"));
    }

    #[test]
    fn merges_intervals() {
        let merged = merged_intervals(&[(10, 20), (18, 30), (35, 40), (5, 8)]);
        assert_eq!(merged, vec![(5, 8), (10, 30), (35, 40)]);
        assert_eq!(
            merged_active_ns(&[(10, 20), (18, 30), (35, 40), (5, 8)]),
            28
        );
    }

    #[test]
    fn computes_intersection() {
        let a = merged_intervals(&[(0, 10), (20, 30)]);
        let b = merged_intervals(&[(5, 25)]);
        assert_eq!(intersection_ns(&a, &b), 10);
    }

    #[test]
    fn percentile_uses_ceil_rank() {
        let values = [10, 20, 30, 40, 50];
        assert_eq!(percentile(&values, 0), 10);
        assert_eq!(percentile(&values, 50), 30);
        assert_eq!(percentile(&values, 95), 50);
        assert_eq!(percentile(&values, 100), 50);
    }
}
