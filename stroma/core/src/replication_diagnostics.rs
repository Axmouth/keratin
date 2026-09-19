//! Failure-only, metadata-only event windows. The full mismatch remains an error.
use crate::{StromaError, StromaEvent};
use keratin_log::{IoError, Keratin};
use std::sync::Arc;

const WINDOW: usize = 8;
const MAX_DECODE_BYTES: usize = 64 * 1024;

// These fields deliberately exclude declaration values, cursor names, DLQ
// destinations, message headers, and bodies. Debug output is the log schema.
#[allow(dead_code)]
#[derive(Debug)]
struct EventCoordinate {
    event_offset: u64,
    kind: &'static str,
    max_message_offset: Option<u64>,
}

fn coordinate(offset: u64, event: &StromaEvent) -> EventCoordinate {
    use StromaEvent::*;
    let kind = match event {
        Enqueue { .. } => "Enqueue",
        EnqueueMany { .. } => "EnqueueMany",
        EnqueueDelayed { .. } => "EnqueueDelayed",
        EnqueueDelayedMany { .. } => "EnqueueDelayedMany",
        CancelEnqueueMany { .. } => "CancelEnqueueMany",
        MarkInflight { .. } => "MarkInflight",
        MarkInflightMany { .. } => "MarkInflightMany",
        Ack { .. } => "Ack",
        AckMany { .. } => "AckMany",
        ReleaseInflightMany { .. } => "ReleaseInflightMany",
        Nack { .. } => "Nack",
        NackMany { .. } => "NackMany",
        DeadLetter { .. } => "DeadLetter",
        DeadLetterCommit { .. } => "DeadLetterCommit",
        Declare(_) => "Declare",
        ResetQueue { .. } => "ResetQueue",
        Snapshot { .. } => "Snapshot",
        CursorCommit { .. } => "CursorCommit",
        CursorCommitBatch { .. } => "CursorCommitBatch",
        StreamTruncate { .. } => "StreamTruncate",
    };
    EventCoordinate {
        event_offset: offset,
        kind,
        max_message_offset: event.max_referenced_msg_offset(),
    }
}

fn local_window(log: &Keratin, from: u64) -> Option<Vec<EventCoordinate>> {
    let records = log.reader().scan_from(from, WINDOW).ok()?;
    Some(
        records
            .into_iter()
            .map(|record| {
                let event = (record.payload.len() <= MAX_DECODE_BYTES)
                    .then(|| StromaEvent::decode(&record.payload).ok())
                    .flatten();
                match event {
                    Some(event) => coordinate(record.offset, &event),
                    None => EventCoordinate {
                        event_offset: record.offset,
                        kind: "undecoded",
                        max_message_offset: None,
                    },
                }
            })
            .collect(),
    )
}

/// Both message- and event-log errors identify their namespace. Event windows
/// are read only for the first report in each per-log 30-second interval.
pub(crate) async fn overlap_error(
    error: IoError,
    log: Arc<Keratin>,
    topic: &str,
    partition: u32,
    group: Option<&str>,
    epoch: u64,
    first: u64,
    count: usize,
    events: Option<&[StromaEvent]>,
) -> StromaError {
    let Some(diagnostic) = error.overlap_diagnostic() else {
        return StromaError::Io(error.to_string());
    };
    let kind = if events.is_some() {
        "events"
    } else {
        "messages"
    };
    let mut context = format!(
        "{error} [log={kind}, epoch={epoch}, incoming_first={first}, incoming_count={count}]"
    );
    if diagnostic.emit_details {
        if let Some(events) = events {
            let offset = diagnostic.offset;
            let start = offset.saturating_sub(first).saturating_sub(3) as usize;
            let incoming: Vec<_> = events
                .iter()
                .enumerate()
                .skip(start)
                .take(WINDOW)
                .map(|(i, event)| coordinate(first + i as u64, event))
                .collect();
            // A later checkpoint may replace history; these reads are explicitly
            // best-effort observations, not an atomic distributed snapshot.
            let windows = tokio::task::spawn_blocking(move || {
                let around = local_window(&log, offset.saturating_sub(3).max(log.head_offset()));
                let latest = local_window(
                    &log,
                    log.next_offset()
                        .saturating_sub(WINDOW as u64)
                        .max(log.head_offset()),
                );
                (around, latest)
            })
            .await
            .ok();
            context.push_str(&format!(
                " local_event_windows={windows:?} incoming_events={incoming:?}"
            ));
            tracing::error!(diagnostic_id = diagnostic.report_id, topic, partition, group,
                epoch, log_kind = kind, local_event_windows = ?windows, incoming_events = ?incoming,
                "replication overlap event coordinates");
        } else {
            tracing::error!(
                diagnostic_id = diagnostic.report_id,
                topic,
                partition,
                group,
                epoch,
                log_kind = kind,
                first_offset = first,
                record_count = count,
                "replication overlap message coordinates"
            );
        }
    }
    StromaError::Io(context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DLQDiscardPolicyWire, DeclareMeta};

    #[test]
    fn event_coordinates_exclude_user_control_values() {
        let event = StromaEvent::Declare(DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::CustomDQL {
                tp: "secret-destination".into(),
                part: 123,
                group: Some("private-group".into()),
            }),
            dlq_max_retries: Some(123456),
            default_message_ttl_ms: Some(987654),
        });
        let text = format!("{:?}", coordinate(42, &event));
        assert!(text.contains("Declare") && text.contains("42"));
        for secret in ["secret-destination", "private-group", "123456", "987654"] {
            assert!(!text.contains(secret));
        }
        let cursor = StromaEvent::CursorCommit {
            name: "private-cursor".into(),
            offset: 9,
        };
        assert!(!format!("{:?}", coordinate(43, &cursor)).contains("private-cursor"));
    }
}
