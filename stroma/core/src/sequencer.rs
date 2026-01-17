use dashmap::DashMap;
use keratin_log::{AppendCompletion, IoError, Keratin};
use keratin_log::{CompletionPair, KeratinAppendCompletion};
use std::sync::Arc;

use crate::event::StromaEvent;
use crate::stroma::event_msg;
use crate::{Message, QueueState};

pub(crate) struct PublishIntent {
    pub(crate) tp: Box<str>,
    pub(crate) part: u32,
    pub(crate) payload: Vec<u8>,
    // completion to call when BOTH payload + event are durably appended
    pub(crate) outer_completion: Box<dyn AppendCompletion<IoError>>,
}

pub(crate) async fn run_msg_sequencer(
    mut rx: tokio::sync::mpsc::Receiver<PublishIntent>,
    tp: &str,
    part: u32,
    msg_log_map: Arc<DashMap<(Box<str>, u32), Arc<Keratin>>>,
    event_log_map: Arc<DashMap<(Box<str>, u32), Arc<Keratin>>>,
    queues: Arc<DashMap<(Box<str>, u32), QueueState>>,
) {
    while let Some(intent) = rx.recv().await {
        let msg_log = match msg_log_map.get(&(tp.into(), part)) {
            Some(l) => l.clone(),
            None => {
                intent
                    .outer_completion
                    .complete(Err(IoError::new("no message log")));
                continue;
            }
        };

        let event_log = match event_log_map.get(&(tp.into(), part)) {
            Some(l) => l.clone(),
            None => {
                intent
                    .outer_completion
                    .complete(Err(IoError::new("no event log")));
                continue;
            }
        };

        // Step 1: append payload, but do not block hot path (we are in sequencer task)
        let (payload_comp, payload_rx) = KeratinAppendCompletion::pair();

        if let Err(e) = msg_log
            .append_enqueue(
                Message {
                    flags: 0,
                    headers: vec![],
                    payload: intent.payload.clone(),
                },
                None,
                payload_comp,
            )
            .map_err(IoError::new)
        {
            intent.outer_completion.complete(Err(e));
            continue;
        };

        // Wait for payload append result (sequencer can also pipeline; see below)
        let payload_res = payload_rx.await;
        let append_res = match payload_res {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                intent.outer_completion.complete(Err(IoError::new(e)));
                continue;
            }
            Err(_) => {
                intent
                    .outer_completion
                    .complete(Err(IoError::new("canceled")));
                continue;
            }
        };

        let offset = append_res.base_offset; // whatever your AppendResult exposes

        // Step 2: append event
        let (event_comp, event_rx) = KeratinAppendCompletion::pair();

        let ev = StromaEvent::Enqueue {
            retries: 0,
            tp: intent.tp.clone(),
            part: intent.part,
            off: offset,
        };

        let msg = match event_msg(&ev) {
            Ok(m) => m,
            Err(e) => {
                intent.outer_completion.complete(Err(IoError::new(e)));
                continue;
            }
        };

        if let Err(e) = event_log
            .append_enqueue(msg, None, event_comp)
            .map_err(IoError::new)
        {
            intent.outer_completion.complete(Err(e));
            continue;
        };

        match event_rx.await {
            Ok(Ok(_)) => {
                intent.outer_completion.complete(Ok(append_res));
                if let Some(mut q) = queues.get_mut(&(tp.into(), part)) {
                    q.enqueue(offset, 0);
                }
            } // or return event offset too
            Ok(Err(e)) => intent.outer_completion.complete(Err(IoError::new(e))),
            Err(_) => intent
                .outer_completion
                .complete(Err(IoError::new("canceled"))),
        }
    }
}
