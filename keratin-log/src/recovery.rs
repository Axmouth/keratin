use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::record::{RecordError, decode_record_prefix};

pub struct ScanResult {
    pub last_good_pos: u64,
    pub last_offset: Option<u64>,
}

/// Sequentially scan the log file from `start_pos` and find last valid record boundary.
/// This is used on startup to repair after crash.
pub fn scan_last_good(mut file: &File, start_pos: u64, buf_size: usize) -> io::Result<ScanResult> {
    // NOTE: simple implementation: read chunks and decode record-by-record.
    // For v0 correctness, easiest is to read progressively and maintain a window buffer.

    let mut pos = start_pos;
    let mut last_good_pos = start_pos;
    let mut last_offset = None;

    let mut buf = vec![0u8; buf_size];
    let mut window: Vec<u8> = Vec::new();

    loop {
        file.seek(SeekFrom::Start(pos))?;
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        window.extend_from_slice(&buf[..n]);

        let mut consumed = 0usize;
        loop {
            let slice = &window[consumed..];
            if slice.is_empty() {
                break;
            }
            match decode_record_prefix(slice) {
                Ok((rec, used)) => {
                    consumed += used;
                    last_good_pos = pos + consumed as u64;
                    last_offset = Some(rec.offset);
                }
                Err(RecordError::Truncated) => {
                    // need more bytes
                    break;
                }
                Err(_bad) => {
                    // corruption / partial write -> stop; truncate to last_good_pos
                    return Ok(ScanResult {
                        last_good_pos,
                        last_offset,
                    });
                }
            }
        }

        // Drop consumed bytes
        if consumed > 0 {
            window.drain(0..consumed);
        }

        // Advance read position by n bytes (but account for bytes still in window)
        pos += n as u64;
    }

    // If we ended with leftover undecodable bytes, treat as truncated/partial.
    Ok(ScanResult {
        last_good_pos,
        last_offset,
    })
}
