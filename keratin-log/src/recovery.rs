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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Record, encode_record};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn rec(offset: u64, payload_len: usize) -> Vec<u8> {
        let mut b = Vec::new();
        encode_record(
            &mut b,
            &Record {
                flags: 0,
                timestamp_ms: 1_000 + offset,
                offset,
                headers: b"h",
                payload: &vec![7u8; payload_len],
            },
        )
        .unwrap();
        b
    }

    /// Scan `data` written to a real file (scan_last_good takes `&File`).
    fn scan_bytes(data: &[u8]) -> ScanResult {
        let uniq = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("keratin_scan_probe_{uniq}"));
        std::fs::write(&path, data).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let res = scan_last_good(&file, 0, 512).unwrap();
        let _ = std::fs::remove_file(&path);
        res
    }

    /// Preallocation leaves the segment tail zero-filled. The scan MUST stop at
    /// the last real record and never decode the zero run as a bogus record.
    /// This is the invariant segment preallocation relies on.
    #[test]
    fn scan_stops_cleanly_at_zero_padding() {
        let mut data = Vec::new();
        for off in 0..3u64 {
            data.extend_from_slice(&rec(off, 16));
        }
        let records_end = data.len() as u64;
        data.extend_from_slice(&[0u8; 8192]); // preallocated, unwritten

        let res = scan_bytes(&data);
        assert_eq!(res.last_good_pos, records_end);
        assert_eq!(res.last_offset, Some(2));
    }

    /// A partial record (crash mid-write) followed by the zero padding: the CRC
    /// covers the zero-filled tail, so it fails and the partial is discarded back
    /// to the last complete record.
    #[test]
    fn scan_discards_partial_record_before_padding() {
        let mut data = Vec::new();
        for off in 0..3u64 {
            data.extend_from_slice(&rec(off, 16));
        }
        let records_end = data.len() as u64;
        let partial = rec(3, 16);
        data.extend_from_slice(&partial[..partial.len() - 8]); // truncated (loses crc)
        data.extend_from_slice(&[0u8; 8192]);

        let res = scan_bytes(&data);
        assert_eq!(res.last_good_pos, records_end, "partial record must be discarded");
        assert_eq!(res.last_offset, Some(2));
    }

    /// A freshly preallocated, all-zero segment body has no valid record, so the
    /// tail stays at the start.
    #[test]
    fn scan_of_only_zeros_returns_start() {
        let res = scan_bytes(&vec![0u8; 8192]);
        assert_eq!(res.last_good_pos, 0);
        assert_eq!(res.last_offset, None);
    }
}
