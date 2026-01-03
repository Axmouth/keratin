use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::record::{
    ByteRemainder, DecodedRecord, RECORD_HEADER_LEN, decode_header_prefix, decode_record_prefix,
};

#[derive(Debug, Clone)]
pub struct OwnedRecord {
    pub flags: u16,
    pub timestamp_ms: u64,
    pub offset: u64,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

pub struct LogReader {
    root: PathBuf,
    segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
}

impl LogReader {
    pub fn new(
        root: impl AsRef<Path>,
        segment_mapping: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            segment_mapping,
        }
    }

    pub fn fetch(&self, offset: u64) -> io::Result<Option<OwnedRecord>> {
        let base = match self.find_segment_base(offset)? {
            Some(b) => b,
            None => return Ok(None),
        };

        let mut log = self.open_log(base)?;
        let pos = self.seek_near(base, offset)?;

        match self.scan_forward(&mut log, pos, Some(offset))? {
            Some((r, _next_pos)) => Ok(Some(r)),
            None => Ok(None),
        }
    }

    pub fn scan_from(&self, from: u64, max: usize) -> io::Result<Vec<OwnedRecord>> {
        let mut out = Vec::with_capacity(max);
        let mut cur = from;

        let mut i = 0;
        while out.len() < max {
            i += 1;
            // dbg!(i);
            // let now = std::time::Instant::now();
            let base = match self.find_segment_base(cur)? {
                Some(b) => b,
                None => break,
            };

            let mut log = self.open_log(base)?;
            let pos = self.seek_near(base, cur)?;

            let before_cur = cur;
            let before_len = out.len();

            self.scan_forward_exact(&mut log, pos, &mut cur, None, &mut out, max)?;

            if cur == before_cur && out.len() == before_len {
                if let Some(next) = self.next_segment_base(base) {
                    // next segment offsets start at `next`
                    cur = cur.max(next);
                    continue;
                } else {
                    break; // no more segments
                }
            }
        }

        Ok(out)
    }

    // ---- internal helpers ----

    fn open_log(&self, base: u64) -> io::Result<std::fs::File> {
        let p = self.root.join("segments").join(format!("{:020}.log", base));
        OpenOptions::new().read(true).open(p)
    }

    fn open_idx(&self, base: u64) -> io::Result<std::fs::File> {
        let p = self.root.join("segments").join(format!("{:020}.idx", base));
        OpenOptions::new().read(true).open(p)
    }

    fn seek_near(&self, base: u64, offset: u64) -> io::Result<u64> {
        let mut idx = self.open_idx(base)?;
        let target_rel = (offset - base) as u32;

        let len = idx.metadata()?.len() as usize;
        if len < crate::index::IDX_HEADER_LEN as usize {
            return Ok(crate::segment::LOG_HEADER_LEN as u64);
        }
        let entries_bytes = len - crate::index::IDX_HEADER_LEN as usize;
        let n = entries_bytes / crate::index::IDX_ENTRY_LEN;
        if n == 0 {
            return Ok(crate::segment::LOG_HEADER_LEN as u64);
        }

        let mut lo: isize = 0;
        let mut hi: isize = n as isize - 1;
        let mut best_pos: u64 = crate::segment::LOG_HEADER_LEN as u64;

        let mut buf = [0u8; crate::index::IDX_ENTRY_LEN];

        while lo <= hi {
            let mid = (lo + hi) / 2;
            let off = crate::index::IDX_HEADER_LEN as u64
                + (mid as u64) * crate::index::IDX_ENTRY_LEN as u64;
            idx.seek(SeekFrom::Start(off))?;
            idx.read_exact(&mut buf)?;

            let rel = u32::from_be_bytes(buf[0..4].try_into().unwrap());
            let pos = u64::from_be_bytes(buf[8..16].try_into().unwrap());

            if rel <= target_rel {
                best_pos = pos;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        Ok(best_pos)
    }

    fn scan_forward_exact(
        &self,
        file: &mut std::fs::File,
        start_pos: u64,
        cur: &mut u64,
        want: Option<u64>,
        out: &mut Vec<OwnedRecord>,
        max: usize,
    ) -> io::Result<()> {
        const SLAB: usize = 4096; // SSD page aligned

        let mut buf = vec![0u8; SLAB];
        let mut window: Vec<u8> = Vec::with_capacity(SLAB * 2);
        let mut file_pos = start_pos;

        file.seek(SeekFrom::Start(start_pos))?;

        while out.len() < max {
            // Ensure we have enough data to attempt a decode
            if window.len() < RECORD_HEADER_LEN {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    return Ok(()); // EOF
                }
                window.extend_from_slice(&buf[..n]);
                continue;
            }

            match decode_record_prefix(&window) {
                Ok((rec, used)) => {
                    let rec_start = file_pos;
                    let next_pos = rec_start + used as u64;

                    // Enforce monotonic offsets
                    if rec.offset < *cur {
                        window.drain(..used);
                        file_pos = next_pos;
                        continue;
                    }

                    *cur = rec.offset + 1;

                    if want.map(|o| rec.offset == o).unwrap_or(true) {
                        out.push(to_owned(rec));
                        if out.len() >= max {
                            return Ok(());
                        }
                    }

                    window.drain(..used);
                    file_pos = next_pos;
                }

                Err(crate::record::RecordError::Truncated) => {
                    // Need more bytes
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        return Ok(()); // EOF in middle of record
                    }
                    window.extend_from_slice(&buf[..n]);
                }

                Err(e) => {
                    // Corruption: resync by shifting one byte forward
                    tracing::error!("{:#?}", e);
                    window.drain(..1);
                    file_pos += 1;
                }
            }
        }

        Ok(())
    }

    fn scan_forward(
        &self,
        log: &mut std::fs::File,
        start_pos: u64,
        want: Option<u64>,
    ) -> io::Result<Option<(OwnedRecord, u64)>> {
        // TODO: Match storage unit for most ssds?
        let mut buf = vec![0u8; 2 * 1024];
        let mut window: Vec<u8> = Vec::with_capacity(2 * 1024);
        // TODO: Read header only and use to optimize read exact length? Consume

        // Where we are in the file (byte position) at the start of `window`
        let mut file_pos = start_pos;

        log.seek(SeekFrom::Start(start_pos))?;
        loop {
            let n = log.read(&mut buf)?;
            if n == 0 {
                return Ok(None);
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
                        // rec starts at: file_pos + consumed
                        let rec_start = file_pos + consumed as u64;
                        consumed += used;

                        // next record would start at:
                        let next_pos = rec_start + used as u64;

                        if want.map(|o| rec.offset == o).unwrap_or(true) {
                            return Ok(Some((to_owned(rec), next_pos)));
                        }

                        if let Some(o) = want
                            && rec.offset > o
                        {
                            return Ok(None);
                        }
                    }
                    Err(crate::record::RecordError::Truncated) => break,
                    Err(_) => return Ok(None),
                }
            }

            // We consumed `consumed` bytes from the start of window, so advance file_pos too.
            window.drain(0..consumed);
            file_pos += consumed as u64;
        }
    }

    fn next_segment_base(&self, base: u64) -> Option<u64> {
        self.segment_mapping
            .read()
            .range((base + 1)..)
            .next()
            .map(|(k, _)| *k)
    }

    fn find_segment_base(&self, offset: u64) -> io::Result<Option<u64>> {
        // let mut bases = Vec::new();
        // for e in std::fs::read_dir(self.root.join("segments"))? {
        //     let e = e?;
        //     if let Some(s) = e.file_name().to_str()
        //         && let Some(stem) = s.strip_suffix(".log")
        //         && let Ok(b) = stem.parse::<u64>()
        //     {
        //         bases.push(b);
        //     }
        // }
        // bases.sort_unstable();
        // Ok(bases.into_iter().rfind(|b| *b <= offset))
        Ok(self
            .segment_mapping
            .read()
            .keys()
            .copied()
            .rfind(|b| *b <= offset))
    }
}

fn to_owned(r: DecodedRecord<'_>) -> OwnedRecord {
    OwnedRecord {
        flags: r.flags,
        timestamp_ms: r.timestamp_ms,
        offset: r.offset,
        headers: r.headers.to_vec(),
        payload: r.payload.to_vec(),
    }
}
