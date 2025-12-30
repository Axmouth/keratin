use std::io;

pub type Offset = u64;
pub type UnixMillis = u64;

pub const STROMA_MAGIC: &[u8; 8] = b"STROMA\0\0";
pub const STROMA_VER: u16 = 1;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    MarkInflight = 1,
    Ack = 2,
    ClearInflight = 3,
    ResetGroup = 4,
    Snapshot = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StromaEvent {
    MarkInflight {
        tp: String,
        part: u32,
        group: String,
        off: Offset,
        deadline: UnixMillis,
    },
    Ack {
        tp: String,
        part: u32,
        group: String,
        off: Offset,
    },
    ClearInflight {
        tp: String,
        part: u32,
        group: String,
        off: Offset,
    },
    ResetGroup {
        tp: String,
        part: u32,
        group: String,
    },
    /// Snapshot is a complete state image for a single (tp,part,group).
    /// It’s OK if it’s “big”; it happens rarely.
    Snapshot {
        tp: String,
        part: u32,
        group: String,
        /// Encoded GroupState snapshot payload (see state snapshot helpers below)
        blob: Vec<u8>,
    },
}

// ---- encoding helpers (big endian + length-prefixed strings)

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) -> io::Result<()> {
    let b = s.as_bytes();
    if b.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string too long",
        ));
    }
    put_u16(out, b.len() as u16);
    out.extend_from_slice(b);
    Ok(())
}

fn rd_u16(b: &[u8], i: &mut usize) -> io::Result<u16> {
    if *i + 2 > b.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u16"));
    }
    let v = u16::from_be_bytes(b[*i..*i + 2].try_into().unwrap());
    *i += 2;
    Ok(v)
}
fn rd_u32(b: &[u8], i: &mut usize) -> io::Result<u32> {
    if *i + 4 > b.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u32"));
    }
    let v = u32::from_be_bytes(b[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}
fn rd_u64(b: &[u8], i: &mut usize) -> io::Result<u64> {
    if *i + 8 > b.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u64"));
    }
    let v = u64::from_be_bytes(b[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}
fn rd_str(b: &[u8], i: &mut usize) -> io::Result<String> {
    let len = rd_u16(b, i)? as usize;
    if *i + len > b.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "str"));
    }
    let s = std::str::from_utf8(&b[*i..*i + len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "utf8"))?;
    *i += len;
    Ok(s.to_string())
}

impl StromaEvent {
    /// Encodes an event into bytes to be stored as Keratin record payload.
    /// (CRC is already handled by Keratin record framing, so no double-CRC here.)
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(STROMA_MAGIC);
        put_u16(&mut out, STROMA_VER);

        match self {
            StromaEvent::MarkInflight {
                tp,
                part,
                group,
                off,
                deadline,
            } => {
                put_u16(&mut out, EventType::MarkInflight as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, group)?;
                put_u64(&mut out, *off);
                put_u64(&mut out, *deadline);
            }
            StromaEvent::Ack {
                tp,
                part,
                group,
                off,
            } => {
                put_u16(&mut out, EventType::Ack as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, group)?;
                put_u64(&mut out, *off);
            }
            StromaEvent::ClearInflight {
                tp,
                part,
                group,
                off,
            } => {
                put_u16(&mut out, EventType::ClearInflight as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, group)?;
                put_u64(&mut out, *off);
            }
            StromaEvent::ResetGroup { tp, part, group } => {
                put_u16(&mut out, EventType::ResetGroup as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, group)?;
            }
            StromaEvent::Snapshot {
                tp,
                part,
                group,
                blob,
            } => {
                put_u16(&mut out, EventType::Snapshot as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, group)?;
                if blob.len() > u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "snapshot too big",
                    ));
                }
                put_u32(&mut out, blob.len() as u32);
                out.extend_from_slice(blob);
            }
        }

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 8 + 2 + 2 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "event header"));
        }
        if &bytes[0..8] != STROMA_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not stroma event",
            ));
        }

        let mut i = 8usize;
        let ver = rd_u16(bytes, &mut i)?;
        if ver != STROMA_VER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "stroma version"));
        }

        let ty = rd_u16(bytes, &mut i)?;
        match ty {
            x if x == EventType::MarkInflight as u16 => {
                let tp = rd_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group = rd_str(bytes, &mut i)?;
                let off = rd_u64(bytes, &mut i)?;
                let deadline = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::MarkInflight {
                    tp,
                    part,
                    group,
                    off,
                    deadline,
                })
            }
            x if x == EventType::Ack as u16 => {
                let tp = rd_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group = rd_str(bytes, &mut i)?;
                let off = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::Ack {
                    tp,
                    part,
                    group,
                    off,
                })
            }
            x if x == EventType::ClearInflight as u16 => {
                let tp = rd_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group = rd_str(bytes, &mut i)?;
                let off = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::ClearInflight {
                    tp,
                    part,
                    group,
                    off,
                })
            }
            x if x == EventType::ResetGroup as u16 => {
                let tp = rd_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group = rd_str(bytes, &mut i)?;
                Ok(StromaEvent::ResetGroup { tp, part, group })
            }
            x if x == EventType::Snapshot as u16 => {
                let tp = rd_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group = rd_str(bytes, &mut i)?;
                let len = rd_u32(bytes, &mut i)? as usize;
                if i + len > bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "snapshot blob",
                    ));
                }
                let blob = bytes[i..i + len].to_vec();
                Ok(StromaEvent::Snapshot {
                    tp,
                    part,
                    group,
                    blob,
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown event type",
            )),
        }
    }
}
