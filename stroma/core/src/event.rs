use std::io;

pub type Offset = u64;
pub type UnixMillis = u64;

pub const STROMA_MAGIC: &[u8; 8] = b"STROMA\0\0";
pub const STROMA_VER: u16 = 1;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Enqueue = 0,
    EnqueueMany = 1,
    MarkInflight = 10,
    MarkInflightMany = 11,
    Ack = 20,
    AckMany = 21,
    Nack = 30,
    NackMany = 31,
    DeadLetter = 40,
    ClearInflight = 50,
    ResetQueue = 60,
    Snapshot = 70,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueEventMeta {
    pub off: Offset,
    pub retries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckEventMeta {
    pub off: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackEventMeta {
    pub off: Offset,
    pub requeue: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkInflightEventMeta {
    pub off: Offset,
    pub deadline: UnixMillis,
}

// TODO: Add events for setting DLQ target and policy, timeouts, retry limits, etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StromaEvent {
    Enqueue {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        off: Offset,
        retries: u32,
    },
    EnqueueMany {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        reqs: Vec<EnqueueEventMeta>,
    },
    MarkInflight {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        off: Offset,
        deadline: UnixMillis,
    },
    MarkInflightMany {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        reqs: Vec<MarkInflightEventMeta>,
    },
    Ack {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        off: Offset,
    },
    AckMany {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        reqs: Vec<AckEventMeta>,
    },
    Nack {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        off: Offset,
        requeue: bool,
    },
    NackMany {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        reqs: Vec<NackEventMeta>,
    },
    DeadLetter {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        off: Offset,
    },
    ClearInflight {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        off: Offset,
    },
    ResetQueue {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
    },
    /// Snapshot is a complete state image for a single (tp,part).
    /// It’s OK if it’s “big”; it happens rarely.
    Snapshot {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
        /// Encoded QueueState snapshot payload (see state snapshot helpers below)
        blob: Vec<u8>,
    },
}

// ---- encoding helpers (big endian + length-prefixed strings)

fn put_bool(out: &mut Vec<u8>, v: bool) {
    put_u8(out, v as u8);
}

fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

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

fn rd_bool(b: &[u8], i: &mut usize) -> io::Result<bool> {
    let v = rd_u8(b, i)?;
    match v {
        0 => Ok(false),
        _ => Ok(true),
    }
}
fn rd_u8(b: &[u8], i: &mut usize) -> io::Result<u8> {
    if *i + 1 > b.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u8"));
    }
    let v = b[*i];
    *i += 1;
    Ok(v)
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
fn rd_box_str(b: &[u8], i: &mut usize) -> io::Result<Box<str>> {
    let len = rd_u16(b, i)? as usize;
    if *i + len > b.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "str"));
    }
    let s = std::str::from_utf8(&b[*i..*i + len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "utf8"))?;
    *i += len;
    Ok(s.into())
}

impl StromaEvent {
    /// Encodes an event into bytes to be stored as Keratin record payload.
    /// (CRC is already handled by Keratin record framing, so no double-CRC here.)
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(STROMA_MAGIC);
        put_u16(&mut out, STROMA_VER);

        match self {
            StromaEvent::Enqueue {
                tp,
                part,
                group,
                off,
                retries,
            } => {
                put_u16(&mut out, EventType::Enqueue as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u64(&mut out, *off);
                put_u32(&mut out, *retries);
            }
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
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
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
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u64(&mut out, *off);
            }
            StromaEvent::Nack {
                tp,
                part,
                group,
                off,
                requeue,
            } => {
                put_u16(&mut out, EventType::Nack as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u64(&mut out, *off);
                put_bool(&mut out, *requeue);
            }
            StromaEvent::DeadLetter {
                tp,
                part,
                group,
                off,
            } => {
                put_u16(&mut out, EventType::DeadLetter as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
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
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u64(&mut out, *off);
            }
            StromaEvent::ResetQueue { tp, part, group } => {
                put_u16(&mut out, EventType::ResetQueue as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
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
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                // TODO: Evaluate if u32 size limit(4gb?) is acceptable here
                if blob.len() > u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "snapshot too big",
                    ));
                }
                put_u32(&mut out, blob.len() as u32);
                out.extend_from_slice(blob);
            }
            StromaEvent::EnqueueMany { tp, part, group, reqs } => {
                put_u16(&mut out, EventType::EnqueueMany as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_u32(&mut out, req.retries);
                }
            }
            StromaEvent::MarkInflightMany { tp, part, group, reqs } => {
                put_u16(&mut out, EventType::MarkInflightMany as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_u64(&mut out, req.deadline);
                }
            }
            StromaEvent::AckMany { tp, part, group, reqs } => {
                put_u16(&mut out, EventType::AckMany as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                }
            }
            StromaEvent::NackMany { tp, part, group, reqs } => {
                put_u16(&mut out, EventType::NackMany as u16);
                put_str(&mut out, tp)?;
                put_u32(&mut out, *part);
                put_str(&mut out, &group.clone().unwrap_or("".into()))?;
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_bool(&mut out, req.requeue);
                }
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
            x if x == EventType::Enqueue as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let off = rd_u64(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let retries = rd_u32(bytes, &mut i)?;
                Ok(StromaEvent::Enqueue {
                    tp,
                    part,
                    group,
                    off,
                    retries,
                })
            }
            x if x == EventType::MarkInflight as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
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
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let off = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::Ack {
                    tp,
                    part,
                    group,
                    off,
                })
            }
            x if x == EventType::Nack as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let off = rd_u64(bytes, &mut i)?;
                let requeue = rd_bool(bytes, &mut i)?;
                Ok(StromaEvent::Nack {
                    tp,
                    part,
                    group,
                    off,
                    requeue,
                })
            }
            x if x == EventType::DeadLetter as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let off = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::DeadLetter {
                    tp,
                    part,
                    group,
                    off,
                })
            }
            x if x == EventType::ClearInflight as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let off = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::ClearInflight {
                    tp,
                    part,
                    group,
                    off,
                })
            }
            x if x == EventType::ResetQueue as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                Ok(StromaEvent::ResetQueue { tp, part, group })
            }
            x if x == EventType::Snapshot as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
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
            x if x == EventType::EnqueueMany as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let retries = rd_u32(bytes, &mut i)?;
                    reqs.push(EnqueueEventMeta { off, retries });
                }
                Ok(StromaEvent::EnqueueMany {
                    tp,
                    part,
                    group,
                    reqs,
                })
            }
            x if x == EventType::MarkInflightMany as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let deadline = rd_u64(bytes, &mut i)?;
                    reqs.push(MarkInflightEventMeta { off, deadline });
                }
                Ok(StromaEvent::MarkInflightMany {
                    tp,
                    part,
                    group,
                    reqs,
                })
            }
            x if x == EventType::AckMany as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    reqs.push(AckEventMeta { off });
                }
                Ok(StromaEvent::AckMany {
                    tp,
                    part,
                    group,
                    reqs,
                })
            }
            x if x == EventType::NackMany as u16 => {
                let tp = rd_box_str(bytes, &mut i)?;
                let part = rd_u32(bytes, &mut i)?;
                let group_str = rd_box_str(bytes, &mut i)?;
                let group = if group_str.is_empty() {
                    None
                } else {
                    Some(group_str)
                };
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let requeue = rd_bool(bytes, &mut i)?;
                    reqs.push(NackEventMeta { off, requeue });
                }
                Ok(StromaEvent::NackMany {
                    tp,
                    part,
                    group,
                    reqs,
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown event type",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_encode_decode() {
        let event = StromaEvent::Enqueue {
            tp: "topic".into(),
            part: 1,
            group: Some("group".into()),
            off: 100,
            retries: 5,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_enqueue_without_group() {
        let event = StromaEvent::Enqueue {
            tp: "topic".into(),
            part: 0,
            group: None,
            off: 0,
            retries: 0,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_mark_inflight_encode_decode() {
        let event = StromaEvent::MarkInflight {
            tp: "topic".into(),
            part: 2,
            group: Some("group".into()),
            off: 200,
            deadline: 1234567890,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_ack_encode_decode() {
        let event = StromaEvent::Ack {
            tp: "topic".into(),
            part: 3,
            group: None,
            off: 300,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_nack_encode_decode() {
        let event = StromaEvent::Nack {
            tp: "topic".into(),
            part: 1,
            group: Some("group".into()),
            off: 150,
            requeue: true,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_nack_no_requeue() {
        let event = StromaEvent::Nack {
            tp: "topic".into(),
            part: 1,
            group: None,
            off: 150,
            requeue: false,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_dead_letter_encode_decode() {
        let event = StromaEvent::DeadLetter {
            tp: "topic".into(),
            part: 2,
            group: Some("group".into()),
            off: 250,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_clear_inflight_encode_decode() {
        let event = StromaEvent::ClearInflight {
            tp: "topic".into(),
            part: 0,
            group: None,
            off: 50,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_reset_queue_encode_decode() {
        let event = StromaEvent::ResetQueue {
            tp: "topic".into(),
            part: 4,
            group: Some("group".into()),
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_snapshot_encode_decode() {
        let event = StromaEvent::Snapshot {
            tp: "topic".into(),
            part: 1,
            group: None,
            blob: vec![1, 2, 3, 4, 5],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_enqueue_many_encode_decode() {
        let event = StromaEvent::EnqueueMany {
            tp: "topic".into(),
            part: 2,
            group: Some("group".into()),
            reqs: vec![
                EnqueueEventMeta { off: 100, retries: 1 },
                EnqueueEventMeta { off: 101, retries: 2 },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_mark_inflight_many_encode_decode() {
        let event = StromaEvent::MarkInflightMany {
            tp: "topic".into(),
            part: 1,
            group: None,
            reqs: vec![
                MarkInflightEventMeta { off: 200, deadline: 1000 },
                MarkInflightEventMeta { off: 201, deadline: 2000 },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_ack_many_encode_decode() {
        let event = StromaEvent::AckMany {
            tp: "topic".into(),
            part: 3,
            group: Some("group".into()),
            reqs: vec![AckEventMeta { off: 300 }, AckEventMeta { off: 301 }],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_nack_many_encode_decode() {
        let event = StromaEvent::NackMany {
            tp: "topic".into(),
            part: 0,
            group: None,
            reqs: vec![
                NackEventMeta { off: 400, requeue: true },
                NackEventMeta { off: 401, requeue: false },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_invalid_magic() {
        let mut bytes = vec![0u8; 20];
        let decoded = StromaEvent::decode(&bytes);
        assert!(decoded.is_err());
    }

    #[test]
    fn test_invalid_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(STROMA_MAGIC);
        bytes.extend_from_slice(&99u16.to_be_bytes());
        let decoded = StromaEvent::decode(&bytes);
        assert!(decoded.is_err());
    }

    #[test]
    fn test_truncated_header() {
        let bytes = vec![0u8; 5];
        let decoded = StromaEvent::decode(&bytes);
        assert!(decoded.is_err());
    }
}
