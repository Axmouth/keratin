use std::io;

use serde::{Deserialize, Serialize};

use crate::state::ConsumerId;

pub type Offset = u64;
pub type UnixMillis = u64;

pub const STROMA_MAGIC: &[u8; 8] = b"STROMA\0\0";
pub const STROMA_VER: u16 = 2;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Enqueue = 0,
    EnqueueMany = 1,
    EnqueueDelayed = 2,
    EnqueueDelayedMany = 3,
    MarkInflight = 10,
    MarkInflightMany = 11,
    Ack = 20,
    AckMany = 21,
    Nack = 30,
    NackMany = 31,
    DeadLetter = 40,
    DeadLetterCommit = 41,
    Declare = 50,
    ResetQueue = 60,
    Snapshot = 70,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueEventMeta {
    pub off: Offset,
    pub retries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueDelayedEventMeta {
    pub off: Offset,
    pub not_before: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckEventMeta {
    pub off: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum NackType {
    Discard,
    RetryNow,
    RetryLater { not_before: UnixMillis },
    RequeueNow,
    RequeueLater { not_before: UnixMillis },
}

impl NackType {
    pub fn write_bytes(&self, out: &mut Vec<u8>) -> Result<(), io::Error> {
        match self {
            NackType::Discard => {
                put_u8(out, 0);
            }
            NackType::RetryNow => {
                put_u8(out, 1);
            }
            NackType::RetryLater { not_before: ts } => {
                put_u8(out, 2);
                put_u64(out, *ts);
            }
            NackType::RequeueNow => {
                put_u8(out, 3);
            }
            NackType::RequeueLater { not_before: ts } => {
                put_u8(out, 4);
                put_u64(out, *ts);
            }
        }
        Ok(())
    }

    pub fn read_from_bytes(&self, input: &[u8]) -> Result<Self, io::Error> {
        let mut i = 0;

        let tag = rd_u8(input, &mut i)?;

        match tag {
            0 => Ok(NackType::Discard),
            1 => Ok(NackType::RetryNow),
            2 => {
                let ts = rd_u64(input, &mut i)?;
                Ok(NackType::RetryLater { not_before: ts })
            }
            3 => Ok(NackType::RequeueNow),
            4 => {
                let ts = rd_u64(input, &mut i)?;
                Ok(NackType::RequeueLater { not_before: ts })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid NackType tag",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackEventMeta {
    pub off: Offset,
    pub requeue: bool,
    pub not_before: Option<UnixMillis>,
}

// TODO: Add delivery tag?
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkInflightEventMeta {
    pub off: Offset,
    pub deadline: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeadLetterReason {
    RetriesExhausted,
    TerminalNack,
    PendingRecovery,
}

impl DeadLetterReason {
    pub fn as_header(&self) -> &'static str {
        match self {
            DeadLetterReason::RetriesExhausted => "retries_exhausted",
            DeadLetterReason::TerminalNack => "terminal_nack",
            DeadLetterReason::PendingRecovery => "pending_recovery",
        }
    }

    fn tag(&self) -> u8 {
        match self {
            DeadLetterReason::RetriesExhausted => 0,
            DeadLetterReason::TerminalNack => 1,
            DeadLetterReason::PendingRecovery => 2,
        }
    }

    fn from_tag(tag: u8) -> io::Result<Self> {
        match tag {
            0 => Ok(DeadLetterReason::RetriesExhausted),
            1 => Ok(DeadLetterReason::TerminalNack),
            2 => Ok(DeadLetterReason::PendingRecovery),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid dead letter reason tag",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterMeta {
    pub off: Offset,
    pub retry_count: u32,
    pub reason: DeadLetterReason,
    pub target_tp: Box<str>,
    pub target_part: u32,
    pub target_group: Option<Box<str>>,
}

/// Settings update; None = leave unchanged.
/// Add fields here as new settings are introduced.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeclareMeta {
    pub dlq_policy: Option<DLQDiscardPolicyWire>,
    pub dlq_max_retries: Option<u32>,
}

/// Wire form of DLQDiscardPolicy. Mirrors state::DLQDiscardPolicy
/// but lives in event.rs so this module stays free of state imports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DLQDiscardPolicyWire {
    Discard,
    GlobalDQL,
    CustomDQL {
        tp: Box<str>,
        part: u32,
        group: Option<Box<str>>,
    },
}

// TODO: Add events for setting DLQ target and policy, timeouts, retry limits, etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StromaEvent {
    Enqueue {
        off: Offset,
        retries: u32,
    },
    EnqueueMany {
        reqs: Vec<EnqueueEventMeta>,
    },
    EnqueueDelayed {
        off: Offset,
        not_before: UnixMillis,
    },
    EnqueueDelayedMany {
        reqs: Vec<EnqueueDelayedEventMeta>,
    },
    MarkInflight {
        off: Offset,
        deadline: UnixMillis,
    },
    MarkInflightMany {
        reqs: Vec<MarkInflightEventMeta>,
    },
    Ack {
        off: Offset,
    },
    AckMany {
        reqs: Vec<AckEventMeta>,
    },
    Nack {
        off: Offset,
        requeue: bool,
    },
    NackMany {
        reqs: Vec<NackEventMeta>,
    },
    DeadLetter {
        reqs: Vec<DeadLetterMeta>,
    },
    DeadLetterCommit {
        offs: Vec<Offset>,
    },
    Declare(DeclareMeta),
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
            StromaEvent::Enqueue { off, retries } => {
                put_u16(&mut out, EventType::Enqueue as u16);
                put_u64(&mut out, *off);
                put_u32(&mut out, *retries);
            }
            StromaEvent::EnqueueMany { reqs } => {
                put_u16(&mut out, EventType::EnqueueMany as u16);
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_u32(&mut out, req.retries);
                }
            }
            StromaEvent::EnqueueDelayed { off, not_before } => {
                put_u16(&mut out, EventType::EnqueueDelayed as u16);
                put_u64(&mut out, *off);
                put_u64(&mut out, *not_before);
            }
            StromaEvent::EnqueueDelayedMany { reqs } => {
                put_u16(&mut out, EventType::EnqueueDelayedMany as u16);
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_u64(&mut out, req.not_before);
                }
            }
            StromaEvent::MarkInflight { off, deadline } => {
                put_u16(&mut out, EventType::MarkInflight as u16);
                put_u64(&mut out, *off);
                put_u64(&mut out, *deadline);
            }
            StromaEvent::Ack { off } => {
                put_u16(&mut out, EventType::Ack as u16);
                put_u64(&mut out, *off);
            }
            StromaEvent::Nack { off, requeue } => {
                put_u16(&mut out, EventType::Nack as u16);
                put_u64(&mut out, *off);
                put_bool(&mut out, *requeue);
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
            StromaEvent::MarkInflightMany { reqs } => {
                put_u16(&mut out, EventType::MarkInflightMany as u16);
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_u64(&mut out, req.deadline);
                }
            }
            StromaEvent::AckMany { reqs } => {
                put_u16(&mut out, EventType::AckMany as u16);
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                }
            }
            StromaEvent::NackMany { reqs } => {
                put_u16(&mut out, EventType::NackMany as u16);
                put_u32(&mut out, reqs.len() as u32);
                for req in reqs {
                    put_u64(&mut out, req.off);
                    put_bool(&mut out, req.requeue);
                    match req.not_before {
                        Some(not_before) => {
                            put_bool(&mut out, true);
                            put_u64(&mut out, not_before);
                        }
                        None => put_bool(&mut out, false),
                    }
                }
            }
            StromaEvent::DeadLetter { reqs } => {
                put_u16(&mut out, EventType::DeadLetter as u16);
                put_u32(&mut out, reqs.len() as u32);
                for r in reqs {
                    put_u64(&mut out, r.off);
                    put_u32(&mut out, r.retry_count);
                    put_u8(&mut out, r.reason.tag());
                    put_str(&mut out, &r.target_tp)?;
                    put_u32(&mut out, r.target_part);
                    put_str(&mut out, r.target_group.as_deref().unwrap_or(""))?;
                }
            }
            StromaEvent::DeadLetterCommit { offs } => {
                put_u16(&mut out, EventType::DeadLetterCommit as u16);
                put_u32(&mut out, offs.len() as u32);
                for o in offs {
                    put_u64(&mut out, *o);
                }
            }
            StromaEvent::Declare(m) => {
                put_u16(&mut out, EventType::Declare as u16);
                // bitmap of which Option fields are present, then values in order
                let mut flags: u16 = 0;
                if m.dlq_policy.is_some() {
                    flags |= 1 << 0;
                }
                if m.dlq_max_retries.is_some() {
                    flags |= 1 << 1;
                }
                put_u16(&mut out, flags);
                if let Some(p) = &m.dlq_policy {
                    match p {
                        DLQDiscardPolicyWire::Discard => put_u8(&mut out, 0),
                        DLQDiscardPolicyWire::GlobalDQL => put_u8(&mut out, 1),
                        DLQDiscardPolicyWire::CustomDQL { tp, part, group } => {
                            put_u8(&mut out, 2);
                            put_str(&mut out, tp)?;
                            put_u32(&mut out, *part);
                            put_str(&mut out, &group.as_deref().unwrap_or_default());
                        }
                    }
                }
                if let Some(n) = m.dlq_max_retries {
                    put_u32(&mut out, n);
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
                let off = rd_u64(bytes, &mut i)?;
                let retries = rd_u32(bytes, &mut i)?;
                Ok(StromaEvent::Enqueue { off, retries })
            }
            x if x == EventType::EnqueueMany as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let retries = rd_u32(bytes, &mut i)?;
                    reqs.push(EnqueueEventMeta { off, retries });
                }
                Ok(StromaEvent::EnqueueMany { reqs })
            }
            x if x == EventType::EnqueueDelayed as u16 => {
                let off = rd_u64(bytes, &mut i)?;
                let not_before = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::EnqueueDelayed { off, not_before })
            }
            x if x == EventType::EnqueueDelayedMany as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let not_before = rd_u64(bytes, &mut i)?;
                    reqs.push(EnqueueDelayedEventMeta { off, not_before });
                }
                Ok(StromaEvent::EnqueueDelayedMany { reqs })
            }
            x if x == EventType::MarkInflight as u16 => {
                let off = rd_u64(bytes, &mut i)?;
                let deadline = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::MarkInflight { off, deadline })
            }
            x if x == EventType::Ack as u16 => {
                let off = rd_u64(bytes, &mut i)?;
                Ok(StromaEvent::Ack { off })
            }
            x if x == EventType::Nack as u16 => {
                let off = rd_u64(bytes, &mut i)?;
                let requeue = rd_bool(bytes, &mut i)?;
                Ok(StromaEvent::Nack { off, requeue })
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
            x if x == EventType::MarkInflightMany as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let deadline = rd_u64(bytes, &mut i)?;
                    reqs.push(MarkInflightEventMeta { off, deadline });
                }
                Ok(StromaEvent::MarkInflightMany { reqs })
            }
            x if x == EventType::AckMany as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    reqs.push(AckEventMeta { off });
                }
                Ok(StromaEvent::AckMany { reqs })
            }
            x if x == EventType::NackMany as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let requeue = rd_bool(bytes, &mut i)?;
                    let not_before = rd_bool(bytes, &mut i)?
                        .then(|| rd_u64(bytes, &mut i))
                        .transpose()?;
                    reqs.push(NackEventMeta {
                        off,
                        requeue,
                        not_before,
                    });
                }
                Ok(StromaEvent::NackMany { reqs })
            }
            x if x == EventType::DeadLetter as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut reqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    let retry_count = rd_u32(bytes, &mut i)?;
                    let reason = DeadLetterReason::from_tag(rd_u8(bytes, &mut i)?)?;
                    let target_tp = rd_box_str(bytes, &mut i)?;
                    let target_part = rd_u32(bytes, &mut i)?;
                    let target_group_str = rd_box_str(bytes, &mut i)?;
                    let target_group = if target_group_str.is_empty() {
                        None
                    } else {
                        Some(target_group_str)
                    };
                    reqs.push(DeadLetterMeta {
                        off,
                        retry_count,
                        reason,
                        target_tp,
                        target_part,
                        target_group,
                    });
                }
                Ok(StromaEvent::DeadLetter { reqs })
            }
            x if x == EventType::DeadLetterCommit as u16 => {
                let count = rd_u32(bytes, &mut i)? as usize;
                let mut offs = Vec::with_capacity(count);
                for _ in 0..count {
                    let off = rd_u64(bytes, &mut i)?;
                    offs.push(off);
                }
                Ok(StromaEvent::DeadLetterCommit { offs })
            }
            x if x == EventType::Declare as u16 => {
                let flags = rd_u16(bytes, &mut i)?;
                let dlq_policy = if flags & (1 << 0) != 0 {
                    let tag = rd_u8(bytes, &mut i)?;
                    match tag {
                        0 => Some(DLQDiscardPolicyWire::Discard),
                        1 => Some(DLQDiscardPolicyWire::GlobalDQL),
                        2 => {
                            let tp = rd_box_str(bytes, &mut i)?;
                            let part = rd_u32(bytes, &mut i)?;
                            let mut group = None;
                            let group_tmp = rd_box_str(bytes, &mut i)?;
                            if !group_tmp.is_empty() {
                                group = Some(group_tmp);
                            }
                            Some(DLQDiscardPolicyWire::CustomDQL { tp, part, group })
                        }
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "invalid DLQ policy tag",
                            ));
                        }
                    }
                } else {
                    None
                };
                let dlq_max_retries = if flags & (1 << 1) != 0 {
                    Some(rd_u32(bytes, &mut i)?)
                } else {
                    None
                };
                Ok(StromaEvent::Declare(DeclareMeta {
                    dlq_policy,
                    dlq_max_retries,
                }))
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
            off: 100,
            retries: 5,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_enqueue_without_group() {
        let event = StromaEvent::Enqueue { off: 0, retries: 0 };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_mark_inflight_encode_decode() {
        let event = StromaEvent::MarkInflight {
            off: 200,
            deadline: 1234567890,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_ack_encode_decode() {
        let event = StromaEvent::Ack { off: 300 };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_nack_encode_decode() {
        let event = StromaEvent::Nack {
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
            off: 150,
            requeue: false,
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
    fn test_reset_queue_without_group() {
        let event = StromaEvent::ResetQueue {
            tp: "topic".into(),
            part: 4,
            group: None,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_dead_letter_encode_decode() {
        let event = StromaEvent::DeadLetter {
            reqs: vec![
                DeadLetterMeta {
                    off: 500,
                    retry_count: 3,
                    reason: DeadLetterReason::RetriesExhausted,
                    target_tp: "dlq_topic".into(),
                    target_part: 1,
                    target_group: Some("dlq_group".into()),
                },
                DeadLetterMeta {
                    off: 501,
                    retry_count: 0,
                    reason: DeadLetterReason::TerminalNack,
                    target_tp: "another_dlq".into(),
                    target_part: 2,
                    target_group: None,
                },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_dead_letter_commit_encode_decode() {
        let event = StromaEvent::DeadLetterCommit {
            offs: vec![600, 601, 602],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_declare_encode_decode_no_options() {
        let event = StromaEvent::Declare(DeclareMeta {
            dlq_policy: None,
            dlq_max_retries: None,
        });
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_declare_encode_decode_with_discard_policy() {
        let event = StromaEvent::Declare(DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::Discard),
            dlq_max_retries: None,
        });
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_declare_encode_decode_with_global_dlq_policy() {
        let event = StromaEvent::Declare(DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
            dlq_max_retries: Some(10),
        });
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_declare_encode_decode_with_custom_dlq_policy() {
        let event = StromaEvent::Declare(DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::CustomDQL {
                tp: "custom_dlq".into(),
                part: 5,
                group: Some("custom_dlq_group".into()),
            }),
            dlq_max_retries: Some(5),
        });
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_declare_encode_decode_with_retries_only() {
        let event = StromaEvent::Declare(DeclareMeta {
            dlq_policy: None,
            dlq_max_retries: Some(3),
        });
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
            reqs: vec![
                EnqueueEventMeta {
                    off: 100,
                    retries: 1,
                },
                EnqueueEventMeta {
                    off: 101,
                    retries: 2,
                },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_mark_inflight_many_encode_decode() {
        let event = StromaEvent::MarkInflightMany {
            reqs: vec![
                MarkInflightEventMeta {
                    off: 200,
                    deadline: 1000,
                },
                MarkInflightEventMeta {
                    off: 201,
                    deadline: 2000,
                },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_ack_many_encode_decode() {
        let event = StromaEvent::AckMany {
            reqs: vec![AckEventMeta { off: 300 }, AckEventMeta { off: 301 }],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_nack_many_encode_decode() {
        let event = StromaEvent::NackMany {
            reqs: vec![
                NackEventMeta {
                    off: 400,
                    requeue: true,
                    not_before: None,
                },
                NackEventMeta {
                    off: 401,
                    requeue: true,
                    not_before: Some(12_345),
                },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_invalid_magic() {
        let bytes = vec![0u8; 20];
        let decoded = StromaEvent::decode(&bytes);
        assert!(decoded.is_err());
        assert_eq!(
            decoded.map_err(|e| e.to_string()),
            Err(io::Error::new(io::ErrorKind::InvalidData, "not stroma event",).to_string())
        )
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

    #[test]
    fn test_enqueue_delayed_encode_decode() {
        let event = StromaEvent::EnqueueDelayed {
            off: 100,
            not_before: 1234567890,
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_enqueue_delayed_many_encode_decode() {
        let event = StromaEvent::EnqueueDelayedMany {
            reqs: vec![
                EnqueueDelayedEventMeta {
                    off: 100,
                    not_before: 1000,
                },
                EnqueueDelayedEventMeta {
                    off: 101,
                    not_before: 2000,
                },
            ],
        };
        let encoded = event.encode().unwrap();
        let decoded = StromaEvent::decode(&encoded).unwrap();
        assert_eq!(event, decoded);
    }
}
