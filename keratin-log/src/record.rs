use crc32c::crc32c;

use crate::util::unix_millis;

pub const RECORD_MAGIC: u16 = 0x4B52; // "KR"
pub const RECORD_VERSION: u16 = 1;
pub const RECORD_HEADER_LEN: usize = 2 + 2 + 2 + 2 + 4 + 4 + 8 + 8;

/// Disk format is big-endian.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    pub flags: u16,
    pub timestamp_ms: u64,
    pub offset: u64,
    pub headers: &'a [u8],
    pub payload: &'a [u8],
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("bad record magic: {0:#x}")]
    BadMagic(u16),
    #[error("unsupported record version: {0}")]
    BadVersion(u16),
    #[error("truncated record")]
    Truncated,
    #[error("crc mismatch")]
    CrcMismatch,
    #[error("length overflow")]
    LengthOverflow,
}

/// Encode a record into `dst`, appending bytes.
/// Returns the file position delta (bytes written).
pub fn encode_record(dst: &mut Vec<u8>, r: &Record<'_>) -> Result<usize, RecordError> {
    let header_len: u32 = r
        .headers
        .len()
        .try_into()
        .map_err(|_| RecordError::LengthOverflow)?;
    let payload_len: u32 = r
        .payload
        .len()
        .try_into()
        .map_err(|_| RecordError::LengthOverflow)?;

    let start_len = dst.len();

    // Fixed header fields.
    dst.extend_from_slice(&RECORD_MAGIC.to_be_bytes());
    dst.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    dst.extend_from_slice(&r.flags.to_be_bytes());
    dst.extend_from_slice(&0u16.to_be_bytes()); // reserved0
    dst.extend_from_slice(&header_len.to_be_bytes());
    dst.extend_from_slice(&payload_len.to_be_bytes());
    dst.extend_from_slice(&r.timestamp_ms.to_be_bytes());
    dst.extend_from_slice(&r.offset.to_be_bytes());

    // Variable parts.
    dst.extend_from_slice(r.headers);
    dst.extend_from_slice(r.payload);

    // CRC covers everything *except* magic and crc field itself.
    // Layout: [magic(2)][version..payload][crc(4)]
    // Compute crc over bytes from version to end of payload.
    let crc_start = start_len + 2; // skip magic
    let crc_end = dst.len();
    let crc = crc32c(&dst[crc_start..crc_end]);
    dst.extend_from_slice(&crc.to_be_bytes());

    Ok(dst.len() - start_len)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRemainder {
    Missing(usize),
    Extra(usize),
    Zero,
}

/// Attempt to decode a record header from `buf` beginning at offset 0.
/// On success, returns (record_meta, remaining_bytes).
///
/// This is designed for sequential scanning faster.
pub fn decode_header_prefix<'a>(buf: &[u8]) -> Result<(DecodedHeader, ByteRemainder), RecordError> {
    // Fixed header without CRC (32 bytes)
    // Need at least fixed header: magic(2)+ver(2)+flags(2)+res(2)+hlen(4)+plen(4)+ts(8)+off(8), crc (4) comes later
    const FIXED: usize = 2 + 2 + 2 + 2 + 4 + 4 + 8 + 8;
    if buf.len() < FIXED {
        return Err(RecordError::Truncated);
    }

    let magic = u16::from_be_bytes([buf[0], buf[1]]);
    if magic != RECORD_MAGIC {
        return Err(RecordError::BadMagic(magic));
    }

    let version = u16::from_be_bytes([buf[2], buf[3]]);
    if version != RECORD_VERSION {
        return Err(RecordError::BadVersion(version));
    }

    let flags = u16::from_be_bytes([buf[4], buf[5]]);
    // reserved0 at [6..8]
    let header_len = u32::from_be_bytes(buf[8..12].try_into().unwrap());
    let payload_len = u32::from_be_bytes(buf[12..16].try_into().unwrap());
    let timestamp_ms = u64::from_be_bytes(buf[16..24].try_into().unwrap());
    let offset = u64::from_be_bytes(buf[24..32].try_into().unwrap());

    let var_total = (header_len as usize)
        .checked_add(payload_len as usize)
        .and_then(|v| v.checked_add(4)) // crc
        .ok_or(RecordError::LengthOverflow)?;

    let total_len = FIXED
        .checked_add(var_total)
        .ok_or(RecordError::LengthOverflow)?;

    let decoded_header = DecodedHeader {
        flags,
        timestamp_ms,
        offset,
        header_len,
        payload_len
    };

    let buf_len = buf.len();
    let res = if buf_len > total_len {
        ByteRemainder::Extra(buf_len - total_len)
    } else if buf_len < total_len {
        ByteRemainder::Missing(total_len - buf_len)
    } else {
        ByteRemainder::Zero
    };

    Ok((decoded_header, res))
}

#[derive(Debug)]
pub struct DecodedHeader {
    pub flags: u16,
    pub timestamp_ms: u64,
    pub offset: u64,
    pub header_len: u32,
    pub payload_len: u32,
}

/// Attempt to decode a record from `buf` beginning at offset 0.
/// On success, returns (record_meta, total_bytes_consumed, crc_ok).
///
/// This is designed for sequential scanning in recovery.
pub fn decode_record_prefix<'a>(buf: &'a [u8]) -> Result<(DecodedRecord<'a>, usize), RecordError> {
    // Fixed header without CRC (32 bytes)
    // Need at least fixed header: magic(2)+ver(2)+flags(2)+res(2)+hlen(4)+plen(4)+ts(8)+off(8), crc (4) comes later
    const FIXED: usize = 2 + 2 + 2 + 2 + 4 + 4 + 8 + 8;
    if buf.len() < FIXED {
        return Err(RecordError::Truncated);
    }

    let magic = u16::from_be_bytes([buf[0], buf[1]]);
    if magic != RECORD_MAGIC {
        return Err(RecordError::BadMagic(magic));
    }

    let version = u16::from_be_bytes([buf[2], buf[3]]);
    if version != RECORD_VERSION {
        return Err(RecordError::BadVersion(version));
    }

    let flags = u16::from_be_bytes([buf[4], buf[5]]);
    // reserved0 at [6..8]
    let header_len = u32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(buf[12..16].try_into().unwrap()) as usize;
    let timestamp_ms = u64::from_be_bytes(buf[16..24].try_into().unwrap());
    let offset = u64::from_be_bytes(buf[24..32].try_into().unwrap());

    let var_total = header_len
        .checked_add(payload_len)
        .and_then(|v| v.checked_add(4)) // crc
        .ok_or(RecordError::LengthOverflow)?;

    let total_len = FIXED
        .checked_add(var_total)
        .ok_or(RecordError::LengthOverflow)?;

    if buf.len() < total_len {
        return Err(RecordError::Truncated);
    }

    let headers_start = FIXED;
    let headers_end = headers_start + header_len;
    let payload_end = headers_end + payload_len;

    let headers = &buf[headers_start..headers_end];
    let payload = &buf[headers_end..payload_end];

    let stored_crc = u32::from_be_bytes(buf[payload_end..payload_end + 4].try_into().unwrap());

    // CRC covers bytes from version to end of payload.
    let crc = crc32c(&buf[2..payload_end]);
    if crc != stored_crc {
        return Err(RecordError::CrcMismatch);
    }

    Ok((
        DecodedRecord {
            flags,
            timestamp_ms,
            offset,
            headers,
            payload,
        },
        total_len,
    ))
}

#[derive(Debug)]
pub struct DecodedRecord<'a> {
    pub flags: u16,
    pub timestamp_ms: u64,
    pub offset: u64,
    pub headers: &'a [u8],
    pub payload: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct Message {
    pub flags: u16,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn encode_msg(m: &Self, offset: u64) -> Result<Vec<u8>, RecordError> {
        let record = Record {
            flags: m.flags,
            timestamp_ms: unix_millis(),
            offset,
            headers: &m.headers,
            payload: &m.payload,
        };

        let mut buf = Vec::with_capacity(m.bytes_len());
        encode_record(&mut buf, &record)?;

        Ok(buf)
    }

    pub fn decode_msg(bytes: &[u8]) -> Result<Self, RecordError> {
        let (record, _length) = decode_record_prefix(bytes)?;

        Ok(Message {
            flags: record.flags,
            headers: record.headers.to_vec(),
            payload: record.payload.to_vec(),
        })
    }

    pub fn bytes_len(&self) -> usize {
        RECORD_HEADER_LEN + self.headers.len() + self.payload.len() + 4 // crc
    }
}

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub offset: u64,
    pub flags: u16,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

impl ReceivedMessage {
    pub fn encode_msg(m: &Self) -> Result<Vec<u8>, RecordError> {
        let record = Record {
            flags: m.flags,
            timestamp_ms: unix_millis(),
            offset: m.offset,
            headers: &m.headers,
            payload: &m.payload,
        };

        let mut buf = Vec::with_capacity(128);
        encode_record(&mut buf, &record)?;

        Ok(buf)
    }

    pub fn decode_msg(bytes: &[u8]) -> Result<Self, RecordError> {
        let (record, _length) = decode_record_prefix(bytes)?;

        Ok(ReceivedMessage {
            flags: record.flags,
            offset: record.offset,
            headers: record.headers.to_vec(),
            payload: record.payload.to_vec(),
        })
    }
}
