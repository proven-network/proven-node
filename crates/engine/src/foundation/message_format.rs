//! Binary format for efficient message storage
//!
//! Format:
//! ```
//! [timestamp: u64]
//! [sequence: u64]
//! [header_count: u8]
//! For each header:
//!   [header_id: u8]
//!   If header_id == 0xFF (custom):
//!     [name_len: u16][name_bytes]
//!   [value_len: u16][value_bytes]
//! [payload_len: u32]
//! [payload_bytes]
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Write};

use super::message::{Message, headers};

/// Errors that can occur during serialization/deserialization
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    /// Buffer too small
    #[error("Buffer too small")]
    BufferTooSmall,

    /// Invalid header ID
    #[error("Invalid header ID: {0}")]
    InvalidHeaderId(u8),

    /// String decode error
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Serialize a message with timestamp and sequence into binary format
pub fn serialize_entry(
    message: &Message,
    timestamp_millis: u64,
    sequence: u64,
) -> Result<Bytes, FormatError> {
    // Pre-calculate size
    let mut size = 8 + 8 + 1 + 4; // timestamp + sequence + header_count + payload_len
    size += message.payload.len();

    // Calculate header sizes
    for (key, value) in &message.headers {
        if let Some(_header_id) = headers::get_header_id(key) {
            size += 1 + 2 + value.len(); // id + value_len + value
        } else {
            size += 1 + 2 + key.len() + 2 + value.len(); // 0xFF + name_len + name + value_len + value
        }
    }

    let mut buf = BytesMut::with_capacity(size);

    // Write timestamp
    buf.put_u64(timestamp_millis);

    // Write sequence
    buf.put_u64(sequence);

    // Write header count
    buf.put_u8(message.headers.len() as u8);

    // Write headers
    for (key, value) in &message.headers {
        if let Some(header_id) = headers::get_header_id(key) {
            // Known header
            buf.put_u8(header_id);
        } else {
            // Custom header
            buf.put_u8(headers::CUSTOM);
            buf.put_u16(key.len() as u16);
            buf.put_slice(key.as_bytes());
        }

        // Write value
        buf.put_u16(value.len() as u16);
        buf.put_slice(value.as_bytes());
    }

    // Write payload length and data
    buf.put_u32(message.payload.len() as u32);
    buf.put_slice(&message.payload);

    Ok(buf.freeze())
}

/// Deserialize a message from binary format
pub fn deserialize_entry(data: &[u8]) -> Result<(Message, u64, u64), FormatError> {
    let mut buf = data;

    if buf.len() < 17 {
        // minimum: timestamp(8) + sequence(8) + header_count(1)
        return Err(FormatError::BufferTooSmall);
    }

    // Read timestamp
    let timestamp = buf.get_u64();

    // Read sequence
    let sequence = buf.get_u64();

    // Read header count
    let header_count = buf.get_u8();

    // Read headers
    let mut headers = Vec::with_capacity(header_count as usize);

    for _ in 0..header_count {
        if buf.is_empty() {
            return Err(FormatError::BufferTooSmall);
        }

        let header_id = buf.get_u8();

        // Get header name
        let header_name = if header_id == headers::CUSTOM {
            // Custom header - read name
            if buf.len() < 2 {
                return Err(FormatError::BufferTooSmall);
            }
            let name_len = buf.get_u16() as usize;

            if buf.len() < name_len {
                return Err(FormatError::BufferTooSmall);
            }
            let name_bytes = buf.copy_to_bytes(name_len);
            String::from_utf8(name_bytes.to_vec())?
        } else {
            // Known header - look up name
            headers::get_header_name(header_id)
                .ok_or(FormatError::InvalidHeaderId(header_id))?
                .to_string()
        };

        // Read value
        if buf.len() < 2 {
            return Err(FormatError::BufferTooSmall);
        }
        let value_len = buf.get_u16() as usize;

        if buf.len() < value_len {
            return Err(FormatError::BufferTooSmall);
        }
        let value_bytes = buf.copy_to_bytes(value_len);
        let value = String::from_utf8(value_bytes.to_vec())?;

        headers.push((header_name, value));
    }

    // Read payload
    if buf.len() < 4 {
        return Err(FormatError::BufferTooSmall);
    }
    let payload_len = buf.get_u32() as usize;

    if buf.len() < payload_len {
        return Err(FormatError::BufferTooSmall);
    }
    let payload = buf.copy_to_bytes(payload_len);

    let message = Message { payload, headers };

    Ok((message, timestamp, sequence))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_no_headers() {
        let msg = Message::new("hello world");
        let timestamp = 1234567890;
        let sequence = 42;

        let serialized = serialize_entry(&msg, timestamp, sequence).unwrap();
        let (decoded_msg, decoded_ts, decoded_seq) = deserialize_entry(&serialized).unwrap();

        assert_eq!(decoded_msg.payload, msg.payload);
        assert_eq!(decoded_msg.headers, msg.headers);
        assert_eq!(decoded_ts, timestamp);
        assert_eq!(decoded_seq, sequence);
    }

    #[test]
    fn test_roundtrip_with_known_headers() {
        let msg = Message::new("data")
            .with_header("message_id", "123")
            .with_header("correlation_id", "456");
        let timestamp = 1234567890;
        let sequence = 100;

        let serialized = serialize_entry(&msg, timestamp, sequence).unwrap();
        let (decoded_msg, decoded_ts, decoded_seq) = deserialize_entry(&serialized).unwrap();

        assert_eq!(decoded_msg.payload, msg.payload);
        assert_eq!(decoded_msg.headers, msg.headers);
        assert_eq!(decoded_ts, timestamp);
        assert_eq!(decoded_seq, sequence);
    }

    #[test]
    fn test_roundtrip_with_custom_headers() {
        let msg = Message::new("test")
            .with_header("x-custom-header", "custom-value")
            .with_header("message_id", "789");
        let timestamp = 9876543210;
        let sequence = 1;

        let serialized = serialize_entry(&msg, timestamp, sequence).unwrap();
        let (decoded_msg, decoded_ts, decoded_seq) = deserialize_entry(&serialized).unwrap();

        assert_eq!(decoded_msg.payload, msg.payload);
        assert_eq!(decoded_msg.headers, msg.headers);
        assert_eq!(decoded_ts, timestamp);
        assert_eq!(decoded_seq, sequence);
    }
}
