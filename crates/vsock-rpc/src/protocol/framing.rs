//! Message framing for the wire protocol.

use crate::error::{ProtocolError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum frame size (10MB by default).
pub const MAX_FRAME_SIZE: usize = 10 * 1024 * 1024;

/// Frame header size (4 bytes length + 1 byte type + 4 bytes checksum).
pub const FRAME_HEADER_SIZE: usize = 9;

/// Type of frame being sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Request frame.
    Request = 0x01,
    /// Response frame.
    Response = 0x02,
    /// Stream data frame.
    Stream = 0x03,
    /// Error frame.
    Error = 0x04,
    /// Heartbeat frame.
    Heartbeat = 0x05,
    /// Close frame.
    Close = 0x06,
    /// Acknowledgment frame.
    Ack = 0x07,
}

impl TryFrom<u8> for FrameType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> std::result::Result<Self, ProtocolError> {
        match value {
            0x01 => Ok(Self::Request),
            0x02 => Ok(Self::Response),
            0x03 => Ok(Self::Stream),
            0x04 => Ok(Self::Error),
            0x05 => Ok(Self::Heartbeat),
            0x06 => Ok(Self::Close),
            0x07 => Ok(Self::Ack),
            _ => Err(ProtocolError::InvalidFrame(format!(
                "Unknown frame type: {value:#x}"
            ))),
        }
    }
}

/// A frame in the wire protocol.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Type of this frame.
    pub frame_type: FrameType,
    /// Frame payload.
    pub payload: Bytes,
    /// Optional checksum for integrity.
    pub checksum: Option<u32>,
}

impl Frame {
    /// Create a new frame.
    pub fn new(frame_type: FrameType, payload: Bytes) -> Self {
        let checksum = Some(crc32fast::hash(&payload));
        Self {
            frame_type,
            payload,
            checksum,
        }
    }

    /// Create a frame without checksum.
    pub const fn new_unchecked(frame_type: FrameType, payload: Bytes) -> Self {
        Self {
            frame_type,
            payload,
            checksum: None,
        }
    }

    /// Verify the checksum if present.
    ///
    /// # Errors
    ///
    /// Returns an error if the checksum is invalid.
    pub fn verify_checksum(&self) -> Result<()> {
        if let Some(expected) = self.checksum {
            let actual = crc32fast::hash(&self.payload);
            if expected != actual {
                return Err(ProtocolError::ChecksumMismatch { expected, actual }.into());
            }
        }
        Ok(())
    }
}

/// Codec for encoding/decoding frames.
pub struct FrameCodec {
    max_frame_size: usize,
    verify_checksum: bool,
}

impl FrameCodec {
    /// Create a new frame codec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_frame_size: MAX_FRAME_SIZE,
            verify_checksum: true,
        }
    }

    /// Create a codec with custom max frame size.
    #[must_use]
    pub const fn with_max_frame_size(mut self, size: usize) -> Self {
        self.max_frame_size = size;
        self
    }

    /// Disable checksum verification (for testing).
    #[must_use]
    pub const fn without_checksum_verification(mut self) -> Self {
        self.verify_checksum = false;
        self
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        // Need at least header size
        if buf.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Parse header without consuming
        let mut header = &buf[..FRAME_HEADER_SIZE];
        let payload_len = header.get_u32() as usize;
        let frame_type_byte = header.get_u8();
        let checksum = header.get_u32();

        // Validate frame size
        if payload_len > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ProtocolError::FrameTooLarge {
                    size: payload_len,
                    max: self.max_frame_size,
                },
            ));
        }

        // Check if we have the full frame
        let frame_len = FRAME_HEADER_SIZE + payload_len;
        if buf.len() < frame_len {
            // Reserve space for the full frame
            buf.reserve(frame_len - buf.len());
            return Ok(None);
        }

        // Parse frame type
        let frame_type = FrameType::try_from(frame_type_byte)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Consume header and payload
        buf.advance(FRAME_HEADER_SIZE);
        let payload = buf.split_to(payload_len).freeze();

        // Create frame
        let frame = Frame {
            frame_type,
            payload,
            checksum: if checksum != 0 { Some(checksum) } else { None },
        };

        // Verify checksum if enabled
        if self.verify_checksum {
            frame
                .verify_checksum()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        Ok(Some(frame))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, buf: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        let payload_len = frame.payload.len();

        // Validate frame size
        if payload_len > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ProtocolError::FrameTooLarge {
                    size: payload_len,
                    max: self.max_frame_size,
                },
            ));
        }

        // Reserve space for header and payload
        buf.reserve(FRAME_HEADER_SIZE + payload_len);

        // Write header
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32(payload_len as u32);
        buf.put_u8(frame.frame_type as u8);
        buf.put_u32(frame.checksum.unwrap_or(0));

        // Write payload
        buf.put(frame.payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_roundtrip() {
        let mut codec = FrameCodec::new();
        let mut buf = BytesMut::new();

        let frame = Frame::new(FrameType::Request, Bytes::from("Hello, World!"));

        // Encode
        codec.encode(frame.clone(), &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(decoded.frame_type, frame.frame_type);
        assert_eq!(decoded.payload, frame.payload);
        assert_eq!(decoded.checksum, frame.checksum);
    }

    #[test]
    fn test_partial_frame() {
        let mut codec = FrameCodec::new();
        let mut buf = BytesMut::new();

        // Write partial header
        buf.put_u32(100); // length
        buf.put_u8(FrameType::Request as u8);
        // Missing checksum and payload

        // Should return None (needs more data)
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_checksum_verification() {
        let mut codec = FrameCodec::new();
        let mut buf = BytesMut::new();

        // Write frame with wrong checksum
        let payload = b"Hello, World!";
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32(payload.len() as u32);
        buf.put_u8(FrameType::Request as u8);
        buf.put_u32(12345); // Wrong checksum
        buf.put_slice(payload);

        // Should fail checksum verification
        assert!(codec.decode(&mut buf).is_err());
    }
}
