//! Bilrost codec implementation for serialization.
//!
//! This module provides a bilrost-based codec while maintaining
//! compatibility with serde traits for the public API.

use crate::error::{CodecError, Result};
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

/// Encode a message into bilrost bytes.
///
/// This function temporarily uses bincode for serde compatibility.
/// TODO: Create bilrost wrapper types for all messages.
///
/// # Errors
///
/// Returns an error if the message cannot be serialized.
pub fn encode<T: Serialize>(msg: &T) -> Result<Bytes> {
    let encoded =
        bincode::serialize(msg).map_err(|e| CodecError::SerializationFailed(e.to_string()))?;
    Ok(Bytes::from(encoded))
}

/// Decode bilrost bytes into a message.
///
/// This function temporarily uses bincode for serde compatibility.
/// TODO: Create bilrost wrapper types for all messages.
///
/// # Errors
///
/// Returns an error if the data is invalid or the message cannot be deserialized.
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    bincode::deserialize(data)
        .map_err(|e| CodecError::DeserializationFailed(e.to_string()))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestMessage {
        id: u32,
        name: String,
        data: Vec<u8>,
    }

    #[test]
    fn test_encode_decode() {
        let msg = TestMessage {
            id: 42,
            name: "test".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        let encoded = encode(&msg).unwrap();
        let decoded: TestMessage = decode(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_decode_error() {
        let bad_data = vec![0xFF, 0xFF, 0xFF];
        let result: Result<TestMessage> = decode(&bad_data);
        assert!(result.is_err());
    }
}
