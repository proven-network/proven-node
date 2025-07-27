//! Core message format used across streams and pubsub
//!
//! This module defines the common message structure and header registry
//! that provides efficient binary encoding for common headers.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Message structure used for both streams and pubsub
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    /// Message payload
    pub payload: Bytes,
    /// Message headers as key-value pairs
    pub headers: Vec<(String, String)>,
}

impl Message {
    /// Create a new message with just a payload
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            headers: Vec::new(),
        }
    }

    /// Create a new message with payload and headers
    pub fn with_headers(payload: impl Into<Bytes>, headers: Vec<(String, String)>) -> Self {
        Self {
            payload: payload.into(),
            headers,
        }
    }

    /// Add a header to the message
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Get a header value by key
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Add a subject header (convenience method for pubsub)
    pub fn with_subject(self, subject: impl Into<String>) -> Self {
        self.with_header(headers::SUBJECT_STR, subject)
    }

    /// Get the subject from headers (convenience method for pubsub)
    pub fn subject(&self) -> Option<&str> {
        self.get_header(headers::SUBJECT_STR)
    }
}

impl From<Vec<u8>> for Message {
    fn from(bytes: Vec<u8>) -> Self {
        Message::new(bytes)
    }
}

impl From<&[u8]> for Message {
    fn from(bytes: &[u8]) -> Self {
        Message::new(bytes.to_vec())
    }
}

impl From<Bytes> for Message {
    fn from(bytes: Bytes) -> Self {
        Message::new(bytes)
    }
}

impl From<String> for Message {
    fn from(s: String) -> Self {
        Message::new(s)
    }
}

impl From<&str> for Message {
    fn from(s: &str) -> Self {
        Message::new(s.to_string())
    }
}

/// Header registry for efficient encoding of common headers
pub mod headers {
    /// Message ID header byte code
    pub const MESSAGE_ID: u8 = 0x01;
    /// Message ID header string name
    pub const MESSAGE_ID_STR: &str = "message_id";

    /// Correlation ID header byte code
    pub const CORRELATION_ID: u8 = 0x02;
    /// Correlation ID header string name
    pub const CORRELATION_ID_STR: &str = "correlation_id";

    /// Reply-to header byte code
    pub const REPLY_TO: u8 = 0x03;
    /// Reply-to header string name
    pub const REPLY_TO_STR: &str = "reply_to";

    /// Content type header byte code
    pub const CONTENT_TYPE: u8 = 0x04;
    /// Content type header string name
    pub const CONTENT_TYPE_STR: &str = "content_type";

    /// Content encoding header byte code
    pub const CONTENT_ENCODING: u8 = 0x05;
    /// Content encoding header string name
    pub const CONTENT_ENCODING_STR: &str = "content_encoding";

    /// User ID header byte code
    pub const USER_ID: u8 = 0x06;
    /// User ID header string name
    pub const USER_ID_STR: &str = "user_id";

    /// Trace ID header byte code
    pub const TRACE_ID: u8 = 0x07;
    /// Trace ID header string name
    pub const TRACE_ID_STR: &str = "trace_id";

    /// Source header byte code
    pub const SOURCE: u8 = 0x08;
    /// Source header string name
    pub const SOURCE_STR: &str = "source";

    /// Priority header byte code
    pub const PRIORITY: u8 = 0x09;
    /// Priority header string name
    pub const PRIORITY_STR: &str = "priority";

    /// TTL header byte code
    pub const TTL: u8 = 0x0A;
    /// TTL header string name
    pub const TTL_STR: &str = "ttl";

    /// Timestamp header byte code
    pub const TIMESTAMP: u8 = 0x0B;
    /// Timestamp header string name
    pub const TIMESTAMP_STR: &str = "timestamp";

    /// Subject header byte code
    pub const SUBJECT: u8 = 0x0C;
    /// Subject header string name
    pub const SUBJECT_STR: &str = "subject";

    /// Custom header marker - indicates full string encoding follows
    pub const CUSTOM: u8 = 0xFF;

    /// Registry of known headers for encoding
    pub static HEADER_REGISTRY: &[(u8, &str)] = &[
        (MESSAGE_ID, MESSAGE_ID_STR),
        (CORRELATION_ID, CORRELATION_ID_STR),
        (REPLY_TO, REPLY_TO_STR),
        (CONTENT_TYPE, CONTENT_TYPE_STR),
        (CONTENT_ENCODING, CONTENT_ENCODING_STR),
        (USER_ID, USER_ID_STR),
        (TRACE_ID, TRACE_ID_STR),
        (SOURCE, SOURCE_STR),
        (PRIORITY, PRIORITY_STR),
        (TTL, TTL_STR),
        (TIMESTAMP, TIMESTAMP_STR),
        (SUBJECT, SUBJECT_STR),
    ];

    /// Get header ID from string name
    pub fn get_header_id(name: &str) -> Option<u8> {
        HEADER_REGISTRY
            .iter()
            .find(|(_, n)| *n == name)
            .map(|(id, _)| *id)
    }

    /// Get header name from ID
    pub fn get_header_name(id: u8) -> Option<&'static str> {
        HEADER_REGISTRY
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, name)| *name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new("hello");
        assert_eq!(msg.payload, Bytes::from("hello"));
        assert!(msg.headers.is_empty());
    }

    #[test]
    fn test_message_with_headers() {
        let msg = Message::new("data")
            .with_header("message_id", "123")
            .with_header("user_id", "alice");

        assert_eq!(msg.get_header("message_id"), Some("123"));
        assert_eq!(msg.get_header("user_id"), Some("alice"));
        assert_eq!(msg.get_header("missing"), None);
    }

    #[test]
    fn test_header_registry() {
        use headers::*;

        assert_eq!(get_header_id("message_id"), Some(MESSAGE_ID));
        assert_eq!(get_header_id("correlation_id"), Some(CORRELATION_ID));
        assert_eq!(get_header_id("unknown"), None);

        assert_eq!(get_header_name(MESSAGE_ID), Some("message_id"));
        assert_eq!(get_header_name(CORRELATION_ID), Some("correlation_id"));
        assert_eq!(get_header_name(0xFE), None);
    }
}
