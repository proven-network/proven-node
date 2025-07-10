//! Protocol layer for the RPC framework.
//!
//! This module contains the core protocol definitions including:
//! - Message traits and types
//! - Communication patterns
//! - Framing and codec implementations

pub mod codec;
pub mod framing;
pub mod message;
pub mod patterns;

pub use framing::{Frame, FrameCodec, FrameType};
pub use message::{MessageId, ResponseEnvelope, RpcMessage};
pub use patterns::{MessagePattern, RequestOptions};
