//! Routes local HTTP requests from loopback to target services over engine streams.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod client;
mod error;
mod request;
mod response;
mod service;

pub use client::*;
pub use error::*;
pub use request::*;
pub use response::*;
pub use service::*;

/// The default stream prefix for HTTP proxy.
pub const DEFAULT_STREAM_PREFIX: &str = "http-proxy";
