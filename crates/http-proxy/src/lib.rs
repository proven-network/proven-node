//! Routes local HTTP requests from loopback to target services over messaging primitives.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod client;
mod error;
mod request;
mod response;
mod service;
mod service_handler;

pub use client::*;
pub use error::*;
pub use request::*;
pub use response::*;
pub use service::*;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;
