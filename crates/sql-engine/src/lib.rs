//! SQL storage implementation using engine streams.
//!
//! This crate provides SQL storage capabilities built on top of the engine's
//! streaming infrastructure, replacing the messaging-based approach.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod connection;
mod coordination;
mod error;
mod handle_request;
mod libsql_error;
mod pool;
mod request;
mod response;
mod service;
mod store;
mod transaction;

pub use connection::Connection;
pub use error::Error;
pub use libsql_error::{LibsqlError, StatementType};
pub use request::Request;
pub use response::Response;
pub use store::{SqlEngineStore, SqlEngineStore1, SqlEngineStore2, SqlEngineStore3};
