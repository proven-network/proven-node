//! Abstract interface for managing distributed messaging.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod handler;
mod subject;
mod subscriber;

pub use handler::*;
pub use subject::*;
pub use subscriber::*;
