#[cfg(target_os = "linux")]
mod tracing;
#[cfg(not(target_os = "linux"))]
mod tracing_mock;

mod error;

#[cfg(target_os = "linux")]
pub use tracing::*;
#[cfg(not(target_os = "linux"))]
pub use tracing_mock::*;

pub use crate::error::{Error, Result};
