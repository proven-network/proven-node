#[cfg(target_os = "linux")]
mod proxy;
#[cfg(not(target_os = "linux"))]
mod proxy_mock;

mod error;

#[cfg(target_os = "linux")]
pub use proxy::*;
#[cfg(not(target_os = "linux"))]
pub use proxy_mock::*;

pub use crate::error::{Error, Result};
