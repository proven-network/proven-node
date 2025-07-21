//! Compatibility bridges for other logging crates

#[cfg(feature = "log-compat")]
pub mod log_bridge;

#[cfg(feature = "tracing-compat")]
pub mod tracing_bridge;

#[cfg(any(feature = "log-compat", feature = "tracing-compat"))]
mod auto;

#[cfg(any(feature = "log-compat", feature = "tracing-compat"))]
pub use auto::*;
