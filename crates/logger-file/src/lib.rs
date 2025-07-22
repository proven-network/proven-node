//! File-based logger implementation with rotation and async I/O
//!
//! This crate provides a high-performance file-based logger that supports:
//! - Async I/O for non-blocking log writes
//! - Automatic file rotation based on size or time
//! - Configurable log formatting (JSON or plain text)
//! - Log categorization with trait-based extensibility
//! - Symlink management for easy access to latest logs

#![warn(missing_docs, unreachable_pub)]
#![forbid(unsafe_code)]

mod categorizer;
mod config;
mod error;
mod formatter;
mod writer;

pub use categorizer::{
    ComponentCategorizer, DefaultCategorizer, LevelCategorizer, LogCategorizer, NodeIdCategorizer,
};
pub use config::{FileLoggerConfig, FileLoggerConfigBuilder, RotationPolicy};
pub use error::{Error, Result};
pub use formatter::{JsonFormatter, LogFormatter, PlainTextFormatter};
pub use writer::{AsyncLogWriter, FileLogger};
