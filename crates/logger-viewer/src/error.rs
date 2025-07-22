//! Error types for log viewer operations

use std::io;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Failed to memory map file at {path}: {source}")]
    MemoryMap { path: PathBuf, source: io::Error },

    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Invalid line index: {index} (total lines: {total})")]
    InvalidLineIndex { index: usize, total: usize },

    #[error("Failed to parse log entry: {0}")]
    ParseError(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("File watch error: {0}")]
    WatchError(String),

    #[error("Channel send error: {0}")]
    ChannelSend(String),

    #[error("Invalid file pattern: {0}")]
    InvalidPattern(String),

    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),

    #[error("Glob error: {0}")]
    Glob(#[from] glob::GlobError),
}

pub type Result<T> = std::result::Result<T, Error>;
