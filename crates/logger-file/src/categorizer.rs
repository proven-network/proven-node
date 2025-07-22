//! Log categorization for organizing logs into different files/directories

use proven_logger::{Level, Record};
use std::path::PathBuf;

/// Trait for categorizing logs into different files or directories
pub trait LogCategorizer: Send + Sync + 'static {
    /// Get the category for a log record
    ///
    /// Returns a path component that will be used to organize the log.
    /// For example, returning "node-1" might result in logs being written
    /// to "logs/node-1/app.log"
    fn categorize(&self, record: &Record) -> Option<PathBuf>;

    /// Get all active categories
    ///
    /// This is used for creating symlinks and managing rotations
    fn active_categories(&self) -> Vec<PathBuf>;
}

/// Default categorizer that doesn't categorize logs
#[derive(Debug, Clone)]
pub struct DefaultCategorizer;

impl LogCategorizer for DefaultCategorizer {
    fn categorize(&self, _record: &Record) -> Option<PathBuf> {
        None
    }

    fn active_categories(&self) -> Vec<PathBuf> {
        vec![]
    }
}

/// Categorizer that organizes logs by level
#[derive(Debug, Clone)]
pub struct LevelCategorizer;

impl LogCategorizer for LevelCategorizer {
    fn categorize(&self, record: &Record) -> Option<PathBuf> {
        let level_str = match record.level {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        };
        Some(PathBuf::from(level_str))
    }

    fn active_categories(&self) -> Vec<PathBuf> {
        vec![
            PathBuf::from("error"),
            PathBuf::from("warn"),
            PathBuf::from("info"),
            PathBuf::from("debug"),
            PathBuf::from("trace"),
        ]
    }
}

/// Categorizer that uses node ID from context
#[derive(Debug, Clone)]
pub struct NodeIdCategorizer;

impl LogCategorizer for NodeIdCategorizer {
    fn categorize(&self, record: &Record) -> Option<PathBuf> {
        record
            .context
            .and_then(|ctx| ctx.node_id.as_ref())
            .map(|node_id| PathBuf::from(node_id.as_ref()))
    }

    fn active_categories(&self) -> Vec<PathBuf> {
        // Cannot determine active categories without runtime information
        vec![]
    }
}

/// Categorizer that uses component from context
#[derive(Debug, Clone)]
pub struct ComponentCategorizer;

impl LogCategorizer for ComponentCategorizer {
    fn categorize(&self, record: &Record) -> Option<PathBuf> {
        record.context.map(|ctx| PathBuf::from(ctx.component))
    }

    fn active_categories(&self) -> Vec<PathBuf> {
        // Cannot determine active categories without runtime information
        vec![]
    }
}
