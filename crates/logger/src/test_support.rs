//! Test support utilities
//!
//! This module provides utilities for capturing logs during tests.
//! It's only available when the `test-support` feature is enabled.

use crate::{Context, Level, Logger, Record};
use std::fmt::Write as FmtWrite;
use std::sync::{Arc, Mutex};

/// A logger that captures all logs in memory for testing
#[derive(Clone)]
pub struct CaptureLogger {
    logs: Arc<Mutex<String>>,
    min_level: Level,
    context: Context,
}

impl CaptureLogger {
    /// Create a new capture logger
    pub fn new() -> Self {
        Self {
            logs: Arc::new(Mutex::new(String::new())),
            min_level: Level::Trace,
            context: Context::new("test"),
        }
    }

    /// Create with a specific level
    pub fn with_level(mut self, level: Level) -> Self {
        self.min_level = level;
        self
    }

    /// Get all captured logs
    pub fn logs(&self) -> String {
        self.logs.lock().unwrap().clone()
    }

    /// Clear captured logs
    pub fn clear(&self) {
        self.logs.lock().unwrap().clear();
    }

    /// Check if logs contain a specific string
    pub fn contains(&self, text: &str) -> bool {
        self.logs.lock().unwrap().contains(text)
    }
}

impl Default for CaptureLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger for CaptureLogger {
    fn log(&self, record: Record) {
        let record = if record.context.is_none() {
            record.with_context(&self.context)
        } else {
            record
        };

        if let Ok(mut logs) = self.logs.lock() {
            let node_str = record
                .context
                .and_then(|ctx| ctx.node_id.as_ref())
                .map(|id| format!("[{id}] "))
                .unwrap_or_default();

            let location = if let (Some(file), Some(line)) = (record.file, record.line) {
                format!(" {file}:{line}")
            } else {
                String::new()
            };

            let _ = writeln!(
                logs,
                "{}{} [{}]{} {}",
                node_str, record.level, record.target, location, record.message
            );
        }
    }

    fn flush(&self) {
        // No-op for in-memory logger
    }

    #[inline(always)]
    fn is_enabled(&self, level: Level) -> bool {
        level >= self.min_level && level.is_enabled_static()
    }

    fn with_context(&self, context: Context) -> Arc<dyn Logger> {
        Arc::new(CaptureLogger {
            logs: self.logs.clone(),
            min_level: self.min_level,
            context: self.context.merge(&context),
        })
    }
}

/// Test guard that captures logs and prints them on test failure
pub struct TestLogGuard {
    logger: CaptureLogger,
    test_name: String,
    printed: bool,
}

impl TestLogGuard {
    /// Create a new test log guard
    pub fn new(test_name: impl Into<String>) -> Self {
        let logger = CaptureLogger::new();
        // Initialize the global logger with our capture logger
        let _ = crate::init(Arc::new(logger.clone()));

        Self {
            logger,
            test_name: test_name.into(),
            printed: false,
        }
    }

    /// Mark that the test passed (logs won't be printed)
    pub fn passed(&mut self) {
        self.printed = true;
    }

    /// Get the capture logger
    pub fn logger(&self) -> &CaptureLogger {
        &self.logger
    }
}

impl Drop for TestLogGuard {
    fn drop(&mut self) {
        if !self.printed && std::thread::panicking() {
            let logs = self.logger.logs();
            if !logs.is_empty() {
                eprintln!("\n===== Logs from failed test '{}' =====", self.test_name);
                eprint!("{logs}");
                eprintln!("===== End of logs =====\n");
            }
        }
    }
}

// Re-export the procedural macros
pub use proven_logger_macros::{logged_test, logged_tokio_test};
