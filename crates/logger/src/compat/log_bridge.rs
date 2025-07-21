//! Bridge from the `log` crate to proven-logger

use crate::{Level, Logger, Record};
use log::{Log, Metadata, Record as LogRecord};
use std::sync::Arc;

/// Wrapper to implement the log crate's Log trait
pub struct LogBridge {
    logger: Arc<dyn Logger>,
}

impl LogBridge {
    /// Create a new log bridge
    pub fn new(logger: Arc<dyn Logger>) -> Self {
        Self { logger }
    }
}

impl Log for LogBridge {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let level = map_level(metadata.level());
        self.logger.is_enabled(level)
    }

    fn log(&self, record: &LogRecord) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let level = map_level(record.level());

        // Create our record
        // Convert target to 'static str safely
        let target = record.target();
        let target_static: &'static str = Box::leak(target.to_string().into_boxed_str());

        let mut proven_record =
            Record::new(level, format!("{}", record.args())).with_target(target_static);

        if let (Some(file), Some(line)) = (record.file(), record.line()) {
            let file_static: &'static str = Box::leak(file.to_string().into_boxed_str());
            proven_record = proven_record.with_location(file_static, line);
        }

        self.logger.log(proven_record);
    }

    fn flush(&self) {
        self.logger.flush();
    }
}

/// Map log levels to our levels
fn map_level(level: log::Level) -> Level {
    match level {
        log::Level::Error => Level::Error,
        log::Level::Warn => Level::Warn,
        log::Level::Info => Level::Info,
        log::Level::Debug => Level::Debug,
        log::Level::Trace => Level::Trace,
    }
}

/// Initialize the log crate to use proven-logger
///
/// This will capture all logs from crates using the `log` crate macros.
///
/// # Example
/// ```no_run
/// use proven_logger::{StdoutLogger, compat::init_log_bridge};
/// use std::sync::Arc;
///
/// let logger = Arc::new(StdoutLogger::new());
/// init_log_bridge(logger.clone()).expect("Failed to set log bridge");
/// ```
pub fn init_log_bridge(logger: Arc<dyn Logger>) -> Result<(), log::SetLoggerError> {
    // We need to leak the bridge because log::set_logger requires 'static
    let bridge = Box::leak(Box::new(LogBridge::new(logger)));

    // Set our bridge as the global logger
    log::set_logger(bridge)?;

    // Set max level based on compile-time features
    let max_level = match Level::static_max() {
        Level::Error => log::LevelFilter::Error,
        Level::Warn => log::LevelFilter::Warn,
        Level::Info => log::LevelFilter::Info,
        Level::Debug => log::LevelFilter::Debug,
        Level::Trace => log::LevelFilter::Trace,
    };

    log::set_max_level(max_level);
    Ok(())
}
