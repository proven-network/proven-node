//! Adapters for converting between TUI LogLevel and proven-logger Level

use crate::messages::LogLevel;
use proven_logger::Level;

/// Convert from TUI `LogLevel` to proven-logger Level
impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => Self::Error,
            LogLevel::Warn => Self::Warn,
            LogLevel::Info => Self::Info,
            LogLevel::Debug => Self::Debug,
            LogLevel::Trace => Self::Trace,
        }
    }
}

/// Convert from proven-logger Level to TUI `LogLevel`
impl From<Level> for LogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => Self::Error,
            Level::Warn => Self::Warn,
            Level::Info => Self::Info,
            Level::Debug => Self::Debug,
            Level::Trace => Self::Trace,
        }
    }
}

/// Extension trait for `LogLevel` conversions
pub trait LogLevelExt {
    /// Convert to proven-logger Level
    fn to_logger_level(&self) -> Level;
}

impl LogLevelExt for LogLevel {
    fn to_logger_level(&self) -> Level {
        (*self).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_conversions() {
        // Test TUI to logger
        assert_eq!(Level::from(LogLevel::Error), Level::Error);
        assert_eq!(Level::from(LogLevel::Warn), Level::Warn);
        assert_eq!(Level::from(LogLevel::Info), Level::Info);
        assert_eq!(Level::from(LogLevel::Debug), Level::Debug);
        assert_eq!(Level::from(LogLevel::Trace), Level::Trace);

        // Test logger to TUI
        assert_eq!(LogLevel::from(Level::Error), LogLevel::Error);
        assert_eq!(LogLevel::from(Level::Warn), LogLevel::Warn);
        assert_eq!(LogLevel::from(Level::Info), LogLevel::Info);
        assert_eq!(LogLevel::from(Level::Debug), LogLevel::Debug);
        assert_eq!(LogLevel::from(Level::Trace), LogLevel::Trace);
    }

    #[test]
    fn test_log_level_ext() {
        assert_eq!(LogLevel::Error.to_logger_level(), Level::Error);
        assert_eq!(LogLevel::Info.to_logger_level(), Level::Info);
    }
}
