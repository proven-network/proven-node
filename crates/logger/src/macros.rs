//! High-performance logging macros

/// Log at error level
#[macro_export]
#[clippy::format_args]
macro_rules! error {
    ($($arg:tt)*) => {{
        if $crate::Level::Error.is_enabled_static() {
            if let Some(logger) = $crate::__private_try_get() {
                if logger.is_enabled($crate::Level::Error) {
                    logger.log($crate::Record::new($crate::Level::Error, format!($($arg)*))
                        .with_target(module_path!())
                        .with_location(file!(), line!()));
                }
            }
        }
    }};
}

/// Log at warning level
#[macro_export]
#[clippy::format_args]
macro_rules! warn {
    ($($arg:tt)*) => {{
        if $crate::Level::Warn.is_enabled_static() {
            if let Some(logger) = $crate::__private_try_get() {
                if logger.is_enabled($crate::Level::Warn) {
                    logger.log($crate::Record::new($crate::Level::Warn, format!($($arg)*))
                        .with_target(module_path!())
                        .with_location(file!(), line!()));
                }
            }
        }
    }};
}

/// Log at info level
#[macro_export]
#[clippy::format_args]
macro_rules! info {
    ($($arg:tt)*) => {{
        if $crate::Level::Info.is_enabled_static() {
            if let Some(logger) = $crate::__private_try_get() {
                if logger.is_enabled($crate::Level::Info) {
                    logger.log($crate::Record::new($crate::Level::Info, format!($($arg)*))
                        .with_target(module_path!())
                        .with_location(file!(), line!()));
                }
            }
        }
    }};
}

/// Log at debug level
#[macro_export]
#[clippy::format_args]
macro_rules! debug {
    ($($arg:tt)*) => {{
        if $crate::Level::Debug.is_enabled_static() {
            if let Some(logger) = $crate::__private_try_get() {
                if logger.is_enabled($crate::Level::Debug) {
                    logger.log($crate::Record::new($crate::Level::Debug, format!($($arg)*))
                        .with_target(module_path!())
                        .with_location(file!(), line!()));
                }
            }
        }
    }};
}

/// Log at trace level
#[macro_export]
#[clippy::format_args]
macro_rules! trace {
    ($($arg:tt)*) => {{
        if $crate::Level::Trace.is_enabled_static() {
            if let Some(logger) = $crate::__private_try_get() {
                if logger.is_enabled($crate::Level::Trace) {
                    logger.log($crate::Record::new($crate::Level::Trace, format!($($arg)*))
                        .with_target(module_path!())
                        .with_location(file!(), line!()));
                }
            }
        }
    }};
}

/// Check if a log level is enabled
#[macro_export]
macro_rules! log_enabled {
    ($level:expr) => {{
        // Compile-time check first
        if $level.is_enabled_static() {
            // Then runtime check
            $crate::__private_try_get()
                .map(|logger| logger.is_enabled($level))
                .unwrap_or(false)
        } else {
            false
        }
    }};
}
