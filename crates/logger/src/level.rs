//! Log levels with compile-time filtering support

use serde::{Deserialize, Serialize};

/// Log level enum - ordered by severity for fast comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Level {
    /// Trace level - lowest priority
    Trace = 0,
    /// Debug level
    Debug = 1,
    /// Info level
    Info = 2,
    /// Warning level
    Warn = 3,
    /// Error level - highest priority
    Error = 4,
}

impl Level {
    /// Check if this level is enabled at compile time
    #[inline(always)]
    pub const fn is_enabled_static(self) -> bool {
        self as u8 >= Self::static_max() as u8
    }

    /// Get the compile-time maximum level
    #[inline(always)]
    pub const fn static_max() -> Self {
        // Release-specific features take precedence when in release mode
        #[cfg(all(not(debug_assertions), feature = "release-max-level-off"))]
        return Self::Error; // Actually disables all

        #[cfg(all(not(debug_assertions), feature = "release-max-level-error"))]
        return Self::Error;

        #[cfg(all(not(debug_assertions), feature = "release-max-level-warn"))]
        return Self::Warn;

        #[cfg(all(not(debug_assertions), feature = "release-max-level-info"))]
        return Self::Info;

        #[cfg(all(not(debug_assertions), feature = "release-max-level-debug"))]
        return Self::Debug;

        // Regular features apply otherwise
        #[cfg(feature = "max-level-off")]
        return Self::Error; // Actually disables all

        #[cfg(feature = "max-level-error")]
        return Self::Error;

        #[cfg(feature = "max-level-warn")]
        return Self::Warn;

        #[cfg(feature = "max-level-info")]
        return Self::Info;

        #[cfg(feature = "max-level-debug")]
        return Self::Debug;

        #[cfg(not(any(
            feature = "max-level-off",
            feature = "max-level-error",
            feature = "max-level-warn",
            feature = "max-level-info",
            feature = "max-level-debug",
            feature = "release-max-level-off",
            feature = "release-max-level-error",
            feature = "release-max-level-warn",
            feature = "release-max-level-info",
            feature = "release-max-level-debug"
        )))]
        return Self::Trace;
    }
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Level::Trace => write!(f, "TRACE"),
            Level::Debug => write!(f, "DEBUG"),
            Level::Info => write!(f, "INFO"),
            Level::Warn => write!(f, "WARN"),
            Level::Error => write!(f, "ERROR"),
        }
    }
}
