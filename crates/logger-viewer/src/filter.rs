//! Log filtering and parsing

use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl FromStr for LogLevel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "trace" => Ok(LogLevel::Trace),
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" | "warning" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Err(Error::ParseError(format!("Invalid log level: {s}"))),
        }
    }
}

/// A parsed log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub target: Option<String>,
    pub message: String,
    pub fields: Option<serde_json::Value>,
}

/// Log filter configuration
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    /// Minimum log level to include
    pub min_level: Option<LogLevel>,
    /// Target patterns to include (supports wildcards)
    pub include_targets: Vec<String>,
    /// Target patterns to exclude
    pub exclude_targets: Vec<String>,
    /// Message patterns to include
    pub include_messages: Vec<String>,
    /// Message patterns to exclude
    pub exclude_messages: Vec<String>,
    /// Time range filter
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

impl LogFilter {
    /// Check if a log entry matches this filter
    pub fn matches(&self, entry: &LogEntry) -> bool {
        // Check level
        if let Some(min_level) = self.min_level
            && entry.level < min_level
        {
            return false;
        }

        // Check time range
        if let Some((start, end)) = &self.time_range
            && (entry.timestamp < *start || entry.timestamp > *end)
        {
            return false;
        }

        // Check target filters
        if let Some(target) = &entry.target {
            // Check excludes first
            for pattern in &self.exclude_targets {
                if Self::matches_pattern(target, pattern) {
                    return false;
                }
            }

            // Check includes if any specified
            if !self.include_targets.is_empty() {
                let mut matched = false;
                for pattern in &self.include_targets {
                    if Self::matches_pattern(target, pattern) {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return false;
                }
            }
        }

        // Check message filters
        // Check excludes first
        for pattern in &self.exclude_messages {
            if entry.message.contains(pattern) {
                return false;
            }
        }

        // Check includes if any specified
        if !self.include_messages.is_empty() {
            let mut matched = false;
            for pattern in &self.include_messages {
                if entry.message.contains(pattern) {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return false;
            }
        }

        true
    }

    /// Check if a target matches a pattern (supports wildcards)
    fn matches_pattern(target: &str, pattern: &str) -> bool {
        if pattern.contains('*') {
            // Simple wildcard matching
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.is_empty() {
                return true;
            }

            let mut pos = 0;
            for (i, part) in parts.iter().enumerate() {
                if part.is_empty() {
                    continue;
                }

                if i == 0 && !pattern.starts_with('*') {
                    // Pattern doesn't start with *, so target must start with this part
                    if !target.starts_with(part) {
                        return false;
                    }
                    pos = part.len();
                } else if i == parts.len() - 1 && !pattern.ends_with('*') {
                    // Pattern doesn't end with *, so target must end with this part
                    return target.ends_with(part);
                } else {
                    // Find this part in the remaining target
                    if let Some(idx) = target[pos..].find(part) {
                        pos += idx + part.len();
                    } else {
                        return false;
                    }
                }
            }
            true
        } else {
            target == pattern
        }
    }
}

/// Parse a log line into a LogEntry
pub fn parse_log_line(line: &str) -> Result<LogEntry> {
    // Try to parse as JSON first
    if line.starts_with('{')
        && let Ok(entry) = serde_json::from_str::<LogEntry>(line)
    {
        return Ok(entry);
    }

    // Try common log formats
    // Format: [TIMESTAMP] LEVEL TARGET - MESSAGE
    if line.starts_with('[')
        && let Some(end_bracket) = line.find(']')
    {
        let timestamp_str = &line[1..end_bracket];
        let rest = &line[end_bracket + 1..].trim();

        // Parse timestamp
        let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
            .or_else(|_| DateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f %z"))
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        // Parse level and rest
        let parts: Vec<&str> = rest.splitn(3, ' ').collect();
        if parts.len() >= 2 {
            let level = LogLevel::from_str(parts[0]).unwrap_or(LogLevel::Info);

            let (target, message) = if parts.len() >= 3 && parts[1].ends_with(':') {
                (
                    Some(parts[1].trim_end_matches(':').to_string()),
                    parts[2].to_string(),
                )
            } else {
                (None, parts[1..].join(" "))
            };

            return Ok(LogEntry {
                timestamp,
                level,
                target,
                message,
                fields: None,
            });
        }
    }

    // Default parsing - treat entire line as message
    Ok(LogEntry {
        timestamp: Utc::now(),
        level: LogLevel::Info,
        target: None,
        message: line.to_string(),
        fields: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        assert!(LogFilter::matches_pattern("module::submodule", "module::*"));
        assert!(LogFilter::matches_pattern(
            "module::submodule",
            "*::submodule"
        ));
        assert!(LogFilter::matches_pattern(
            "module::submodule",
            "module::submodule"
        ));
        assert!(!LogFilter::matches_pattern("module::submodule", "other::*"));
        assert!(LogFilter::matches_pattern("module::sub::deep", "*::sub::*"));
    }

    #[test]
    fn test_filter_matching() {
        let filter = LogFilter {
            min_level: Some(LogLevel::Warn),
            include_targets: vec!["app::*".to_string()],
            exclude_targets: vec!["app::noisy".to_string()],
            ..Default::default()
        };

        let entry1 = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Error,
            target: Some("app::module".to_string()),
            message: "Error message".to_string(),
            fields: None,
        };
        assert!(filter.matches(&entry1));

        let entry2 = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            target: Some("app::module".to_string()),
            message: "Info message".to_string(),
            fields: None,
        };
        assert!(!filter.matches(&entry2)); // Level too low

        let entry3 = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Error,
            target: Some("app::noisy".to_string()),
            message: "Noisy error".to_string(),
            fields: None,
        };
        assert!(!filter.matches(&entry3)); // Excluded target
    }

    #[test]
    fn test_log_parsing() {
        let line = "[2024-01-15T10:30:00Z] ERROR app::module - Something went wrong";
        let entry = parse_log_line(line).unwrap();
        assert_eq!(entry.level, LogLevel::Error);
        assert_eq!(entry.target, Some("app::module".to_string()));
        assert_eq!(entry.message, "Something went wrong");

        // Test JSON parsing
        let json_line = r#"{"timestamp":"2024-01-15T10:30:00Z","level":"Error","target":"app::module","message":"Something went wrong","fields":null}"#;
        let entry = parse_log_line(json_line).unwrap();
        assert_eq!(entry.level, LogLevel::Error);
        assert_eq!(entry.target, Some("app::module".to_string()));
        assert_eq!(entry.message, "Something went wrong");
    }
}
