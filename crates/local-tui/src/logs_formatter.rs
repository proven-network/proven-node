//! TUI-specific log formatting for proven-logger-file

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, TuiNodeId};
use chrono::Utc;
use proven_logger::{Level, Record};
use proven_logger_file::{LogFormatter, Result};

/// Convert proven-logger Level to TUI `LogLevel`
const fn convert_level(level: Level) -> LogLevel {
    match level {
        Level::Error => LogLevel::Error,
        Level::Warn => LogLevel::Warn,
        Level::Info => LogLevel::Info,
        Level::Debug => LogLevel::Debug,
        Level::Trace => LogLevel::Trace,
    }
}

/// Extract node ID from the logger context or thread name
fn extract_node_id(record: &Record) -> TuiNodeId {
    // First try to get from context
    if let Some(context) = record.context
        && let Some(node_id_str) = &context.node_id
    {
        // Parse "node-1-25" format
        if let Some(node_part) = node_id_str.strip_prefix("node-") {
            if node_part == "main" {
                return MAIN_THREAD_NODE_ID;
            }
            let mut parts = node_part.split('-');
            if let (Some(exec_str), Some(poke_str)) = (parts.next(), parts.next())
                && let (Ok(exec), Ok(poke)) = (exec_str.parse::<u8>(), poke_str.parse::<u8>())
            {
                return TuiNodeId::with_values(exec, poke);
            }
        } else if node_id_str == "main" {
            return MAIN_THREAD_NODE_ID;
        }
    }

    // Fall back to thread name parsing for backward compatibility
    extract_node_id_from_thread_name().unwrap_or(MAIN_THREAD_NODE_ID)
}

/// Extract node ID from the current thread name (backward compatibility)
fn extract_node_id_from_thread_name() -> Option<TuiNodeId> {
    let thread = std::thread::current();
    let thread_name = thread.name()?;

    // Parse "node-123-45" -> execution_order = 123, pokemon_id = 45
    let node_part = thread_name.strip_prefix("node-")?;
    let mut parts = node_part.split('-');

    let execution_order: u8 = parts.next()?.parse().ok()?;
    let pokemon_id: u8 = parts.next()?.parse().ok()?;

    Some(TuiNodeId::with_values(execution_order, pokemon_id))
}

/// TUI log formatter that outputs JSON Lines format compatible with existing log reading
pub struct TuiLogFormatter;

impl LogFormatter for TuiLogFormatter {
    fn format(&self, record: &Record) -> Result<Vec<u8>> {
        // Extract node ID from context or thread name
        let node_id = extract_node_id(record);

        // Create LogEntry compatible with existing TUI log reading
        let log_entry = LogEntry {
            node_id,
            level: convert_level(record.level),
            message: record.message.to_string(),
            timestamp: Utc::now(),
            target: Some(record.target.to_string()),
        };

        // Serialize to JSON Lines format
        let mut json = serde_json::to_vec(&log_entry)?;
        json.push(b'\n');

        Ok(json)
    }
}
