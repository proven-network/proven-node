//! TUI-specific log categorization for proven-logger-file

use crate::messages::TuiNodeId;
use proven_logger::Record;
use proven_logger_file::LogCategorizer;
use std::path::PathBuf;

/// Extract node ID from the current thread name
/// Thread names follow the pattern "node-{execution_order}-{pokemon_id}" (e.g., "node-1-42", "node-2-7")
/// Returns None for non-node threads (main thread, etc.)
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

/// TUI-specific log categorizer that organizes logs by node
pub struct TuiLogCategorizer;

impl LogCategorizer for TuiLogCategorizer {
    fn categorize(&self, _record: &Record) -> Option<PathBuf> {
        // Check thread name to determine node ID
        extract_node_id_from_thread_name().map_or_else(
            || Some(PathBuf::from("debug")),
            |node_id| Some(PathBuf::from(format!("node_{}", node_id.execution_order()))),
        )
    }

    fn active_categories(&self) -> Vec<PathBuf> {
        // Return a minimal set - actual categories are created dynamically
        vec![PathBuf::from("debug")]
    }
}

/// All logs categorizer - no categorization, everything goes to root
pub struct AllLogsCategorizer;

impl LogCategorizer for AllLogsCategorizer {
    fn categorize(&self, _record: &Record) -> Option<PathBuf> {
        // No categorization - everything goes to the root log directory
        None
    }

    fn active_categories(&self) -> Vec<PathBuf> {
        vec![]
    }
}
