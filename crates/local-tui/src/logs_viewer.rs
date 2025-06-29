//! Log viewing and filtering system for reading logs from disk

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, NodeId};

use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tracing::debug;

/// Viewport-based log reader for efficient memory usage
pub struct LogViewer {
    file_path: PathBuf,
    level_filter: LogLevel,
    node_filter: Option<NodeId>,
    /// Cache of line positions for efficient seeking
    line_positions: Vec<u64>,
    /// Whether line positions have been indexed
    indexed: bool,
}

impl LogViewer {
    /// Create a new log viewer for a specific log file
    pub const fn new(
        file_path: PathBuf,
        level_filter: LogLevel,
        node_filter: Option<NodeId>,
    ) -> Self {
        Self {
            file_path,
            level_filter,
            node_filter,
            line_positions: Vec::new(),
            indexed: false,
        }
    }

    /// Index the file to build line position cache (for efficient seeking)
    fn ensure_indexed(&mut self) -> Result<()> {
        if self.indexed || !self.file_path.exists() {
            return Ok(());
        }

        let file = File::open(&self.file_path)
            .with_context(|| format!("Failed to open log file: {}", self.file_path.display()))?;

        let mut reader = BufReader::new(file);
        let mut position = 0u64;
        let mut line = String::new();

        self.line_positions.clear();
        self.line_positions.push(0); // First line starts at position 0

        while reader.read_line(&mut line)? > 0 {
            position += line.len() as u64;
            self.line_positions.push(position);
            line.clear();
        }

        // Remove the last position (EOF)
        self.line_positions.pop();
        self.indexed = true;

        Ok(())
    }

    /// Get total number of lines in the file
    pub fn total_lines(&mut self) -> Result<usize> {
        self.ensure_indexed()?;
        Ok(self.line_positions.len())
    }

    /// Read logs for a specific viewport (`start_line` to `start_line` + count)
    /// Returns (logs, `total_filtered_count`)
    pub fn read_viewport(
        &mut self,
        start_line: usize,
        count: usize,
    ) -> Result<(Vec<LogEntry>, usize)> {
        if !self.file_path.exists() {
            return Ok((Vec::new(), 0));
        }

        self.ensure_indexed()?;

        let total_lines = self.line_positions.len();
        if start_line >= total_lines {
            return Ok((Vec::new(), 0));
        }

        let end_line = (start_line + count).min(total_lines);
        let mut logs = Vec::new();
        let mut total_filtered = 0;

        // Open file and seek to start position
        let mut file = File::open(&self.file_path)?;
        let start_pos = self.line_positions[start_line];
        file.seek(std::io::SeekFrom::Start(start_pos))?;

        let reader = BufReader::new(file);
        let lines_to_read = end_line - start_line;

        for (i, line) in reader.lines().take(lines_to_read).enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<LogEntry>(&line) {
                Ok(entry) => {
                    // Apply filters
                    let passes_level = Self::should_show_level(entry.level, self.level_filter);
                    let passes_node = self
                        .node_filter
                        .is_none_or(|filter_node| entry.node_id == filter_node);

                    if passes_level && passes_node {
                        logs.push(entry);
                        total_filtered += 1;
                    }
                }
                Err(e) => {
                    debug!(
                        "Failed to parse log line {}: {} - Error: {}",
                        start_line + i,
                        line,
                        e
                    );
                }
            }
        }

        Ok((logs, total_filtered))
    }

    /// Read the most recent N logs (for tailing)
    /// This reads a much larger chunk of raw lines to ensure we get enough filtered results
    pub fn read_recent(&mut self, count: usize) -> Result<Vec<LogEntry>> {
        self.ensure_indexed()?;
        let total_lines = self.line_positions.len();

        if total_lines == 0 {
            return Ok(Vec::new());
        }

        // Read a much larger chunk to account for filtering
        // Start from much earlier in the file to ensure we get enough filtered results
        let chunk_size = (count * 10).max(2000); // Read 10x more lines or at least 2000
        let start_line = total_lines.saturating_sub(chunk_size);

        // Read the large chunk and filter it
        let (mut all_filtered_logs, _) = self.read_viewport(start_line, chunk_size)?;

        // Take only the most recent N filtered logs
        // Since logs are in chronological order in the file, the last N are the most recent
        if all_filtered_logs.len() > count {
            all_filtered_logs = all_filtered_logs.split_off(all_filtered_logs.len() - count);
        }

        Ok(all_filtered_logs)
    }

    /// Update filters for this viewer
    pub const fn update_filters(&mut self, level_filter: LogLevel, node_filter: Option<NodeId>) {
        self.level_filter = level_filter;
        self.node_filter = node_filter;
    }

    /// Check if a log level should be shown given the current filter
    const fn should_show_level(entry_level: LogLevel, filter_level: LogLevel) -> bool {
        match filter_level {
            LogLevel::Error => matches!(entry_level, LogLevel::Error),
            LogLevel::Warn => matches!(entry_level, LogLevel::Error | LogLevel::Warn),
            LogLevel::Info => matches!(
                entry_level,
                LogLevel::Error | LogLevel::Warn | LogLevel::Info
            ),
            LogLevel::Debug => matches!(
                entry_level,
                LogLevel::Error | LogLevel::Warn | LogLevel::Info | LogLevel::Debug
            ),
            LogLevel::Trace => true, // Show all levels
        }
    }

    /// Refresh the file index (call when file may have grown)
    pub fn refresh(&mut self) {
        self.indexed = false;
        self.line_positions.clear();
    }
}

/// High-performance log reader that provides a unified interface for reading logs
pub struct LogReader {
    /// Current log level filter
    current_level_filter: Arc<RwLock<LogLevel>>,
    /// Current node filter
    current_node_filter: Arc<RwLock<Option<NodeId>>>,
    /// Current session directory for log reading
    session_dir: Arc<RwLock<PathBuf>>,
}

impl LogReader {
    /// Create a new log reader with session directory
    pub fn new(session_dir: PathBuf) -> Self {
        Self {
            current_level_filter: Arc::new(RwLock::new(LogLevel::Info)),
            current_node_filter: Arc::new(RwLock::new(None)),
            session_dir: Arc::new(RwLock::new(session_dir)),
        }
    }

    /// Set the current log level filter
    pub fn set_level_filter(&self, level: LogLevel) {
        *self.current_level_filter.write() = level;
    }

    /// Get the current log level filter
    pub fn get_level_filter(&self) -> LogLevel {
        *self.current_level_filter.read()
    }

    /// Set the current node filter
    pub fn set_node_filter(&self, node_id: Option<NodeId>) {
        *self.current_node_filter.write() = node_id;
    }

    /// Get the current node filter
    pub fn get_node_filter(&self) -> Option<NodeId> {
        *self.current_node_filter.read()
    }

    /// Get filtered logs from disk (blocking operation)
    pub fn get_filtered_logs_blocking(&self, _timeout: Duration) -> Vec<LogEntry> {
        let level_filter = self.get_level_filter();
        let node_filter = self.get_node_filter();

        let session_dir = self.session_dir.read();

        // Determine which file to read based on node filter
        let file_path = node_filter.map_or_else(
            || session_dir.join("all.log"),
            |node_id| {
                if node_id == MAIN_THREAD_NODE_ID {
                    // Read debug logs for main thread
                    session_dir.join("debug.log")
                } else {
                    // Read specific node log file (no subdirectory anymore)
                    session_dir.join(format!("node_{}.log", node_id.execution_order()))
                }
            },
        );

        // Use LogViewer for efficient reading
        let mut viewer = LogViewer::new(file_path, level_filter, node_filter);

        // Read the most recent 500 logs for the viewport
        match viewer.read_recent(500) {
            Ok(logs) => logs,
            Err(e) => {
                debug!("Failed to read logs using LogViewer: {}", e);
                Vec::new()
            }
        }
    }
}

impl Clone for LogReader {
    fn clone(&self) -> Self {
        let session_dir = self.session_dir.read().clone();
        Self::new(session_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs_writer::{DiskLogConfig, LogWriter};
    use chrono::Utc;
    use tempfile::TempDir;

    fn create_test_config() -> (DiskLogConfig, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = DiskLogConfig {
            base_dir: temp_dir.path().to_path_buf(),

            flush_interval_ms: 100,
        };
        (config, temp_dir)
    }

    fn create_test_log_entry() -> LogEntry {
        LogEntry {
            node_id: NodeId::new(),
            level: LogLevel::Info,
            message: "Test log message".to_string(),
            timestamp: Utc::now(),
            target: Some("test".to_string()),
        }
    }

    fn reset_node_id_state() {
        #[cfg(test)]
        crate::node_id::NodeId::reset_global_state();
    }

    #[test]
    fn test_log_viewer_reading() {
        reset_node_id_state();
        let (config, _temp_dir) = create_test_config();
        let writer = LogWriter::with_config(config).expect("Failed to create writer");

        // Write some test entries
        let entry1 = create_test_log_entry();
        let mut entry2 = create_test_log_entry();
        entry2.level = LogLevel::Error;
        entry2.message = "Error message".to_string();

        writer.add_log(entry1);
        writer.add_log(entry2);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test reading with LogViewer
        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        let all_log_path = session_dir.join("all.log");
        let mut viewer = LogViewer::new(all_log_path, LogLevel::Info, None);
        let logs = viewer.read_recent(10).expect("Failed to read logs");

        assert_eq!(logs.len(), 2);
        assert!(
            logs.iter()
                .any(|log| log.message.contains("Test log message"))
        );
        assert!(logs.iter().any(|log| log.message.contains("Error message")));
    }

    #[test]
    fn test_log_reader_interface() {
        reset_node_id_state();
        let (config, _temp_dir) = create_test_config();
        let writer = LogWriter::with_config(config).expect("Failed to create writer");

        // Write a test entry
        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test LogReader
        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        let reader = LogReader::new(session_dir);
        let logs = reader.get_filtered_logs_blocking(Duration::from_millis(100));

        assert!(!logs.is_empty());
        assert!(logs[0].message.contains("Test log message"));
    }

    #[test]
    fn test_level_filtering() {
        reset_node_id_state();
        let (config, _temp_dir) = create_test_config();
        let writer = LogWriter::with_config(config).expect("Failed to create writer");

        // Write entries with different levels
        let mut info_entry = create_test_log_entry();
        info_entry.level = LogLevel::Info;
        info_entry.message = "Info message".to_string();

        let mut error_entry = create_test_log_entry();
        error_entry.level = LogLevel::Error;
        error_entry.message = "Error message".to_string();

        writer.add_log(info_entry);
        writer.add_log(error_entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test filtering to only show errors
        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        let reader = LogReader::new(session_dir);
        reader.set_level_filter(LogLevel::Error);
        let logs = reader.get_filtered_logs_blocking(Duration::from_millis(100));

        // Should only show error messages when filtering for Error level
        assert!(logs.iter().all(|log| matches!(log.level, LogLevel::Error)));
        assert!(logs.iter().any(|log| log.message.contains("Error message")));
    }
}
