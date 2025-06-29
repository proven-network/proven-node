//! Log viewing and filtering system for reading logs from disk

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, NodeId};

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tracing::debug;

/// File-based log reader for efficient memory usage and line indexing
pub struct LogFileReader {
    file_path: PathBuf,
    /// Cache of line positions for efficient seeking
    line_positions: Vec<u64>,
    /// Whether line positions have been indexed
    indexed: bool,
    /// Last modification time of the file
    last_modified: Option<std::time::SystemTime>,
}

impl LogFileReader {
    /// Create a new log file reader
    pub const fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            line_positions: Vec::new(),
            indexed: false,
            last_modified: None,
        }
    }

    /// Check if file has been modified since last indexing
    fn needs_reindex(&self) -> bool {
        if !self.indexed {
            return true;
        }

        if let Ok(metadata) = std::fs::metadata(&self.file_path) {
            if let Ok(modified) = metadata.modified() {
                return self.last_modified.map_or(true, |last| modified > last);
            }
        }

        false
    }

    /// Index the file to build line position cache (for efficient seeking)
    fn ensure_indexed(&mut self) -> Result<()> {
        if !self.needs_reindex() {
            return Ok(());
        }

        if !self.file_path.exists() {
            self.line_positions.clear();
            self.indexed = true;
            self.last_modified = None;
            return Ok(());
        }

        let file = File::open(&self.file_path)
            .with_context(|| format!("Failed to open log file: {}", self.file_path.display()))?;

        // Update last modified time
        if let Ok(metadata) = file.metadata() {
            if let Ok(modified) = metadata.modified() {
                self.last_modified = Some(modified);
            }
        }

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

    /// Read all log entries from the file (used for building filtered views)
    pub fn read_all_entries(&mut self) -> Result<Vec<LogEntry>> {
        if !self.file_path.exists() {
            return Ok(Vec::new());
        }

        self.ensure_indexed()?;

        let mut entries = Vec::new();
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<LogEntry>(&line) {
                Ok(entry) => {
                    entries.push(entry);
                }
                Err(e) => {
                    debug!(
                        "Failed to parse log line {}: {} - Error: {}",
                        line_num, line, e
                    );
                }
            }
        }

        Ok(entries)
    }

    /// Read new entries since the last read (for incremental updates)
    pub fn read_new_entries(&mut self, since_line: usize) -> Result<Vec<LogEntry>> {
        self.ensure_indexed()?;
        let total_lines = self.line_positions.len();

        if since_line >= total_lines {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        let mut file = File::open(&self.file_path)?;
        let start_pos = self.line_positions[since_line];
        file.seek(std::io::SeekFrom::Start(start_pos))?;

        let reader = BufReader::new(file);
        let lines_to_read = total_lines - since_line;

        for (i, line) in reader.lines().take(lines_to_read).enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<LogEntry>(&line) {
                Ok(entry) => {
                    entries.push(entry);
                }
                Err(e) => {
                    debug!(
                        "Failed to parse log line {}: {} - Error: {}",
                        since_line + i,
                        line,
                        e
                    );
                }
            }
        }

        Ok(entries)
    }

    /// Check if file has been updated
    pub fn has_updates(&mut self) -> bool {
        self.needs_reindex()
    }
}

/// Request sent from UI thread to background log thread
#[derive(Debug, Clone)]
pub enum LogRequest {
    /// Update filters and request refresh
    UpdateFilters {
        level: LogLevel,
        node_filter: Option<NodeId>,
    },
    /// Update viewport size when UI resizes
    UpdateViewportSize {
        viewport_size: usize,
    },
    /// Scroll commands
    ScrollUp {
        amount: usize,
    },
    ScrollDown {
        amount: usize,
    },
    ScrollToTop,
    ScrollToBottom,
    /// Update session directory
    UpdateSession {
        session_dir: PathBuf,
    },
    /// Request initial data load
    RequestInitialData,
    /// Shutdown the background thread
    Shutdown,
}

/// Response sent from background log thread to UI thread
#[derive(Debug, Clone)]
pub enum LogResponse {
    /// Full viewport update with logs for display
    ViewportUpdate {
        logs: Vec<LogEntry>,
        total_filtered_lines: usize,
        scroll_position: usize,
        viewport_size: usize,
    },
    /// New logs have been detected (triggers a refresh)
    NewLogsDetected,
    /// Error occurred
    Error { message: String },
}

/// Filtered log state maintained by background thread
struct FilteredLogState {
    /// All filtered logs in chronological order
    filtered_logs: Vec<LogEntry>,
    /// Current filters
    level_filter: LogLevel,
    node_filter: Option<NodeId>,
    /// Current viewport position and size
    scroll_position: usize,
    viewport_size: usize,
    /// Whether auto-scroll is enabled (explicitly tracks user intent)
    auto_scroll_enabled: bool,
}

impl FilteredLogState {
    fn new() -> Self {
        Self {
            filtered_logs: Vec::new(),
            level_filter: LogLevel::Info,
            node_filter: None,
            scroll_position: 0,
            viewport_size: 50,
            auto_scroll_enabled: true, // Start with auto-scroll enabled
        }
    }

    /// Apply current filters to a list of log entries
    fn apply_filters(&self, entries: &[LogEntry]) -> Vec<LogEntry> {
        entries
            .iter()
            .filter(|entry| {
                // Apply level filter
                let passes_level = Self::should_show_level(entry.level, self.level_filter);

                // Apply node filter (only when reading from all.log, handled by caller)
                let passes_node = self
                    .node_filter
                    .is_none_or(|filter_node| entry.node_id == filter_node);

                passes_level && passes_node
            })
            .cloned()
            .collect()
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

    /// Update filters and rebuild filtered logs
    fn update_filters(
        &mut self,
        all_logs: &[LogEntry],
        level: LogLevel,
        node_filter: Option<NodeId>,
    ) {
        // Check if filters are actually changing
        let filters_changed = self.level_filter != level || self.node_filter != node_filter;

        self.level_filter = level;
        self.node_filter = node_filter;
        self.rebuild_filtered_logs(all_logs);

        // ONLY reset scroll position when filters actually changed
        // This preserves user's scroll position and auto-scroll preference during file updates
        if filters_changed {
            // Reset scroll to bottom when filters change and enable auto-scroll
            self.scroll_position = 0;
            self.auto_scroll_enabled = true;
        }
        // If filters didn't change, preserve user's current scroll position and auto-scroll preference
    }

    /// Rebuild the filtered logs from all logs
    fn rebuild_filtered_logs(&mut self, all_logs: &[LogEntry]) {
        let old_filtered_count = self.filtered_logs.len();
        self.filtered_logs = self.apply_filters(all_logs);
        let new_filtered_count = self.filtered_logs.len();

        // If auto-scroll is disabled and we have more logs than before,
        // adjust scroll position to maintain same visual content
        if !self.auto_scroll_enabled && new_filtered_count > old_filtered_count {
            let new_logs_added = new_filtered_count - old_filtered_count;
            self.scroll_position += new_logs_added;
        }

        // CRITICAL: Validate scroll position after rebuilding filtered logs
        // If user had scrolled up but scroll position is now invalid, clamp it
        let max_scroll = self.max_scroll_position();
        if self.scroll_position > max_scroll {
            self.scroll_position = max_scroll;
            // Don't change auto_scroll_enabled - preserve user's intent
        }
    }

    /// Add new log entries and apply filters
    fn add_new_logs(&mut self, new_logs: &[LogEntry]) {
        let new_filtered = self.apply_filters(new_logs);
        let new_filtered_count = new_filtered.len();

        self.filtered_logs.extend(new_filtered);

        // If auto-scroll is disabled, adjust scroll position to maintain same visual content
        if !self.auto_scroll_enabled && new_filtered_count > 0 {
            // Increase scroll position by number of new filtered logs to keep same content visible
            self.scroll_position += new_filtered_count;
        }

        // Validate scroll position in case extending logs changes max scroll
        let max_scroll = self.max_scroll_position();
        if self.scroll_position > max_scroll {
            self.scroll_position = max_scroll;
            // Don't change auto_scroll_enabled - preserve user's intent
        }
    }

    /// Update viewport size only (not scroll position)
    const fn update_viewport_size(&mut self, viewport_size: usize) {
        self.viewport_size = viewport_size;
    }

    /// Get maximum scroll position based on current filtered logs and viewport size
    const fn max_scroll_position(&self) -> usize {
        if self.filtered_logs.len() <= self.viewport_size {
            0
        } else {
            self.filtered_logs.len() - self.viewport_size
        }
    }

    /// Check if auto-scroll is enabled and we should follow new logs
    const fn should_auto_scroll(&self) -> bool {
        self.auto_scroll_enabled
    }

    /// Scroll up (towards older logs) - disables auto-scroll
    fn scroll_up(&mut self, amount: usize) {
        let max_scroll = self.max_scroll_position();
        if max_scroll > 0 {
            self.scroll_position = (self.scroll_position + amount).min(max_scroll);
            // User scrolled up manually - disable auto-scroll
            self.auto_scroll_enabled = false;
        }
    }

    /// Scroll down (towards newer logs) - re-enables auto-scroll if at bottom
    fn scroll_down(&mut self, amount: usize) {
        if self.scroll_position >= amount {
            self.scroll_position -= amount;
        } else {
            self.scroll_position = 0;
        }

        // Re-enable auto-scroll if user scrolled back to bottom
        if self.scroll_position == 0 {
            self.auto_scroll_enabled = true;
        }
    }

    /// Scroll to top (oldest logs) - disables auto-scroll
    fn scroll_to_top(&mut self) {
        let max_scroll = self.max_scroll_position();
        if max_scroll > 0 {
            self.scroll_position = max_scroll;
        }
        // User explicitly went to top - disable auto-scroll
        self.auto_scroll_enabled = false;
    }

    /// Scroll to bottom (newest logs) - enables auto-scroll
    fn scroll_to_bottom(&mut self) {
        self.scroll_position = 0;
        // User explicitly went to bottom - enable auto-scroll
        self.auto_scroll_enabled = true;
    }

    /// Get the current viewport slice of logs
    fn get_viewport_logs(&self) -> Vec<LogEntry> {
        let total_logs = self.filtered_logs.len();
        let viewport_size = self.viewport_size;

        if total_logs <= viewport_size {
            // All logs fit in viewport
            self.filtered_logs.clone()
        } else {
            // Calculate viewport based on scroll position
            // scroll_position 0 = show newest (bottom), higher = show older (top)
            let start_from_end = self.scroll_position;
            let end_index = total_logs.saturating_sub(start_from_end);
            let start_index = end_index.saturating_sub(viewport_size);

            self.filtered_logs[start_index..end_index].to_vec()
        }
    }

    /// Get total number of filtered lines
    fn total_filtered_lines(&self) -> usize {
        self.filtered_logs.len()
    }
}

/// Background log monitoring and filtering system
struct LogWorker {
    session_dir: PathBuf,
    file_readers: HashMap<PathBuf, LogFileReader>,
    state: FilteredLogState,
    last_check: Instant,
    last_line_counts: HashMap<PathBuf, usize>,
}

impl LogWorker {
    fn new(session_dir: PathBuf) -> Self {
        Self {
            session_dir,
            file_readers: HashMap::new(),
            state: FilteredLogState::new(),
            last_check: Instant::now(),
            last_line_counts: HashMap::new(),
        }
    }

    fn get_log_file_path(&self, node_filter: Option<NodeId>) -> PathBuf {
        node_filter.map_or_else(
            || self.session_dir.join("all.log"),
            |node_id| {
                if node_id == MAIN_THREAD_NODE_ID {
                    self.session_dir.join("debug.log")
                } else {
                    self.session_dir
                        .join(format!("node_{}.log", node_id.execution_order()))
                }
            },
        )
    }

    fn get_or_create_file_reader(&mut self, file_path: PathBuf) -> &mut LogFileReader {
        self.file_readers
            .entry(file_path.clone())
            .or_insert_with(|| LogFileReader::new(file_path))
    }

    fn rebuild_complete_state(&mut self) -> Result<bool> {
        let file_path = self.get_log_file_path(self.state.node_filter);

        // Read all entries from the appropriate file
        let (all_logs, current_line_count) = {
            let reader = self.get_or_create_file_reader(file_path.clone());
            let logs = reader.read_all_entries()?;
            let line_count = reader.total_lines()?;
            (logs, line_count)
        };

        // For node-specific files, don't apply node filtering again
        let should_apply_node_filter = file_path
            .file_name()
            .map_or(false, |name| name == "all.log");

        if should_apply_node_filter {
            self.state.rebuild_filtered_logs(&all_logs);
        } else {
            // For node-specific files, only apply level filtering
            let old_node_filter = self.state.node_filter;
            self.state.node_filter = None; // Temporarily disable node filtering
            self.state.rebuild_filtered_logs(&all_logs);
            self.state.node_filter = old_node_filter; // Restore node filter for UI state
        }

        // Update line count
        let had_updates =
            self.last_line_counts.get(&file_path).copied().unwrap_or(0) != current_line_count;
        self.last_line_counts.insert(file_path, current_line_count);

        Ok(had_updates)
    }

    fn check_for_updates(&mut self) -> Result<bool> {
        let file_path = self.get_log_file_path(self.state.node_filter);

        let has_updates = {
            let reader = self.get_or_create_file_reader(file_path.clone());
            reader.has_updates()
        };

        if has_updates {
            // File has been updated, rebuild complete state
            self.rebuild_complete_state()
        } else {
            Ok(false)
        }
    }

    fn handle_request(&mut self, request: LogRequest) -> Result<Option<LogResponse>> {
        match request {
            LogRequest::UpdateFilters { level, node_filter } => {
                self.state.update_filters(&[], level, node_filter);
                self.rebuild_complete_state()?;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::UpdateViewportSize { viewport_size } => {
                self.state.update_viewport_size(viewport_size);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::ScrollUp { amount } => {
                self.state.scroll_up(amount);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::ScrollDown { amount } => {
                self.state.scroll_down(amount);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::ScrollToTop => {
                self.state.scroll_to_top();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::ScrollToBottom => {
                self.state.scroll_to_bottom();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::UpdateSession { session_dir } => {
                self.session_dir = session_dir;
                self.file_readers.clear();
                self.last_line_counts.clear();
                self.state = FilteredLogState::new();
                Ok(None)
            }
            LogRequest::RequestInitialData => {
                self.rebuild_complete_state()?;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                    viewport_size: self.state.viewport_size,
                }))
            }
            LogRequest::Shutdown => Ok(None), // Handled by main loop
        }
    }

    fn run(mut self, receiver: mpsc::Receiver<LogRequest>, sender: mpsc::Sender<LogResponse>) {
        loop {
            // Handle requests with a short timeout
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(LogRequest::Shutdown) => break,
                Ok(request) => {
                    match self.handle_request(request) {
                        Ok(Some(response)) => {
                            if sender.send(response).is_err() {
                                break; // UI thread disconnected
                            }
                        }
                        Ok(None) => {} // No response needed
                        Err(e) => {
                            let error_response = LogResponse::Error {
                                message: format!("Worker error: {}", e),
                            };
                            if sender.send(error_response).is_err() {
                                break;
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Check for file updates during timeout
                    if self.last_check.elapsed() >= Duration::from_millis(100) {
                        self.last_check = Instant::now();

                        match self.check_for_updates() {
                            Ok(true) => {
                                // File was updated, auto-scroll only if explicitly enabled
                                if self.state.should_auto_scroll() {
                                    // User wants to follow new logs - move to bottom WITHOUT changing auto-scroll state
                                    self.state.scroll_position = 0; // Move to bottom but don't call scroll_to_bottom()
                                }

                                // Always send viewport update with current position (whether auto-scroll or not)
                                // This ensures UI gets new logs but scroll position is preserved
                                let update = LogResponse::ViewportUpdate {
                                    logs: self.state.get_viewport_logs(),
                                    total_filtered_lines: self.state.total_filtered_lines(),
                                    scroll_position: self.state.scroll_position,
                                    viewport_size: self.state.viewport_size,
                                };
                                if sender.send(update).is_err() {
                                    break;
                                }
                            }
                            Ok(false) => {} // No updates
                            Err(e) => {
                                let error_response = LogResponse::Error {
                                    message: format!("Update check error: {}", e),
                                };
                                if sender.send(error_response).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

/// High-performance log reader that uses a background thread for push-based log updates
pub struct LogReader {
    /// Channel to send requests to background thread
    request_sender: mpsc::Sender<LogRequest>,
    /// Channel to receive responses from background thread
    response_receiver: mpsc::Receiver<LogResponse>,
    /// Current filters (cached for UI queries)
    current_level_filter: Arc<RwLock<LogLevel>>,
    current_node_filter: Arc<RwLock<Option<NodeId>>>,
    /// Background thread handle
    _worker_thread: thread::JoinHandle<()>,
}

impl LogReader {
    /// Create a new log reader with session directory and start background thread
    pub fn new(session_dir: PathBuf) -> Self {
        let (request_sender, request_receiver) = mpsc::channel();
        let (response_sender, response_receiver) = mpsc::channel();

        let worker = LogWorker::new(session_dir);
        let worker_thread = thread::spawn(move || {
            worker.run(request_receiver, response_sender);
        });

        Self {
            request_sender,
            response_receiver,
            current_level_filter: Arc::new(RwLock::new(LogLevel::Info)),
            current_node_filter: Arc::new(RwLock::new(None)),
            _worker_thread: worker_thread,
        }
    }

    /// Set the current log level filter
    pub fn set_level_filter(&self, level: LogLevel) {
        *self.current_level_filter.write() = level;
        let _ = self.request_sender.send(LogRequest::UpdateFilters {
            level,
            node_filter: *self.current_node_filter.read(),
        });
    }

    /// Get the current log level filter
    pub fn get_level_filter(&self) -> LogLevel {
        *self.current_level_filter.read()
    }

    /// Set the current node filter
    pub fn set_node_filter(&self, node_id: Option<NodeId>) {
        *self.current_node_filter.write() = node_id;
        let _ = self.request_sender.send(LogRequest::UpdateFilters {
            level: *self.current_level_filter.read(),
            node_filter: node_id,
        });
    }

    /// Get the current node filter
    pub fn get_node_filter(&self) -> Option<NodeId> {
        *self.current_node_filter.read()
    }

    /// Update viewport size (when UI is resized)
    pub fn update_viewport_size(&self, viewport_size: usize) {
        let _ = self
            .request_sender
            .send(LogRequest::UpdateViewportSize { viewport_size });
    }

    /// Scroll up by amount
    pub fn scroll_up(&self, amount: usize) {
        let _ = self.request_sender.send(LogRequest::ScrollUp { amount });
    }

    /// Scroll down by amount
    pub fn scroll_down(&self, amount: usize) {
        let _ = self.request_sender.send(LogRequest::ScrollDown { amount });
    }

    /// Scroll to top (oldest logs)
    pub fn scroll_to_top(&self) {
        let _ = self.request_sender.send(LogRequest::ScrollToTop);
    }

    /// Scroll to bottom (newest logs)
    pub fn scroll_to_bottom(&self) {
        let _ = self.request_sender.send(LogRequest::ScrollToBottom);
    }

    /// Request initial data load
    pub fn request_initial_data(&self) {
        let _ = self.request_sender.send(LogRequest::RequestInitialData);
    }

    /// Update session directory
    pub fn update_session(&self, session_dir: PathBuf) {
        let _ = self
            .request_sender
            .send(LogRequest::UpdateSession { session_dir });
    }

    /// Get any available responses (non-blocking)
    pub fn try_get_response(&self) -> Option<LogResponse> {
        self.response_receiver.try_recv().ok()
    }

    /// Get filtered logs from disk (blocking operation) - kept for backward compatibility
    pub fn get_filtered_logs_blocking(&self, timeout: Duration) -> Vec<LogEntry> {
        // Send request for initial data
        let _ = self.request_sender.send(LogRequest::RequestInitialData);

        // Wait for response
        if let Ok(response) = self.response_receiver.recv_timeout(timeout) {
            match response {
                LogResponse::ViewportUpdate { logs, .. } => logs,
                LogResponse::Error { message } => {
                    debug!("Error getting logs: {}", message);
                    Vec::new()
                }
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        }
    }
}

impl Drop for LogReader {
    fn drop(&mut self) {
        let _ = self.request_sender.send(LogRequest::Shutdown);
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
    fn test_file_reader_basic() {
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

        // Test reading with LogFileReader
        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        let all_log_path = session_dir.join("all.log");
        let mut reader = LogFileReader::new(all_log_path);
        let logs = reader.read_all_entries().expect("Failed to read logs");

        assert_eq!(logs.len(), 2);
        assert!(
            logs.iter()
                .any(|log| log.message.contains("Test log message"))
        );
        assert!(logs.iter().any(|log| log.message.contains("Error message")));
    }

    #[test]
    fn test_log_reader_push_system() {
        reset_node_id_state();
        let (config, _temp_dir) = create_test_config();
        let writer = LogWriter::with_config(config).expect("Failed to create writer");

        // Write a test entry
        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test LogReader with push system
        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        let reader = LogReader::new(session_dir);

        // Request initial data
        reader.request_initial_data();

        // Wait a bit for background thread to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Check for response
        if let Some(response) = reader.try_get_response() {
            match response {
                LogResponse::ViewportUpdate { logs, .. } => {
                    assert!(!logs.is_empty());
                    assert!(logs[0].message.contains("Test log message"));
                }
                _ => panic!("Expected ViewportUpdate response"),
            }
        } else {
            panic!("No response received from background thread");
        }
    }

    #[test]
    fn test_level_filtering_push() {
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

        // Wait for filter update to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Check for response
        if let Some(response) = reader.try_get_response() {
            match response {
                LogResponse::ViewportUpdate { logs, .. } => {
                    // Should only show error messages when filtering for Error level
                    assert!(logs.iter().all(|log| matches!(log.level, LogLevel::Error)));
                    assert!(logs.iter().any(|log| log.message.contains("Error message")));
                }
                _ => panic!("Expected ViewportUpdate response"),
            }
        } else {
            panic!("No response received from background thread");
        }
    }

    #[test]
    fn test_auto_scroll_preservation_after_new_logs() {
        reset_node_id_state();

        // Create initial logs
        let mut initial_logs = Vec::new();
        for i in 0..20 {
            let mut entry = create_test_log_entry();
            entry.message = format!("Initial log {}", i);
            initial_logs.push(entry);
        }

        // Create FilteredLogState and set viewport size
        let mut state = FilteredLogState::new();
        state.update_viewport_size(10); // Show 10 logs at a time

        // Load initial logs - should start at bottom with auto-scroll enabled
        state.rebuild_filtered_logs(&initial_logs);

        println!("Initial state:");
        println!("  scroll_position: {}", state.scroll_position);
        println!("  auto_scroll_enabled: {}", state.auto_scroll_enabled);
        println!("  total_filtered_lines: {}", state.total_filtered_lines());
        println!("  max_scroll_position: {}", state.max_scroll_position());

        // Verify we start at bottom with auto-scroll enabled
        assert_eq!(state.scroll_position, 0, "Should start at bottom");
        assert!(
            state.auto_scroll_enabled,
            "Auto-scroll should be enabled initially"
        );

        // User scrolls up 5 positions - this should disable auto-scroll
        state.scroll_up(5);

        println!("\nAfter user scrolls up 5:");
        println!("  scroll_position: {}", state.scroll_position);
        println!("  auto_scroll_enabled: {}", state.auto_scroll_enabled);

        // Verify scroll position and auto-scroll disabled
        assert_eq!(state.scroll_position, 5, "Should be scrolled up by 5");
        assert!(
            !state.auto_scroll_enabled,
            "Auto-scroll should be disabled after user scrolls up"
        );

        // Create new logs (simulating file updates)
        let mut new_logs = Vec::new();
        for i in 20..25 {
            let mut entry = create_test_log_entry();
            entry.message = format!("New log {i}");
            new_logs.push(entry);
        }

        // Add new logs - scroll position should be adjusted to maintain same visual content
        state.add_new_logs(&new_logs);

        println!("\nAfter new logs added:");
        println!("  scroll_position: {}", state.scroll_position);
        println!("  auto_scroll_enabled: {}", state.auto_scroll_enabled);
        println!("  total_filtered_lines: {}", state.total_filtered_lines());
        println!("  max_scroll_position: {}", state.max_scroll_position());

        // CRITICAL TEST: Scroll position should be adjusted to maintain same visual content
        // We had 5 new logs added, so scroll position should increase by 5 to show same content
        assert_eq!(
            state.scroll_position, 10,
            "Scroll position should be adjusted by number of new logs to maintain same visual content"
        );
        assert!(
            !state.auto_scroll_enabled,
            "Auto-scroll should remain disabled after new logs"
        );

        // Also test rebuild_filtered_logs (which happens during file updates)
        // This simulates adding 1 more log via rebuild rather than add_new_logs
        let mut all_logs = initial_logs;
        all_logs.extend(new_logs);
        all_logs.extend(vec![{
            let mut entry = create_test_log_entry();
            entry.message = "Even newer log".to_string();
            entry
        }]);

        // Simulate calling update_filters with same filters (no actual filter change)
        let old_level = state.level_filter;
        let old_node = state.node_filter;
        state.update_filters(&all_logs, old_level, old_node);

        println!("\nAfter update_filters with same filters:");
        println!("  scroll_position: {}", state.scroll_position);
        println!("  auto_scroll_enabled: {}", state.auto_scroll_enabled);

        // CRITICAL TEST: When filters don't actually change, scroll position should be adjusted for new logs
        // We added 1 more log ("Even newer log"), so scroll position should increase by 1 more
        assert_eq!(
            state.scroll_position, 11,
            "Scroll position should be adjusted for additional new logs when filters don't change"
        );
        assert!(
            !state.auto_scroll_enabled,
            "Auto-scroll should remain disabled when filters don't change"
        );

        // But if filters DO change, it should reset to bottom
        state.update_filters(&all_logs, LogLevel::Error, old_node);

        println!("\nAfter update_filters with different level filter:");
        println!("  scroll_position: {}", state.scroll_position);
        println!("  auto_scroll_enabled: {}", state.auto_scroll_enabled);

        // When filters actually change, should reset to bottom and enable auto-scroll
        assert_eq!(
            state.scroll_position, 0,
            "Should reset to bottom when filters actually change"
        );
        assert!(
            state.auto_scroll_enabled,
            "Should re-enable auto-scroll when filters actually change"
        );
    }
}
