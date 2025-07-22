//! New log viewer using proven-logger-viewer for memory-mapped performance

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, TuiNodeId};
use anyhow::{Context, Result};
use parking_lot::RwLock;
use proven_logger::debug;
use proven_logger_viewer::{LogReader as ViewerLogReader, MultiFileReader, ViewerConfigBuilder};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Parse a JSON log entry into TUI `LogEntry`
fn parse_log_entry(line: &str) -> Result<LogEntry> {
    // Try to parse as JSON
    serde_json::from_str::<LogEntry>(line)
        .with_context(|| format!("Failed to parse log entry: {line}"))
}

/// Request sent from UI thread to background log thread
#[derive(Debug, Clone)]
pub enum LogRequest {
    /// Update filters and request refresh
    UpdateFilters {
        /// Log level filter
        level: LogLevel,
        /// Node filter
        node_filter: Option<TuiNodeId>,
    },
    /// Update viewport size when UI resizes
    UpdateViewportSize {
        /// New viewport size
        viewport_size: usize,
    },
    /// Scroll commands
    ScrollUp {
        /// Amount to scroll
        amount: usize,
    },
    /// Scroll up by viewport size
    ScrollUpByViewportSize,
    /// Scroll down by amount
    ScrollDown {
        /// Amount to scroll
        amount: usize,
    },
    /// Scroll down by viewport size
    ScrollDownByViewportSize,
    /// Scroll to top
    ScrollToTop,
    /// Scroll to bottom
    ScrollToBottom,
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
        /// Logs to display
        logs: Vec<LogEntry>,
        /// Total number of filtered lines
        total_filtered_lines: usize,
        /// Current scroll position
        scroll_position: usize,
    },
    /// Error occurred
    Error {
        /// Error message
        message: String,
    },
}

/// Filtered log state maintained by background thread
struct FilteredLogState {
    /// All filtered logs in chronological order
    filtered_logs: Vec<LogEntry>,
    /// Current filters
    level_filter: LogLevel,
    node_filter: Option<TuiNodeId>,
    /// Current viewport position and size
    scroll_position: usize,
    viewport_size: usize,
    /// Whether auto-scroll is enabled
    auto_scroll_enabled: bool,
}

impl FilteredLogState {
    const fn new() -> Self {
        Self {
            filtered_logs: Vec::new(),
            level_filter: LogLevel::Info,
            node_filter: None,
            scroll_position: 0,
            viewport_size: 50,
            auto_scroll_enabled: true,
        }
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

    /// Apply filters to log entries
    fn apply_filters(&self, entries: &[LogEntry]) -> Vec<LogEntry> {
        entries
            .iter()
            .filter(|entry| {
                // Apply level filter
                let passes_level = Self::should_show_level(entry.level, self.level_filter);

                // Apply node filter
                let passes_node = self
                    .node_filter
                    .is_none_or(|filter_node| entry.node_id == filter_node);

                passes_level && passes_node
            })
            .cloned()
            .collect()
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

        // Validate scroll position after rebuilding filtered logs
        let max_scroll = self.max_scroll_position();
        if self.scroll_position > max_scroll {
            self.scroll_position = max_scroll;
        }
    }

    /// Update viewport size only
    const fn update_viewport_size(&mut self, viewport_size: usize) {
        self.viewport_size = viewport_size;
    }

    /// Get maximum scroll position
    const fn max_scroll_position(&self) -> usize {
        if self.filtered_logs.len() <= self.viewport_size {
            0
        } else {
            self.filtered_logs.len() - self.viewport_size
        }
    }

    /// Check if auto-scroll is enabled
    const fn should_auto_scroll(&self) -> bool {
        self.auto_scroll_enabled
    }

    /// Scroll up (towards older logs) - disables auto-scroll
    fn scroll_up(&mut self, amount: usize) {
        let max_scroll = self.max_scroll_position();
        if max_scroll > 0 {
            self.scroll_position = (self.scroll_position + amount).min(max_scroll);
            self.auto_scroll_enabled = false;
        }
    }

    fn scroll_up_by_viewport_size(&mut self) {
        self.scroll_up(self.viewport_size);
    }

    /// Scroll down (towards newer logs) - re-enables auto-scroll if at bottom
    const fn scroll_down(&mut self, amount: usize) {
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

    const fn scroll_down_by_viewport_size(&mut self) {
        self.scroll_down(self.viewport_size);
    }

    /// Scroll to top (oldest logs) - disables auto-scroll
    const fn scroll_to_top(&mut self) {
        let max_scroll = self.max_scroll_position();
        if max_scroll > 0 {
            self.scroll_position = max_scroll;
        }
        self.auto_scroll_enabled = false;
    }

    /// Scroll to bottom (newest logs) - enables auto-scroll
    const fn scroll_to_bottom(&mut self) {
        self.scroll_position = 0;
        self.auto_scroll_enabled = true;
    }

    /// Get the current viewport slice of logs
    fn get_viewport_logs(&self) -> Vec<LogEntry> {
        let total_logs = self.filtered_logs.len();
        let viewport_size = self.viewport_size;

        if total_logs <= viewport_size {
            self.filtered_logs.clone()
        } else {
            let start_from_end = self.scroll_position;
            let end_index = total_logs.saturating_sub(start_from_end);
            let start_index = end_index.saturating_sub(viewport_size);

            self.filtered_logs[start_index..end_index].to_vec()
        }
    }

    /// Get total number of filtered lines
    const fn total_filtered_lines(&self) -> usize {
        self.filtered_logs.len()
    }
}

/// Background log monitoring and filtering system using memory-mapped viewer
struct LogWorker {
    session_dir: PathBuf,
    readers: std::collections::HashMap<PathBuf, Arc<MultiFileReader>>,
    state: FilteredLogState,
    last_check: Instant,
    last_line_counts: std::collections::HashMap<PathBuf, usize>,
}

impl LogWorker {
    fn new(session_dir: PathBuf) -> Self {
        Self {
            session_dir,
            readers: std::collections::HashMap::new(),
            state: FilteredLogState::new(),
            last_check: Instant::now(),
            last_line_counts: std::collections::HashMap::new(),
        }
    }

    fn get_log_file_path(&self, node_filter: Option<TuiNodeId>) -> PathBuf {
        node_filter.map_or_else(
            || self.session_dir.join("all"),
            |node_id| {
                if node_id == MAIN_THREAD_NODE_ID {
                    self.session_dir.join("debug")
                } else {
                    self.session_dir
                        .join(format!("node_{}", node_id.execution_order()))
                }
            },
        )
    }

    fn get_or_create_reader(&mut self, directory_path: &Path) -> Result<&Arc<MultiFileReader>> {
        let directory_pathbuf = directory_path.to_path_buf();
        if !self.readers.contains_key(&directory_pathbuf) {
            let config = ViewerConfigBuilder::new()
                .log_dir(directory_pathbuf.clone())
                .file_pattern("*.log")
                .discovery_interval(Duration::from_secs(1))
                .watch_files(false) // We'll poll manually
                .build();

            let reader = MultiFileReader::new(config)?;
            self.readers
                .insert(directory_pathbuf.clone(), Arc::new(reader));
        }

        Ok(self.readers.get(&directory_pathbuf).unwrap())
    }

    fn rebuild_complete_state(&mut self) -> Result<bool> {
        let directory_path = self.get_log_file_path(self.state.node_filter);

        // Get or create reader for this directory
        let reader = self.get_or_create_reader(&directory_path)?.clone();

        // Rediscover files to ensure readers are up to date
        reader.discover_files()?;

        // Get total line count
        let current_line_count = reader.total_lines()?;

        // Check if we need to read logs
        let had_updates = self
            .last_line_counts
            .get(&directory_path)
            .copied()
            .unwrap_or(0)
            != current_line_count;

        if had_updates || self.state.filtered_logs.is_empty() {
            // Read all entries and parse them
            let mut all_logs = Vec::new();
            let total_lines = reader.total_lines()?;

            for i in 0..total_lines {
                if let Some(line) = reader.read_line(i)? {
                    match parse_log_entry(&line) {
                        Ok(entry) => all_logs.push(entry),
                        Err(e) => {
                            debug!("Failed to parse log line {i}: {e}");
                        }
                    }
                }
            }

            // Apply filters
            let should_apply_node_filter =
                directory_path.file_name().is_some_and(|name| name == "all");

            if should_apply_node_filter {
                self.state.rebuild_filtered_logs(&all_logs);
            } else {
                // For node-specific directories, temporarily disable node filtering
                let old_node_filter = self.state.node_filter;
                self.state.node_filter = None;
                self.state.rebuild_filtered_logs(&all_logs);
                self.state.node_filter = old_node_filter;
            }
        }

        self.last_line_counts
            .insert(directory_path, current_line_count);

        Ok(had_updates)
    }

    fn check_for_updates(&mut self) -> bool {
        let directory_path = self.get_log_file_path(self.state.node_filter);

        // Check if reader exists and has updates
        if let Ok(reader) = self.get_or_create_reader(&directory_path) {
            // Force rediscovery of files
            reader.discover_files().ok();
        }

        // Rebuild state
        match self.rebuild_complete_state() {
            Ok(updated) => updated,
            Err(e) => {
                eprintln!("Failed to rebuild log state: {e}");
                false
            }
        }
    }

    fn handle_request(&mut self, request: &LogRequest) -> Result<Option<LogResponse>> {
        match request {
            LogRequest::UpdateFilters { level, node_filter } => {
                // Update filters but don't clear logs yet
                self.state.level_filter = *level;
                self.state.node_filter = *node_filter;
                // Now rebuild with actual data
                self.rebuild_complete_state()?;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::UpdateViewportSize { viewport_size } => {
                self.state.update_viewport_size(*viewport_size);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollUp { amount } => {
                self.state.scroll_up(*amount);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollUpByViewportSize => {
                self.state.scroll_up_by_viewport_size();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollDown { amount } => {
                self.state.scroll_down(*amount);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollDownByViewportSize => {
                self.state.scroll_down_by_viewport_size();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollToTop => {
                self.state.scroll_to_top();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollToBottom => {
                self.state.scroll_to_bottom();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::RequestInitialData => {
                self.rebuild_complete_state()?;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.state.get_viewport_logs(),
                    total_filtered_lines: self.state.total_filtered_lines(),
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::Shutdown => Ok(None), // Handled by main loop
        }
    }

    fn run(mut self, receiver: &mpsc::Receiver<LogRequest>, sender: &mpsc::Sender<LogResponse>) {
        loop {
            // Handle requests with a short timeout
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(LogRequest::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Ok(request) => {
                    match self.handle_request(&request) {
                        Ok(Some(response)) => {
                            if sender.send(response).is_err() {
                                break; // UI thread disconnected
                            }
                        }
                        Ok(None) => {} // No response needed
                        Err(e) => {
                            let error_response = LogResponse::Error {
                                message: format!("Worker error: {e}"),
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

                        if self.check_for_updates() {
                            // File was updated, auto-scroll only if explicitly enabled
                            if self.state.should_auto_scroll() {
                                self.state.scroll_position = 0;
                            }

                            // Send viewport update
                            let update = LogResponse::ViewportUpdate {
                                logs: self.state.get_viewport_logs(),
                                total_filtered_lines: self.state.total_filtered_lines(),
                                scroll_position: self.state.scroll_position,
                            };
                            if sender.send(update).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// High-performance log reader using memory-mapped files
pub struct LogReader {
    /// Channel to send requests to background thread
    request_sender: mpsc::Sender<LogRequest>,
    /// Channel to receive responses from background thread
    response_receiver: mpsc::Receiver<LogResponse>,
    /// Current filters (cached for UI queries)
    current_level_filter: Arc<RwLock<LogLevel>>,
    current_node_filter: Arc<RwLock<Option<TuiNodeId>>>,
    /// Background thread handle
    _worker_thread: thread::JoinHandle<()>,
}

impl LogReader {
    /// Create a new log reader with session directory and start background thread
    #[must_use]
    pub fn new(session_dir: PathBuf) -> Self {
        let (request_sender, request_receiver) = mpsc::channel();
        let (response_sender, response_receiver) = mpsc::channel();

        let worker = LogWorker::new(session_dir);
        let worker_thread = thread::spawn(move || {
            worker.run(&request_receiver, &response_sender);
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
    #[must_use]
    pub fn get_level_filter(&self) -> LogLevel {
        *self.current_level_filter.read()
    }

    /// Set the current node filter
    pub fn set_node_filter(&self, node_id: Option<TuiNodeId>) {
        *self.current_node_filter.write() = node_id;
        let _ = self.request_sender.send(LogRequest::UpdateFilters {
            level: *self.current_level_filter.read(),
            node_filter: node_id,
        });
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

    /// Scroll up by viewport size
    pub fn scroll_up_by_viewport_size(&self) {
        let _ = self.request_sender.send(LogRequest::ScrollUpByViewportSize);
    }

    /// Scroll down by amount
    pub fn scroll_down(&self, amount: usize) {
        let _ = self.request_sender.send(LogRequest::ScrollDown { amount });
    }

    /// Scroll down by viewport size
    pub fn scroll_down_by_viewport_size(&self) {
        let _ = self
            .request_sender
            .send(LogRequest::ScrollDownByViewportSize);
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

    /// Get any available responses (non-blocking)
    #[must_use]
    pub fn try_get_response(&self) -> Option<LogResponse> {
        self.response_receiver.try_recv().ok()
    }
}

impl Drop for LogReader {
    fn drop(&mut self) {
        let _ = self.request_sender.send(LogRequest::Shutdown);
    }
}
