//! Log viewing and filtering system for reading logs from disk

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, NodeId};

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use tracing::debug;

/// Information about a single log file
#[derive(Debug, Clone)]
struct LogFileInfo {
    file_path: PathBuf,
    /// Timestamp parsed from filename
    timestamp: DateTime<Utc>,
    /// Total lines in this file
    total_lines: usize,
    /// Line positions for efficient seeking
    line_positions: Vec<u64>,
    /// Whether line positions have been indexed
    indexed: bool,
    /// Last modification time of the file
    last_modified: Option<std::time::SystemTime>,
}

impl LogFileInfo {
    const fn new(file_path: PathBuf, timestamp: DateTime<Utc>) -> Self {
        Self {
            file_path,
            timestamp,
            total_lines: 0,
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

        std::fs::metadata(&self.file_path).map_or(true, |metadata| {
            metadata.modified().map_or(true, |modified| {
                self.last_modified.is_none_or(|last| modified > last)
            })
        })
    }

    /// Index the file to build line position cache
    fn ensure_indexed(&mut self) -> Result<()> {
        if !self.needs_reindex() {
            return Ok(());
        }

        if !self.file_path.exists() {
            self.line_positions.clear();
            self.total_lines = 0;
            self.indexed = true;
            self.last_modified = None;
            return Ok(());
        }

        let file = File::open(&self.file_path)
            .with_context(|| format!("Failed to open log file: {}", self.file_path.display()))?;

        // Update last modified time
        if let Ok(metadata) = file.metadata()
            && let Ok(modified) = metadata.modified()
        {
            self.last_modified = Some(modified);
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
        self.total_lines = self.line_positions.len();
        self.indexed = true;

        Ok(())
    }

    /// Read all log entries from this file
    fn read_all_entries(&mut self) -> Result<Vec<LogEntry>> {
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
                        "Failed to parse log line {} in {}: {} - Error: {}",
                        line_num,
                        self.file_path.display(),
                        line,
                        e
                    );
                }
            }
        }

        Ok(entries)
    }
}

/// Multi-file log reader that seamlessly handles multiple log files in chronological order
pub struct MultiFileReader {
    /// Base directory for logs (either `session_dir` or category subdirectory)
    base_dir: PathBuf,
    /// Log files sorted by timestamp (oldest first)
    files: Vec<LogFileInfo>,
    /// Whether we've discovered files in the directory
    discovered: bool,
    /// Last discovery time
    last_discovery: Option<Instant>,
    /// Discovery interval
    discovery_interval: Duration,
}

impl MultiFileReader {
    /// Create a new multi-file reader
    pub const fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            files: Vec::new(),
            discovered: false,
            last_discovery: None,
            discovery_interval: Duration::from_secs(1), // Check for new files every second
        }
    }

    /// Parse timestamp from filename (e.g., "2024-01-15_14-30-45.log")
    fn parse_timestamp_from_filename(filename: &str) -> Option<DateTime<Utc>> {
        let name = filename.strip_suffix(".log")?;
        DateTime::parse_from_str(&format!("{name} +0000"), "%Y-%m-%d_%H-%M-%S %z")
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// Discover log files in the directory
    fn discover_files(&mut self) -> Result<()> {
        if !self.base_dir.exists() {
            self.files.clear();
            self.discovered = true;
            self.last_discovery = Some(Instant::now());
            return Ok(());
        }

        let mut new_files = Vec::new();

        // Read directory entries
        let entries = fs::read_dir(&self.base_dir)
            .with_context(|| format!("Failed to read directory: {}", self.base_dir.display()))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file()
                && let Some(filename) = path.file_name().and_then(|n| n.to_str())
                && let Some(timestamp) = Self::parse_timestamp_from_filename(filename)
            {
                new_files.push(LogFileInfo::new(path, timestamp));
            }
        }

        // Sort by timestamp (oldest first)
        new_files.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        // Update existing files and add new ones
        let mut updated_files = Vec::new();

        for new_file in new_files {
            if let Some(existing_idx) = self
                .files
                .iter()
                .position(|f| f.file_path == new_file.file_path)
            {
                // Keep existing file info but update metadata
                let mut existing = self.files[existing_idx].clone();
                existing.timestamp = new_file.timestamp; // Update timestamp if needed
                updated_files.push(existing);
            } else {
                // New file
                updated_files.push(new_file);
            }
        }

        self.files = updated_files;
        self.discovered = true;
        self.last_discovery = Some(Instant::now());

        Ok(())
    }

    /// Check if we need to rediscover files
    fn should_rediscover(&self) -> bool {
        if !self.discovered {
            return true;
        }

        self.last_discovery
            .is_none_or(|last_discovery| last_discovery.elapsed() >= self.discovery_interval)
    }

    /// Ensure files are discovered and indexed
    fn ensure_discovered(&mut self) -> Result<()> {
        if self.should_rediscover() {
            self.discover_files()?;
        }

        // Index all files
        for file in &mut self.files {
            file.ensure_indexed()?;
        }

        Ok(())
    }

    /// Get total number of lines across all files
    pub fn total_lines(&mut self) -> Result<usize> {
        self.ensure_discovered()?;
        Ok(self.files.iter().map(|f| f.total_lines).sum())
    }

    /// Read all log entries from all files in chronological order
    pub fn read_all_entries(&mut self) -> Result<Vec<LogEntry>> {
        self.ensure_discovered()?;

        let mut all_entries = Vec::new();

        for file in &mut self.files {
            let mut entries = file.read_all_entries()?;
            all_entries.append(&mut entries);
        }

        Ok(all_entries)
    }

    /// Check if any files have been updated
    pub fn has_updates(&self) -> bool {
        // Check if we should rediscover files
        if self.should_rediscover() {
            return true;
        }

        // Check if any existing files have been updated
        self.files.iter().any(LogFileInfo::needs_reindex)
    }
}

/// File-based log reader for efficient memory usage and line indexing (legacy, now uses `MultiFileReader`)
pub struct LogFileReader {
    multi_reader: MultiFileReader,
}

impl LogFileReader {
    /// Create a new log file reader
    pub const fn new(directory_path: PathBuf) -> Self {
        // The path should now be a directory containing timestamped log files
        Self {
            multi_reader: MultiFileReader::new(directory_path),
        }
    }

    /// Get total number of lines in all files
    pub fn total_lines(&mut self) -> Result<usize> {
        self.multi_reader.total_lines()
    }

    /// Read all log entries from all files
    pub fn read_all_entries(&mut self) -> Result<Vec<LogEntry>> {
        self.multi_reader.read_all_entries()
    }

    /// Check if any files have been updated
    pub fn has_updates(&self) -> bool {
        self.multi_reader.has_updates()
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
    UpdateViewportSize { viewport_size: usize },
    /// Scroll commands
    ScrollUp { amount: usize },
    /// Scroll up by viewport size
    ScrollUpByViewportSize,
    /// Scroll down by amount
    ScrollDown { amount: usize },
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
        logs: Vec<LogEntry>,
        total_filtered_lines: usize,
        scroll_position: usize,
    },
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
    const fn new() -> Self {
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
        // User explicitly went to top - disable auto-scroll
        self.auto_scroll_enabled = false;
    }

    /// Scroll to bottom (newest logs) - enables auto-scroll
    const fn scroll_to_bottom(&mut self) {
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
    const fn total_filtered_lines(&self) -> usize {
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

    fn get_or_create_file_reader(&mut self, directory_path: PathBuf) -> &mut LogFileReader {
        self.file_readers
            .entry(directory_path.clone())
            .or_insert_with(|| LogFileReader::new(directory_path))
    }

    fn rebuild_complete_state(&mut self) -> Result<bool> {
        let directory_path = self.get_log_file_path(self.state.node_filter);

        // Read all entries from the appropriate directory
        let (all_logs, current_line_count) = {
            let reader = self.get_or_create_file_reader(directory_path.clone());
            let logs = reader.read_all_entries()?;
            let line_count = reader.total_lines()?;
            (logs, line_count)
        };

        // For node-specific directories, don't apply node filtering again
        let should_apply_node_filter = directory_path.file_name().is_some_and(|name| name == "all");

        if should_apply_node_filter {
            self.state.rebuild_filtered_logs(&all_logs);
        } else {
            // For node-specific directories, only apply level filtering
            let old_node_filter = self.state.node_filter;
            self.state.node_filter = None; // Temporarily disable node filtering
            self.state.rebuild_filtered_logs(&all_logs);
            self.state.node_filter = old_node_filter; // Restore node filter for UI state
        }

        // Update line count
        let had_updates = self
            .last_line_counts
            .get(&directory_path)
            .copied()
            .unwrap_or(0)
            != current_line_count;
        self.last_line_counts
            .insert(directory_path, current_line_count);

        Ok(had_updates)
    }

    fn check_for_updates(&mut self) -> bool {
        let directory_path = self.get_log_file_path(self.state.node_filter);

        let has_updates = {
            let reader = self.get_or_create_file_reader(directory_path);
            reader.has_updates()
        };

        if has_updates {
            // Directory has been updated, rebuild complete state
            match self.rebuild_complete_state() {
                Ok(updated) => updated,
                Err(e) => {
                    // Log the error but continue monitoring
                    eprintln!("Failed to rebuild log state: {e}");
                    false
                }
            }
        } else {
            false
        }
    }

    fn handle_request(&mut self, request: &LogRequest) -> Result<Option<LogResponse>> {
        match request {
            LogRequest::UpdateFilters { level, node_filter } => {
                self.state.update_filters(&[], *level, *node_filter);
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
                                // User wants to follow new logs - move to bottom WITHOUT changing auto-scroll state
                                self.state.scroll_position = 0; // Move to bottom but don't call scroll_to_bottom()
                            }

                            // Always send viewport update with current position (whether auto-scroll or not)
                            // This ensures UI gets new logs but scroll position is preserved
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
    pub fn try_get_response(&self) -> Option<LogResponse> {
        self.response_receiver.try_recv().ok()
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
    use crate::logs_writer::LogWriter;
    use chrono::Utc;
    use tempfile::TempDir;

    fn create_test_config() -> PathBuf {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        temp_dir.path().to_path_buf()
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
        let base_dir = create_test_config();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        // Write some test entries
        let entry1 = create_test_log_entry();
        let mut entry2 = create_test_log_entry();
        entry2.level = LogLevel::Error;
        entry2.message = "Error message".to_string();

        writer.add_log(entry1);
        writer.add_log(entry2);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test reading with LogFileReader (using new directory structure)
        let session_dir = writer.get_session_dir();
        let all_log_dir = session_dir.join("all");
        let mut reader = LogFileReader::new(all_log_dir);
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
        let base_dir = create_test_config();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        // Write a test entry
        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test LogReader with push system
        let session_dir = writer.get_session_dir();
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
                LogResponse::Error { message } => {
                    panic!("Expected ViewportUpdate response, got error: {message}");
                }
            }
        } else {
            panic!("No response received from background thread");
        }
    }

    #[test]
    fn test_level_filtering_push() {
        reset_node_id_state();
        let base_dir = create_test_config();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

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
        let session_dir = writer.get_session_dir();
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
                LogResponse::Error { message } => {
                    panic!("Expected ViewportUpdate response, got error: {message}");
                }
            }
        } else {
            panic!("No response received from background thread");
        }
    }
}
