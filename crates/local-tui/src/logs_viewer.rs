//! SQL-based log viewing and filtering system

use crate::logs_writer::LogWriter;
use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, TuiNodeId};

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, params_from_iter, types::Value};
use tracing::debug;

/// Request sent from UI thread to background log thread
#[derive(Debug, Clone)]
pub enum LogRequest {
    /// Update filters and request refresh
    UpdateFilters {
        level: LogLevel,
        node_filter: Option<TuiNodeId>,
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

/// SQL query builder for log filtering
struct LogQuery {
    level_filter: Option<LogLevel>,
    node_filter: Option<TuiNodeId>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl LogQuery {
    const fn new() -> Self {
        Self {
            level_filter: None,
            node_filter: None,
            limit: None,
            offset: None,
        }
    }

    const fn with_level(mut self, level: LogLevel) -> Self {
        self.level_filter = Some(level);
        self
    }

    const fn with_node(mut self, node_id: Option<TuiNodeId>) -> Self {
        self.node_filter = node_id;
        self
    }

    const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    const fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Build SQL query and parameters
    fn build_sql(&self) -> (String, Vec<Value>) {
        let mut sql = String::from(
            "SELECT id, timestamp, level, node_id, message, target, execution_order FROM logs",
        );
        let mut params = Vec::new();
        let mut conditions = Vec::new();

        // Level filter
        if let Some(level) = &self.level_filter {
            let levels = match level {
                LogLevel::Error => vec!["ERROR"],
                LogLevel::Warn => vec!["ERROR", "WARN"],
                LogLevel::Info => vec!["ERROR", "WARN", "INFO"],
                LogLevel::Debug => vec!["ERROR", "WARN", "INFO", "DEBUG"],
                LogLevel::Trace => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
            };
            let placeholders = levels.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            conditions.push(format!("level IN ({placeholders})"));
            for level_str in levels {
                params.push(Value::Text(level_str.to_string()));
            }
        }

        // Node filter
        if let Some(node_id) = &self.node_filter {
            conditions.push("node_id = ?".to_string());
            let node_id_str = if *node_id == crate::messages::MAIN_THREAD_NODE_ID {
                "main".to_string()
            } else {
                format!("{}-{}", node_id.execution_order(), node_id.pokemon_name())
            };
            params.push(Value::Text(node_id_str));
        }

        // Add WHERE clause if needed
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Order by timestamp descending (newest first)
        sql.push_str(" ORDER BY timestamp DESC, id DESC");

        // Limit and offset
        if let Some(limit) = self.limit {
            sql.push_str(" LIMIT ?");
            params.push(Value::Integer(i64::try_from(limit).unwrap_or(i64::MAX)));
        }

        if let Some(offset) = self.offset {
            sql.push_str(" OFFSET ?");
            params.push(Value::Integer(i64::try_from(offset).unwrap_or(i64::MAX)));
        }

        (sql, params)
    }

    /// Build count query
    fn build_count_sql(&self) -> (String, Vec<Value>) {
        let mut sql = String::from("SELECT COUNT(*) FROM logs");
        let mut params = Vec::new();
        let mut conditions = Vec::new();

        // Level filter
        if let Some(level) = &self.level_filter {
            let levels = match level {
                LogLevel::Error => vec!["ERROR"],
                LogLevel::Warn => vec!["ERROR", "WARN"],
                LogLevel::Info => vec!["ERROR", "WARN", "INFO"],
                LogLevel::Debug => vec!["ERROR", "WARN", "INFO", "DEBUG"],
                LogLevel::Trace => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
            };
            let placeholders = levels.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            conditions.push(format!("level IN ({placeholders})"));
            for level_str in levels {
                params.push(Value::Text(level_str.to_string()));
            }
        }

        // Node filter
        if let Some(node_id) = &self.node_filter {
            conditions.push("node_id = ?".to_string());
            let node_id_str = if *node_id == crate::messages::MAIN_THREAD_NODE_ID {
                "main".to_string()
            } else {
                format!("{}-{}", node_id.execution_order(), node_id.pokemon_name())
            };
            params.push(Value::Text(node_id_str));
        }

        // Add WHERE clause if needed
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        (sql, params)
    }
}

/// Filtered log state maintained by background thread
struct FilteredLogState {
    /// Current filters
    level_filter: LogLevel,
    node_filter: Option<TuiNodeId>,
    /// Current viewport position and size
    scroll_position: usize,
    viewport_size: usize,
    /// Total filtered count (cached)
    total_filtered_count: usize,
    /// Whether auto-scroll is enabled
    auto_scroll_enabled: bool,
    /// Anchor log ID - the ID of the first log in the viewport when not auto-scrolling
    viewport_anchor_id: Option<i64>,
}

impl FilteredLogState {
    const fn new() -> Self {
        Self {
            level_filter: LogLevel::Info,
            node_filter: None,
            scroll_position: 0,
            viewport_size: 50,
            total_filtered_count: 0,
            auto_scroll_enabled: true,
            viewport_anchor_id: None,
        }
    }

    /// Get maximum scroll position based on current filtered logs and viewport size
    const fn max_scroll_position(&self) -> usize {
        self.total_filtered_count.saturating_sub(self.viewport_size)
    }

    /// Scroll up (towards older logs) - disables auto-scroll
    fn scroll_up(&mut self, amount: usize) {
        let max_scroll = self.max_scroll_position();
        if max_scroll > 0 {
            self.scroll_position = (self.scroll_position + amount).min(max_scroll);
            // User scrolled up manually - disable auto-scroll
            self.auto_scroll_enabled = false;
            // Clear anchor so it gets recalculated on next viewport fetch
            self.viewport_anchor_id = None;
        }
    }

    fn scroll_up_by_viewport_size(&mut self) {
        self.scroll_up(self.viewport_size);
    }

    /// Scroll down (towards newer logs) - re-enables auto-scroll if at bottom
    #[allow(clippy::missing_const_for_fn)]
    fn scroll_down(&mut self, amount: usize) {
        if self.scroll_position >= amount {
            self.scroll_position -= amount;
        } else {
            self.scroll_position = 0;
        }

        // Re-enable auto-scroll if user scrolled back to bottom
        if self.scroll_position == 0 {
            self.auto_scroll_enabled = true;
        } else {
            // Still scrolled, clear anchor so it gets recalculated
        }
        self.viewport_anchor_id = None;
    }

    fn scroll_down_by_viewport_size(&mut self) {
        self.scroll_down(self.viewport_size);
    }

    /// Scroll to top (oldest logs) - disables auto-scroll
    #[allow(clippy::missing_const_for_fn)]
    fn scroll_to_top(&mut self) {
        let max_scroll = self.max_scroll_position();
        if max_scroll > 0 {
            self.scroll_position = max_scroll;
        }
        // User explicitly went to top - disable auto-scroll
        self.auto_scroll_enabled = false;
        self.viewport_anchor_id = None;
    }

    /// Scroll to bottom (newest logs) - enables auto-scroll
    #[allow(clippy::missing_const_for_fn)]
    fn scroll_to_bottom(&mut self) {
        self.scroll_position = 0;
        // User explicitly went to bottom - enable auto-scroll
        self.auto_scroll_enabled = true;
        // Clear anchor when auto-scrolling
        self.viewport_anchor_id = None;
    }
}

/// Background log monitoring and filtering system
struct LogWorker {
    connection: Arc<Mutex<Connection>>,
    state: FilteredLogState,
    last_check: Instant,
    last_log_id: Option<i64>,
}

impl LogWorker {
    fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self {
            connection,
            state: FilteredLogState::new(),
            last_check: Instant::now(),
            last_log_id: None,
        }
    }

    /// Get filtered logs for the current viewport
    #[allow(clippy::significant_drop_in_scrutinee)]
    #[allow(clippy::significant_drop_tightening)]
    fn get_viewport_logs(&mut self) -> Result<Vec<LogEntry>> {
        let query = LogQuery::new()
            .with_level(self.state.level_filter)
            .with_node(self.state.node_filter)
            .limit(self.state.viewport_size)
            .offset(self.state.scroll_position);

        let (sql, params) = query.build_sql();

        let conn = self.connection.lock();
        let mut stmt = conn.prepare(&sql)?;

        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            let timestamp_micros: i64 = row.get(1)?;
            let level_str: String = row.get(2)?;
            let node_id_str: String = row.get(3)?;
            let message: String = row.get(4)?;
            let target: Option<String> = row.get(5)?;

            let timestamp =
                DateTime::from_timestamp_micros(timestamp_micros).unwrap_or_else(Utc::now);

            let level = match level_str.as_str() {
                "ERROR" => LogLevel::Error,
                "WARN" => LogLevel::Warn,
                "DEBUG" => LogLevel::Debug,
                "TRACE" => LogLevel::Trace,
                _ => LogLevel::Info, // Default for unknown levels (including "INFO")
            };

            // Parse node_id from string
            let node_id = if node_id_str == "main" {
                MAIN_THREAD_NODE_ID
            } else {
                // Parse from format like "1-pikachu"
                let parts: Vec<&str> = node_id_str.split('-').collect();
                if parts.len() >= 2 {
                    parts[0]
                        .parse::<u8>()
                        .map_or(MAIN_THREAD_NODE_ID, |execution_order| {
                            let pokemon_id = crate::node_id::pokemon_id_from_name(parts[1]);
                            TuiNodeId::with_values(execution_order, pokemon_id)
                        })
                } else {
                    MAIN_THREAD_NODE_ID
                }
            };

            Ok(LogEntry {
                node_id,
                level,
                message,
                timestamp,
                target,
            })
        })?;

        let mut entries = Vec::new();
        for entry in rows {
            entries.push(entry?);
        }

        // Reverse entries to show oldest first (since we queried newest first)
        entries.reverse();

        // When not auto-scrolling and we don't have an anchor yet, set it to the first visible log
        if !self.state.auto_scroll_enabled
            && self.state.viewport_anchor_id.is_none()
            && !entries.is_empty()
        {
            // Get the ID of the first log in our viewport
            let first_log_query = LogQuery::new()
                .with_level(self.state.level_filter)
                .with_node(self.state.node_filter)
                .limit(1)
                .offset(self.state.scroll_position);

            let (first_sql, first_params) = first_log_query.build_sql();
            let first_sql = first_sql.replace(
                "SELECT id, timestamp, level, node_id, message, target, execution_order",
                "SELECT id",
            );

            let mut first_stmt = conn.prepare(&first_sql)?;
            if let Ok(first_id) = first_stmt
                .query_row(params_from_iter(first_params.iter()), |row| {
                    row.get::<_, i64>(0)
                })
            {
                self.state.viewport_anchor_id = Some(first_id);
            }
        }

        // Track the last log ID we've seen
        if !entries.is_empty() {
            // Get the max ID from the current result set
            let max_id_query = LogQuery::new()
                .with_level(self.state.level_filter)
                .with_node(self.state.node_filter);

            let (sql, params) = max_id_query.build_sql();
            let sql = sql.replace(
                "SELECT id, timestamp, level, node_id, message, target, execution_order",
                "SELECT MAX(id)",
            );

            let mut stmt = conn.prepare(&sql)?;
            let max_id: Option<i64> =
                stmt.query_row(params_from_iter(params.iter()), |row| row.get(0))?;
            self.last_log_id = max_id;
        }

        Ok(entries)
    }

    /// Get total count of filtered logs
    #[allow(clippy::significant_drop_in_scrutinee)]
    #[allow(clippy::significant_drop_tightening)]
    fn get_filtered_count(&self) -> Result<usize> {
        let conn = self.connection.lock();

        let query = LogQuery::new()
            .with_level(self.state.level_filter)
            .with_node(self.state.node_filter);

        let (sql, params) = query.build_count_sql();
        let mut stmt = conn.prepare(&sql)?;
        let count: i64 = stmt.query_row(params_from_iter(params.iter()), |row| row.get(0))?;

        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    /// Check if there are new logs
    #[allow(clippy::significant_drop_in_scrutinee)]
    #[allow(clippy::significant_drop_tightening)]
    fn check_for_updates(&mut self) -> Result<bool> {
        let conn = self.connection.lock();

        // Check if there are any new logs since last check
        let mut stmt = conn.prepare("SELECT MAX(id) FROM logs")?;
        let current_max_id: Option<i64> = stmt.query_row([], |row| row.get(0))?;

        drop(stmt);
        drop(conn);

        // Save the old values before updating
        let old_max_scroll = self.state.max_scroll_position();
        let was_at_top = self.state.scroll_position >= old_max_scroll && old_max_scroll > 0;

        // Update filtered count
        self.state.total_filtered_count = self.get_filtered_count()?;

        // Check if we have new logs
        let has_updates = match (self.last_log_id, current_max_id) {
            (Some(last), Some(current)) => current > last,
            (None, Some(_)) => true,
            _ => false,
        };

        if has_updates {
            if self.state.auto_scroll_enabled {
                // Auto-scroll to bottom to show new logs
                self.state.scroll_position = 0;
            } else if let Some(anchor_id) = self.state.viewport_anchor_id {
                // Maintain position by adjusting scroll offset based on how many logs are before our anchor
                let conn = self.connection.lock();

                // Count logs older than our anchor
                let mut count_sql = String::from("SELECT COUNT(*) FROM logs WHERE id < ?");
                let mut params = vec![Value::Integer(anchor_id)];
                let mut conditions = Vec::new();

                // Add filters
                let levels = match self.state.level_filter {
                    LogLevel::Error => vec!["ERROR"],
                    LogLevel::Warn => vec!["ERROR", "WARN"],
                    LogLevel::Info => vec!["ERROR", "WARN", "INFO"],
                    LogLevel::Debug => vec!["ERROR", "WARN", "INFO", "DEBUG"],
                    LogLevel::Trace => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
                };
                let placeholders = levels.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
                conditions.push(format!("level IN ({placeholders})"));
                for level_str in levels {
                    params.push(Value::Text(level_str.to_string()));
                }

                if let Some(node_id) = &self.state.node_filter {
                    conditions.push("node_id = ?".to_string());
                    let node_id_str = if *node_id == crate::messages::MAIN_THREAD_NODE_ID {
                        "main".to_string()
                    } else {
                        format!("{}-{}", node_id.execution_order(), node_id.pokemon_name())
                    };
                    params.push(Value::Text(node_id_str));
                }

                if !conditions.is_empty() {
                    count_sql.push_str(" AND ");
                    count_sql.push_str(&conditions.join(" AND "));
                }

                let mut stmt = conn.prepare(&count_sql)?;
                let older_count: i64 =
                    stmt.query_row(params_from_iter(params.iter()), |row| row.get(0))?;

                // Update scroll position to maintain view
                self.state.scroll_position = self.state.total_filtered_count.saturating_sub(
                    usize::try_from(older_count).unwrap_or(0) + self.state.viewport_size,
                );
            } else if !self.state.auto_scroll_enabled {
                // We're in manual scroll mode but don't have an anchor yet
                // Special case: if we were at the very top, stay at the top
                if was_at_top {
                    // We were at the very top, stay there by updating to new max_scroll
                    self.state.scroll_position = self.state.max_scroll_position();
                }
                // Otherwise don't change scroll position - it will be recalculated when viewport is fetched
            }
        }

        Ok(has_updates)
    }

    fn handle_request(&mut self, request: &LogRequest) -> Result<Option<LogResponse>> {
        match request {
            LogRequest::UpdateFilters { level, node_filter } => {
                self.state.level_filter = *level;
                self.state.node_filter = *node_filter;
                self.state.total_filtered_count = self.get_filtered_count()?;

                // Reset scroll to bottom when filters change
                self.state.scroll_position = 0;
                self.state.auto_scroll_enabled = true;
                self.state.viewport_anchor_id = None;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::UpdateViewportSize { viewport_size } => {
                self.state.viewport_size = *viewport_size;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollUp { amount } => {
                self.state.scroll_up(*amount);

                // Get the viewport and let it set the anchor if needed
                let logs = self.get_viewport_logs()?;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollUpByViewportSize => {
                self.state.scroll_up_by_viewport_size();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollDown { amount } => {
                self.state.scroll_down(*amount);

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollDownByViewportSize => {
                self.state.scroll_down_by_viewport_size();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollToTop => {
                self.state.scroll_to_top();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::ScrollToBottom => {
                self.state.scroll_to_bottom();

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
                    scroll_position: self.state.scroll_position,
                }))
            }
            LogRequest::RequestInitialData => {
                self.state.total_filtered_count = self.get_filtered_count()?;

                Ok(Some(LogResponse::ViewportUpdate {
                    logs: self.get_viewport_logs()?,
                    total_filtered_lines: self.state.total_filtered_count,
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
                    // Check for updates during timeout
                    // Increased interval to allow time for user scrolling
                    if self.last_check.elapsed() >= Duration::from_millis(250) {
                        self.last_check = Instant::now();

                        match self.check_for_updates() {
                            Ok(true) => {
                                // New logs available
                                if self.state.auto_scroll_enabled {
                                    // Auto-scroll enabled - send full viewport update
                                    match self.get_viewport_logs() {
                                        Ok(logs) => {
                                            let update = LogResponse::ViewportUpdate {
                                                logs,
                                                total_filtered_lines: self
                                                    .state
                                                    .total_filtered_count,
                                                scroll_position: self.state.scroll_position,
                                            };
                                            if sender.send(update).is_err() {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            let error_response = LogResponse::Error {
                                                message: format!(
                                                    "Error getting viewport logs: {e}"
                                                ),
                                            };
                                            if sender.send(error_response).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    // Auto-scroll disabled - still send metadata update so scrollbar updates
                                    // but don't fetch new logs to avoid disrupting the view
                                    let update = LogResponse::ViewportUpdate {
                                        logs: vec![], // Empty logs to signal no content update
                                        total_filtered_lines: self.state.total_filtered_count,
                                        scroll_position: self.state.scroll_position,
                                    };
                                    if sender.send(update).is_err() {
                                        break;
                                    }
                                }
                            }
                            Ok(false) => {} // No updates
                            Err(e) => {
                                debug!("Error checking for updates: {}", e);
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
    current_node_filter: Arc<RwLock<Option<TuiNodeId>>>,
    /// Background thread handle
    _worker_thread: thread::JoinHandle<()>,
}

impl LogReader {
    /// Create a new log reader with shared database connection
    pub fn new_from_writer(log_writer: &LogWriter) -> Self {
        let (request_sender, request_receiver) = mpsc::channel();
        let (response_sender, response_receiver) = mpsc::channel();

        let connection = log_writer.get_connection();
        let worker = LogWorker::new(connection);

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

    fn create_test_config() -> std::path::PathBuf {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        temp_dir.path().to_path_buf()
    }

    fn create_test_log_entry() -> LogEntry {
        LogEntry {
            node_id: TuiNodeId::new(),
            level: LogLevel::Info,
            message: "Test log message".to_string(),
            timestamp: Utc::now(),
            target: Some("test".to_string()),
        }
    }

    fn reset_node_id_state() {
        #[cfg(test)]
        crate::node_id::TuiNodeId::reset_global_state();
    }

    #[test]
    fn test_log_reader_sql() {
        reset_node_id_state();
        let base_dir = create_test_config();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        // Write a test entry
        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Test LogReader with SQL
        let reader = LogReader::new_from_writer(&writer);

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
    fn test_level_filtering_sql() {
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
        let reader = LogReader::new_from_writer(&writer);
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
