//! Simplified SQL-based log viewer for local-tui
//!
//! This module provides a much simpler log viewer that queries logs
//! directly from the in-memory SQLite database without any serialization.

use crate::messages::{LogLevel, MAIN_THREAD_NODE_ID, TuiNodeId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use libsql::Connection;
use proven_logger_libsql::{LibsqlSubscriber, LogQuery, LogRow};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::time::interval;
use tracing::Level;

/// Convert TUI log level to logger level
fn tui_level_to_logger_level(level: LogLevel) -> Level {
    match level {
        LogLevel::Error => Level::ERROR,
        LogLevel::Warn => Level::WARN,
        LogLevel::Info => Level::INFO,
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Trace => Level::TRACE,
    }
}

/// Convert logger level to TUI log level
fn logger_level_to_tui_level(level: Level) -> LogLevel {
    match level {
        Level::ERROR => LogLevel::Error,
        Level::WARN => LogLevel::Warn,
        Level::INFO => LogLevel::Info,
        Level::DEBUG => LogLevel::Debug,
        Level::TRACE => LogLevel::Trace,
    }
}

/// Convert node ID for display
fn format_node_id(node_id: &str) -> TuiNodeId {
    if node_id == "main" {
        MAIN_THREAD_NODE_ID
    } else {
        // Parse the node ID as execution order
        node_id
            .parse::<u8>()
            .map(|order| TuiNodeId::with_values(order, order))
            .unwrap_or(MAIN_THREAD_NODE_ID)
    }
}

/// Log viewer state
pub struct LogViewerState {
    /// Current log level filter
    pub level_filter: LogLevel,
    /// Current node filter
    pub node_filter: Option<TuiNodeId>,
    /// Current viewport size
    pub viewport_size: usize,
    /// Current scroll offset
    pub scroll_offset: usize,
    /// Whether auto-scroll is enabled
    pub auto_scroll: bool,
    /// Last seen log ID (for auto-scroll)
    pub last_log_id: Option<i64>,
    /// Cached log count for the current filter
    pub total_logs: usize,
}

impl Default for LogViewerState {
    fn default() -> Self {
        Self {
            level_filter: LogLevel::Info,
            node_filter: None,
            viewport_size: 50,
            scroll_offset: 0,
            auto_scroll: true,
            last_log_id: None,
            total_logs: 0,
        }
    }
}

/// Log entry for display
#[derive(Debug, Clone)]
pub struct DisplayLogEntry {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub node_id: TuiNodeId,
    pub component: String,
    pub target: Option<String>,
    pub message: String,
}

impl From<LogRow> for DisplayLogEntry {
    fn from(row: LogRow) -> Self {
        Self {
            id: row.id,
            timestamp: row.timestamp,
            level: logger_level_to_tui_level(row.level),
            node_id: format_node_id(&row.node_id),
            component: row.component,
            target: row.target,
            message: row.message,
        }
    }
}

/// SQL-based log viewer
pub struct SqlLogViewer {
    /// Connection to the logger database
    connection: Arc<Mutex<Connection>>,
    /// Current state
    state: LogViewerState,
}

impl SqlLogViewer {
    /// Create a new log viewer connected to the subscriber
    pub fn new(subscriber: &LibsqlSubscriber) -> Self {
        Self {
            connection: subscriber.get_connection(),
            state: LogViewerState::default(),
        }
    }

    /// Update filters
    pub fn set_filters(&mut self, level: LogLevel, node_filter: Option<TuiNodeId>) {
        self.state.level_filter = level;
        self.state.node_filter = node_filter;
        // Reset scroll when filters change
        self.state.scroll_offset = 0;
    }

    /// Update viewport size
    pub fn set_viewport_size(&mut self, size: usize) {
        self.state.viewport_size = size;
    }

    /// Enable/disable auto-scroll
    pub fn set_auto_scroll(&mut self, enabled: bool) {
        self.state.auto_scroll = enabled;
    }

    /// Scroll up by amount
    pub fn scroll_up(&mut self, amount: usize) {
        self.state.scroll_offset = self.state.scroll_offset.saturating_sub(amount);
        self.state.auto_scroll = false;
    }

    /// Scroll down by amount
    pub fn scroll_down(&mut self, amount: usize) {
        let max_offset = self
            .state
            .total_logs
            .saturating_sub(self.state.viewport_size);
        self.state.scroll_offset = (self.state.scroll_offset + amount).min(max_offset);

        // Re-enable auto-scroll if we're at the bottom
        if self.state.scroll_offset >= max_offset {
            self.state.auto_scroll = true;
        }
    }

    /// Scroll to top
    pub fn scroll_to_top(&mut self) {
        self.state.scroll_offset = 0;
        self.state.auto_scroll = false;
    }

    /// Scroll to bottom
    pub fn scroll_to_bottom(&mut self) {
        let max_offset = self
            .state
            .total_logs
            .saturating_sub(self.state.viewport_size);
        self.state.scroll_offset = max_offset;
        self.state.auto_scroll = true;
    }

    /// Build query from current state
    fn build_query(&self) -> LogQuery {
        let mut query = LogQuery::new()
            .with_min_level(tui_level_to_logger_level(self.state.level_filter))
            .limit(self.state.viewport_size);

        // Add node filter
        if let Some(node_filter) = &self.state.node_filter {
            let node_id = if *node_filter == MAIN_THREAD_NODE_ID {
                "main".to_string()
            } else {
                node_filter.execution_order().to_string()
            };
            query = query.with_node(node_id);
        }

        // Add offset for scrolling
        if !self.state.auto_scroll {
            query = query.offset(self.state.scroll_offset);
        }

        query
    }

    /// Get logs for display
    pub async fn get_logs(&mut self) -> Result<Vec<DisplayLogEntry>> {
        let conn = self.connection.lock().await;

        // First, get the total count
        let count_query = self.build_count_query();
        let mut count_result = conn.query(&count_query.0, count_query.1).await?;

        if let Some(row) = count_result.next().await? {
            self.state.total_logs = row.get::<i64>(0)? as usize;
        }

        // Build and execute the main query
        let query = self.build_query();
        let (sql, params) = query.build_sql();

        let mut rows = conn.query(&sql, params).await?;
        let mut logs = Vec::new();

        while let Some(row) = rows.next().await? {
            let log_row = LogRow::from_row(row)?;

            // Track last log ID for auto-scroll
            if self.state.auto_scroll && logs.is_empty() {
                self.state.last_log_id = Some(log_row.id);
            }

            logs.push(DisplayLogEntry::from(log_row));
        }

        // Reverse to show newest at bottom
        logs.reverse();

        Ok(logs)
    }

    /// Get new logs for auto-scroll
    pub async fn get_new_logs(&mut self) -> Result<Vec<DisplayLogEntry>> {
        if !self.state.auto_scroll || self.state.last_log_id.is_none() {
            return Ok(Vec::new());
        }

        let conn = self.connection.lock().await;

        // Query for logs newer than last_log_id
        let mut query = LogQuery::new()
            .with_min_level(tui_level_to_logger_level(self.state.level_filter))
            .after_id(self.state.last_log_id.unwrap());

        // Add node filter
        if let Some(node_filter) = &self.state.node_filter {
            let node_id = if *node_filter == MAIN_THREAD_NODE_ID {
                "main".to_string()
            } else {
                node_filter.execution_order().to_string()
            };
            query = query.with_node(node_id);
        }

        let (sql, params) = query.build_sql();
        let mut rows = conn.query(&sql, params).await?;
        let mut new_logs = Vec::new();

        while let Some(row) = rows.next().await? {
            let log_row = LogRow::from_row(row)?;

            // Update last log ID
            if log_row.id > self.state.last_log_id.unwrap_or(0) {
                self.state.last_log_id = Some(log_row.id);
            }

            new_logs.push(DisplayLogEntry::from(log_row));
        }

        // Update total count
        self.state.total_logs += new_logs.len();

        // Reverse to show newest at bottom
        new_logs.reverse();

        Ok(new_logs)
    }

    /// Build count query
    fn build_count_query(&self) -> (String, Vec<libsql::Value>) {
        let mut sql = String::from("SELECT COUNT(*) FROM logs");
        let mut params = Vec::new();
        let mut conditions = Vec::new();

        // Level filter
        let level = tui_level_to_logger_level(self.state.level_filter);
        let levels = match level {
            Level::ERROR => vec!["ERROR"],
            Level::WARN => vec!["ERROR", "WARN"],
            Level::INFO => vec!["ERROR", "WARN", "INFO"],
            Level::DEBUG => vec!["ERROR", "WARN", "INFO", "DEBUG"],
            Level::TRACE => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
        };

        let placeholders = levels.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        conditions.push(format!("level IN ({})", placeholders));
        for level in levels {
            params.push(libsql::Value::Text(level.to_string()));
        }

        // Node filter
        if let Some(node_filter) = &self.state.node_filter {
            let node_id = if *node_filter == MAIN_THREAD_NODE_ID {
                "main".to_string()
            } else {
                node_filter.execution_order().to_string()
            };
            conditions.push("node_id = ?".to_string());
            params.push(libsql::Value::Text(node_id));
        }

        // Add WHERE clause if needed
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        (sql, params)
    }
}

/// Background log viewer that runs in a separate task
pub struct BackgroundLogViewer {
    viewer: SqlLogViewer,
    update_interval: Duration,
}

impl BackgroundLogViewer {
    /// Create a new background viewer
    pub fn new(subscriber: &LibsqlSubscriber) -> Self {
        Self {
            viewer: SqlLogViewer::new(subscriber),
            update_interval: Duration::from_millis(100),
        }
    }

    /// Run the background viewer
    pub async fn run(
        mut self,
        mut request_rx: mpsc::UnboundedReceiver<ViewerRequest>,
        response_tx: mpsc::UnboundedSender<ViewerResponse>,
    ) {
        let mut ticker = interval(self.update_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Handle requests
                Some(request) = request_rx.recv() => {
                    match request {
                        ViewerRequest::SetFilters { level, node_filter } => {
                            self.viewer.set_filters(level, node_filter);
                            if let Ok(logs) = self.viewer.get_logs().await {
                                let _ = response_tx.send(ViewerResponse::Logs(logs));
                            }
                        }
                        ViewerRequest::SetViewportSize(size) => {
                            self.viewer.set_viewport_size(size);
                        }
                        ViewerRequest::ScrollUp(amount) => {
                            self.viewer.scroll_up(amount);
                            if let Ok(logs) = self.viewer.get_logs().await {
                                let _ = response_tx.send(ViewerResponse::Logs(logs));
                            }
                        }
                        ViewerRequest::ScrollDown(amount) => {
                            self.viewer.scroll_down(amount);
                            if let Ok(logs) = self.viewer.get_logs().await {
                                let _ = response_tx.send(ViewerResponse::Logs(logs));
                            }
                        }
                        ViewerRequest::ScrollToTop => {
                            self.viewer.scroll_to_top();
                            if let Ok(logs) = self.viewer.get_logs().await {
                                let _ = response_tx.send(ViewerResponse::Logs(logs));
                            }
                        }
                        ViewerRequest::ScrollToBottom => {
                            self.viewer.scroll_to_bottom();
                            if let Ok(logs) = self.viewer.get_logs().await {
                                let _ = response_tx.send(ViewerResponse::Logs(logs));
                            }
                        }
                        ViewerRequest::Refresh => {
                            if let Ok(logs) = self.viewer.get_logs().await {
                                let _ = response_tx.send(ViewerResponse::Logs(logs));
                            }
                        }
                    }
                }

                // Auto-update for new logs
                _ = ticker.tick() => {
                    if self.viewer.state.auto_scroll {
                        if let Ok(new_logs) = self.viewer.get_new_logs().await {
                            if !new_logs.is_empty() {
                                let _ = response_tx.send(ViewerResponse::NewLogs(new_logs));
                            }
                        }
                    }
                }

                // Channel closed
                else => break,
            }
        }
    }
}

/// Viewer requests
#[derive(Debug)]
pub enum ViewerRequest {
    SetFilters {
        level: LogLevel,
        node_filter: Option<TuiNodeId>,
    },
    SetViewportSize(usize),
    ScrollUp(usize),
    ScrollDown(usize),
    ScrollToTop,
    ScrollToBottom,
    Refresh,
}

/// Viewer responses
#[derive(Debug)]
pub enum ViewerResponse {
    Logs(Vec<DisplayLogEntry>),
    NewLogs(Vec<DisplayLogEntry>),
}
