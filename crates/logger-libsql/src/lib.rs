//! In-memory SQLite logger implementation for Proven Network
//!
//! This crate provides a high-performance logger that stores logs in an
//! in-memory SQLite database, enabling efficient filtering and querying
//! without serialization overhead.

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use libsql::{Builder, Connection, params};
use proven_logger::{Context, Level, Logger, Record};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as TokioMutex, mpsc};
use tokio::time::interval;

/// SQL for creating the logs table
const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    level TEXT NOT NULL CHECK(level IN ('ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE')),
    node_id TEXT NOT NULL,
    component TEXT NOT NULL,
    target TEXT,
    message TEXT NOT NULL,
    trace_id TEXT,
    file TEXT,
    line INTEGER
);

-- Indexes for efficient filtering
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_node_id ON logs(node_id);
CREATE INDEX IF NOT EXISTS idx_logs_component ON logs(component);
"#;

/// SQL for inserting a log entry
const INSERT_LOG_SQL: &str = r#"
INSERT INTO logs (timestamp, level, node_id, component, target, message, trace_id, file, line)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"#;

/// SQL for cleaning up old logs
const CLEANUP_SQL: &str = r#"
DELETE FROM logs 
WHERE id IN (
    SELECT id FROM logs 
    ORDER BY id ASC 
    LIMIT MAX(0, (SELECT COUNT(*) FROM logs) - ?)
)
"#;

/// A log entry to be inserted
#[derive(Debug, Clone)]
struct LogEntry {
    timestamp: i64,
    level: String,
    node_id: String,
    component: String,
    target: Option<String>,
    message: String,
    trace_id: Option<String>,
    file: Option<String>,
    line: Option<i32>,
}

/// Configuration for the libsql logger
#[derive(Debug, Clone)]
pub struct LibsqlLoggerConfig {
    /// Maximum number of logs to keep in memory
    pub max_logs: usize,
    /// Batch insert interval
    pub batch_interval: Duration,
    /// Maximum batch size
    pub max_batch_size: usize,
}

impl Default for LibsqlLoggerConfig {
    fn default() -> Self {
        Self {
            max_logs: 100_000,
            batch_interval: Duration::from_millis(10),
            max_batch_size: 1000,
        }
    }
}

/// In-memory SQLite logger
pub struct LibsqlLogger {
    connection: Arc<TokioMutex<Connection>>,
    sender: mpsc::UnboundedSender<LogEntry>,
    config: LibsqlLoggerConfig,
    context: Context,
}

impl LibsqlLogger {
    /// Create a new in-memory logger
    pub async fn new(config: LibsqlLoggerConfig) -> Result<Arc<Self>> {
        // Create in-memory database using :memory: path
        let db = Builder::new_local(":memory:")
            .build()
            .await
            .context("Failed to create in-memory database")?;

        let connection = db.connect().context("Failed to connect to database")?;

        // Create tables
        connection
            .execute(CREATE_TABLE_SQL, params![])
            .await
            .context("Failed to create tables")?;

        let connection = Arc::new(TokioMutex::new(connection));
        let (sender, receiver) = mpsc::unbounded_channel();

        let logger = Arc::new(Self {
            connection: connection.clone(),
            sender,
            config: config.clone(),
            context: Context::default(),
        });

        // Spawn background task for batched inserts
        tokio::spawn(Self::batch_processor(connection, receiver, config));

        Ok(logger)
    }

    /// Background task that processes log entries in batches
    async fn batch_processor(
        connection: Arc<TokioMutex<Connection>>,
        mut receiver: mpsc::UnboundedReceiver<LogEntry>,
        config: LibsqlLoggerConfig,
    ) {
        let mut batch = Vec::with_capacity(config.max_batch_size);
        let mut ticker = interval(config.batch_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Receive log entries
                Some(entry) = receiver.recv() => {
                    batch.push(entry);

                    // Flush if batch is full
                    if batch.len() >= config.max_batch_size {
                        Self::flush_batch(&connection, &mut batch, config.max_logs).await;
                    }
                }

                // Periodic flush
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        Self::flush_batch(&connection, &mut batch, config.max_logs).await;
                    }
                }

                // Channel closed
                else => break,
            }
        }

        // Final flush
        if !batch.is_empty() {
            Self::flush_batch(&connection, &mut batch, config.max_logs).await;
        }
    }

    /// Flush a batch of log entries to the database
    async fn flush_batch(
        connection: &Arc<TokioMutex<Connection>>,
        batch: &mut Vec<LogEntry>,
        max_logs: usize,
    ) {
        if batch.is_empty() {
            return;
        }

        // Lock connection for batch insert
        let conn = connection.lock().await;

        // Start transaction for batch insert
        if let Err(e) = conn.execute("BEGIN", params![]).await {
            eprintln!("Failed to start transaction: {e}");
            batch.clear();
            return;
        }

        // Insert all entries
        for entry in batch.drain(..) {
            let params = params![
                entry.timestamp,
                entry.level,
                entry.node_id,
                entry.component,
                entry.target,
                entry.message,
                entry.trace_id,
                entry.file,
                entry.line.map(|l| l as i64),
            ];

            if let Err(e) = conn.execute(INSERT_LOG_SQL, params).await {
                eprintln!("Failed to insert log: {e}");
            }
        }

        // Cleanup old logs
        if let Err(e) = conn.execute(CLEANUP_SQL, params![max_logs as i64]).await {
            eprintln!("Failed to cleanup old logs: {e}");
        }

        // Commit transaction
        if let Err(e) = conn.execute("COMMIT", params![]).await {
            eprintln!("Failed to commit transaction: {e}");
        }
    }

    /// Get a connection for querying logs
    pub fn get_connection(&self) -> Arc<TokioMutex<Connection>> {
        self.connection.clone()
    }
}

impl Logger for LibsqlLogger {
    fn log(&self, record: Record) {
        // Convert log level
        let level = match record.level {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
            Level::Trace => "TRACE",
        };

        // Get timestamp in microseconds
        let timestamp = chrono::Utc::now().timestamp_micros();

        // Extract context fields
        let (node_id, component, trace_id) =
            if let Some(ctx) = record.context.or(Some(&self.context)) {
                (
                    ctx.node_id.as_deref().unwrap_or("main").to_string(),
                    ctx.component.to_string(),
                    ctx.trace_id.clone(),
                )
            } else {
                ("main".to_string(), "unknown".to_string(), None)
            };

        let entry = LogEntry {
            timestamp,
            level: level.to_string(),
            node_id,
            component,
            target: Some(record.target.to_string()),
            message: record.message.into_owned(),
            trace_id,
            file: record.file.map(|f| f.to_string()),
            line: record.line.map(|l| l as i32),
        };

        // Send to background processor (ignore if channel is closed)
        let _ = self.sender.send(entry);
    }

    fn flush(&self) {
        // In-memory database, nothing to flush
    }

    fn is_enabled(&self, level: Level) -> bool {
        level.is_enabled_static()
    }

    fn with_context(&self, context: Context) -> Arc<dyn Logger> {
        Arc::new(Self {
            connection: self.connection.clone(),
            sender: self.sender.clone(),
            config: self.config.clone(),
            context: self.context.merge(&context),
        })
    }
}

/// Query builder for log queries
#[derive(Default)]
pub struct LogQuery {
    level_filter: Option<Vec<&'static str>>,
    node_filter: Option<String>,
    component_filter: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    after_id: Option<i64>,
}

impl LogQuery {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            level_filter: None,
            node_filter: None,
            component_filter: None,
            limit: None,
            offset: None,
            after_id: None,
        }
    }

    /// Filter by log level (and all levels above)
    pub fn with_min_level(mut self, level: Level) -> Self {
        let levels = match level {
            Level::Error => vec!["ERROR"],
            Level::Warn => vec!["ERROR", "WARN"],
            Level::Info => vec!["ERROR", "WARN", "INFO"],
            Level::Debug => vec!["ERROR", "WARN", "INFO", "DEBUG"],
            Level::Trace => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
        };
        self.level_filter = Some(levels);
        self
    }

    /// Filter by node ID
    pub fn with_node(mut self, node_id: impl Into<String>) -> Self {
        self.node_filter = Some(node_id.into());
        self
    }

    /// Filter by component
    pub fn with_component(mut self, component: impl Into<String>) -> Self {
        self.component_filter = Some(component.into());
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set result offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Get logs after a specific ID (for auto-scroll)
    pub fn after_id(mut self, id: i64) -> Self {
        self.after_id = Some(id);
        self
    }

    /// Build the SQL query
    pub fn build_sql(&self) -> (String, Vec<libsql::Value>) {
        let mut sql = String::from(
            "SELECT id, timestamp, level, node_id, component, target, message, trace_id, file, line FROM logs",
        );
        let mut params = Vec::new();
        let mut conditions = Vec::new();

        // Level filter
        if let Some(levels) = &self.level_filter {
            let placeholders = levels.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            conditions.push(format!("level IN ({placeholders})"));
            for level in levels {
                params.push(libsql::Value::Text(level.to_string()));
            }
        }

        // Node filter
        if let Some(node_id) = &self.node_filter {
            conditions.push("node_id = ?".to_string());
            params.push(libsql::Value::Text(node_id.clone()));
        }

        // Component filter
        if let Some(component) = &self.component_filter {
            conditions.push("component = ?".to_string());
            params.push(libsql::Value::Text(component.clone()));
        }

        // After ID filter
        if let Some(id) = self.after_id {
            conditions.push("id > ?".to_string());
            params.push(libsql::Value::Integer(id));
        }

        // Add WHERE clause if needed
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Order by ID descending (newest first)
        sql.push_str(" ORDER BY id DESC");

        // Limit and offset
        if let Some(limit) = self.limit {
            sql.push_str(" LIMIT ?");
            params.push(libsql::Value::Integer(limit as i64));
        }

        if let Some(offset) = self.offset {
            sql.push_str(" OFFSET ?");
            params.push(libsql::Value::Integer(offset as i64));
        }

        (sql, params)
    }
}

/// Represents a log entry from the database
#[derive(Debug, Clone)]
pub struct LogRow {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub level: Level,
    pub node_id: String,
    pub component: String,
    pub target: Option<String>,
    pub message: String,
    pub trace_id: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
}

impl LogRow {
    /// Parse a log row from database values
    pub fn from_row(row: libsql::Row) -> Result<Self> {
        let id = row.get::<i64>(0)?;
        let timestamp_micros = row.get::<i64>(1)?;
        let level_str = row.get::<String>(2)?;
        let node_id = row.get::<String>(3)?;
        let component = row.get::<String>(4)?;
        let target = row.get::<Option<String>>(5)?;
        let message = row.get::<String>(6)?;
        let trace_id = row.get::<Option<String>>(7)?;
        let file = row.get::<Option<String>>(8)?;
        let line = row.get::<Option<i64>>(9)?;

        let timestamp = DateTime::from_timestamp_micros(timestamp_micros).unwrap_or_else(Utc::now);

        let level = match level_str.as_str() {
            "ERROR" => Level::Error,
            "WARN" => Level::Warn,
            "INFO" => Level::Info,
            "DEBUG" => Level::Debug,
            "TRACE" => Level::Trace,
            _ => Level::Info,
        };

        Ok(Self {
            id,
            timestamp,
            level,
            node_id,
            component,
            target,
            message,
            trace_id,
            file,
            line: line.map(|l| l as u32),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use proven_logger::LoggerExt;

    #[tokio::test]
    async fn test_basic_logging() {
        let logger = LibsqlLogger::new(LibsqlLoggerConfig::default())
            .await
            .unwrap();

        // Log some messages
        logger.info("Test info message");
        logger.warn("Test warning");
        logger.error("Test error");

        // Wait for batch processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Query logs
        let conn = logger.get_connection();
        let conn = conn.lock().await;

        let query = LogQuery::new().limit(10);
        let (sql, params) = query.build_sql();

        let mut rows = conn.query(&sql, params).await.unwrap();
        let mut count = 0;

        while let Some(row) = rows.next().await.unwrap() {
            let log = LogRow::from_row(row).unwrap();
            println!("{:?}: {}", log.level, log.message);
            count += 1;
        }

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_level_filtering() {
        let logger = LibsqlLogger::new(LibsqlLoggerConfig::default())
            .await
            .unwrap();

        // Log messages at different levels
        logger.trace("Trace");
        logger.debug("Debug");
        logger.info("Info");
        logger.warn("Warn");
        logger.error("Error");

        // Wait for batch processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Query only WARN and above
        let conn = logger.get_connection();
        let conn = conn.lock().await;

        let query = LogQuery::new().with_min_level(Level::Warn);
        let (sql, params) = query.build_sql();

        let mut rows = conn.query(&sql, params).await.unwrap();
        let mut count = 0;

        while let Some(row) = rows.next().await.unwrap() {
            let log = LogRow::from_row(row).unwrap();
            assert!(matches!(log.level, Level::Warn | Level::Error));
            count += 1;
        }

        assert_eq!(count, 2);
    }
}
