//! Tracing subscriber that stores logs in an in-memory SQLite database
//!
//! This crate provides a high-performance tracing subscriber that stores logs
//! in an in-memory SQLite database (via libsql), enabling efficient filtering
//! and querying without serialization overhead.

mod span_ext;
pub use span_ext::*;

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use libsql::{Builder, Connection, params};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as TokioMutex, mpsc};
use tokio::time::interval;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, layer::Context};

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
    line INTEGER,
    span_id INTEGER,
    span_name TEXT
);

-- Indexes for efficient filtering
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_node_id ON logs(node_id);
CREATE INDEX IF NOT EXISTS idx_logs_component ON logs(component);
CREATE INDEX IF NOT EXISTS idx_logs_span_id ON logs(span_id);
"#;

/// SQL for inserting a log entry
const INSERT_LOG_SQL: &str = r#"
INSERT INTO logs (timestamp, level, node_id, component, target, message, trace_id, file, line, span_id, span_name)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    span_id: Option<u64>,
    span_name: Option<String>,
}

/// Configuration for the libsql subscriber
#[derive(Debug, Clone)]
pub struct LibsqlSubscriberConfig {
    /// Maximum number of logs to keep in memory
    pub max_logs: usize,
    /// Batch insert interval
    pub batch_interval: Duration,
    /// Maximum batch size
    pub max_batch_size: usize,
}

impl Default for LibsqlSubscriberConfig {
    fn default() -> Self {
        Self {
            max_logs: 100_000,
            batch_interval: Duration::from_millis(10),
            max_batch_size: 1000,
        }
    }
}

/// Tracing subscriber that stores logs in SQLite
#[derive(Clone)]
pub struct LibsqlSubscriber {
    connection: Arc<TokioMutex<Connection>>,
    sender: mpsc::UnboundedSender<LogEntry>,
}

impl LibsqlSubscriber {
    /// Create a new in-memory subscriber
    pub async fn new(config: LibsqlSubscriberConfig) -> Result<Self> {
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

        let subscriber = Self {
            connection: connection.clone(),
            sender,
        };

        // Spawn background task for batched inserts
        tokio::spawn(Self::batch_processor(connection, receiver, config));

        Ok(subscriber)
    }

    /// Background task that processes log entries in batches
    async fn batch_processor(
        connection: Arc<TokioMutex<Connection>>,
        mut receiver: mpsc::UnboundedReceiver<LogEntry>,
        config: LibsqlSubscriberConfig,
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
                entry.span_id.map(|id| id as i64),
                entry.span_name,
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

impl<S> Layer<S> for LibsqlSubscriber
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Get timestamp
        let timestamp = chrono::Utc::now().timestamp_micros();

        // Get level
        let level = match *event.metadata().level() {
            Level::ERROR => "ERROR",
            Level::WARN => "WARN",
            Level::INFO => "INFO",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };

        // Extract message
        let mut message = String::new();
        let mut visitor = MessageVisitor(&mut message);
        event.record(&mut visitor);

        // Extract span context
        let (node_id, component, trace_id, span_id, span_name) =
            if let Some(span) = ctx.current_span().id().and_then(|id| ctx.span(id)) {
                let extensions = span.extensions();

                // Try to get values from span attributes
                let node_id = "main".to_string();
                let mut component = "unknown".to_string();
                let trace_id: Option<String> = None;

                // Get span metadata
                let span_id = span.id().into_u64();
                let span_name = span.name().to_string();

                // Use span name as component if it's not a generic name
                if !span.name().starts_with("span") {
                    component = span.name().to_string();
                }

                // For now, we'll encode node_id in the span name
                // In production, you'd implement proper field extraction
                // This is a limitation of the current tracing API

                (node_id, component, trace_id, Some(span_id), Some(span_name))
            } else {
                ("main".to_string(), "unknown".to_string(), None, None, None)
            };

        // Get target
        let target = Some(event.metadata().target().to_string());

        // Get file and line
        let (file, line) =
            if let (Some(file), Some(line)) = (event.metadata().file(), event.metadata().line()) {
                (Some(file.to_string()), Some(line as i32))
            } else {
                (None, None)
            };

        let entry = LogEntry {
            timestamp,
            level: level.to_string(),
            node_id,
            component,
            target,
            message,
            trace_id,
            file,
            line,
            span_id,
            span_name,
        };

        // Send to background processor (ignore if channel is closed)
        let _ = self.sender.send(entry);
    }
}

/// Visitor to extract the message from an event
struct MessageVisitor<'a>(&'a mut String);

impl<'a> tracing::field::Visit for MessageVisitor<'a> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write;
            let _ = write!(self.0, "{:?}", value);
        }
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
        Self::default()
    }

    /// Filter by log level (and all levels above)
    pub fn with_min_level(mut self, level: Level) -> Self {
        let levels = match level {
            Level::ERROR => vec!["ERROR"],
            Level::WARN => vec!["ERROR", "WARN"],
            Level::INFO => vec!["ERROR", "WARN", "INFO"],
            Level::DEBUG => vec!["ERROR", "WARN", "INFO", "DEBUG"],
            Level::TRACE => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
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
            "SELECT id, timestamp, level, node_id, component, target, message, trace_id, file, line, span_id, span_name FROM logs",
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
    pub span_id: Option<u64>,
    pub span_name: Option<String>,
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
        let span_id = row.get::<Option<i64>>(10)?;
        let span_name = row.get::<Option<String>>(11)?;

        let timestamp = DateTime::from_timestamp_micros(timestamp_micros).unwrap_or_else(Utc::now);

        let level = match level_str.as_str() {
            "ERROR" => Level::ERROR,
            "WARN" => Level::WARN,
            "INFO" => Level::INFO,
            "DEBUG" => Level::DEBUG,
            "TRACE" => Level::TRACE,
            _ => Level::INFO,
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
            span_id: span_id.map(|id| id as u64),
            span_name,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tracing::{Level, error, info, span, warn};
    use tracing_subscriber::layer::SubscriberExt;

    #[tokio::test]
    async fn test_basic_logging() {
        let libsql_subscriber = LibsqlSubscriber::new(LibsqlSubscriberConfig::default())
            .await
            .unwrap();

        // Keep a clone for querying later
        let query_subscriber = libsql_subscriber.clone();

        let subscriber = tracing_subscriber::registry().with(libsql_subscriber);

        tracing::subscriber::set_global_default(subscriber).unwrap();

        // Log some messages
        info!("Test info message");
        warn!("Test warning");
        error!("Test error");

        // Wait for batch processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Query logs
        let conn = query_subscriber.get_connection();
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
    async fn test_span_context() {
        let libsql_subscriber = LibsqlSubscriber::new(LibsqlSubscriberConfig::default())
            .await
            .unwrap();

        // Keep a clone for querying later
        let query_subscriber = libsql_subscriber.clone();

        let subscriber = tracing_subscriber::registry().with(libsql_subscriber);

        tracing::subscriber::set_global_default(subscriber).unwrap();

        // Create span with node context
        let span = span!(Level::INFO, "test_component", node_id = "test-node-1");
        let _guard = span.enter();

        info!("Message with span context");

        // Wait for batch processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Query logs
        let conn = query_subscriber.get_connection();
        let conn = conn.lock().await;

        let query = LogQuery::new().with_node("test-node-1");
        let (sql, params) = query.build_sql();

        let mut rows = conn.query(&sql, params).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let log = LogRow::from_row(row).unwrap();

        assert_eq!(log.component, "test_component");
        assert!(log.span_name.is_some());
    }
}
