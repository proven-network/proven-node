//! Log reader for LocalCluster's SQLite database - direct queries, no buffering

use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use proven_local_cluster::{LogEntry, LogLevel};
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Arc;
use tracing::debug;

/// Log reader that polls LocalCluster's SQLite database directly
pub struct LogReader {
    /// Database connection
    connection: Arc<Mutex<Connection>>,

    /// Last read log ID for detecting new logs
    last_id: Arc<Mutex<i64>>,
}

impl LogReader {
    /// Create a new log reader
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();

        // Verify database exists
        if !db_path.exists() {
            return Err(anyhow::anyhow!(
                "Log database does not exist: {:?}",
                db_path
            ));
        }

        let connection = Connection::open(&db_path)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            last_id: Arc::new(Mutex::new(0i64)),
        })
    }

    /// Request initial data load - reset to read all logs
    pub fn request_initial_data(&self) {
        *self.last_id.lock() = 0;
        debug!("Requested initial log data");
    }

    /// Poll for updates (compatibility method)
    pub fn poll_updates(&self) {
        // No-op - we query directly when needed
    }

    /// Get new logs if available - returns all logs since last query
    pub fn get_new_logs(&self, log_level_filter: Option<LogLevel>) -> Option<Vec<LogEntry>> {
        let conn = self.connection.lock();
        let current_last_id = *self.last_id.lock();

        // Build query based on log level filter
        let (query, level_values) = if let Some(filter) = log_level_filter {
            // Build WHERE clause for log level filtering
            let levels = match filter {
                LogLevel::Error => vec!["ERROR"],
                LogLevel::Warn => vec!["ERROR", "WARN"],
                LogLevel::Info => vec!["ERROR", "WARN", "INFO"],
                LogLevel::Debug => vec!["ERROR", "WARN", "INFO", "DEBUG"],
                LogLevel::Trace => vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
            };

            let placeholders = levels.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let query = format!(
                r#"
                SELECT id, timestamp, level, node_id, message, target, execution_order
                FROM logs
                WHERE id > ? AND level IN ({placeholders})
                ORDER BY id ASC
                LIMIT 1000
            "#
            );

            (query, levels)
        } else {
            // No filter, get all logs
            let query = r#"
                SELECT id, timestamp, level, node_id, message, target, execution_order
                FROM logs
                WHERE id > ?
                ORDER BY id ASC
                LIMIT 1000
            "#
            .to_string();

            (query, vec![])
        };

        let mut stmt = match conn.prepare(&query) {
            Ok(s) => s,
            Err(_) => return None,
        };

        // Build parameters for the query
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(current_last_id)];
        for level in &level_values {
            params_vec.push(Box::new(level.to_string()));
        }

        let log_iter = match stmt.query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
            let id: i64 = row.get(0)?;
            let timestamp_micros: i64 = row.get(1)?;
            let level_str: String = row.get(2)?;
            let node_id: String = row.get(3)?;
            let message: String = row.get(4)?;
            let target: Option<String> = row.get(5)?;
            let execution_order: u32 = row.get(6)?;

            // Convert level string to enum
            let level = match level_str.as_str() {
                "ERROR" => proven_local_cluster::LogLevel::Error,
                "WARN" => proven_local_cluster::LogLevel::Warn,
                "INFO" => proven_local_cluster::LogLevel::Info,
                "DEBUG" => proven_local_cluster::LogLevel::Debug,
                "TRACE" => proven_local_cluster::LogLevel::Trace,
                _ => proven_local_cluster::LogLevel::Info,
            };

            // Convert timestamp from microseconds to DateTime
            let timestamp =
                DateTime::<Utc>::from_timestamp_micros(timestamp_micros).unwrap_or_else(Utc::now);

            Ok((
                id,
                LogEntry {
                    node_id,
                    execution_order,
                    level,
                    message,
                    timestamp,
                    target,
                },
            ))
        }) {
            Ok(iter) => iter,
            Err(_) => return None,
        };

        let mut new_logs = Vec::new();
        let mut max_id = current_last_id;

        for (id, entry) in log_iter.flatten() {
            max_id = max_id.max(id);
            new_logs.push(entry);
        }

        // Update last ID if we got new logs
        if !new_logs.is_empty() {
            *self.last_id.lock() = max_id;
            Some(new_logs)
        } else {
            None
        }
    }

    /// Search logs with a filter
    pub fn search_logs(&self, filter: &str) -> Result<Vec<LogEntry>> {
        let conn = self.connection.lock();

        let query = r#"
            SELECT timestamp, level, node_id, message, target, execution_order
            FROM logs
            WHERE message LIKE ?
            ORDER BY id DESC
            LIMIT 1000
        "#;

        let mut stmt = conn.prepare(query)?;
        let search_pattern = format!("%{filter}%");

        let log_iter = stmt.query_map(params![search_pattern], |row| {
            let timestamp_micros: i64 = row.get(0)?;
            let level_str: String = row.get(1)?;
            let node_id: String = row.get(2)?;
            let message: String = row.get(3)?;
            let target: Option<String> = row.get(4)?;
            let execution_order: u32 = row.get(5)?;

            let level = match level_str.as_str() {
                "ERROR" => proven_local_cluster::LogLevel::Error,
                "WARN" => proven_local_cluster::LogLevel::Warn,
                "INFO" => proven_local_cluster::LogLevel::Info,
                "DEBUG" => proven_local_cluster::LogLevel::Debug,
                "TRACE" => proven_local_cluster::LogLevel::Trace,
                _ => proven_local_cluster::LogLevel::Info,
            };

            let timestamp =
                DateTime::<Utc>::from_timestamp_micros(timestamp_micros).unwrap_or_else(Utc::now);

            Ok(LogEntry {
                node_id,
                execution_order,
                level,
                message,
                timestamp,
                target,
            })
        })?;

        let mut results = Vec::new();
        for entry in log_iter.flatten() {
            results.push(entry);
        }

        Ok(results)
    }
}
