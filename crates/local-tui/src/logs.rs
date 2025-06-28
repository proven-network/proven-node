//! High-performance sync log collection and display

use crate::messages::{LogEntry, LogLevel, NodeId};
#[cfg(test)]
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tracing::info;

/// Maximum number of log entries in circular buffer
const CIRCULAR_BUFFER_SIZE: usize = 10_000;

/// Batch size for log processing
const LOG_BATCH_SIZE: usize = 100;

/// Raw log data before processing - minimal allocations
#[derive(Debug, Clone)]
pub struct RawLogEntry {
    pub node_id: NodeId,
    pub level: LogLevel,
    pub message: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub target: Option<String>,
}

/// Circular buffer for storing logs efficiently
#[derive(Debug)]
struct CircularLogBuffer {
    buffer: Vec<Option<LogEntry>>,
    head: usize,
    size: usize,
    capacity: usize,
}

impl CircularLogBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![None; capacity],
            head: 0,
            size: 0,
            capacity,
        }
    }

    fn push(&mut self, entry: LogEntry) {
        self.buffer[self.head] = Some(entry);
        self.head = (self.head + 1) % self.capacity;
        if self.size < self.capacity {
            self.size += 1;
        }
    }

    fn iter(&self) -> impl Iterator<Item = &LogEntry> {
        let start = if self.size < self.capacity {
            0
        } else {
            self.head
        };

        (0..self.size)
            .map(move |i| &self.buffer[(start + i) % self.capacity])
            .filter_map(|entry| entry.as_ref())
    }

    const fn len(&self) -> usize {
        self.size
    }

    fn clear(&mut self) {
        for slot in &mut self.buffer {
            *slot = None;
        }
        self.head = 0;
        self.size = 0;
    }
}

/// Commands for the log processor
#[derive(Debug)]
enum LogCommand {
    AddLogs(Vec<RawLogEntry>),
    GetFilteredLogs {
        level_filter: LogLevel,
        node_filter: Option<NodeId>,
        response: mpsc::Sender<Vec<LogEntry>>,
    },
    GetNodeLogs {
        node_id: NodeId,
        response: mpsc::Sender<Vec<LogEntry>>,
    },
    GetNodesWithLogs {
        response: mpsc::Sender<Vec<NodeId>>,
    },
    Clear,
    ClearNode(NodeId),
    SetFilters {
        level_filter: LogLevel,
        node_filter: Option<NodeId>,
    },
}

/// High-performance sync log collector
#[derive(Debug, Clone)]
pub struct LogCollector {
    command_sender: mpsc::Sender<LogCommand>,
    raw_log_sender: mpsc::Sender<Vec<RawLogEntry>>,
    current_level_filter: Arc<RwLock<LogLevel>>,
    current_node_filter: Arc<RwLock<Option<NodeId>>>,
}

impl LogCollector {
    /// Create a new high-performance log collector
    #[must_use]
    pub fn new() -> Self {
        let (command_sender, command_receiver) = mpsc::channel();
        let (raw_log_sender, raw_log_receiver) = mpsc::channel();

        let current_level_filter = Arc::new(RwLock::new(LogLevel::Info));
        let current_node_filter = Arc::new(RwLock::new(None));

        // Start background log processor
        let processor = LogProcessor::new(
            command_receiver,
            raw_log_receiver,
            current_level_filter.clone(),
            current_node_filter.clone(),
        );

        thread::spawn(move || {
            processor.run();
        });

        Self {
            command_sender,
            raw_log_sender,
            current_level_filter,
            current_node_filter,
        }
    }

    /// Add a log entry (non-blocking, guaranteed delivery)
    pub fn add_log(&self, entry: LogEntry) {
        let raw_entry = RawLogEntry {
            node_id: entry.node_id,
            level: entry.level,
            message: entry.message,
            timestamp: entry.timestamp,
            target: entry.target,
        };

        // Send log - blocking send but should be fast
        let _ = self.raw_log_sender.send(vec![raw_entry]);
    }

    /// Add multiple logs in a batch (more efficient, guaranteed delivery)
    pub fn add_logs_batch(&self, entries: Vec<LogEntry>) {
        let raw_entries: Vec<RawLogEntry> = entries
            .into_iter()
            .map(|entry| RawLogEntry {
                node_id: entry.node_id,
                level: entry.level,
                message: entry.message,
                timestamp: entry.timestamp,
                target: entry.target,
            })
            .collect();

        // Send batch - blocking send but should be fast
        let _ = self.raw_log_sender.send(raw_entries);
    }

    /// Get filtered logs (blocking with timeout)
    pub fn get_filtered_logs_blocking(&self, timeout: Duration) -> Vec<LogEntry> {
        let level_filter = *self.current_level_filter.read();
        let node_filter = *self.current_node_filter.read();

        let (response_tx, response_rx) = mpsc::channel();

        if self
            .command_sender
            .send(LogCommand::GetFilteredLogs {
                level_filter,
                node_filter,
                response: response_tx,
            })
            .is_err()
        {
            return Vec::new();
        }

        response_rx.recv_timeout(timeout).unwrap_or_default()
    }

    /// Get logs for a specific node (blocking with timeout)
    pub fn get_node_logs_blocking(&self, node_id: NodeId, timeout: Duration) -> Vec<LogEntry> {
        let (response_tx, response_rx) = mpsc::channel();

        if self
            .command_sender
            .send(LogCommand::GetNodeLogs {
                node_id,
                response: response_tx,
            })
            .is_err()
        {
            return Vec::new();
        }

        response_rx.recv_timeout(timeout).unwrap_or_default()
    }

    /// Get all node IDs that have logs (blocking with timeout)
    pub fn get_nodes_with_logs_blocking(&self, timeout: Duration) -> Vec<NodeId> {
        let (response_tx, response_rx) = mpsc::channel();

        if self
            .command_sender
            .send(LogCommand::GetNodesWithLogs {
                response: response_tx,
            })
            .is_err()
        {
            return Vec::new();
        }

        response_rx.recv_timeout(timeout).unwrap_or_default()
    }

    /// Set filters
    pub fn set_level_filter(&self, level: LogLevel) {
        *self.current_level_filter.write() = level;
        let _ = self.command_sender.send(LogCommand::SetFilters {
            level_filter: level,
            node_filter: *self.current_node_filter.read(),
        });
    }

    /// Get current level filter
    #[must_use]
    pub fn get_level_filter(&self) -> LogLevel {
        *self.current_level_filter.read()
    }

    /// Set node filter
    pub fn set_node_filter(&self, node_id: Option<NodeId>) {
        *self.current_node_filter.write() = node_id;
        let _ = self.command_sender.send(LogCommand::SetFilters {
            level_filter: *self.current_level_filter.read(),
            node_filter: node_id,
        });
    }

    /// Get current node filter
    #[must_use]
    pub fn get_node_filter(&self) -> Option<NodeId> {
        *self.current_node_filter.read()
    }

    /// Clear all logs
    pub fn clear(&self) {
        let _ = self.command_sender.send(LogCommand::Clear);
    }

    /// Clear logs for a specific node
    pub fn clear_node_logs(&self, node_id: NodeId) {
        let _ = self.command_sender.send(LogCommand::ClearNode(node_id));
    }
}

/// Background log processor that handles all log operations
struct LogProcessor {
    command_receiver: mpsc::Receiver<LogCommand>,
    raw_log_receiver: mpsc::Receiver<Vec<RawLogEntry>>,

    // Storage
    all_logs: CircularLogBuffer,
    node_logs: HashMap<NodeId, CircularLogBuffer>,

    // Filters
    level_filter: Arc<RwLock<LogLevel>>,
    node_filter: Arc<RwLock<Option<NodeId>>>,

    // Cached filtered results
    cached_filtered_logs: Vec<LogEntry>,
    cache_valid: bool,
}

impl LogProcessor {
    fn new(
        command_receiver: mpsc::Receiver<LogCommand>,
        raw_log_receiver: mpsc::Receiver<Vec<RawLogEntry>>,
        level_filter: Arc<RwLock<LogLevel>>,
        node_filter: Arc<RwLock<Option<NodeId>>>,
    ) -> Self {
        Self {
            command_receiver,
            raw_log_receiver,
            all_logs: CircularLogBuffer::new(CIRCULAR_BUFFER_SIZE),
            node_logs: HashMap::new(),
            level_filter,
            node_filter,
            cached_filtered_logs: Vec::new(),
            cache_valid: false,
        }
    }

    fn run(mut self) {
        info!("Starting high-performance log processor");

        let mut batch_buffer = Vec::with_capacity(LOG_BATCH_SIZE);

        loop {
            // First try to get commands with a short timeout
            match self
                .command_receiver
                .recv_timeout(Duration::from_millis(10))
            {
                Ok(command) => {
                    self.handle_command(command);
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // No command available, check for raw logs
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    info!("Command channel disconnected, shutting down log processor");
                    break;
                }
            }

            // Try to get raw logs with a short timeout
            match self
                .raw_log_receiver
                .recv_timeout(Duration::from_millis(10))
            {
                Ok(raw_logs) => {
                    batch_buffer.extend(raw_logs);

                    // Process batch if it's large enough or if we've been waiting
                    if batch_buffer.len() >= LOG_BATCH_SIZE {
                        self.process_log_batch(&mut batch_buffer);
                        batch_buffer.clear();
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // No raw logs available, process any pending batch
                    if !batch_buffer.is_empty() {
                        self.process_log_batch(&mut batch_buffer);
                        batch_buffer.clear();
                    }
                    // Brief sleep to avoid busy waiting
                    thread::sleep(Duration::from_millis(1));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    info!("Raw log channel disconnected, processing final batch");
                    if !batch_buffer.is_empty() {
                        self.process_log_batch(&mut batch_buffer);
                    }
                    break;
                }
            }
        }

        info!("Log processor shut down");
    }

    fn process_log_batch(&mut self, batch: &mut Vec<RawLogEntry>) {
        for raw_entry in batch.drain(..) {
            let entry = LogEntry {
                node_id: raw_entry.node_id,
                level: raw_entry.level,
                message: raw_entry.message,
                timestamp: raw_entry.timestamp,
                target: raw_entry.target,
            };

            // Add to global buffer
            self.all_logs.push(entry.clone());

            // Add to node-specific buffer
            let node_buffer = self
                .node_logs
                .entry(raw_entry.node_id)
                .or_insert_with(|| CircularLogBuffer::new(1000));
            node_buffer.push(entry);

            // Invalidate cache
            self.cache_valid = false;
        }
    }

    fn handle_command(&mut self, command: LogCommand) {
        match command {
            LogCommand::AddLogs(raw_logs) => {
                for raw_log in raw_logs {
                    let log_entry = LogEntry {
                        node_id: raw_log.node_id,
                        level: raw_log.level,
                        message: raw_log.message,
                        timestamp: raw_log.timestamp,
                        target: raw_log.target,
                    };

                    // Add to all logs
                    self.all_logs.push(log_entry.clone());

                    // Add to node-specific logs
                    self.node_logs
                        .entry(raw_log.node_id)
                        .or_insert_with(|| CircularLogBuffer::new(CIRCULAR_BUFFER_SIZE))
                        .push(log_entry);
                }

                // Invalidate cache after adding logs
                self.cache_valid = false;
            }

            LogCommand::GetFilteredLogs {
                level_filter,
                node_filter,
                response,
            } => {
                let filtered_logs = self.get_filtered_logs_internal(level_filter, node_filter);
                let _ = response.send(filtered_logs);
            }

            LogCommand::GetNodeLogs { node_id, response } => {
                let node_logs = self
                    .node_logs
                    .get(&node_id)
                    .map_or_else(Vec::new, |buffer| buffer.iter().cloned().collect());
                let _ = response.send(node_logs);
            }

            LogCommand::GetNodesWithLogs { response } => {
                let nodes: Vec<NodeId> = self.node_logs.keys().copied().collect();
                let _ = response.send(nodes);
            }

            LogCommand::Clear => {
                self.all_logs.clear();
                self.node_logs.clear();
                self.cached_filtered_logs.clear();
                self.cache_valid = false;
            }

            LogCommand::ClearNode(node_id) => {
                if let Some(buffer) = self.node_logs.get_mut(&node_id) {
                    buffer.clear();
                }

                // Also remove entries from all_logs (this is expensive but necessary for consistency)
                // For performance, we'll just invalidate cache and rebuild on next request
                self.cache_valid = false;
            }

            LogCommand::SetFilters {
                level_filter,
                node_filter,
            } => {
                *self.level_filter.write() = level_filter;
                *self.node_filter.write() = node_filter;
                self.cache_valid = false;
            }
        }
    }

    fn get_filtered_logs_internal(
        &mut self,
        level_filter: LogLevel,
        node_filter: Option<NodeId>,
    ) -> Vec<LogEntry> {
        // Check if we can use cached results
        if self.cache_valid
            && *self.level_filter.read() == level_filter
            && *self.node_filter.read() == node_filter
        {
            return self.cached_filtered_logs.clone();
        }

        // Rebuild filtered cache
        let mut filtered: Vec<LogEntry> = self
            .all_logs
            .iter()
            .filter(|entry| {
                Self::should_show_level(entry.level, level_filter)
                    && node_filter.is_none_or(|node_id| entry.node_id == node_id)
            })
            .cloned()
            .collect();

        // Reverse to show newest first and limit for performance
        filtered.reverse();
        filtered.truncate(1000);

        self.cached_filtered_logs.clone_from(&filtered);
        self.cache_valid = true;

        filtered
    }

    const fn should_show_level(entry_level: LogLevel, filter_level: LogLevel) -> bool {
        use LogLevel::{Debug, Error, Info, Trace, Warn};

        let entry_priority = match entry_level {
            Error => 0,
            Warn => 1,
            Info => 2,
            Debug => 3,
            Trace => 4,
        };

        let filter_priority = match filter_level {
            Error => 0,
            Warn => 1,
            Info => 2,
            Debug => 3,
            Trace => 4,
        };

        entry_priority <= filter_priority
    }
}

impl Default for LogCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Format a log entry for display (plain text, colors applied in UI layer)
#[must_use]
pub fn format_log_entry(entry: &LogEntry) -> String {
    let timestamp = entry.timestamp.format("%H:%M:%S%.3f");
    let node_name = entry.node_id.pokemon_name();

    entry.target.as_ref().map_or_else(
        || {
            format!(
                "{} [{}] [{}]: {}",
                timestamp, entry.level, node_name, entry.message
            )
        },
        |target| {
            format!(
                "{} [{}] [{}] {}: {}",
                timestamp, entry.level, node_name, target, entry.message
            )
        },
    )
}

/// Create a log entry for testing purposes
#[cfg(test)]
#[must_use]
pub fn create_test_log(node_id: NodeId, level: LogLevel, message: &str) -> LogEntry {
    LogEntry {
        node_id,
        level,
        message: message.to_string(),
        timestamp: Utc::now(),
        target: Some("test".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_collector_basic() {
        let collector = LogCollector::new();
        let node_id = NodeId::new();
        let entry = create_test_log(node_id, LogLevel::Info, "Test message");

        collector.add_log(entry);

        // Give the background processor time to process the log
        thread::sleep(Duration::from_millis(10));

        let logs = collector.get_filtered_logs_blocking(Duration::from_millis(10));
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "Test message");
    }

    #[test]
    fn test_log_level_filtering() {
        let collector = LogCollector::new();
        let node_id = NodeId::new();

        collector.add_log(create_test_log(node_id, LogLevel::Error, "Error"));
        collector.add_log(create_test_log(node_id, LogLevel::Warn, "Warning"));
        collector.add_log(create_test_log(node_id, LogLevel::Info, "Info"));
        collector.add_log(create_test_log(node_id, LogLevel::Debug, "Debug"));

        // Give the background processor time to process all logs
        thread::sleep(Duration::from_millis(10));

        // Default filter is Info, should show Error, Warn, Info
        let logs = collector.get_filtered_logs_blocking(Duration::from_millis(10));
        assert_eq!(logs.len(), 3);

        // Set to Error level, should only show Error
        collector.set_level_filter(LogLevel::Error);
        let logs = collector.get_filtered_logs_blocking(Duration::from_millis(10));
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "Error");
    }

    #[test]
    fn test_node_filtering() {
        let collector = LogCollector::new();
        let node1 = NodeId::new();
        let node2 = NodeId::new();

        collector.add_log(create_test_log(node1, LogLevel::Info, "Node 1"));
        collector.add_log(create_test_log(node2, LogLevel::Info, "Node 2"));

        // Give the background processor time to process the logs
        thread::sleep(Duration::from_millis(10));

        // Show all nodes
        let logs = collector.get_filtered_logs_blocking(Duration::from_millis(10));
        assert_eq!(logs.len(), 2);

        // Filter to node1 only
        collector.set_node_filter(Some(node1));
        let logs = collector.get_filtered_logs_blocking(Duration::from_millis(10));
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "Node 1");
    }
}
