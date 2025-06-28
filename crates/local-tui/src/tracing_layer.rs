//! Custom tracing layer for capturing logs and sending them to the TUI

use crate::logs::LogCollector;
use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, NodeId};
use chrono::Utc;
use std::fmt::Write;
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// Extract node ID from the current thread name
/// Thread names follow the pattern "node-{execution_order}-{pokemon_id}" (e.g., "node-1-42", "node-2-7")
/// Returns None for non-node threads (main thread, etc.)
fn extract_node_id_from_thread_name() -> Option<NodeId> {
    let thread = std::thread::current();
    let thread_name = thread.name()?;

    // Parse "node-123-45" -> execution_order = 123, pokemon_id = 45
    let node_part = thread_name.strip_prefix("node-")?;
    let mut parts = node_part.split('-');

    let execution_order: u8 = parts.next()?.parse().ok()?;
    let pokemon_id: u8 = parts.next()?.parse().ok()?;

    Some(NodeId::with_values(execution_order, pokemon_id))
}

/// A tracing layer that captures log messages and sends them to the high-performance log collector
pub struct TuiTracingLayer {
    log_collector: Arc<LogCollector>,
}

impl TuiTracingLayer {
    /// Create a new TUI tracing layer
    #[must_use]
    pub const fn new(log_collector: Arc<LogCollector>) -> Self {
        Self { log_collector }
    }
}

/// A visitor for extracting message fields from tracing events
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else if self.message.is_empty() {
            // If no message field, try to capture any field
            if !self.message.is_empty() {
                self.message.push_str(", ");
            }
            write!(self.message, "{}={:?}", field.name(), value).ok();
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else if self.message.is_empty() {
            if !self.message.is_empty() {
                self.message.push_str(", ");
            }
            write!(self.message, "{}={}", field.name(), value).ok();
        }
    }
}

impl<S> Layer<S> for TuiTracingLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Extract node ID from thread name, use MAIN_THREAD_NODE_ID for non-node threads
        let node_id = extract_node_id_from_thread_name().unwrap_or(MAIN_THREAD_NODE_ID);

        // Extract log level
        let level = match *event.metadata().level() {
            tracing::Level::ERROR => LogLevel::Error,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::DEBUG => LogLevel::Debug,
            tracing::Level::TRACE => LogLevel::Trace,
        };

        // Extract target
        let target = Some(event.metadata().target().to_string());

        // Ignore targets containing "async_nats"
        if target.as_deref().unwrap_or_default().contains("async_nats") {
            return;
        }

        // Extract message using visitor pattern
        let mut visitor = MessageVisitor {
            message: String::new(),
        };

        event.record(&mut visitor);

        // If no message was extracted, use the event name as fallback
        if visitor.message.is_empty() {
            visitor.message = event.metadata().name().to_string();
        }

        let log_entry = LogEntry {
            node_id,
            level,
            message: visitor.message,
            timestamp: Utc::now(),
            target,
        };

        // Send to high-performance log collector (guaranteed delivery)
        self.log_collector.add_log(log_entry);
    }
}
