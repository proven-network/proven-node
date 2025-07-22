//! Log formatting implementations

use proven_logger::{Level, Record};
use serde::Serialize;

/// Trait for formatting log records
pub trait LogFormatter: Send + Sync + 'static {
    /// Format a log record into bytes
    fn format(&self, record: &Record) -> crate::Result<Vec<u8>>;
}

/// Plain text formatter
#[derive(Debug, Clone)]
pub struct PlainTextFormatter {
    /// Whether to include timestamps
    pub include_timestamp: bool,
    /// Whether to include file/line information
    pub include_location: bool,
    /// Whether to include context fields
    pub include_context: bool,
}

impl Default for PlainTextFormatter {
    fn default() -> Self {
        Self {
            include_timestamp: true,
            include_location: true,
            include_context: true,
        }
    }
}

impl LogFormatter for PlainTextFormatter {
    fn format(&self, record: &Record) -> crate::Result<Vec<u8>> {
        let mut parts = Vec::new();

        // Timestamp
        if self.include_timestamp {
            #[cfg(feature = "timestamps")]
            {
                let timestamp = chrono::Utc::now()
                    .format("%Y-%m-%d %H:%M:%S%.3f")
                    .to_string();
                parts.push(timestamp);
            }
        }

        // Level
        parts.push(format!("[{}]", level_str(record.level)));

        // Target
        parts.push(format!("[{}]", record.target));

        // Location
        if self.include_location {
            if let (Some(file), Some(line)) = (record.file, record.line) {
                parts.push(format!("[{file}:{line}]"));
            }
        }

        // Context
        if self.include_context {
            if let Some(context) = record.context {
                let mut ctx_parts = Vec::new();

                if !context.component.is_empty() && context.component != "unknown" {
                    ctx_parts.push(format!("component={}", context.component));
                }

                if let Some(node_id) = &context.node_id {
                    ctx_parts.push(format!("node_id={node_id}"));
                }

                if let Some(trace_id) = &context.trace_id {
                    ctx_parts.push(format!("trace_id={trace_id}"));
                }

                if !ctx_parts.is_empty() {
                    parts.push(format!("[{}]", ctx_parts.join(" ")));
                }
            }
        }

        // Message
        parts.push(record.message.to_string());

        let mut line = parts.join(" ");
        line.push('\n');

        Ok(line.into_bytes())
    }
}

/// JSON formatter
#[derive(Debug, Default, Clone)]
pub struct JsonFormatter {
    /// Whether to pretty-print JSON
    pub pretty: bool,
}

#[derive(Serialize)]
struct JsonLogRecord<'a> {
    #[cfg(feature = "timestamps")]
    timestamp: String,
    level: &'static str,
    target: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    component: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<&'a str>,
}

impl LogFormatter for JsonFormatter {
    fn format(&self, record: &Record) -> crate::Result<Vec<u8>> {
        let json_record = JsonLogRecord {
            #[cfg(feature = "timestamps")]
            timestamp: chrono::Utc::now()
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            level: level_str(record.level),
            target: record.target,
            message: &record.message,
            file: record.file,
            line: record.line,
            component: record.context.map(|ctx| ctx.component),
            node_id: record
                .context
                .and_then(|ctx| ctx.node_id.as_ref().map(|id| id.as_ref())),
            trace_id: record.context.and_then(|ctx| ctx.trace_id.as_deref()),
        };

        let mut bytes = if self.pretty {
            serde_json::to_vec_pretty(&json_record)?
        } else {
            serde_json::to_vec(&json_record)?
        };

        bytes.push(b'\n');
        Ok(bytes)
    }
}

fn level_str(level: Level) -> &'static str {
    match level {
        Level::Error => "ERROR",
        Level::Warn => "WARN",
        Level::Info => "INFO",
        Level::Debug => "DEBUG",
        Level::Trace => "TRACE",
    }
}
