//! Bridge from tracing to proven-logger

use crate::{Level, Logger, Record};
use std::sync::Arc;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// A tracing layer that forwards to proven-logger
pub struct TracingBridge<S> {
    logger: Arc<dyn Logger>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> TracingBridge<S> {
    /// Create a new tracing bridge
    pub fn new(logger: Arc<dyn Logger>) -> Self {
        Self {
            logger,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S> Layer<S> for TracingBridge<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Map tracing levels to our levels
        let level = match *event.metadata().level() {
            tracing::Level::ERROR => Level::Error,
            tracing::Level::WARN => Level::Warn,
            tracing::Level::INFO => Level::Info,
            tracing::Level::DEBUG => Level::Debug,
            tracing::Level::TRACE => Level::Trace,
        };

        // Skip if not enabled
        if !self.logger.is_enabled(level) {
            return;
        }

        // Collect the message and fields
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        // Build span context if available
        let mut span_context = Vec::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                span_context.push(span.name().to_string());
            }
        }

        // Create the message with span context
        let message = if span_context.is_empty() {
            visitor.message
        } else {
            format!("{}: {}", span_context.join("::"), visitor.message)
        };

        // Create our record
        let mut record = Record::new(level, message).with_target(event.metadata().target());

        if let (Some(file), Some(line)) = (event.metadata().file(), event.metadata().line()) {
            record = record.with_location(file, line);
        }

        self.logger.log(record);
    }
}

/// Visitor to extract the message from tracing fields
#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            // Append other fields as key=value
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            use std::fmt::Write;
            let _ = write!(&mut self.message, "{}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            use std::fmt::Write;
            let _ = write!(&mut self.message, "{}={:?}", field.name(), value);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        use std::fmt::Write;
        let _ = write!(&mut self.message, "{}={}", field.name(), value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        use std::fmt::Write;
        let _ = write!(&mut self.message, "{}={}", field.name(), value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        use std::fmt::Write;
        let _ = write!(&mut self.message, "{}={}", field.name(), value);
    }
}

/// Initialize tracing to forward to proven-logger
///
/// This sets up a global subscriber that captures all tracing events and spans.
///
/// # Example
/// ```no_run
/// use proven_logger::{StdoutLogger, compat::init_tracing_bridge};
/// use std::sync::Arc;
///
/// let logger = Arc::new(StdoutLogger::new());
/// init_tracing_bridge(logger).expect("Failed to set tracing bridge");
/// ```
pub fn init_tracing_bridge(logger: Arc<dyn Logger>) -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::prelude::*;

    let layer = TracingBridge::new(logger);

    tracing_subscriber::registry().with(layer).try_init()?;

    Ok(())
}
