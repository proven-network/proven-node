//! Simple stdout logger for development

use crate::{Context, Level, Logger, Record};
use std::io::Write;
use std::sync::{Arc, Mutex};

#[cfg(feature = "color")]
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

/// Simple logger that writes to stdout
#[derive(Debug)]
pub struct StdoutLogger {
    context: Context,
    /// Minimum log level
    min_level: Level,
    /// Lock for stdout (to prevent interleaving)
    #[cfg(not(feature = "color"))]
    stdout: Arc<Mutex<std::io::Stdout>>,
    #[cfg(feature = "color")]
    stdout: Arc<Mutex<StandardStream>>,
}

impl StdoutLogger {
    /// Create a new stdout logger
    pub fn new() -> Self {
        Self {
            context: Context::new("app"),
            min_level: Level::Trace,
            #[cfg(not(feature = "color"))]
            stdout: Arc::new(Mutex::new(std::io::stdout())),
            #[cfg(feature = "color")]
            stdout: Arc::new(Mutex::new(StandardStream::stdout(ColorChoice::Auto))),
        }
    }

    /// Create with a specific minimum level
    pub fn with_level(mut self, level: Level) -> Self {
        self.min_level = level;
        self
    }

    /// Create with context
    pub fn with_context(mut self, context: Context) -> Self {
        self.context = context;
        self
    }
}

impl Default for StdoutLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger for StdoutLogger {
    fn log(&self, record: Record) {
        // Create an owned record with our context if needed
        let record = if record.context.is_none() {
            record.with_context(&self.context)
        } else {
            record
        };

        // Write with lock
        if let Ok(mut stdout) = self.stdout.lock() {
            #[cfg(feature = "color")]
            {
                // Set colors based on level
                let level_color = match record.level {
                    Level::Error => Color::Red,
                    Level::Warn => Color::Yellow,
                    Level::Info => Color::Green,
                    Level::Debug => Color::Blue,
                    Level::Trace => Color::Magenta,
                };

                // Node ID in cyan
                if let Some(ctx) = record.context
                    && let Some(node_id) = &ctx.node_id
                {
                    let _ = stdout.set_color(ColorSpec::new().set_fg(Some(Color::Cyan)));
                    let _ = write!(stdout, "[{node_id}] ");
                }

                // Level with color
                let _ = stdout.set_color(ColorSpec::new().set_fg(Some(level_color)).set_bold(true));
                let _ = write!(stdout, "{}", record.level);
                let _ = stdout.reset();

                // Target in dimmed white
                let _ = stdout.set_color(ColorSpec::new().set_dimmed(true));
                let _ = write!(stdout, " [{}]", record.target);

                // Location in dimmed white
                if let (Some(file), Some(line)) = (record.file, record.line) {
                    let _ = write!(stdout, " {file}:{line}");
                }
                let _ = stdout.reset();

                // Message in normal color
                let _ = writeln!(stdout, " {}", record.message);
            }

            #[cfg(not(feature = "color"))]
            {
                let node_str = record
                    .context
                    .and_then(|ctx| ctx.node_id.as_ref())
                    .map(|id| format!("[{id}] "))
                    .unwrap_or_default();

                let location = if let (Some(file), Some(line)) = (record.file, record.line) {
                    format!(" {file}:{line}")
                } else {
                    String::new()
                };

                let line = format!(
                    "{}{} [{}]{} {}\n",
                    node_str, record.level, record.target, location, record.message
                );

                let _ = stdout.write_all(line.as_bytes());
            }

            let _ = stdout.flush();
        }
    }

    fn flush(&self) {
        if let Ok(mut stdout) = self.stdout.lock() {
            let _ = stdout.flush();
        }
    }

    #[inline(always)]
    fn is_enabled(&self, level: Level) -> bool {
        level >= self.min_level && level.is_enabled_static()
    }

    fn with_context(&self, context: Context) -> Arc<dyn Logger> {
        Arc::new(StdoutLogger {
            context: self.context.merge(&context),
            min_level: self.min_level,
            stdout: self.stdout.clone(),
        })
    }
}
