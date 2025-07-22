//! Binary entry point for SQL-based TUI

use anyhow::Result;
use proven_logger::info;
use proven_logger_libsql::{LibsqlLogger, LibsqlLoggerConfig};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{Level, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Set up signal handling for graceful shutdown
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    setup_signal_handlers(shutdown_requested.clone())?;

    // Create SQL logger first
    let logger = LibsqlLogger::new(LibsqlLoggerConfig::default()).await?;

    // Initialize the global logger
    proven_logger::init(logger.clone()).expect("Failed to initialize logger");

    // Create the application with the logger
    let mut app = proven_local_tui::app::App::new(logger);

    info!("Starting Proven Node TUI with SQL-based logging");

    // Pass the shutdown flag to the app
    app.set_shutdown_flag(shutdown_requested);

    // Run the application synchronously
    app.run()?;

    info!("Proven Node TUI shutdown complete");
    Ok(())
}

/// Set up signal handlers for graceful shutdown
fn setup_signal_handlers(shutdown_requested: Arc<AtomicBool>) -> Result<()> {
    use signal_hook::{consts::SIGINT, iterator::Signals};
    use std::thread;

    let mut signals = Signals::new([SIGINT])?;

    thread::spawn(move || {
        for sig in signals.forever() {
            match sig {
                SIGINT => {
                    info!("Received SIGINT (Ctrl+C), initiating graceful shutdown...");
                    shutdown_requested.store(true, Ordering::SeqCst);
                    break;
                }
                _ => {
                    eprintln!("Received unexpected signal: {sig}");
                }
            }
        }
    });

    Ok(())
}

/// Set up tracing/logging with integrated log writer
fn setup_logging(log_writer: logs_writer::LogWriter) {
    // Create integrated tracing layer (LogWriter implements Layer trait)
    // Set up the subscriber to capture all log levels - filtering is done in the TUI
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::TRACE, // Capture all levels, let TUI filter them
        ))
        .with(log_writer)
        .init();
}
