//! Proven Node TUI - Terminal User Interface for managing multiple local nodes
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod app;
mod ip_allocator;
mod logs_viewer;
mod logs_writer;
mod messages;
mod node_id;
mod node_manager;
mod ui;

use app::App;

use anyhow::Result;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{Level, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> Result<()> {
    info!("Starting Proven Node TUI");

    // Set up signal handling for graceful shutdown
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    setup_signal_handlers(shutdown_requested.clone())?;

    // Create the application synchronously
    let mut app = App::new();

    // Set up logging with integrated TUI log writer
    setup_logging(app.get_log_writer());

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
                    warn!("Received unexpected signal: {}", sig);
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
