//! Proven Node TUI - Terminal User Interface for managing multiple local nodes
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod app;
mod ip_allocator;
mod logs_categorizer;
mod logs_formatter;
mod logs_viewer;
mod logs_writer;
mod messages;
mod node_id;
mod node_manager;
mod rpc_client;
mod ui;

use app::App;

use anyhow::Result;
use proven_logger::{info, warn};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn main() -> Result<()> {
    // Set up signal handling for graceful shutdown
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    setup_signal_handlers(shutdown_requested.clone())?;

    // Create the application synchronously
    let mut app = App::new();

    // Set up the log writer as the global logger
    let log_writer = app.get_log_writer();
    proven_logger::init(Arc::new(log_writer)).expect("Failed to initialize logger");

    info!("Starting Proven Node TUI");

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
                    warn!("Received unexpected signal: {sig}");
                }
            }
        }
    });

    Ok(())
}
