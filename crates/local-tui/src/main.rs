//! Binary entry point for SQL-based TUI

use anyhow::Result;
use proven_logger_libsql::{LibsqlSubscriber, LibsqlSubscriberConfig};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Set up signal handling for graceful shutdown
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    setup_signal_handlers(shutdown_requested.clone())?;

    // Create SQL subscriber first
    let subscriber = LibsqlSubscriber::new(LibsqlSubscriberConfig::default()).await?;

    // Initialize the global tracing subscriber
    tracing_subscriber::registry()
        .with(subscriber.clone())
        .init();

    // Create the application with the subscriber
    let mut app = proven_local_tui::app::App::new(subscriber);

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
                    // Can't use tracing from here since we're in a different thread
                    eprintln!("Received SIGINT (Ctrl+C), initiating graceful shutdown...");
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
