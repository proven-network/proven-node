//! Proven Node TUI - Terminal User Interface for managing multiple local nodes
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod app;
mod events;
mod logs;
mod messages;
mod node_id;
mod node_manager;
mod tracing_layer;
mod ui;

use app::App;
use tracing_layer::TuiTracingLayer;

use anyhow::Result;
use clap::Parser;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{Level, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "proven-local-tui")]
#[command(about = "Terminal User Interface for managing multiple Proven nodes")]
#[command(version)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Log to file instead of stdout
    #[arg(long)]
    log_file: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    info!("Starting Proven Node TUI");

    // Set up signal handling for graceful shutdown
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    setup_signal_handlers(shutdown_requested.clone())?;

    // Create the application synchronously
    let mut app = App::new()?;

    // Set up logging with TUI integration
    setup_logging(&args, app.get_log_collector());

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

/// Set up tracing/logging with high-performance log collector
fn setup_logging(args: &Args, log_collector: logs::LogCollector) {
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // Create TUI layer for capturing logs and sending to high-performance collector
    let tui_layer = TuiTracingLayer::new(Arc::new(log_collector));

    // Set up the subscriber with just the TUI layer
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            log_level,
        ))
        .with(tui_layer);

    if let Some(log_file) = &args.log_file {
        // If user specifically requests file logging, add a file layer
        let format_layer = tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_thread_ids(true)
            .with_thread_names(true);

        let file_appender = tracing_appender::rolling::daily("logs", log_file);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        registry.with(format_layer.with_writer(non_blocking)).init();

        // Keep the guard alive for the duration of the program
        std::mem::forget(guard);
    } else {
        // Just use TUI logging, no file output
        registry.init();
    }
}
