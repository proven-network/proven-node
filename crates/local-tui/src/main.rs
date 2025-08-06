//! Proven Node TUI - Terminal User Interface for managing multiple local nodes

use anyhow::Result;
use proven_local_tui::App;
use signal_hook::consts::SIGINT;
use signal_hook::flag;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{info, warn};

/// Main entry point for the TUI
fn main() -> Result<()> {
    info!("Starting Proven Local TUI");

    // Create shutdown flag for signal handling
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Register signal handler for graceful shutdown
    if let Err(e) = flag::register(SIGINT, Arc::clone(&shutdown_flag)) {
        warn!("Failed to register SIGINT handler: {}", e);
    }

    // Create and run the application
    let mut app = App::new()?;

    // Pass the shutdown flag to the app
    app.set_shutdown_flag(shutdown_flag.clone());

    // Create a custom panic hook that cleans up the terminal
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // Try to cleanup terminal
        let _ = crossterm::execute!(
            std::io::stderr(),
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::event::DisableMouseCapture
        );
        let _ = crossterm::terminal::disable_raw_mode();

        // Call the default panic handler
        default_panic(panic_info);
    }));

    // Run the application
    if let Err(e) = app.run() {
        // The app handles its own terminal cleanup, but ensure we're in a good state
        let _ = crossterm::terminal::disable_raw_mode();
        eprintln!("Application error: {e}");
        std::process::exit(1);
    }

    // Check if we were interrupted
    if shutdown_flag.load(Ordering::Relaxed) {
        info!("Shutdown via signal complete");
    }

    info!("Proven Local TUI exiting normally");
    Ok(())
}
