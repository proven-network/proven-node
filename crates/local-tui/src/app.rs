//! Main application structure

use crate::{
    logs_viewer::LogReader,
    logs_writer::LogWriter,
    node_id::NodeId,
    node_manager::NodeManager,
    ui::{UiState, render_ui},
};
use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{self, DisableMouseCapture, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use proven_attestation_mock::MockAttestor;
use proven_governance::Version;
use proven_governance_mock::MockGovernance;
use proven_local::NodeStatus;
use ratatui::{
    Terminal,
    backend::{Backend, CrosstermBackend},
};
use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tracing::info;

/// Events that can occur in the TUI
#[derive(Debug, Clone)]
enum Event {
    /// Terminal input event (keyboard, mouse, etc.)
    Input(crossterm::event::Event),

    /// Timer tick for regular updates
    Tick,
}

/// Result of handling a key event
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyEventResult {
    /// Continue normal operation
    Continue,

    /// Force quit requested (immediate exit)
    ForceQuit,

    /// Graceful shutdown initiated
    GracefulShutdown,
}

/// Main application state
pub struct App {
    /// Whether the app should quit
    should_quit: bool,

    /// Whether we're in the process of shutting down
    shutting_down: bool,

    /// UI state
    ui_state: UiState,

    /// Node manager - single source of truth for node state
    node_manager: Arc<NodeManager>,

    /// Log writer for disk-based logging
    log_writer: LogWriter,

    /// Log reader for reading logs from disk
    log_reader: LogReader,

    /// Shutdown flag set by signal handler
    shutdown_flag: Option<Arc<AtomicBool>>,
}

impl App {
    /// Create a new application - now fully synchronous
    ///
    /// # Errors
    ///
    /// Returns an error if the channels fail to initialize or if the shared
    /// governance cannot be created.
    pub fn new() -> Self {
        // Create shared governance synchronously
        let governance = Arc::new(Self::create_shared_governance());

        // Generate session ID
        let session_id = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        info!("Starting TUI application with session_id: {}", session_id);

        // Create log writer with session_id-based config in /tmp (synchronous)
        let log_writer = LogWriter::new(&PathBuf::from(format!("/tmp/proven/{session_id}")))
            .expect("Failed to create log writer");

        // Create log reader for the same session
        let session_dir = log_writer.get_session_dir();
        let log_reader = LogReader::new(session_dir);

        // Create node manager with clean interface
        let node_manager = Arc::new(NodeManager::new(governance, session_id));

        Self {
            should_quit: false,
            shutting_down: false,
            ui_state: UiState::new(),
            node_manager,
            log_writer,
            log_reader,
            shutdown_flag: None,
        }
    }

    /// Create a shared governance instance for the TUI
    fn create_shared_governance() -> MockGovernance {
        // Create a version from mock attestation (synchronous)
        let pcrs = MockAttestor::new().pcrs_sync();
        let version = Version::from_pcrs(pcrs);

        // Create an empty governance instance that nodes can be added to
        MockGovernance::new(
            Vec::new(),                          // No initial nodes
            vec![version],                       // Single version
            "http://localhost:3200".to_string(), // Default primary auth gateway
            Vec::new(),                          // No alternate auth gateways
        )
    }

    /// Get the log writer for the tracing layer
    #[must_use]
    pub fn get_log_writer(&self) -> LogWriter {
        self.log_writer.clone()
    }

    /// Set the shutdown flag for signal handling
    pub fn set_shutdown_flag(&mut self, shutdown_flag: Arc<AtomicBool>) {
        self.shutdown_flag = Some(shutdown_flag);
    }

    /// Run the application with synchronous event handling
    ///
    /// # Errors
    ///
    /// Returns an error if the terminal cannot be initialized or if there's an error
    /// during the main application loop.
    pub fn run(&mut self) -> Result<()> {
        // Setup terminal (synchronous)
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Main loop (synchronous)
        let result = self.run_loop(&mut terminal);

        // Cleanup terminal (synchronous)
        Self::cleanup_terminal(&mut terminal)?;

        result
    }

    /// Cleanup terminal state
    fn cleanup_terminal<B: Backend + std::io::Write>(terminal: &mut Terminal<B>) -> Result<()> {
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;
        Ok(())
    }

    /// Main application loop with synchronous operations
    #[allow(clippy::cognitive_complexity)]
    fn run_loop<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()> {
        info!("Starting TUI application");

        // Request initial logs from background thread
        self.log_reader.request_initial_data();

        while !self.should_quit {
            // Check for shutdown signal
            if let Some(shutdown_flag) = &self.shutdown_flag {
                if shutdown_flag.load(Ordering::SeqCst) && !self.shutting_down {
                    info!(
                        "Signal-triggered shutdown initiated - sending shutdown command to all nodes"
                    );
                    self.node_manager.shutdown_all();
                    self.shutting_down = true;
                }
            }

            // Check if shutdown is complete by polling NodeManager
            if self.shutting_down {
                let nodes = self.node_manager.get_nodes_for_ui();
                info!("Shutdown check: {} nodes tracked", nodes.len());
                for (id, (name, status)) in &nodes {
                    info!("  Node {}: {} ({})", id.full_pokemon_name(), name, status);
                }

                if self.node_manager.is_shutdown_complete() {
                    info!("All nodes shut down - exiting TUI");
                    self.should_quit = true;
                    break;
                }
            }

            // Draw the UI (synchronous operation)
            terminal.draw(|frame| {
                // Get current node information from NodeManager
                let nodes = self.node_manager.get_nodes_for_ui();
                render_ui(
                    frame,
                    &mut self.ui_state,
                    &nodes,
                    &self.log_reader,
                    self.shutting_down,
                );
            })?;

            // Poll for events directly
            if let Ok(available) = event::poll(Duration::from_millis(100)) {
                if available {
                    if let Ok(crossterm_event) = event::read() {
                        let event = Event::Input(crossterm_event);
                        // Process event (synchronous)
                        self.handle_event(event);
                    }
                }
            }

            // Send periodic tick event
            self.handle_event(Event::Tick);
        }

        info!("TUI application shutting down");
        Ok(())
    }

    /// Handle an event
    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Input(crossterm_event) => {
                if let crossterm::event::Event::Key(key) = crossterm_event {
                    self.handle_key_event(key);
                }
            }

            Event::Tick => {
                // Regular update tick - could be used for animations or periodic updates
            }
        }
    }

    /// Handle keyboard input
    fn handle_key_event(&mut self, key: KeyEvent) {
        // Process global keys first
        let key_result = self.handle_global_key_event(key);

        match key_result {
            KeyEventResult::ForceQuit => {
                // Force quit requested - exit immediately
                self.should_quit = true;
                return;
            }
            KeyEventResult::GracefulShutdown => {
                // Graceful shutdown initiated - wait for completion
                self.shutting_down = true;
                return;
            }
            KeyEventResult::Continue => {
                // Continue with normal processing
            }
        }

        // Handle other global keys that should work even during shutdown
        match key.code {
            KeyCode::Char('?') => {
                // Help toggle always works, even during shutdown
                self.ui_state.show_help = !self.ui_state.show_help;
                return;
            }
            KeyCode::Esc => {
                // Close help or log level modal if open
                if self.ui_state.show_help {
                    self.ui_state.show_help = false;
                } else if self.ui_state.show_log_level_modal {
                    self.ui_state.show_log_level_modal = false;
                }
                return;
            }
            _ => {}
        }

        // Handle logs keys (even during shutdown for navigation)
        if self.shutting_down {
            // During shutdown, still allow some log navigation
            self.handle_logs_keys_during_shutdown(key);
        } else {
            self.handle_logs_keys(key);
        }
    }

    /// Handle global key events
    fn handle_global_key_event(&self, key: KeyEvent) -> KeyEventResult {
        match key.code {
            // Graceful quit the application
            KeyCode::Char('q') => {
                if !self.shutting_down {
                    info!("Initiating graceful shutdown of all nodes...");
                    self.node_manager.shutdown_all();
                    return KeyEventResult::GracefulShutdown;
                }
                // If already shutting down, do nothing
                return KeyEventResult::Continue;
            }

            // Force quit with Ctrl+C (immediate exit)
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                info!("Force quit requested - shutting down immediately");
                self.node_manager.shutdown_all();
                return KeyEventResult::ForceQuit;
            }

            // Start a new node with 'n' (only if not shutting down)
            KeyCode::Char('n') => {
                if !self.shutting_down {
                    self.start_new_node();
                }
            }

            _ => {}
        }

        KeyEventResult::Continue
    }

    /// Start a new node with default configuration
    fn start_new_node(&self) {
        let id = NodeId::new();
        let name = id.display_name();

        // Use NodeManager's clean interface for starting nodes
        self.node_manager.start_node(id, &name, None);
    }

    /// Start/stop a node based on its current status
    #[allow(clippy::cognitive_complexity)]
    fn toggle_node(&self, node_id: NodeId) {
        // Get current node status from NodeManager
        let nodes = self.node_manager.get_nodes_for_ui();

        if let Some((_, status)) = nodes.get(&node_id) {
            match status {
                NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Failed(_) => {
                    // Node is not running, so start it
                    let name = node_id.display_name();
                    self.node_manager.start_node(node_id, &name, None);
                    info!("Starting node: {}", node_id.full_pokemon_name());
                }
                NodeStatus::Running | NodeStatus::Starting => {
                    // Node is running or starting, so stop it
                    self.node_manager.stop_node(node_id);
                    info!("Stopping node: {}", node_id.full_pokemon_name());
                }
                NodeStatus::Stopping => {
                    // Node is already stopping, do nothing
                    info!("Node {} is already stopping", node_id.full_pokemon_name());
                }
            }
        } else {
            // Node not found in our list, assume it's not started and try to start it
            let name = node_id.display_name();
            self.node_manager.start_node(node_id, &name, None);
            info!("Starting new node: {}", node_id.full_pokemon_name());
        }
    }

    /// Restart a node
    fn restart_node(&self, node_id: NodeId) {
        self.node_manager.restart_node(node_id);
        info!("Restarting node: {}", node_id.full_pokemon_name());
    }

    /// Handle keys in logs mode
    #[allow(clippy::too_many_lines)]
    fn handle_logs_keys(&mut self, key: KeyEvent) {
        // Handle modal-specific keys first
        if self.ui_state.show_log_level_modal {
            self.handle_log_level_modal_keys(key);
            return;
        }

        match key.code {
            // Backtick "`" for "All" selection
            KeyCode::Char('`') => {
                self.ui_state.logs_sidebar_selected = 0;
                self.ui_state.logs_sidebar_debug_selected = false;
            }

            // Number keys 1-9 for quick node selection
            KeyCode::Char(c) if c.is_ascii_digit() && c != '0' => {
                let num = c.to_digit(10).unwrap() as usize;
                if num <= self.ui_state.logs_sidebar_nodes.len() {
                    // "1-9" selects corresponding node
                    self.ui_state.logs_sidebar_selected = num;
                    self.ui_state.logs_sidebar_debug_selected = false;
                }
            }

            // "d" key for debug selection
            KeyCode::Char('d') => {
                self.ui_state.logs_sidebar_debug_selected = true;
                self.ui_state.logs_sidebar_selected = 0; // Reset regular selection
            }

            // Alt+Up/Down for log scrolling (line by line) - put specific pattern first
            KeyCode::Up if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.log_reader.scroll_up(1);
            }

            KeyCode::Down if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.log_reader.scroll_down(1);
            }

            // Home/End for log scrolling
            KeyCode::Home => {
                self.log_reader.scroll_to_top();
            }

            KeyCode::End => {
                self.log_reader.scroll_to_bottom();
            }

            // Up/Down arrow keys for sidebar navigation
            KeyCode::Up => {
                if self.ui_state.logs_sidebar_debug_selected {
                    // Move from debug to last regular node or "All"
                    if self.ui_state.logs_sidebar_nodes.is_empty() {
                        self.ui_state.logs_sidebar_selected = 0; // "All"
                    } else {
                        self.ui_state.logs_sidebar_selected =
                            self.ui_state.logs_sidebar_nodes.len();
                    }
                    self.ui_state.logs_sidebar_debug_selected = false;
                } else if self.ui_state.logs_sidebar_selected > 0 {
                    self.ui_state.logs_sidebar_selected -= 1;
                }
            }

            KeyCode::Down => {
                let max_index = self.ui_state.logs_sidebar_nodes.len();
                if self.ui_state.logs_sidebar_selected < max_index {
                    self.ui_state.logs_sidebar_selected += 1;
                } else if !self.ui_state.logs_sidebar_debug_selected {
                    // Move to debug if at the end of regular nodes (always available)
                    self.ui_state.logs_sidebar_debug_selected = true;
                    self.ui_state.logs_sidebar_selected = 0;
                }
            }

            // Page Up/Down for log scrolling (page by page)
            KeyCode::PageUp => {
                self.log_reader.scroll_up_by_viewport_size();
            }

            KeyCode::PageDown => {
                self.log_reader.scroll_down_by_viewport_size();
            }

            // Open log level selection modal
            KeyCode::Char('l') => {
                // Set initial selection to current log level
                let current_level = self.log_reader.get_level_filter();
                self.ui_state.log_level_modal_selected = match current_level {
                    crate::messages::LogLevel::Error => 0,
                    crate::messages::LogLevel::Warn => 1,
                    crate::messages::LogLevel::Info => 2,
                    crate::messages::LogLevel::Debug => 3,
                    crate::messages::LogLevel::Trace => 4,
                };
                self.ui_state.show_log_level_modal = true;
            }

            // Start/stop selected node with 's' (only for individual nodes, not "All" or "Debug")
            KeyCode::Char('s') => {
                if !self.ui_state.logs_sidebar_debug_selected
                    && self.ui_state.logs_sidebar_selected > 0
                {
                    if let Some(&selected_node_id) = self
                        .ui_state
                        .logs_sidebar_nodes
                        .get(self.ui_state.logs_sidebar_selected - 1)
                    {
                        self.toggle_node(selected_node_id);
                    }
                }
            }

            // Restart selected node with 'r' (only for individual nodes, not "All" or "Debug")
            KeyCode::Char('r') => {
                if !self.ui_state.logs_sidebar_debug_selected
                    && self.ui_state.logs_sidebar_selected > 0
                {
                    if let Some(&selected_node_id) = self
                        .ui_state
                        .logs_sidebar_nodes
                        .get(self.ui_state.logs_sidebar_selected - 1)
                    {
                        self.restart_node(selected_node_id);
                    }
                }
            }

            _ => {}
        }
    }

    /// Handle keys when log level modal is open
    fn handle_log_level_modal_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Navigate up in modal
            KeyCode::Up => {
                if self.ui_state.log_level_modal_selected > 0 {
                    self.ui_state.log_level_modal_selected -= 1;
                }
            }

            // Navigate down in modal
            KeyCode::Down => {
                if self.ui_state.log_level_modal_selected < 4 {
                    self.ui_state.log_level_modal_selected += 1;
                }
            }

            // Select log level and close modal
            KeyCode::Enter => {
                let selected_level = match self.ui_state.log_level_modal_selected {
                    0 => crate::messages::LogLevel::Error,
                    1 => crate::messages::LogLevel::Warn,
                    2 => crate::messages::LogLevel::Info,
                    3 => crate::messages::LogLevel::Debug,
                    4 => crate::messages::LogLevel::Trace,
                    _ => unreachable!(),
                };

                self.log_reader.set_level_filter(selected_level);
                self.ui_state.show_log_level_modal = false;

                // Filter change will trigger background thread to send updated data automatically
            }

            // Close modal without selection
            KeyCode::Esc => {
                self.ui_state.show_log_level_modal = false;
            }

            _ => {}
        }
    }

    /// Handle keys in logs mode during shutdown (limited functionality)
    fn handle_logs_keys_during_shutdown(&mut self, key: KeyEvent) {
        match key.code {
            // Backtick "`" for "All" selection
            KeyCode::Char('`') => {
                self.ui_state.logs_sidebar_selected = 0;
                self.ui_state.logs_sidebar_debug_selected = false;
            }

            // Number keys 1-9 for quick node selection still work during shutdown
            KeyCode::Char(c) if c.is_ascii_digit() && c != '0' => {
                let num = c.to_digit(10).unwrap() as usize;
                if num <= self.ui_state.logs_sidebar_nodes.len() {
                    // "1-9" selects corresponding node
                    self.ui_state.logs_sidebar_selected = num;
                    self.ui_state.logs_sidebar_debug_selected = false;
                }
            }

            // "d" key for debug selection still works during shutdown
            KeyCode::Char('d') => {
                self.ui_state.logs_sidebar_debug_selected = true;
                self.ui_state.logs_sidebar_selected = 0; // Reset regular selection
            }

            // Alt+Up/Down for log scrolling still works during shutdown - put specific pattern first
            KeyCode::Up if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.log_reader.scroll_up(1);
            }

            KeyCode::Down if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.log_reader.scroll_down(1);
            }

            // Home/End for log scrolling still work during shutdown
            KeyCode::Home => {
                self.log_reader.scroll_to_top();
            }

            KeyCode::End => {
                self.log_reader.scroll_to_bottom();
            }

            // Up/Down arrow keys for sidebar navigation still work during shutdown
            KeyCode::Up => {
                if self.ui_state.logs_sidebar_debug_selected {
                    // Move from debug to last regular node or "All"
                    if self.ui_state.logs_sidebar_nodes.is_empty() {
                        self.ui_state.logs_sidebar_selected = 0; // "All"
                    } else {
                        self.ui_state.logs_sidebar_selected =
                            self.ui_state.logs_sidebar_nodes.len();
                    }
                    self.ui_state.logs_sidebar_debug_selected = false;
                } else if self.ui_state.logs_sidebar_selected > 0 {
                    self.ui_state.logs_sidebar_selected -= 1;
                }
            }

            KeyCode::Down => {
                let max_index = self.ui_state.logs_sidebar_nodes.len();
                if self.ui_state.logs_sidebar_selected < max_index {
                    self.ui_state.logs_sidebar_selected += 1;
                } else if !self.ui_state.logs_sidebar_debug_selected {
                    // Move to debug if at the end of regular nodes (always available)
                    self.ui_state.logs_sidebar_debug_selected = true;
                    self.ui_state.logs_sidebar_selected = 0;
                }
            }

            // Page Up/Down for log scrolling still works during shutdown
            KeyCode::PageUp => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.log_reader.scroll_up(page_size);
            }

            KeyCode::PageDown => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.log_reader.scroll_down(page_size);
            }

            _ => {}
        }
    }
}

// Note: No Default implementation - use App::new() instead since construction is sync
