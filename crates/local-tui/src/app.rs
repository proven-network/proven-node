//! Main application structure

use crate::{
    events::{Event, EventHandler, KeyEventResult},
    logs_viewer::LogReader,
    logs_writer::{DiskLogConfig, LogWriter},
    messages::{NodeId, NodeStatus, TuiMessage},
    node_manager::NodeManager,
    ui::{UiState, render_ui},
};
use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{DisableMouseCapture, KeyCode, KeyEvent},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use proven_attestation_mock::MockAttestor;
use proven_governance::Version;
use proven_governance_mock::MockGovernance;
use ratatui::{
    Terminal,
    backend::{Backend, CrosstermBackend},
};
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};
use tracing::{error, info};

/// Main application state
pub struct App {
    /// Whether the app should quit
    should_quit: bool,

    /// Whether we're in the process of shutting down
    shutting_down: bool,

    /// UI state
    ui_state: UiState,

    /// Node statuses
    nodes: HashMap<NodeId, (String, NodeStatus)>,

    /// Log writer for disk-based logging
    log_writer: LogWriter,

    /// Log reader for reading logs from disk
    log_reader: LogReader,

    /// Event handler
    event_handler: EventHandler,

    /// Message receiver for node operations
    message_receiver: mpsc::Receiver<TuiMessage>,

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
        // Create message channels (sync)
        let (message_sender, message_receiver) = mpsc::channel();
        let (command_sender, command_receiver) = mpsc::channel();

        // Create shared governance synchronously
        let governance = Arc::new(Self::create_shared_governance());

        // Generate session ID
        let session_id = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        info!("Starting TUI application with session_id: {}", session_id);

        // Create log writer with session_id-based config in /tmp (synchronous)
        let log_config = DiskLogConfig::new_with_session(&session_id);
        let log_writer = LogWriter::with_config(log_config).expect("Failed to create log writer");

        // Create log reader for the same session
        let session_dir = log_writer
            .get_session_dir()
            .expect("Failed to get session directory from log writer");
        let log_reader = LogReader::new(session_dir);

        // Create event handler (synchronous setup)
        let event_handler = EventHandler::new(command_sender);

        // Create and start node manager (sync operations)
        let node_manager = NodeManager::new(message_sender, governance, session_id);
        node_manager.start_command_handler(command_receiver);

        Self {
            should_quit: false,
            shutting_down: false,
            ui_state: UiState::new(),
            nodes: HashMap::new(),
            log_writer,
            log_reader,
            event_handler,
            message_receiver,
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

        // Start event listeners
        self.event_handler.listen_for_terminal_events();

        // Move the message receiver to the event handler
        let message_receiver = std::mem::replace(&mut self.message_receiver, {
            let (_, rx) = mpsc::channel();
            rx
        });
        self.event_handler.listen_for_messages(message_receiver);

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
                    info!("Signal-triggered shutdown initiated");
                    self.shutting_down = true;
                    // Trigger shutdown through event handler
                    self.event_handler.trigger_shutdown();
                }
            }

            // Log refresh is now handled by the background thread and UI rendering

            // Draw the UI (synchronous operation)
            terminal.draw(|frame| {
                render_ui(
                    frame,
                    &mut self.ui_state,
                    &self.nodes,
                    &self.log_reader,
                    self.shutting_down,
                );
            })?;

            // Handle events (blocking operation with timeout)
            if let Some(event) = self
                .event_handler
                .next_blocking(std::time::Duration::from_millis(100))
            {
                // Process event (synchronous)
                if let Err(e) = self.handle_event(event) {
                    error!("Error handling event: {e}");
                }
            }
        }

        info!("TUI application shutting down");
        Ok(())
    }

    /// Handle an event
    fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Input(crossterm_event) => {
                if let crossterm::event::Event::Key(key) = crossterm_event {
                    self.handle_key_event(key)?;
                }
            }

            Event::Message(message) => {
                self.handle_message(*message);
            }

            Event::Tick => {
                // Regular update tick - could be used for animations or periodic updates
            }
        }

        Ok(())
    }

    /// Handle keyboard input
    fn handle_key_event(&mut self, key: KeyEvent) -> Result<()> {
        // Let the event handler process global keys first
        let key_result = self
            .event_handler
            .handle_key_event(key, self.shutting_down)?;

        match key_result {
            KeyEventResult::ForceQuit => {
                // Force quit requested - exit immediately
                self.should_quit = true;
                return Ok(());
            }
            KeyEventResult::GracefulShutdown => {
                // Graceful shutdown initiated - set flag and wait for ShutdownComplete
                info!("Graceful shutdown initiated - waiting for nodes to stop");
                self.shutting_down = true;
                return Ok(());
            }
            KeyEventResult::Continue => {
                // Continue with normal processing
            }
        }

        // Handle global keys that should work even during shutdown
        match key.code {
            KeyCode::Char('?') => {
                // Help toggle always works, even during shutdown
                self.ui_state.show_help = !self.ui_state.show_help;
                return Ok(());
            }
            KeyCode::Esc => {
                // Close help or log level modal if open
                if self.ui_state.show_help {
                    self.ui_state.show_help = false;
                } else if self.ui_state.show_log_level_modal {
                    self.ui_state.show_log_level_modal = false;
                }
                return Ok(());
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

        Ok(())
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
                self.ui_state.scroll_up(1, &self.log_reader);
            }

            KeyCode::Down if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.ui_state.scroll_down(1, &self.log_reader);
            }

            // Home/End for log scrolling
            KeyCode::Home => {
                self.ui_state.scroll_to_top(&self.log_reader);
            }

            KeyCode::End => {
                self.ui_state.scroll_to_bottom(&self.log_reader);
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
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_up(page_size, &self.log_reader);
            }

            KeyCode::PageDown => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_down(page_size, &self.log_reader);
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
                        self.handle_start_stop_node(selected_node_id);
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
                        self.handle_restart_node(selected_node_id);
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
                self.ui_state.scroll_up(1, &self.log_reader);
            }

            KeyCode::Down if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.ui_state.scroll_down(1, &self.log_reader);
            }

            // Home/End for log scrolling still work during shutdown
            KeyCode::Home => {
                self.ui_state.scroll_to_top(&self.log_reader);
            }

            KeyCode::End => {
                self.ui_state.scroll_to_bottom(&self.log_reader);
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
                self.ui_state.scroll_up(page_size, &self.log_reader);
            }

            KeyCode::PageDown => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_down(page_size, &self.log_reader);
            }

            _ => {}
        }
    }

    /// Handle messages from async tasks
    fn handle_message(&mut self, message: TuiMessage) {
        match message {
            TuiMessage::NodeStarted { id, name } => {
                self.handle_node_started(id, name);
            }

            TuiMessage::NodeStopped { id } => {
                self.handle_node_stopped(id);
            }

            TuiMessage::NodeFailed { id, error } => {
                self.handle_node_failed(id, error);
            }

            TuiMessage::NodeStatusUpdate { id, status } => {
                self.handle_node_status_update(id, status);
            }

            TuiMessage::ShutdownComplete => {
                info!("Received shutdown complete signal, exiting TUI");
                self.should_quit = true;
            }
        }
    }

    /// Handle node started message
    fn handle_node_started(&mut self, id: NodeId, name: String) {
        info!("Node {name} ({id}) start method completed");
        // Update the name but preserve the current status (should be Starting)
        if let Some((_, current_status)) = self.nodes.get(&id) {
            let status = current_status.clone();
            self.nodes.insert(id, (name, status));
        } else {
            // Node doesn't exist yet, assume it's starting
            self.nodes.insert(id, (name, NodeStatus::Starting));
        }
    }

    /// Handle node stopped message
    fn handle_node_stopped(&mut self, id: NodeId) {
        if let Some((name, status)) = self.nodes.get_mut(&id) {
            info!("Node {name} ({id}) stopped");
            *status = NodeStatus::Stopped;
        }
    }

    /// Handle node failed message
    fn handle_node_failed(&mut self, id: NodeId, error: String) {
        if let Some((name, status)) = self.nodes.get_mut(&id) {
            error!("Node {name} ({id}) failed: {error}");
            *status = NodeStatus::Failed(error);
        }
    }

    /// Handle node status update message
    fn handle_node_status_update(&mut self, id: NodeId, status: NodeStatus) {
        if let Some((_, current_status)) = self.nodes.get_mut(&id) {
            *current_status = status;
        } else {
            // Node doesn't exist yet, create it with a placeholder name
            self.nodes.insert(id, (id.pokemon_name(), status));
        }
    }

    /// Handle start/stop for a selected node based on its current status
    #[allow(clippy::cognitive_complexity)]
    fn handle_start_stop_node(&self, node_id: NodeId) {
        if let Some((_, status)) = self.nodes.get(&node_id) {
            match status {
                NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Failed(_) => {
                    // Node is not running, so start it
                    let name = node_id.display_name();
                    let command = crate::messages::NodeCommand::StartNode {
                        id: node_id,
                        name,
                        config: None, // Let NodeManager create the config
                    };
                    let _ = self.event_handler.send_command(command);
                    info!("Starting node: {}", node_id.full_pokemon_name());
                }
                NodeStatus::Running | NodeStatus::Starting => {
                    // Node is running or starting, so stop it
                    let command = crate::messages::NodeCommand::StopNode { id: node_id };
                    let _ = self.event_handler.send_command(command);
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
            let command = crate::messages::NodeCommand::StartNode {
                id: node_id,
                name,
                config: None, // Let NodeManager create the config
            };
            let _ = self.event_handler.send_command(command);
            info!("Starting new node: {}", node_id.full_pokemon_name());
        }
    }

    /// Handle restart for a selected node
    fn handle_restart_node(&self, node_id: NodeId) {
        let command = crate::messages::NodeCommand::RestartNode { id: node_id };
        let _ = self.event_handler.send_command(command);
        info!("Restarting node: {}", node_id.full_pokemon_name());
    }
}

// Note: No Default implementation - use App::new() instead since construction is sync
