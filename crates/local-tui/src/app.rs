//! Main application structure

use crate::{
    events::{Event, EventHandler, KeyEventResult},
    logs::LogCollector,
    messages::{NodeId, NodeStatus, TuiMessage},
    node_manager::NodeManager,
    ui::{UiState, render_ui},
};
use anyhow::Result;
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

/// The main TUI application
pub struct App {
    /// Whether the app should quit
    should_quit: bool,

    /// Whether we're in the process of shutting down
    shutting_down: bool,

    /// UI state
    ui_state: UiState,

    /// Node statuses
    nodes: HashMap<NodeId, (String, NodeStatus)>,

    /// Log collector
    log_collector: LogCollector,

    /// Event handler
    event_handler: EventHandler,

    /// Message receiver for node operations
    message_receiver: mpsc::Receiver<TuiMessage>,

    /// Message sender (for tracing layer)
    message_sender: mpsc::Sender<TuiMessage>,

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

        // Create log collector (synchronous)
        let log_collector = LogCollector::new();

        // Create event handler (synchronous setup)
        let event_handler = EventHandler::new(command_sender, governance.clone());

        // Create and start node manager (sync operations)
        let node_manager = NodeManager::new(message_sender.clone(), governance);
        node_manager.start_command_handler(command_receiver);

        Self {
            should_quit: false,
            shutting_down: false,
            ui_state: UiState::new(),
            nodes: HashMap::new(),
            log_collector,
            event_handler,
            message_receiver,
            message_sender,
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

    /// Get the log collector for the tracing layer
    #[must_use]
    pub fn get_log_collector(&self) -> LogCollector {
        self.log_collector.clone()
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

        // Initialize cached logs (synchronous operation)
        let initial_logs = self
            .log_collector
            .get_filtered_logs_blocking(std::time::Duration::from_millis(100));
        self.ui_state.update_cached_logs(initial_logs);

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

            // Refresh logs periodically (synchronous operation)
            if self.ui_state.should_refresh_logs() {
                let fresh_logs = self
                    .log_collector
                    .get_filtered_logs_blocking(std::time::Duration::from_millis(50));
                self.ui_state.update_cached_logs(fresh_logs);
            }

            // Draw the UI (synchronous operation)
            terminal.draw(|frame| {
                render_ui(
                    frame,
                    &mut self.ui_state,
                    &self.nodes,
                    &self.log_collector,
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

            Event::Resize(_width, _height) => {
                // Terminal was resized - ratatui handles this automatically
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
                // Close help if open, otherwise initiate graceful shutdown
                if self.ui_state.show_help {
                    self.ui_state.show_help = false;
                } else if !self.shutting_down {
                    info!("Initiating graceful shutdown of all nodes...");
                    self.shutting_down = true;
                    // Let the event handler trigger shutdown
                    self.event_handler.trigger_shutdown();
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
    fn handle_logs_keys(&mut self, key: KeyEvent) {
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
                self.ui_state.scroll_up(1);
            }

            KeyCode::Down if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.ui_state.scroll_down(1);
            }

            // Home/End for sidebar navigation
            KeyCode::Home => {
                self.ui_state.logs_sidebar_selected = 0; // "All"
                self.ui_state.logs_sidebar_debug_selected = false;
            }

            KeyCode::End => {
                // End goes to debug if available, otherwise last node
                if self
                    .log_collector
                    .get_nodes_with_logs_blocking(std::time::Duration::from_millis(50))
                    .contains(&crate::messages::MAIN_THREAD_NODE_ID)
                {
                    self.ui_state.logs_sidebar_debug_selected = true;
                    self.ui_state.logs_sidebar_selected = 0;
                } else {
                    let max_index = self.ui_state.logs_sidebar_nodes.len();
                    self.ui_state.logs_sidebar_selected = max_index;
                    self.ui_state.logs_sidebar_debug_selected = false;
                }
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
                } else if !self.ui_state.logs_sidebar_debug_selected
                    && self
                        .log_collector
                        .get_nodes_with_logs_blocking(std::time::Duration::from_millis(50))
                        .contains(&crate::messages::MAIN_THREAD_NODE_ID)
                {
                    // Move to debug if at the end of regular nodes
                    self.ui_state.logs_sidebar_debug_selected = true;
                    self.ui_state.logs_sidebar_selected = 0;
                }
            }

            // Page Up/Down for log scrolling (page by page)
            KeyCode::PageUp => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_up(page_size);
            }

            KeyCode::PageDown => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_down(page_size);
            }

            // Toggle auto-scroll
            KeyCode::Char('a') => {
                self.ui_state.auto_scroll = !self.ui_state.auto_scroll;
                if self.ui_state.auto_scroll {
                    self.ui_state.scroll_to_bottom();
                }
            }

            // Clear logs
            KeyCode::Char('c') => {
                self.log_collector.clear();
                self.ui_state.cached_logs.clear();
                self.ui_state.log_scroll = 0;
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
                self.ui_state.scroll_up(1);
            }

            KeyCode::Down if key.modifiers.contains(crossterm::event::KeyModifiers::ALT) => {
                self.ui_state.scroll_down(1);
            }

            // Home/End for sidebar navigation still work during shutdown
            KeyCode::Home => {
                self.ui_state.logs_sidebar_selected = 0; // "All"
                self.ui_state.logs_sidebar_debug_selected = false;
            }

            KeyCode::End => {
                // End goes to debug if available, otherwise last node
                if self
                    .log_collector
                    .get_nodes_with_logs_blocking(std::time::Duration::from_millis(50))
                    .contains(&crate::messages::MAIN_THREAD_NODE_ID)
                {
                    self.ui_state.logs_sidebar_debug_selected = true;
                    self.ui_state.logs_sidebar_selected = 0;
                } else {
                    let max_index = self.ui_state.logs_sidebar_nodes.len();
                    self.ui_state.logs_sidebar_selected = max_index;
                    self.ui_state.logs_sidebar_debug_selected = false;
                }
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
                } else if !self.ui_state.logs_sidebar_debug_selected
                    && self
                        .log_collector
                        .get_nodes_with_logs_blocking(std::time::Duration::from_millis(50))
                        .contains(&crate::messages::MAIN_THREAD_NODE_ID)
                {
                    // Move to debug if at the end of regular nodes
                    self.ui_state.logs_sidebar_debug_selected = true;
                    self.ui_state.logs_sidebar_selected = 0;
                }
            }

            // Page Up/Down for log scrolling still works during shutdown
            KeyCode::PageUp => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_up(page_size);
            }

            KeyCode::PageDown => {
                let page_size = self.ui_state.log_viewport_height.max(1);
                self.ui_state.scroll_down(page_size);
            }

            // Toggle auto-scroll
            KeyCode::Char('a') => {
                self.ui_state.auto_scroll = !self.ui_state.auto_scroll;
                if self.ui_state.auto_scroll {
                    self.ui_state.scroll_to_bottom();
                }
            }

            _ => {}
        }
    }

    /// Handle messages from async tasks
    fn handle_message(&mut self, message: TuiMessage) {
        match message {
            TuiMessage::NodeStarted {
                id,
                name,
                config: _,
            } => {
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

            TuiMessage::LogEntry(entry) => {
                self.log_collector.add_log(entry);
            }

            TuiMessage::SystemMetrics { .. } => {
                // TODO: Handle system metrics
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
}

// Note: No Default implementation - use App::new() instead since construction is sync
