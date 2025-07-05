//! Main application structure

use crate::{
    logs_viewer::LogReader,
    logs_writer::LogWriter,
    node_id::NodeId,
    node_manager::NodeManager,
    rpc_client::RpcClient,
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
    collections::HashSet,
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tracing::info;
use proven_util::Origin;

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

    /// RPC client for communicating with nodes
    rpc_client: RpcClient,

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

        // Create RPC client for management operations
        let rpc_client = RpcClient::new();

        Self {
            should_quit: false,
            shutting_down: false,
            ui_state: UiState::new(),
            node_manager,
            rpc_client,
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
                for (id, (name, status, _specializations)) in &nodes {
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

        // Handle modal-specific keys first (these take priority)
        if self.ui_state.show_node_type_modal {
            self.handle_node_type_modal_keys(key);
            return;
        }

        if self.ui_state.show_rpc_modal {
            self.handle_rpc_modal_keys(key);
            return;
        }

        if self.ui_state.show_application_manager_modal {
            self.handle_application_manager_modal_keys(key);
            return;
        }

        // Handle other global keys that should work even during shutdown
        match key.code {
            KeyCode::Char('?') => {
                // Help toggle always works, even during shutdown
                self.ui_state.show_help = !self.ui_state.show_help;
                return;
            }
            KeyCode::Char('a') => {
                // Open Application Manager modal (only if not shutting down)
                if !self.shutting_down {
                    self.ui_state.show_application_manager_modal = true;
                    self.ui_state.app_manager_view = 0; // Start with list view
                    self.ui_state.app_manager_selected_index = 0;
                    self.ui_state.app_manager_result = None; // Clear previous result
                    self.load_applications(); // Load applications when opening modal
                }
                return;
            }
            KeyCode::Char('c') => {
                // Open RPC modal (only if not shutting down)
                if !self.shutting_down {
                    self.ui_state.show_rpc_modal = true;
                    self.ui_state.rpc_modal_result = None; // Clear previous result
                }
                return;
            }
            KeyCode::Esc => {
                // Close help or any open modal
                if self.ui_state.show_help {
                    self.ui_state.show_help = false;
                } else if self.ui_state.show_node_type_modal {
                    self.ui_state.show_node_type_modal = false;
                } else if self.ui_state.show_log_level_modal {
                    self.ui_state.show_log_level_modal = false;
                } else if self.ui_state.show_application_manager_modal {
                    self.ui_state.show_application_manager_modal = false;
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
    fn handle_global_key_event(&mut self, key: KeyEvent) -> KeyEventResult {
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
                    self.ui_state.show_node_type_modal = true;
                    self.ui_state.node_modal_selected_index = 0; // Reset to first option
                    self.ui_state.node_specializations_selected = vec![false; 7]; // Reset all selections
                }
            }

            _ => {}
        }

        KeyEventResult::Continue
    }

    /// Launch a new node with selected specializations from the modal
    fn launch_node_with_specializations(&self) {
        let id = NodeId::new();
        let name = id.display_name();

        // Map UI selections to governance specializations
        let spec_types = [
            proven_governance::NodeSpecialization::BitcoinMainnet,
            proven_governance::NodeSpecialization::BitcoinTestnet,
            proven_governance::NodeSpecialization::EthereumMainnet,
            proven_governance::NodeSpecialization::EthereumHolesky,
            proven_governance::NodeSpecialization::EthereumSepolia,
            proven_governance::NodeSpecialization::RadixMainnet,
            proven_governance::NodeSpecialization::RadixStokenet,
        ];

        let mut specializations = HashSet::new();

        for (i, &is_selected) in self
            .ui_state
            .node_specializations_selected
            .iter()
            .enumerate()
        {
            if is_selected {
                if let Some(spec_type) = spec_types.get(i) {
                    specializations.insert(spec_type.clone());
                }
            }
        }

        // Log the node creation with its specializations
        if specializations.is_empty() {
            info!("Creating basic node: {}", name);
        } else {
            let spec_names: Vec<String> =
                specializations.iter().map(|s| format!("{s:?}")).collect();
            info!(
                "Creating specialized node: {} with specializations: {:?}",
                name, spec_names
            );
        }

        self.node_manager.start_node(id, &name, specializations);
    }

    /// Start/stop a node based on its current status
    #[allow(clippy::cognitive_complexity)]
    fn toggle_node(&self, node_id: NodeId) {
        // Get current node status from NodeManager
        let nodes = self.node_manager.get_nodes_for_ui();

        if let Some((_, status, specializations)) = nodes.get(&node_id) {
            match status {
                NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Failed(_) => {
                    // Node is not running, so start it
                    let name = node_id.display_name();
                    self.node_manager
                        .start_node(node_id, &name, specializations.clone());
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
            self.node_manager.start_node(node_id, &name, HashSet::new());
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

    /// Handle keys when node type modal is open
    fn handle_node_type_modal_keys(&mut self, key: KeyEvent) {
        let num_specializations = 7;
        let max_index = num_specializations; // 0-6 for specializations, 7 for launch button

        match key.code {
            // Navigate up in modal
            KeyCode::Up => {
                if self.ui_state.node_modal_selected_index > 0 {
                    self.ui_state.node_modal_selected_index -= 1;
                }
            }

            // Navigate down in modal
            KeyCode::Down => {
                if self.ui_state.node_modal_selected_index < max_index {
                    self.ui_state.node_modal_selected_index += 1;
                }
            }

            // Toggle specialization or launch node
            KeyCode::Enter | KeyCode::Char(' ') => {
                if self.ui_state.node_modal_selected_index < num_specializations {
                    // Toggle specialization
                    let index = self.ui_state.node_modal_selected_index;
                    if let Some(selected) =
                        self.ui_state.node_specializations_selected.get_mut(index)
                    {
                        *selected = !*selected;
                    }
                } else {
                    // Launch button pressed
                    self.ui_state.show_node_type_modal = false;
                    self.launch_node_with_specializations();
                }
            }

            // Close modal without selection
            KeyCode::Esc => {
                self.ui_state.show_node_type_modal = false;
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

    /// Handle keys when RPC modal is open
    fn handle_rpc_modal_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Close modal
            KeyCode::Esc => {
                self.ui_state.show_rpc_modal = false;
                self.ui_state.rpc_modal_result = None;
            }

            // Switch tabs
            KeyCode::Tab => {
                self.ui_state.rpc_modal_tab_selected =
                    (self.ui_state.rpc_modal_tab_selected + 1) % 2;
                self.ui_state.rpc_modal_command_selected = 0; // Reset command selection when switching tabs
            }

            // Navigate commands
            KeyCode::Up => {
                if self.ui_state.rpc_modal_command_selected > 0 {
                    self.ui_state.rpc_modal_command_selected -= 1;
                }
            }

            KeyCode::Down => {
                let max_commands: usize = if self.ui_state.rpc_modal_tab_selected == 0 {
                    4 // Management commands: WhoAmI, CreateApplication, Identify, Anonymize
                } else {
                    0 // Application commands: under construction
                };

                if self.ui_state.rpc_modal_command_selected < max_commands.saturating_sub(1) {
                    self.ui_state.rpc_modal_command_selected += 1;
                }
            }

            // Execute command
            KeyCode::Enter => {
                if self.ui_state.rpc_modal_tab_selected == 0 {
                    // Management RPC
                    self.execute_management_rpc_command();
                } else {
                    // Application RPC - under construction
                    self.ui_state.rpc_modal_result =
                        Some("Application RPC is under construction".to_string());
                }
            }

            _ => {}
        }
    }

    /// Execute the selected management RPC command
    fn execute_management_rpc_command(&mut self) {
        // Find a running node
        let nodes = self.node_manager.get_nodes_for_ui();
        let running_node = nodes
            .iter()
            .find(|(_, (_, status, _specializations))| matches!(status, NodeStatus::Running));

        if let Some((node_id, (name, _, _specializations))) = running_node {
            let node_url = self.node_manager.get_node_url(*node_id);

            if let Some(url) = node_url {
                let command_name = match self.ui_state.rpc_modal_command_selected {
                    0 => "WhoAmI",
                    1 => "CreateApplication",
                    2 => "Identify",
                    3 => "Anonymize",
                    _ => "Unknown",
                };

                info!(
                    "Executing {} command via RPC with node: {} ({}) at {}",
                    command_name,
                    name,
                    node_id.full_pokemon_name(),
                    url
                );

                // Execute the command and store result
                let result = match self.ui_state.rpc_modal_command_selected {
                    0 => {
                        // WhoAmI
                        match self.rpc_client.who_am_i(&url) {
                            Ok(response) => match response {
                                proven_core::WhoAmIResponse::Anonymous { origin, session_id } => {
                                    format!(
                                        "✅ WhoAmI: Anonymous session {session_id} from {origin}"
                                    )
                                }
                                proven_core::WhoAmIResponse::Identified {
                                    identity: _,
                                    origin,
                                    session_id,
                                } => {
                                    format!(
                                        "✅ WhoAmI: Identified session {session_id} from {origin}"
                                    )
                                }
                                proven_core::WhoAmIResponse::Failure(error) => {
                                    format!("⚠️ WhoAmI failed: {error}")
                                }
                            },
                            Err(e) => format!("❌ Failed to execute WhoAmI: {e}"),
                        }
                    }
                    1 => {
                        // CreateApplication
                        match self.rpc_client.create_application(&url, "test-app") {
                            Ok(app_id) => {
                                format!("✅ Application created successfully: {app_id}")
                            }
                            Err(e) => format!("❌ Failed to create application: {e}"),
                        }
                    }
                    2 => {
                        // Identify
                        match self.rpc_client.identify(&url) {
                            Ok(()) => "✅ Management session identified successfully".to_string(),
                            Err(e) => format!("❌ Failed to identify: {e}"),
                        }
                    }
                    3 => {
                        // Anonymize
                        match self.rpc_client.anonymize(&url) {
                            Ok(()) => "✅ Management session anonymized successfully".to_string(),
                            Err(e) => format!("❌ Failed to anonymize: {e}"),
                        }
                    }
                    _ => "❌ Unknown command".to_string(),
                };

                self.ui_state.rpc_modal_result = Some(result);
            } else {
                self.ui_state.rpc_modal_result = Some(format!(
                    "❌ Could not determine URL for node {}",
                    node_id.full_pokemon_name()
                ));
            }
        } else {
            self.ui_state.rpc_modal_result =
                Some("❌ No running nodes available for RPC operations".to_string());
        }
    }

    /// Load applications owned by the current user
    fn load_applications(&mut self) {
        // Find a running node
        let nodes = self.node_manager.get_nodes_for_ui();
        let running_node = nodes
            .iter()
            .find(|(_, (_, status, _specializations))| matches!(status, NodeStatus::Running));

        if let Some((node_id, (name, _, _specializations))) = running_node {
            let node_url = self.node_manager.get_node_url(*node_id);

            if let Some(url) = node_url {
                info!(
                    "Loading applications via RPC with node: {} ({}) at {}",
                    name,
                    node_id.full_pokemon_name(),
                    url
                );

                match self.rpc_client.list_applications_by_owner(&url) {
                    Ok(applications) => {
                        info!("Loaded {} applications", applications.len());
                        self.ui_state.app_manager_applications = applications;
                        self.ui_state.app_manager_result = Some(format!(
                            "✅ Loaded {} applications",
                            self.ui_state.app_manager_applications.len()
                        ));
                    }
                    Err(e) => {
                        self.ui_state.app_manager_result = Some(format!(
                            "❌ Failed to load applications: {e}"
                        ));
                    }
                }
            } else {
                self.ui_state.app_manager_result = Some(format!(
                    "❌ Could not determine URL for node {}",
                    node_id.full_pokemon_name()
                ));
            }
        } else {
            self.ui_state.app_manager_result =
                Some("❌ No running nodes available for application operations".to_string());
        }
    }

    /// Handle keys when application manager modal is open
    fn handle_application_manager_modal_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Close modal
            KeyCode::Esc => {
                self.ui_state.show_application_manager_modal = false;
                self.ui_state.app_manager_result = None;
            }

            // Handle different views
            _ => match self.ui_state.app_manager_view {
                0 => self.handle_app_list_keys(key),        // List view
                1 => self.handle_app_details_keys(key),     // Details view
                2 => self.handle_add_origin_keys(key),      // Add origin view
                3 => self.handle_create_application_keys(key), // Create application view
                _ => {}
            },
        }
    }

    /// Handle keys in application list view
    fn handle_app_list_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Navigate up
            KeyCode::Up => {
                if !self.ui_state.app_manager_applications.is_empty() && self.ui_state.app_manager_selected_index > 0 {
                    self.ui_state.app_manager_selected_index -= 1;
                }
            }

            // Navigate down
            KeyCode::Down => {
                if !self.ui_state.app_manager_applications.is_empty() 
                    && self.ui_state.app_manager_selected_index < self.ui_state.app_manager_applications.len() - 1 {
                    self.ui_state.app_manager_selected_index += 1;
                }
            }

            // View application details
            KeyCode::Enter => {
                if let Some(app) = self.ui_state.app_manager_applications.get(self.ui_state.app_manager_selected_index) {
                    self.ui_state.app_manager_selected_application = Some(app.clone());
                    self.ui_state.app_manager_view = 1; // Switch to details view
                }
            }

            // Create new application
            KeyCode::Char('c') => {
                self.ui_state.app_manager_view = 3; // Switch to create application view
                self.ui_state.app_manager_result = None;
            }

            _ => {}
        }
    }

    /// Handle keys in application details view
    fn handle_app_details_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Go to add origin view
            KeyCode::Char('a') => {
                self.ui_state.app_manager_view = 2; // Switch to add origin view
                self.ui_state.app_manager_origin_input.clear();
                self.ui_state.app_manager_result = None;
            }

            // Go back to list view
            KeyCode::Backspace => {
                self.ui_state.app_manager_view = 0; // Switch to list view
                self.ui_state.app_manager_selected_application = None;
            }

            _ => {}
        }
    }

    /// Handle keys in add origin view
    fn handle_add_origin_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Add the origin
            KeyCode::Enter => {
                if !self.ui_state.app_manager_origin_input.is_empty() {
                    if let Some(app) = &self.ui_state.app_manager_selected_application {
                        let origin_input = self.ui_state.app_manager_origin_input.clone();
                        self.add_origin_to_application(app.id, &origin_input);
                    }
                }
            }

            // Go back to details view
            KeyCode::Backspace => {
                self.ui_state.app_manager_view = 1; // Switch to details view
                self.ui_state.app_manager_origin_input.clear();
                self.ui_state.app_manager_result = None;
            }

            // Handle text input
            KeyCode::Char(c) => {
                self.ui_state.app_manager_origin_input.push(c);
            }

            // Handle backspace in input
            KeyCode::Delete => {
                self.ui_state.app_manager_origin_input.pop();
            }

            _ => {}
        }
    }

    /// Add an origin to an application
    fn add_origin_to_application(&mut self, application_id: uuid::Uuid, origin_str: &str) {
        // Parse the origin string
        let origin = match origin_str.parse::<Origin>() {
            Ok(origin) => origin,
            Err(e) => {
                self.ui_state.app_manager_result = Some(format!(
                    "❌ Invalid origin format: {e}"
                ));
                return;
            }
        };

        // Find a running node
        let nodes = self.node_manager.get_nodes_for_ui();
        let running_node = nodes
            .iter()
            .find(|(_, (_, status, _specializations))| matches!(status, NodeStatus::Running));

        if let Some((node_id, (name, _, _specializations))) = running_node {
            let node_url = self.node_manager.get_node_url(*node_id);

            if let Some(url) = node_url {
                info!(
                    "Adding origin {} to application {} via RPC with node: {} ({}) at {}",
                    origin_str,
                    application_id,
                    name,
                    node_id.full_pokemon_name(),
                    url
                );

                match self.rpc_client.add_allowed_origin(&url, application_id, origin) {
                    Ok(()) => {
                        self.ui_state.app_manager_result = Some(format!(
                            "✅ Successfully added origin: {origin_str}"
                        ));
                        
                        // Reload applications to get updated data
                        self.load_applications();
                        
                        // Update selected application if it's still there
                        if let Some(updated_app) = self.ui_state.app_manager_applications
                            .iter()
                            .find(|app| app.id == application_id) {
                            self.ui_state.app_manager_selected_application = Some(updated_app.clone());
                        }

                        // Clear input and go back to details view
                        self.ui_state.app_manager_origin_input.clear();
                        self.ui_state.app_manager_view = 1;
                    }
                    Err(e) => {
                        self.ui_state.app_manager_result = Some(format!(
                            "❌ Failed to add origin: {e}"
                        ));
                    }
                }
            } else {
                self.ui_state.app_manager_result = Some(format!(
                    "❌ Could not determine URL for node {}",
                    node_id.full_pokemon_name()
                ));
            }
        } else {
            self.ui_state.app_manager_result =
                Some("❌ No running nodes available for application operations".to_string());
        }
    }

    /// Handle keys in create application view
    fn handle_create_application_keys(&mut self, key: KeyEvent) {
        match key.code {
            // Create the application
            KeyCode::Enter => {
                self.create_new_application();
            }

            // Go back to list view
            KeyCode::Backspace => {
                self.ui_state.app_manager_view = 0; // Switch to list view
                self.ui_state.app_manager_result = None;
            }

            _ => {}
        }
    }

    /// Create a new application
    fn create_new_application(&mut self) {
        // Find a running node
        let nodes = self.node_manager.get_nodes_for_ui();
        let running_node = nodes
            .iter()
            .find(|(_, (_, status, _specializations))| matches!(status, NodeStatus::Running));

        if let Some((node_id, (name, _, _specializations))) = running_node {
            let node_url = self.node_manager.get_node_url(*node_id);

            if let Some(url) = node_url {
                info!(
                    "Creating new application via RPC with node: {} ({}) at {}",
                    name,
                    node_id.full_pokemon_name(),
                    url
                );

                match self.rpc_client.create_application(&url, "new-app") {
                    Ok(application_id) => {
                        self.ui_state.app_manager_result = Some(format!(
                            "✅ Successfully created application: {application_id}"
                        ));
                        
                        // Reload applications to get updated data including the new application
                        self.load_applications();
                        
                        // Go back to list view to show the new application
                        self.ui_state.app_manager_view = 0;
                    }
                    Err(e) => {
                        self.ui_state.app_manager_result = Some(format!(
                            "❌ Failed to create application: {e}"
                        ));
                    }
                }
            } else {
                self.ui_state.app_manager_result = Some(format!(
                    "❌ Could not determine URL for node {}",
                    node_id.full_pokemon_name()
                ));
            }
        } else {
            self.ui_state.app_manager_result =
                Some("❌ No running nodes available for application operations".to_string());
        }
    }
}

// Note: No Default implementation - use App::new() instead since construction is sync
