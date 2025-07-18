//! Main application structure

use crate::{
    logs_viewer::LogReader,
    logs_writer::LogWriter,
    node_id::TuiNodeId,
    node_manager::NodeManager,
    rpc_client::RpcClient,
    ui::{UiState, render_ui},
};
use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, KeyCode, KeyEvent, KeyModifiers,
        MouseButton, MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use proven_attestation_mock::MockAttestor;
use proven_local::NodeStatus;
use proven_topology::Version;
use proven_topology_mock::MockTopologyAdaptor;
use proven_util::Origin;
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
use tracing::{info, warn};

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
    fn create_shared_governance() -> MockTopologyAdaptor {
        // Create a version from mock attestation (synchronous)
        let pcrs = MockAttestor::new().pcrs_sync();
        let version = Version::from_pcrs(pcrs);

        // Create an empty governance instance that nodes can be added to
        MockTopologyAdaptor::new(
            Vec::new(),                          // No initial nodes
            vec![version],                       // Single version
            "http://localhost:3000".to_string(), // Default primary auth gateway
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
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
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
            if let Some(shutdown_flag) = &self.shutdown_flag
                && shutdown_flag.load(Ordering::SeqCst)
                && !self.shutting_down
            {
                info!(
                    "Signal-triggered shutdown initiated - sending shutdown command to all nodes"
                );
                self.node_manager.shutdown_all();
                self.shutting_down = true;
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
            if matches!(event::poll(Duration::from_millis(100)), Ok(true))
                && let Ok(crossterm_event) = event::read()
            {
                let event = Event::Input(crossterm_event);
                // Process event (synchronous)
                self.handle_event(event);
            }

            // Check mouse hold timeout for text selection
            self.check_mouse_hold_timeout();

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
                match crossterm_event {
                    crossterm::event::Event::Key(key) => {
                        self.handle_key_event(key);
                    }
                    crossterm::event::Event::Mouse(mouse) => {
                        self.handle_mouse_event(mouse);
                    }
                    _ => {
                        // Ignore other event types (resize, focus, paste, etc.)
                    }
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
                    self.ui_state.node_specializations_selected = vec![false; 7];
                    // Reset all selections
                }
            }

            _ => {}
        }

        KeyEventResult::Continue
    }

    /// Launch a new node with selected specializations from the modal
    fn launch_node_with_specializations(&self) {
        let id = TuiNodeId::new();
        let name = id.display_name();

        // Map UI selections to governance specializations
        let spec_types = [
            proven_topology::NodeSpecialization::BitcoinMainnet,
            proven_topology::NodeSpecialization::BitcoinTestnet,
            proven_topology::NodeSpecialization::EthereumMainnet,
            proven_topology::NodeSpecialization::EthereumHolesky,
            proven_topology::NodeSpecialization::EthereumSepolia,
            proven_topology::NodeSpecialization::RadixMainnet,
            proven_topology::NodeSpecialization::RadixStokenet,
        ];

        let mut specializations = HashSet::new();

        for (i, &is_selected) in self
            .ui_state
            .node_specializations_selected
            .iter()
            .enumerate()
        {
            if is_selected && let Some(spec_type) = spec_types.get(i) {
                specializations.insert(spec_type.clone());
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
    fn toggle_node(&self, node_id: TuiNodeId) {
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
    fn restart_node(&self, node_id: TuiNodeId) {
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
                    && let Some(&selected_node_id) = self
                        .ui_state
                        .logs_sidebar_nodes
                        .get(self.ui_state.logs_sidebar_selected - 1)
                {
                    self.toggle_node(selected_node_id);
                }
            }

            // Restart selected node with 'r' (only for individual nodes, not "All" or "Debug")
            KeyCode::Char('r') => {
                if !self.ui_state.logs_sidebar_debug_selected
                    && self.ui_state.logs_sidebar_selected > 0
                    && let Some(&selected_node_id) = self
                        .ui_state
                        .logs_sidebar_nodes
                        .get(self.ui_state.logs_sidebar_selected - 1)
                {
                    self.restart_node(selected_node_id);
                }
            }

            _ => {}
        }
    }

    /// Handle mouse events
    fn handle_mouse_event(&mut self, mouse: MouseEvent) {
        // Only process mouse events if not shutting down and no modals are open
        if self.shutting_down
            || self.ui_state.show_help
            || self.ui_state.show_log_level_modal
            || self.ui_state.show_rpc_modal
            || self.ui_state.show_node_type_modal
            || self.ui_state.show_application_manager_modal
        {
            return;
        }

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                self.handle_mouse_down(mouse.column, mouse.row);
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.handle_mouse_up(mouse.column, mouse.row);
            }
            MouseEventKind::ScrollUp => {
                self.handle_mouse_scroll_up(mouse.column, mouse.row);
            }
            MouseEventKind::ScrollDown => {
                self.handle_mouse_scroll_down(mouse.column, mouse.row);
            }
            _ => {
                // Ignore other mouse events (drag, right click, etc.)
            }
        }
    }

    /// Handle mouse down events
    fn handle_mouse_down(&mut self, column: u16, row: u16) {
        // Record mouse down state
        self.ui_state.mouse_state.is_down = true;
        self.ui_state.mouse_state.down_time = Some(std::time::Instant::now());

        // Handle immediate clicks for sidebar (only if not in logs area)
        if self.ui_state.sidebar_area.x <= column
            && column < self.ui_state.sidebar_area.x + self.ui_state.sidebar_area.width
            && self.ui_state.sidebar_area.y <= row
            && row < self.ui_state.sidebar_area.y + self.ui_state.sidebar_area.height
        {
            self.handle_sidebar_click(column, row);
        }
    }

    /// Handle mouse up events
    fn handle_mouse_up(&mut self, _column: u16, _row: u16) {
        // Clear mouse down state
        self.ui_state.mouse_state.is_down = false;
        self.ui_state.mouse_state.down_time = None;

        // If mouse capture was disabled, re-enable it
        if !self.ui_state.mouse_state.capture_enabled
            && let Err(e) = self.re_enable_mouse_capture()
        {
            warn!("Failed to re-enable mouse capture: {}", e);
        }
    }

    /// Disable mouse capture to allow native text selection
    fn disable_mouse_capture(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        execute!(std::io::stdout(), DisableMouseCapture)?;
        self.ui_state.mouse_state.capture_enabled = false;
        info!("Mouse capture disabled - text selection mode enabled");
        Ok(())
    }

    /// Re-enable mouse capture
    fn re_enable_mouse_capture(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        execute!(std::io::stdout(), EnableMouseCapture)?;
        self.ui_state.mouse_state.capture_enabled = true;
        info!("Mouse capture re-enabled");
        Ok(())
    }

    /// Check if mouse should be disabled for text selection
    fn check_mouse_hold_timeout(&mut self) {
        if !self.ui_state.mouse_state.is_down || !self.ui_state.mouse_state.capture_enabled {
            return;
        }

        if let Some(down_time) = self.ui_state.mouse_state.down_time {
            let hold_duration = std::time::Duration::from_millis(250); // 250ms threshold
            if down_time.elapsed() > hold_duration
                && let Err(e) = self.disable_mouse_capture()
            {
                warn!("Failed to disable mouse capture: {}", e);
            }
        }
    }

    /// Handle scroll wheel up events  
    fn handle_mouse_scroll_up(&self, column: u16, row: u16) {
        // Only scroll if mouse is over the logs area
        if self.ui_state.logs_area.x <= column
            && column < self.ui_state.logs_area.x + self.ui_state.logs_area.width
            && self.ui_state.logs_area.y <= row
            && row < self.ui_state.logs_area.y + self.ui_state.logs_area.height
        {
            self.log_reader.scroll_up(3); // Scroll 3 lines at a time
        }
    }

    /// Handle scroll wheel down events
    fn handle_mouse_scroll_down(&self, column: u16, row: u16) {
        // Only scroll if mouse is over the logs area
        if self.ui_state.logs_area.x <= column
            && column < self.ui_state.logs_area.x + self.ui_state.logs_area.width
            && self.ui_state.logs_area.y <= row
            && row < self.ui_state.logs_area.y + self.ui_state.logs_area.height
        {
            self.log_reader.scroll_down(3); // Scroll 3 lines at a time
        }
    }

    /// Handle clicks in the sidebar area
    const fn handle_sidebar_click(&mut self, column: u16, row: u16) {
        // Check if click is in debug area first (bottom section)
        if self.ui_state.sidebar_debug_area.x <= column
            && column < self.ui_state.sidebar_debug_area.x + self.ui_state.sidebar_debug_area.width
            && self.ui_state.sidebar_debug_area.y <= row
            && row < self.ui_state.sidebar_debug_area.y + self.ui_state.sidebar_debug_area.height
        {
            // Clicked on debug logs
            self.ui_state.logs_sidebar_debug_selected = true;
            self.ui_state.logs_sidebar_selected = 0;
            return;
        }

        // Check if click is in main area (Overview and nodes)
        if self.ui_state.sidebar_main_area.x <= column
            && column < self.ui_state.sidebar_main_area.x + self.ui_state.sidebar_main_area.width
            && self.ui_state.sidebar_main_area.y <= row
            && row < self.ui_state.sidebar_main_area.y + self.ui_state.sidebar_main_area.height
        {
            // Calculate which item was clicked based on row within main area
            let relative_row = row - self.ui_state.sidebar_main_area.y;

            // Main area layout:
            // Row 0: Empty line
            // Row 1: "Overview" (All logs)
            // Row 2: Empty line
            // Row 3+: Individual nodes

            if relative_row == 1 {
                // Clicked on Overview
                self.ui_state.logs_sidebar_selected = 0;
                self.ui_state.logs_sidebar_debug_selected = false;
            } else if relative_row >= 3 {
                // Calculate which node was clicked
                let node_index = relative_row - 3; // Adjust for empty lines at top

                if (node_index as usize) < self.ui_state.logs_sidebar_nodes.len() {
                    // Clicked on a valid node
                    self.ui_state.logs_sidebar_selected = (node_index + 1) as usize; // +1 because "All" is index 0
                    self.ui_state.logs_sidebar_debug_selected = false;
                }
            }
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
                        self.ui_state.app_manager_result =
                            Some(format!("❌ Failed to load applications: {e}"));
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
                0 => self.handle_app_list_keys(key),           // List view
                1 => self.handle_app_details_keys(key),        // Details view
                2 => self.handle_add_origin_keys(key),         // Add origin view
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
                if !self.ui_state.app_manager_applications.is_empty()
                    && self.ui_state.app_manager_selected_index > 0
                {
                    self.ui_state.app_manager_selected_index -= 1;
                }
            }

            // Navigate down
            KeyCode::Down => {
                if !self.ui_state.app_manager_applications.is_empty()
                    && self.ui_state.app_manager_selected_index
                        < self.ui_state.app_manager_applications.len() - 1
                {
                    self.ui_state.app_manager_selected_index += 1;
                }
            }

            // View application details
            KeyCode::Enter => {
                if let Some(app) = self
                    .ui_state
                    .app_manager_applications
                    .get(self.ui_state.app_manager_selected_index)
                {
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
                if !self.ui_state.app_manager_origin_input.is_empty()
                    && let Some(app) = &self.ui_state.app_manager_selected_application
                {
                    let origin_input = self.ui_state.app_manager_origin_input.clone();
                    self.add_origin_to_application(app.id, &origin_input);
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
                self.ui_state.app_manager_result = Some(format!("❌ Invalid origin format: {e}"));
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

                match self
                    .rpc_client
                    .add_allowed_origin(&url, application_id, origin)
                {
                    Ok(()) => {
                        self.ui_state.app_manager_result =
                            Some(format!("✅ Successfully added origin: {origin_str}"));

                        // Reload applications to get updated data
                        self.load_applications();

                        // Update selected application if it's still there
                        if let Some(updated_app) = self
                            .ui_state
                            .app_manager_applications
                            .iter()
                            .find(|app| app.id == application_id)
                        {
                            self.ui_state.app_manager_selected_application =
                                Some(updated_app.clone());
                        }

                        // Clear input and go back to details view
                        self.ui_state.app_manager_origin_input.clear();
                        self.ui_state.app_manager_view = 1;
                    }
                    Err(e) => {
                        self.ui_state.app_manager_result =
                            Some(format!("❌ Failed to add origin: {e}"));
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
                        self.ui_state.app_manager_result =
                            Some(format!("❌ Failed to create application: {e}"));
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
