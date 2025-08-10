//! Main application structure using LocalCluster backend

use crate::{
    cluster_adapter::ClusterAdapter,
    logs_reader::LogReader,
    node_id::TuiNodeId,
    rpc_operations::RpcOperations,
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
use proven_local_cluster::LogLevel;
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
use tracing::{info, warn};

/// Events that can occur in the TUI
#[derive(Debug, Clone)]
enum Event {
    /// Terminal input event (keyboard, mouse, etc.)
    Input(crossterm::event::Event),
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

    /// Cluster adapter - manages nodes through LocalCluster
    cluster_adapter: ClusterAdapter,

    /// RPC operations handler
    rpc_ops: RpcOperations,

    /// Log reader for reading logs from LocalCluster's database
    log_reader: Option<LogReader>,

    /// Shutdown flag set by signal handler
    shutdown_flag: Option<Arc<AtomicBool>>,
}

impl App {
    /// Create a new application
    pub fn new() -> Result<Self> {
        // Generate session ID
        let session_id = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        info!("Starting TUI application with session_id: {}", session_id);

        // Create log directory
        let log_dir = PathBuf::from(format!("/tmp/proven/{session_id}"));
        std::fs::create_dir_all(&log_dir)?;

        // Create cluster adapter
        let cluster_adapter = ClusterAdapter::new(session_id.clone(), &log_dir)?;

        // Create RPC operations handler
        let rpc_ops = RpcOperations::new();

        // Log reader will be initialized after cluster starts creating logs
        let log_reader = None;

        Ok(Self {
            should_quit: false,
            shutting_down: false,
            ui_state: UiState::new(),
            cluster_adapter,
            rpc_ops,
            log_reader,
            shutdown_flag: None,
        })
    }

    /// Initialize log reader once cluster is running
    fn init_log_reader(&mut self) {
        if self.log_reader.is_none()
            && let Some(db_path) = self.cluster_adapter.get_log_db_path()
        {
            match LogReader::new(db_path) {
                Ok(reader) => {
                    reader.request_initial_data();
                    self.log_reader = Some(reader);
                    info!("Initialized log reader");
                }
                Err(e) => {
                    warn!("Failed to initialize log reader: {}", e);
                }
            }
        }
    }

    /// Set the shutdown flag for signal handling
    pub fn set_shutdown_flag(&mut self, shutdown_flag: Arc<AtomicBool>) {
        self.shutdown_flag = Some(shutdown_flag);
    }

    /// Run the application with synchronous event handling
    pub fn run(&mut self) -> Result<()> {
        // Setup terminal
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Main loop
        let result = self.run_loop(&mut terminal);

        // Cleanup terminal
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

    /// Main application loop
    fn run_loop<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()> {
        info!("Starting TUI application");

        loop {
            // Check for signal-initiated shutdown
            if let Some(flag) = &self.shutdown_flag
                && flag.load(Ordering::Relaxed)
                && !self.shutting_down
            {
                info!("Shutdown signal received");
                self.initiate_shutdown();
            }

            // Initialize log reader if needed
            self.init_log_reader();

            // Update UI state with current node information
            self.update_ui_state();

            // Draw UI
            terminal.draw(|f| render_ui(f, &mut self.ui_state))?;

            // Handle events
            if event::poll(Duration::from_millis(100))? {
                let evt = Event::Input(event::read()?);
                match self.handle_event(evt) {
                    KeyEventResult::ForceQuit => {
                        info!("Force quit requested");
                        break;
                    }
                    KeyEventResult::GracefulShutdown => {
                        info!("Graceful shutdown initiated");
                        self.initiate_shutdown();
                    }
                    KeyEventResult::Continue => {}
                }
            }

            // Check if shutdown is complete
            if self.shutting_down && self.cluster_adapter.is_shutdown_complete() {
                info!("All nodes shut down, exiting");
                self.should_quit = true;
            }

            if self.should_quit {
                break;
            }

            // Update logs with current log level filter
            if let Some(reader) = &self.log_reader {
                reader.poll_updates();
                if let Some(new_logs) = reader.get_new_logs(Some(self.ui_state.log_level_filter)) {
                    self.ui_state.update_logs(new_logs);
                }
            }
        }

        Ok(())
    }

    /// Update UI state with current node information
    fn update_ui_state(&mut self) {
        let nodes = self.cluster_adapter.get_nodes_for_ui();
        self.ui_state.update_nodes(nodes);
    }

    /// Handle events
    fn handle_event(&mut self, event: Event) -> KeyEventResult {
        match event {
            Event::Input(crossterm::event::Event::Key(key)) => self.handle_key(key),
            Event::Input(crossterm::event::Event::Mouse(mouse)) => {
                self.handle_mouse(mouse);
                KeyEventResult::Continue
            }
            _ => KeyEventResult::Continue,
        }
    }

    /// Handle keyboard input
    fn handle_key(&mut self, key: KeyEvent) -> KeyEventResult {
        // Global shortcuts
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
            if self.shutting_down {
                return KeyEventResult::ForceQuit;
            } else {
                return KeyEventResult::GracefulShutdown;
            }
        }

        // Modal handling
        if self.ui_state.has_active_modal() {
            self.handle_modal_key(key);
            return KeyEventResult::Continue;
        }

        // Help overlay - toggle on 'h' key and close on any key when open
        if self.ui_state.show_help {
            self.ui_state.show_help = false;
            return KeyEventResult::Continue;
        }

        // Main keyboard handling
        match key.code {
            // Navigation
            KeyCode::Up => self.ui_state.select_previous_sidebar(),
            KeyCode::Down => self.ui_state.select_next_sidebar(),

            // Quick jump to nodes
            KeyCode::Char('`') => {
                self.ui_state.logs_sidebar_selected = 0;
                self.ui_state.logs_sidebar_debug_selected = false;
                self.ui_state.reset_scroll_to_autoscroll();
            }
            KeyCode::Char('d') => {
                self.ui_state.logs_sidebar_debug_selected = true;
                self.ui_state.reset_scroll_to_autoscroll();
            }
            KeyCode::Char(c) if c.is_ascii_digit() && c != '0' => {
                let index = c.to_digit(10).unwrap() as usize;
                if index <= self.ui_state.logs_sidebar_nodes.len() {
                    self.ui_state.logs_sidebar_selected = index;
                    self.ui_state.logs_sidebar_debug_selected = false;
                    self.ui_state.reset_scroll_to_autoscroll();
                }
            }

            // Node control
            KeyCode::Char('n') => self.ui_state.open_new_node_modal(),
            KeyCode::Char('s') => self.start_selected_node(),
            KeyCode::Char('x') => self.stop_selected_node(),
            KeyCode::Char('r') => self.restart_selected_node(),
            KeyCode::Char('a') => self.open_application_modal(),

            // Log scrolling
            KeyCode::PageUp => self.ui_state.scroll_logs_page_up(),
            KeyCode::PageDown => self.ui_state.scroll_logs_page_down(),
            KeyCode::Home => self.ui_state.scroll_logs_home(),
            KeyCode::End => self.ui_state.scroll_logs_end(),

            // Clear logs
            KeyCode::Char('c') => {
                // Clear both the UI state and the database
                self.ui_state.clear_logs();
                if let Err(e) = self.cluster_adapter.clear_logs() {
                    self.ui_state
                        .show_error(format!("Failed to clear logs: {e}"));
                }
            }

            // Help
            KeyCode::Char('h') => self.ui_state.toggle_help(),

            // Log level filter
            KeyCode::Char('l') => {
                // Set initial selection to current log level
                self.ui_state.log_level_modal_selected = match self.ui_state.log_level_filter {
                    LogLevel::Error => 0,
                    LogLevel::Warn => 1,
                    LogLevel::Info => 2,
                    LogLevel::Debug => 3,
                    LogLevel::Trace => 4,
                };
                self.ui_state.show_log_level_modal = true;
            }

            // Quit
            KeyCode::Char('q') => return KeyEventResult::GracefulShutdown,

            _ => {}
        }

        KeyEventResult::Continue
    }

    /// Get the selected node based on sidebar selection
    fn get_selected_node(&self) -> Option<TuiNodeId> {
        if self.ui_state.logs_sidebar_debug_selected {
            None // Debug logs don't correspond to a node
        } else if self.ui_state.logs_sidebar_selected == 0 {
            // "All" selected - get first node if any
            self.ui_state.logs_sidebar_nodes.first().copied()
        } else {
            // Specific node selected
            self.ui_state
                .logs_sidebar_nodes
                .get(self.ui_state.logs_sidebar_selected - 1)
                .copied()
        }
    }

    /// Handle modal keyboard input
    fn handle_modal_key(&mut self, key: KeyEvent) {
        // Handle input for different modal types - let each handler manage ESC
        if self.ui_state.show_log_level_modal {
            self.handle_log_level_modal_key(key);
        } else if self.ui_state.show_node_type_modal {
            self.handle_node_type_modal_key(key);
        } else if self.ui_state.application_modal_active {
            self.handle_application_modal_key(key);
        }
        // Add other modal handlers as needed
    }

    /// Handle node type modal input
    fn handle_node_type_modal_key(&mut self, key: KeyEvent) {
        let num_specializations = 7; // Number of specialization options

        match key.code {
            // Navigate up
            KeyCode::Up => {
                if self.ui_state.node_modal_selected_index > 0 {
                    self.ui_state.node_modal_selected_index -= 1;
                } else {
                    // Wrap to bottom (launch button)
                    self.ui_state.node_modal_selected_index = num_specializations;
                }
            }

            // Navigate down
            KeyCode::Down => {
                if self.ui_state.node_modal_selected_index < num_specializations {
                    self.ui_state.node_modal_selected_index += 1;
                } else {
                    // Wrap to top
                    self.ui_state.node_modal_selected_index = 0;
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

    /// Launch a node with selected specializations
    fn launch_node_with_specializations(&mut self) {
        let specs = self.ui_state.get_selected_specializations();
        let tui_id = TuiNodeId::new();
        let name = format!("node-{}", tui_id.short_id());

        if let Err(e) = self.cluster_adapter.start_node(tui_id, &name, specs) {
            self.ui_state
                .show_error(format!("Failed to start node: {e}"));
        }
    }

    /// Handle log level modal input
    fn handle_log_level_modal_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up => {
                if self.ui_state.log_level_modal_selected > 0 {
                    self.ui_state.log_level_modal_selected -= 1;
                }
            }
            KeyCode::Down => {
                if self.ui_state.log_level_modal_selected < 5 {
                    self.ui_state.log_level_modal_selected += 1;
                }
            }
            KeyCode::Enter | KeyCode::Char(' ') => {
                if self.ui_state.log_level_modal_selected < 5 {
                    // Apply the selected log level
                    self.ui_state.log_level_filter = match self.ui_state.log_level_modal_selected {
                        0 => LogLevel::Error,
                        1 => LogLevel::Warn,
                        2 => LogLevel::Info,
                        3 => LogLevel::Debug,
                        4 => LogLevel::Trace,
                        _ => LogLevel::Info,
                    };

                    // Clear current logs and request fresh data with new filter
                    self.ui_state.viewport_logs.clear();
                    self.ui_state.log_scroll = 0;

                    // Request initial data again to reload with new filter
                    if let Some(reader) = &self.log_reader {
                        reader.request_initial_data();
                    }

                    // Close modal
                    self.ui_state.show_log_level_modal = false;
                } else {
                    // Toggle the target checkbox
                    self.ui_state.show_log_target = !self.ui_state.show_log_target;
                }
            }
            KeyCode::Esc => {
                // Close without applying
                self.ui_state.show_log_level_modal = false;
            }
            _ => {}
        }
    }

    /// Handle application modal input
    fn handle_application_modal_key(&mut self, key: KeyEvent) {
        // Handle navigation based on current view
        match self.ui_state.app_manager_view {
            0 => self.handle_app_list_view_keys(key),
            1 => self.handle_app_details_view_keys(key),
            2 => self.handle_add_origin_view_keys(key),
            3 => self.handle_create_app_view_keys(key),
            _ => {}
        }
    }

    /// Handle keys in application list view
    fn handle_app_list_view_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up => {
                if self.ui_state.app_manager_selected_index > 0 {
                    self.ui_state.app_manager_selected_index -= 1;
                }
            }
            KeyCode::Down => {
                let max_index = self
                    .ui_state
                    .app_manager_applications
                    .len()
                    .saturating_sub(1);
                if self.ui_state.app_manager_selected_index < max_index {
                    self.ui_state.app_manager_selected_index += 1;
                }
            }
            KeyCode::Enter => {
                // View details of selected application
                if !self.ui_state.app_manager_applications.is_empty() {
                    let selected = self.ui_state.app_manager_applications
                        [self.ui_state.app_manager_selected_index]
                        .clone();
                    self.ui_state.app_manager_selected_application = Some(selected);
                    self.ui_state.app_manager_view = 1; // Switch to details view
                    // Refresh to get latest details
                    self.refresh_selected_application_details();
                }
            }
            KeyCode::Char('c') => {
                // Switch to create application view
                self.ui_state.app_manager_view = 3;
                self.ui_state.app_manager_result = None;
            }
            KeyCode::Esc => {
                self.ui_state.application_modal_active = false;
                self.ui_state.app_manager_view = 0; // Reset to list view
                self.ui_state.app_manager_result = None;
            }
            _ => {}
        }
    }

    /// Handle keys in application details view
    fn handle_app_details_view_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('a') => {
                // Switch to add origin view
                self.ui_state.app_manager_view = 2;
                self.ui_state.app_manager_origin_input.clear();
                self.ui_state.app_manager_result = None;
            }
            KeyCode::Backspace => {
                // Go back to list view and refresh
                self.ui_state.app_manager_view = 0;
                self.ui_state.app_manager_selected_application = None;
                self.refresh_applications_list();
            }
            KeyCode::Esc => {
                self.ui_state.application_modal_active = false;
                self.ui_state.app_manager_view = 0; // Reset to list view
                self.ui_state.app_manager_result = None;
            }
            _ => {}
        }
    }

    /// Handle keys in add origin view
    fn handle_add_origin_view_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                // Add origin to application
                if !self.ui_state.app_manager_origin_input.is_empty()
                    && let Some(app) = &self.ui_state.app_manager_selected_application
                {
                    // Get any available node URL
                    if let Some(url) = self.get_any_node_url() {
                        match self.rpc_ops.add_origin(
                            &url,
                            &app.id.to_string(),
                            &self.ui_state.app_manager_origin_input,
                        ) {
                            Ok(_) => {
                                self.ui_state.app_manager_result = Some(format!(
                                    "Added origin: {}",
                                    self.ui_state.app_manager_origin_input
                                ));
                                self.ui_state.app_manager_origin_input.clear();
                                // Refresh application details and go back to details view
                                self.refresh_selected_application_details();
                                self.ui_state.app_manager_view = 1;
                            }
                            Err(e) => {
                                self.ui_state.app_manager_result =
                                    Some(format!("Failed to add origin: {e}"));
                            }
                        }
                    } else {
                        self.ui_state.app_manager_result =
                            Some("No running nodes available".to_string());
                    }
                }
            }
            KeyCode::Backspace if self.ui_state.app_manager_origin_input.is_empty() => {
                // Go back to details view and refresh
                self.ui_state.app_manager_view = 1;
                self.ui_state.app_manager_result = None;
                self.refresh_selected_application_details();
            }
            KeyCode::Backspace => {
                // Remove last character from input
                self.ui_state.app_manager_origin_input.pop();
            }
            KeyCode::Char(c) => {
                self.ui_state.app_manager_origin_input.push(c);
            }
            KeyCode::Esc => {
                self.ui_state.application_modal_active = false;
                self.ui_state.app_manager_view = 0; // Reset to list view
                self.ui_state.app_manager_result = None;
            }
            _ => {}
        }
    }

    /// Open application modal and load applications
    fn open_application_modal(&mut self) {
        self.ui_state.application_modal_active = true;
        self.ui_state.app_manager_view = 0; // Start with list view
        self.ui_state.app_manager_selected_index = 0;
        self.ui_state.app_manager_result = None;

        // Get any available node URL and load applications
        if let Some(url) = self.get_any_node_url() {
            match self.rpc_ops.list_applications(&url) {
                Ok(apps) => {
                    self.ui_state.app_manager_applications = apps;
                }
                Err(e) => {
                    self.ui_state.app_manager_result =
                        Some(format!("Failed to load applications: {e}"));
                }
            }
        }
    }

    /// Refresh the selected application details
    fn refresh_selected_application_details(&mut self) {
        if let Some(current_app) = &self.ui_state.app_manager_selected_application {
            let app_id = current_app.id;
            if let Some(url) = self.get_any_node_url() {
                match self.rpc_ops.list_applications(&url) {
                    Ok(apps) => {
                        if let Some(updated_app) = apps.iter().find(|a| a.id == app_id) {
                            self.ui_state.app_manager_selected_application =
                                Some(updated_app.clone());
                            // Also update the main list
                            self.ui_state.app_manager_applications = apps;
                        }
                    }
                    Err(e) => {
                        // Don't overwrite success messages
                        if self.ui_state.app_manager_result.is_none()
                            || !self
                                .ui_state
                                .app_manager_result
                                .as_ref()
                                .unwrap()
                                .starts_with("Added")
                        {
                            self.ui_state.app_manager_result =
                                Some(format!("Failed to refresh application details: {e}"));
                        }
                    }
                }
            }
        }
    }

    /// Refresh the applications list
    fn refresh_applications_list(&mut self) {
        if let Some(url) = self.get_any_node_url() {
            match self.rpc_ops.list_applications(&url) {
                Ok(apps) => {
                    self.ui_state.app_manager_applications = apps;
                    // Reset selection index if it's out of bounds
                    if self.ui_state.app_manager_selected_index
                        >= self.ui_state.app_manager_applications.len()
                    {
                        self.ui_state.app_manager_selected_index = 0;
                    }
                }
                Err(e) => {
                    // Don't overwrite the result if we have a success message
                    if self.ui_state.app_manager_result.is_none()
                        || !self
                            .ui_state
                            .app_manager_result
                            .as_ref()
                            .unwrap()
                            .starts_with("Created")
                    {
                        self.ui_state.app_manager_result =
                            Some(format!("Failed to refresh applications: {e}"));
                    }
                }
            }
        }
    }

    /// Get URL of any available node (selected or first available)
    fn get_any_node_url(&self) -> Option<String> {
        // First try the selected node
        if let Some(node_id) = self.get_selected_node()
            && let Some(url) = self.cluster_adapter.get_node_url(node_id)
        {
            return Some(url);
        }

        // Otherwise, get any running node
        for node_id in &self.ui_state.logs_sidebar_nodes {
            if let Some(url) = self.cluster_adapter.get_node_url(*node_id) {
                return Some(url);
            }
        }

        None
    }

    /// Handle keys in create application view
    fn handle_create_app_view_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                // Create new application using any available node
                if let Some(url) = self.get_any_node_url() {
                    match self.rpc_ops.create_application(&url, "Default App") {
                        Ok(app) => {
                            self.ui_state.app_manager_result =
                                Some(format!("Created application with ID: {}", app.id));
                            // Switch back to list view and refresh
                            self.ui_state.app_manager_view = 0;
                            self.refresh_applications_list();
                        }
                        Err(e) => {
                            self.ui_state.app_manager_result =
                                Some(format!("Failed to create application: {e}"));
                            self.ui_state.app_manager_view = 0; // Go back to list to show error
                        }
                    }
                } else {
                    self.ui_state.app_manager_result =
                        Some("No running nodes available. Please start a node first.".to_string());
                    self.ui_state.app_manager_view = 0; // Go back to list to show error
                }
            }
            KeyCode::Backspace => {
                // Go back to list view and refresh
                self.ui_state.app_manager_view = 0;
                self.ui_state.app_manager_result = None;
                self.refresh_applications_list();
            }
            KeyCode::Esc => {
                self.ui_state.application_modal_active = false;
                self.ui_state.app_manager_view = 0; // Reset to list view
                self.ui_state.app_manager_result = None;
            }
            _ => {}
        }
    }

    /// Handle mouse events
    fn handle_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.ui_state.scroll_logs_up();
            }
            MouseEventKind::ScrollDown => {
                self.ui_state.scroll_logs_down();
            }
            MouseEventKind::Down(MouseButton::Left) => {
                // Handle clicks on tabs, buttons, etc.
                self.ui_state.handle_click(mouse.column, mouse.row);
            }
            _ => {}
        }
    }

    /// Start the selected node
    fn start_selected_node(&mut self) {
        if let Some(node_id) = self.get_selected_node() {
            // Node should already exist in adapter
            // This is more of a restart operation
            if let Err(e) = self.cluster_adapter.restart_node(node_id) {
                self.ui_state
                    .show_error(format!("Failed to start node: {e}"));
            }
        }
    }

    /// Stop the selected node
    fn stop_selected_node(&mut self) {
        if let Some(node_id) = self.get_selected_node()
            && let Err(e) = self.cluster_adapter.stop_node(node_id)
        {
            self.ui_state
                .show_error(format!("Failed to stop node: {e}"));
        }
    }

    /// Restart the selected node
    fn restart_selected_node(&mut self) {
        if let Some(node_id) = self.get_selected_node()
            && let Err(e) = self.cluster_adapter.restart_node(node_id)
        {
            self.ui_state
                .show_error(format!("Failed to restart node: {e}"));
        }
    }

    /// Initiate graceful shutdown
    fn initiate_shutdown(&mut self) {
        if !self.shutting_down {
            self.shutting_down = true;
            self.ui_state.show_shutdown_message();
            if let Err(e) = self.cluster_adapter.shutdown_all() {
                warn!("Error during shutdown: {}", e);
            }
        }
    }
}
