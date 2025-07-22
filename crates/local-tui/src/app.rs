//! Main application structure with SQL-based logging

use crate::{
    logs_viewer::{BackgroundLogViewer, DisplayLogEntry, ViewerRequest, ViewerResponse},
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
use proven_logger::info;
use proven_logger_libsql::LibsqlLogger;
use proven_topology::Version;
use proven_topology_mock::MockTopologyAdaptor;
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    collections::HashSet,
    io,
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

/// TUI Application state
pub struct App {
    /// Whether the application should quit
    should_quit: bool,
    /// Whether we're in the process of shutting down
    shutting_down: bool,
    /// UI state
    ui_state: UiState,
    /// Node manager for starting/stopping nodes
    node_manager: Arc<NodeManager>,
    /// RPC client for communication with nodes
    rpc_client: RpcClient,
    /// SQL logger instance
    logger: Arc<LibsqlLogger>,
    /// Log viewer communication channels
    log_request_tx: tokio::sync::mpsc::UnboundedSender<ViewerRequest>,
    log_response_rx: tokio::sync::mpsc::UnboundedReceiver<ViewerResponse>,
    /// Cached logs for display
    cached_logs: Vec<DisplayLogEntry>,
    /// Shutdown flag from signal handler
    shutdown_flag: Option<Arc<AtomicBool>>,
}

impl App {
    /// Create a new application with SQL-based logging
    ///
    /// # Arguments
    ///
    /// * `logger` - The SQL logger instance to use
    #[must_use]
    pub fn new(logger: Arc<LibsqlLogger>) -> Self {
        // Create shared governance synchronously
        let governance = Arc::new(Self::create_shared_governance());

        // Generate session ID
        let session_id = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        info!("Starting TUI application with session_id: {}", session_id);

        // Create log viewer channels
        let (log_request_tx, log_request_rx) = tokio::sync::mpsc::unbounded_channel();
        let (log_response_tx, log_response_rx) = tokio::sync::mpsc::unbounded_channel();

        // Start background log viewer
        let viewer = BackgroundLogViewer::new(&logger);
        tokio::spawn(viewer.run(log_request_rx, log_response_tx));

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
            logger,
            log_request_tx,
            log_response_rx,
            cached_logs: Vec::new(),
            shutdown_flag: None,
        }
    }

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

    /// Set the shutdown flag for signal handling
    pub fn set_shutdown_flag(&mut self, shutdown_flag: Arc<AtomicBool>) {
        self.shutdown_flag = Some(shutdown_flag);
    }

    /// Get the logger for external use
    #[must_use]
    pub fn get_logger(&self) -> Arc<LibsqlLogger> {
        self.logger.clone()
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

        // Initial UI render
        terminal.draw(|frame| render_ui(frame, &self.ui_state, &self.cached_logs))?;

        // Request initial logs
        self.log_request_tx.send(ViewerRequest::Refresh)?;

        // Main loop (synchronous)
        let tick_rate = Duration::from_millis(100);
        let mut last_tick = std::time::Instant::now();

        loop {
            // Check shutdown flag
            if let Some(flag) = &self.shutdown_flag {
                if flag.load(Ordering::SeqCst) && !self.shutting_down {
                    info!("Shutdown requested via signal");
                    self.should_quit = true;
                    self.shutting_down = true;
                }
            }

            // Process any pending log responses
            while let Ok(response) = self.log_response_rx.try_recv() {
                match response {
                    ViewerResponse::Logs(logs) => {
                        self.cached_logs = logs;
                    }
                    ViewerResponse::NewLogs(new_logs) => {
                        self.cached_logs.extend(new_logs);
                        // Keep only the last 1000 logs in cache
                        if self.cached_logs.len() > 1000 {
                            self.cached_logs.drain(..self.cached_logs.len() - 1000);
                        }
                    }
                }
            }

            // Handle events
            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if event::poll(timeout)? {
                let crossterm_event = event::read()?;
                let event = Event::Input(crossterm_event);
                self.handle_event(event)?;
            }

            if last_tick.elapsed() >= tick_rate {
                self.handle_event(Event::Tick)?;
                last_tick = std::time::Instant::now();
            }

            // Exit if quit was requested
            if self.should_quit {
                break;
            }

            // Render UI
            terminal.draw(|frame| render_ui(frame, &self.ui_state, &self.cached_logs))?;
        }

        // Cleanup (synchronous)
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        Ok(())
    }

    /// Handle synchronous events
    fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Input(input) => self.handle_input_event(input)?,
            Event::Tick => self.handle_tick()?,
        }
        Ok(())
    }

    /// Handle input events (keyboard and mouse)
    fn handle_input_event(&mut self, event: crossterm::event::Event) -> Result<()> {
        match event {
            crossterm::event::Event::Key(key) => self.handle_key_event(key)?,
            crossterm::event::Event::Mouse(mouse) => self.handle_mouse_event(mouse)?,
            _ => {}
        }
        Ok(())
    }

    /// Handle keyboard events
    fn handle_key_event(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Char('q' | 'Q') => {
                self.should_quit = true;
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
            }
            KeyCode::Tab => {
                self.ui_state.next_tab();
            }
            KeyCode::BackTab => {
                self.ui_state.previous_tab();
            }
            KeyCode::Up => {
                self.ui_state.scroll_up();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollUp(1))?;
                }
            }
            KeyCode::Down => {
                self.ui_state.scroll_down();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollDown(1))?;
                }
            }
            KeyCode::PageUp => {
                self.ui_state.page_up();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollUp(20))?;
                }
            }
            KeyCode::PageDown => {
                self.ui_state.page_down();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollDown(20))?;
                }
            }
            KeyCode::Home => {
                self.ui_state.scroll_to_top();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollToTop)?;
                }
            }
            KeyCode::End => {
                self.ui_state.scroll_to_bottom();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollToBottom)?;
                }
            }
            KeyCode::Char('s' | 'S') => {
                self.start_node()?;
            }
            KeyCode::Char('x' | 'X') => {
                self.stop_node()?;
            }
            KeyCode::Left => {
                self.ui_state.decrease_log_level();
                self.update_log_filters()?;
            }
            KeyCode::Right => {
                self.ui_state.increase_log_level();
                self.update_log_filters()?;
            }
            KeyCode::Char('n' | 'N') => {
                self.ui_state.toggle_node_filter();
                self.update_log_filters()?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Update log filters based on UI state
    fn update_log_filters(&mut self) -> Result<()> {
        let level = self.ui_state.get_log_level_filter();
        let node_filter = self.ui_state.get_node_filter();

        self.log_request_tx
            .send(ViewerRequest::SetFilters { level, node_filter })?;

        Ok(())
    }

    /// Handle mouse events
    fn handle_mouse_event(&mut self, mouse: MouseEvent) -> Result<()> {
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.ui_state.scroll_up();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollUp(3))?;
                }
            }
            MouseEventKind::ScrollDown => {
                self.ui_state.scroll_down();
                if self.ui_state.is_log_tab_active() {
                    self.log_request_tx.send(ViewerRequest::ScrollDown(3))?;
                }
            }
            MouseEventKind::Down(MouseButton::Left) => {
                // Click on tabs
                if mouse.row < 3 {
                    if mouse.column < 20 {
                        self.ui_state.set_active_tab(0);
                    } else if mouse.column < 40 {
                        self.ui_state.set_active_tab(1);
                    } else if mouse.column < 60 {
                        self.ui_state.set_active_tab(2);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Handle periodic updates
    fn handle_tick(&mut self) -> Result<()> {
        // Update node statuses
        self.update_node_statuses()?;

        // Update viewport size if needed
        if let Ok((_, height)) = crossterm::terminal::size() {
            let viewport_size = (height as usize).saturating_sub(10); // Account for UI chrome
            self.log_request_tx
                .send(ViewerRequest::SetViewportSize(viewport_size))?;
        }

        Ok(())
    }

    /// Start a new node
    fn start_node(&mut self) -> Result<()> {
        let node_id = TuiNodeId::new();
        let name = node_id.display_name();
        let specializations = HashSet::new();

        self.node_manager
            .start_node(node_id, &name, specializations);

        // Update UI state
        self.ui_state.add_node(node_id, NodeStatus::Starting);
        info!("Started node {}", node_id);

        Ok(())
    }

    /// Stop a node
    fn stop_node(&mut self) -> Result<()> {
        if let Some(node_id) = self.ui_state.get_selected_node() {
            self.node_manager.stop_node(node_id);

            // Update UI state
            self.ui_state
                .update_node_status(node_id, NodeStatus::Stopped);
            info!("Stopped node {}", node_id);
        }
        Ok(())
    }

    /// Update node statuses from node manager
    fn update_node_statuses(&mut self) -> Result<()> {
        let statuses = self.node_manager.get_nodes_for_ui();

        for (node_id, (_, status, _)) in statuses {
            self.ui_state.update_node_status(node_id, status);
        }

        Ok(())
    }
}
