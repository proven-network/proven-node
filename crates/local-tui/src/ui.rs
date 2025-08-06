//! TUI interface components - adapted from old ui.rs to work with new backend

use crate::node_id::TuiNodeId;
use proven_applications::Application;
use proven_local::NodeStatus;
use proven_local_cluster::{LogEntry, LogLevel};
use proven_topology::NodeSpecialization;
use std::collections::{HashMap, HashSet};

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, List, ListItem, Padding, Paragraph, Scrollbar, ScrollbarOrientation,
        ScrollbarState,
    },
};

/// UI state management
#[derive(Debug)]
pub struct UiState {
    /// Current scroll position (0 = top, increases downward)
    pub log_scroll: usize,
    /// Current viewport logs for display
    pub viewport_logs: Vec<LogEntry>,
    /// Total number of lines in the current log file
    pub total_log_lines: usize,
    /// Whether auto-scroll is enabled (automatically scroll to bottom for new logs)
    pub auto_scroll_enabled: bool,
    /// Viewport height for logs (set during render)
    pub log_viewport_height: usize,
    /// Scrollbar state for logs
    pub log_scrollbar_state: ScrollbarState,
    /// Logs sidebar selected index (0 = "All", 1+ = node indices)
    pub logs_sidebar_selected: usize,
    /// Whether debug is selected in the sidebar
    pub logs_sidebar_debug_selected: bool,
    /// List of nodes that have logs
    pub logs_sidebar_nodes: Vec<TuiNodeId>,
    /// Whether to show help overlay
    pub show_help: bool,
    /// Whether to show node type selection modal
    pub show_node_type_modal: bool,
    /// Whether to show log level modal
    pub show_log_level_modal: bool,
    /// Selected log level in modal (0=Error, 1=Warn, 2=Info, 3=Debug, 4=Trace)
    pub log_level_modal_selected: usize,
    /// Current log level filter
    pub log_level_filter: LogLevel,
    /// Selected specializations for new node
    pub node_specializations_selected: Vec<bool>,
    /// Currently highlighted index in the specializations modal
    pub node_modal_selected_index: usize,
    /// Whether to show application modal
    pub application_modal_active: bool,
    /// Application modal view (0=list, 1=details, 2=add_origin, 3=create)
    pub app_manager_view: usize,
    /// Selected application index in list
    pub app_manager_selected_index: usize,
    /// List of applications
    pub app_manager_applications: Vec<Application>,
    /// Currently selected application for details view
    pub app_manager_selected_application: Option<Application>,
    /// Origin input for adding to application
    pub app_manager_origin_input: String,
    /// Result message from last operation
    pub app_manager_result: Option<String>,
    /// Application name input (for create view)
    pub app_name: String,
    /// Application origin input (for create view)
    pub app_origin: String,
    /// Modal field index
    pub modal_field_index: usize,
    /// Nodes from cluster
    pub nodes: HashMap<TuiNodeId, (String, NodeStatus, HashSet<NodeSpecialization>)>,
    /// Selected node
    pub selected_node: Option<TuiNodeId>,
    /// Areas for mouse handling
    pub logs_area: Rect,
    pub sidebar_area: Rect,
    pub sidebar_main_area: Rect,
    pub sidebar_debug_area: Rect,
    /// Message to display
    pub message: Option<String>,
    /// Whether shutting down
    pub is_shutting_down: bool,
}

impl UiState {
    /// Create a new UI state
    pub fn new() -> Self {
        Self {
            log_scroll: 0,
            viewport_logs: Vec::new(),
            total_log_lines: 0,
            auto_scroll_enabled: true,
            log_viewport_height: 0,
            log_scrollbar_state: ScrollbarState::default(),
            logs_sidebar_selected: 0,
            logs_sidebar_debug_selected: false,
            logs_sidebar_nodes: Vec::new(),
            show_help: false,
            show_node_type_modal: false,
            show_log_level_modal: false,
            log_level_modal_selected: 2, // Default to Info
            log_level_filter: LogLevel::Info,
            node_specializations_selected: vec![false; 7],
            node_modal_selected_index: 0,
            application_modal_active: false,
            app_manager_view: 0,
            app_manager_selected_index: 0,
            app_manager_applications: Vec::new(),
            app_manager_selected_application: None,
            app_manager_origin_input: String::new(),
            app_manager_result: None,
            app_name: String::new(),
            app_origin: "http://localhost:3000".to_string(),
            modal_field_index: 0,
            nodes: HashMap::new(),
            selected_node: None,
            logs_area: Rect::default(),
            sidebar_area: Rect::default(),
            sidebar_main_area: Rect::default(),
            sidebar_debug_area: Rect::default(),
            message: None,
            is_shutting_down: false,
        }
    }

    /// Update viewport logs from new logs
    pub fn update_logs(&mut self, new_logs: Vec<LogEntry>) {
        let had_logs = !self.viewport_logs.is_empty();

        // Append new logs (already filtered by SQL query)
        self.viewport_logs.extend(new_logs);

        // Keep log size reasonable (but much larger than before)
        // This matches the old implementation's limit
        if self.viewport_logs.len() > 50000 {
            // Remove oldest logs
            let removed_count = 25000;
            self.viewport_logs.drain(0..removed_count);
            // Adjust scroll position if we removed logs before current position
            if self.log_scroll >= removed_count {
                self.log_scroll -= removed_count;
            } else {
                self.log_scroll = 0;
            }
        }

        self.total_log_lines = self.viewport_logs.len();

        // Auto-scroll to bottom if enabled and we have logs
        if self.auto_scroll_enabled && had_logs && self.total_log_lines > self.log_viewport_height {
            self.log_scroll = self
                .total_log_lines
                .saturating_sub(self.log_viewport_height);
        }

        self.update_scrollbar_state();
    }

    /// Update scrollbar state
    pub fn update_scrollbar_state(&mut self) {
        // The scrollbar needs to understand the viewport size
        // content_length is the total number of items
        // position is the index of the first visible item
        // viewport_content_length is how many items are visible at once

        // When we have fewer logs than viewport, no scrollbar needed
        if self.total_log_lines <= self.log_viewport_height {
            self.log_scrollbar_state = ScrollbarState::default();
        } else {
            // The key insight: the scrollbar should represent the SCROLLABLE range,
            // not the total content. The scrollable range is (total - viewport + 1)
            // because when we're at the last position, we still see viewport lines.
            let scrollable_range = self
                .total_log_lines
                .saturating_sub(self.log_viewport_height)
                + 1;

            self.log_scrollbar_state = ScrollbarState::default()
                .content_length(scrollable_range)
                .position(self.log_scroll);
            // Not setting viewport_content_length - it seems to confuse things
        }
    }

    /// Update nodes from cluster
    pub fn update_nodes(
        &mut self,
        nodes: HashMap<TuiNodeId, (String, NodeStatus, HashSet<NodeSpecialization>)>,
    ) {
        self.nodes = nodes;

        // Update nodes list for sidebar
        let mut nodes_with_logs: Vec<TuiNodeId> = self
            .nodes
            .keys()
            .filter(|&&node_id| node_id != crate::node_id::MAIN_THREAD_NODE_ID)
            .copied()
            .collect();

        // Sort by execution order
        nodes_with_logs.sort_by_key(|node_id| node_id.execution_order());
        self.logs_sidebar_nodes = nodes_with_logs;
    }

    /// Navigate sidebar selection
    pub fn select_next_sidebar(&mut self) {
        let max_index = self.logs_sidebar_nodes.len() + 1; // +1 for "All" item
        if self.logs_sidebar_debug_selected {
            self.logs_sidebar_debug_selected = false;
            self.logs_sidebar_selected = 0;
        } else if self.logs_sidebar_selected < max_index - 1 {
            self.logs_sidebar_selected += 1;
        } else {
            self.logs_sidebar_debug_selected = true;
        }
        // Reset scroll when switching tabs
        self.reset_scroll_to_autoscroll();
    }

    pub fn select_previous_sidebar(&mut self) {
        if self.logs_sidebar_debug_selected {
            self.logs_sidebar_debug_selected = false;
            self.logs_sidebar_selected = self.logs_sidebar_nodes.len();
        } else if self.logs_sidebar_selected > 0 {
            self.logs_sidebar_selected -= 1;
        } else {
            self.logs_sidebar_debug_selected = true;
        }
        // Reset scroll when switching tabs
        self.reset_scroll_to_autoscroll();
    }

    /// Toggle help overlay
    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }

    /// Clear logs
    pub fn clear_logs(&mut self) {
        self.viewport_logs.clear();
        self.log_scroll = 0;
        self.total_log_lines = 0;
    }

    /// Log scrolling
    pub fn scroll_logs_up(&mut self) {
        if self.log_scroll > 0 {
            self.log_scroll -= 1;
            // Disable auto-scroll when user scrolls manually
            self.auto_scroll_enabled = false;
        }
    }

    pub fn scroll_logs_down(&mut self) {
        let max_scroll = self
            .total_log_lines
            .saturating_sub(self.log_viewport_height);
        if self.log_scroll < max_scroll {
            self.log_scroll += 1;
            // Re-enable auto-scroll if we reached the bottom
            if self.log_scroll >= max_scroll {
                self.auto_scroll_enabled = true;
            }
        }
    }

    pub fn scroll_logs_page_up(&mut self) {
        self.log_scroll = self.log_scroll.saturating_sub(20);
        // Disable auto-scroll when user scrolls manually
        if self.log_scroll
            < self
                .total_log_lines
                .saturating_sub(self.log_viewport_height)
        {
            self.auto_scroll_enabled = false;
        }
    }

    pub fn scroll_logs_page_down(&mut self) {
        let max_scroll = self
            .total_log_lines
            .saturating_sub(self.log_viewport_height);
        self.log_scroll = (self.log_scroll + 20).min(max_scroll);
        // Re-enable auto-scroll if we reached the bottom
        if self.log_scroll >= max_scroll {
            self.auto_scroll_enabled = true;
        }
    }

    pub fn scroll_logs_home(&mut self) {
        self.log_scroll = 0;
        // Disable auto-scroll when user explicitly scrolls to top
        self.auto_scroll_enabled = false;
    }

    pub fn scroll_logs_end(&mut self) {
        self.log_scroll = self
            .total_log_lines
            .saturating_sub(self.log_viewport_height)
            .max(0);
        // Enable auto-scroll when user explicitly scrolls to bottom
        self.auto_scroll_enabled = true;
    }

    /// Reset scroll to autoscroll position (called when switching tabs)
    pub fn reset_scroll_to_autoscroll(&mut self) {
        // When switching tabs, we should start fresh
        // The actual logs will be filtered in render_logs()
        self.log_scroll = 0;
        self.auto_scroll_enabled = true;
        // Note: total_log_lines will be recalculated in render_logs() based on the filtered logs
    }

    /// Modal management
    pub fn open_new_node_modal(&mut self) {
        self.show_node_type_modal = true;
        self.node_modal_selected_index = 0;
        self.node_specializations_selected = vec![false; 7];
    }

    pub fn open_application_modal(&mut self) {
        self.application_modal_active = true;
        self.app_name.clear();
        self.app_origin = "http://localhost:3000".to_string();
        self.modal_field_index = 0;
    }

    pub fn close_modal(&mut self) {
        self.show_node_type_modal = false;
        self.application_modal_active = false;
        self.show_log_level_modal = false;
    }

    pub fn has_active_modal(&self) -> bool {
        self.show_node_type_modal || self.application_modal_active || self.show_log_level_modal
    }

    pub fn next_modal_field(&mut self) {
        self.modal_field_index = (self.modal_field_index + 1) % 2;
    }

    pub fn previous_modal_field(&mut self) {
        if self.modal_field_index == 0 {
            self.modal_field_index = 1;
        } else {
            self.modal_field_index = 0;
        }
    }

    pub fn handle_modal_input(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::KeyCode;
        if self.application_modal_active {
            match key.code {
                KeyCode::Char(c) => {
                    if self.modal_field_index == 0 {
                        self.app_name.push(c);
                    } else {
                        self.app_origin.push(c);
                    }
                }
                KeyCode::Backspace => {
                    if self.modal_field_index == 0 {
                        self.app_name.pop();
                    } else {
                        self.app_origin.pop();
                    }
                }
                _ => {}
            }
        }
    }

    /// Get selected specializations
    pub fn get_selected_specializations(&self) -> HashSet<NodeSpecialization> {
        let mut specs = HashSet::new();
        let all_specs = [
            NodeSpecialization::BitcoinMainnet,
            NodeSpecialization::BitcoinTestnet,
            NodeSpecialization::EthereumMainnet,
            NodeSpecialization::EthereumHolesky,
            NodeSpecialization::EthereumSepolia,
            NodeSpecialization::RadixMainnet,
            NodeSpecialization::RadixStokenet,
        ];

        for (i, selected) in self.node_specializations_selected.iter().enumerate() {
            if *selected && i < all_specs.len() {
                specs.insert(all_specs[i].clone());
            }
        }

        specs
    }

    pub fn get_application_data(&self) -> Option<(String, String)> {
        if !self.app_name.is_empty() && !self.app_origin.is_empty() {
            Some((self.app_name.clone(), self.app_origin.clone()))
        } else {
            None
        }
    }

    /// Handle click events
    pub fn handle_click(&mut self, x: u16, y: u16) {
        // Check if click is in sidebar
        if x >= self.sidebar_area.x
            && x < self.sidebar_area.x + self.sidebar_area.width
            && y >= self.sidebar_area.y
            && y < self.sidebar_area.y + self.sidebar_area.height
        {
            // Check if in main area or debug area
            if y >= self.sidebar_debug_area.y {
                // Debug area clicked
                self.logs_sidebar_debug_selected = true;
                self.logs_sidebar_selected = 0;
                self.reset_scroll_to_autoscroll();
            } else if y >= self.sidebar_main_area.y {
                // Calculate which item was clicked
                let relative_y = y - self.sidebar_main_area.y;
                if relative_y == 1 {
                    // "All" clicked
                    self.logs_sidebar_selected = 0;
                    self.logs_sidebar_debug_selected = false;
                    self.reset_scroll_to_autoscroll();
                } else if relative_y >= 3 {
                    // Node item clicked
                    let node_index = (relative_y - 3) as usize;
                    if node_index < self.logs_sidebar_nodes.len() {
                        self.logs_sidebar_selected = node_index + 1;
                        self.logs_sidebar_debug_selected = false;
                        self.reset_scroll_to_autoscroll();
                    }
                }
            }
        }
    }

    /// Show error message
    pub fn show_error(&mut self, msg: String) {
        self.message = Some(format!("❌ {msg}"));
    }

    /// Show shutdown message
    pub fn show_shutdown_message(&mut self) {
        self.is_shutting_down = true;
        self.message = Some("🛑 Shutting down all nodes...".to_string());
    }

    pub fn open_search_modal(&mut self) {
        // TODO: Implement search modal
    }

    pub fn open_filter_modal(&mut self) {
        // TODO: Implement filter modal
    }
}

impl Default for UiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Get status icon for a node
const fn get_status_icon(status: &NodeStatus) -> &'static str {
    match status {
        NodeStatus::NotStarted => "○",
        NodeStatus::Running => "●",
        NodeStatus::Starting => "◐",
        NodeStatus::Stopped | NodeStatus::Stopping => "◯",
        NodeStatus::Failed(_) => "✗",
    }
}

/// Get the color for a node based on its execution order
fn get_node_color(node_id: TuiNodeId) -> Color {
    if node_id == crate::node_id::MAIN_THREAD_NODE_ID {
        Color::White
    } else {
        let node_colors = [
            Color::Indexed(6),   // Dark cyan
            Color::Indexed(2),   // Dark green
            Color::Indexed(4),   // Dark blue
            Color::Indexed(5),   // Dark magenta
            Color::Indexed(3),   // Dark yellow
            Color::Indexed(1),   // Dark red
            Color::Indexed(94),  // Bright blue
            Color::Indexed(130), // Dark orange
            Color::Indexed(97),  // Bright cyan
            Color::Indexed(133), // Bright magenta
            Color::Indexed(100), // Bright green
            Color::Indexed(124), // Bright red
        ];
        node_colors[(node_id.execution_order() - 1) as usize % node_colors.len()]
    }
}

/// Main UI rendering function
pub fn render_ui(frame: &mut Frame, ui_state: &mut UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Header
            Constraint::Min(0),    // Main content (logs)
            Constraint::Length(1), // Footer
        ])
        .split(frame.area());

    // Render header at the top
    render_header(frame, chunks[0]);

    // Render logs in the main area with sidebar
    render_logs(frame, chunks[1], ui_state);

    // Render footer at the bottom
    render_footer(frame, chunks[2], ui_state.is_shutting_down, ui_state);

    // Render help overlay if requested
    if ui_state.show_help {
        render_help_overlay(frame, frame.area());
    }

    // Render node type selection modal if requested
    if ui_state.show_node_type_modal {
        render_node_type_modal(frame, frame.area(), ui_state);
    }

    // Render application modal if requested
    if ui_state.application_modal_active {
        render_application_modal(frame, frame.area(), ui_state);
    }

    // Render log level modal if requested
    if ui_state.show_log_level_modal {
        render_log_level_modal(frame, frame.area(), ui_state);
    }
}

/// Render the header with title
fn render_header(frame: &mut Frame, area: Rect) {
    let header_text = Line::from(vec![Span::styled(
        "Proven Network - Local Debugger",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )]);

    let paragraph = Paragraph::new(header_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(paragraph, area);
}

/// Render logs view with sidebar
fn render_logs(frame: &mut Frame, area: Rect, ui_state: &mut UiState) {
    // Use fixed sidebar width
    let sidebar_width = 25;

    // Split layout horizontally: sidebar | logs
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(sidebar_width), // Sidebar
            Constraint::Min(0),                // Logs
        ])
        .split(area);

    // Render sidebar
    render_logs_sidebar(frame, horizontal_chunks[0], ui_state);

    // Render logs in the remaining space
    let logs_area = horizontal_chunks[1];

    // Store logs area for mouse scroll detection
    ui_state.logs_area = logs_area;

    // Filter logs based on sidebar selection (log level already filtered by SQL)
    let filtered_logs: Vec<&LogEntry> = if ui_state.logs_sidebar_debug_selected {
        // Show only main thread (debug) logs
        ui_state
            .viewport_logs
            .iter()
            .filter(|log| log.node_id == "main")
            .collect()
    } else if ui_state.logs_sidebar_selected == 0 {
        // Show all logs
        ui_state.viewport_logs.iter().collect()
    } else if let Some(&selected_node_id) = ui_state
        .logs_sidebar_nodes
        .get(ui_state.logs_sidebar_selected - 1)
    {
        // Show logs from specific node
        // Cluster stores node_id as "node-session-execution_order-short_id"
        // We need to match by execution_order which is in the middle
        let target_execution_order = selected_node_id.execution_order();
        ui_state
            .viewport_logs
            .iter()
            .filter(|log| {
                // Parse thread name format: "node-session-execution_order-short_id"
                if let Some(node_part) = log.node_id.strip_prefix("node-") {
                    let parts: Vec<&str> = node_part.split('-').collect();
                    if parts.len() >= 3 {
                        // execution_order is the second-to-last part
                        if let Ok(exec_order) = parts[parts.len() - 2].parse::<u8>() {
                            return exec_order == target_execution_order;
                        }
                    }
                }
                false
            })
            .collect()
    } else {
        // Invalid selection, show all
        ui_state.viewport_logs.iter().collect()
    };

    // Update total log lines and viewport
    ui_state.total_log_lines = filtered_logs.len();
    let content_height = logs_area.height.saturating_sub(2); // Account for borders
    ui_state.log_viewport_height = content_height as usize;

    // Fix scroll position if it's invalid for the current filtered logs
    // This happens when switching tabs or when logs are fewer than viewport
    if ui_state.auto_scroll_enabled {
        if ui_state.total_log_lines > ui_state.log_viewport_height {
            // More logs than viewport - scroll to show latest
            ui_state.log_scroll = ui_state
                .total_log_lines
                .saturating_sub(ui_state.log_viewport_height);
        } else {
            // Fewer logs than viewport - no scrolling needed
            ui_state.log_scroll = 0;
        }
    } else {
        // Ensure scroll position is valid even when not auto-scrolling
        if ui_state.log_scroll >= ui_state.total_log_lines {
            ui_state.log_scroll = ui_state.total_log_lines.saturating_sub(1).max(0);
        }
    }

    // Determine whether to show node names in logs
    let show_node_names =
        ui_state.logs_sidebar_selected == 0 && !ui_state.logs_sidebar_debug_selected;

    // Convert logs to display lines
    let display_lines: Vec<Line> = filtered_logs
        .iter()
        .skip(ui_state.log_scroll)
        .take(ui_state.log_viewport_height)
        .map(|entry| create_colored_log_line(entry, show_node_names, &ui_state.nodes))
        .collect();

    // Create dynamic title based on selection
    let base_title = if ui_state.logs_sidebar_debug_selected {
        " Logs - Debug".to_string()
    } else if ui_state.logs_sidebar_selected == 0 {
        " Logs - All".to_string()
    } else if let Some(&selected_node_id) = ui_state
        .logs_sidebar_nodes
        .get(ui_state.logs_sidebar_selected - 1)
    {
        let pokemon_name = selected_node_id.full_pokemon_name();
        format!(" Logs - {pokemon_name}")
    } else {
        " Logs".to_string()
    };

    // Add auto-scroll indicator and debug info
    let title_text = if ui_state.auto_scroll_enabled {
        // Debug: show scroll position info
        format!(
            "{base_title} [AUTO] [S:{}/{} V:{}] ",
            ui_state.log_scroll, ui_state.total_log_lines, ui_state.log_viewport_height
        )
    } else {
        format!(
            "{base_title} [S:{}/{} V:{}] ",
            ui_state.log_scroll, ui_state.total_log_lines, ui_state.log_viewport_height
        )
    };

    let title_style = if ui_state.logs_sidebar_debug_selected {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else if ui_state.logs_sidebar_selected == 0 {
        Style::default().add_modifier(Modifier::BOLD)
    } else if let Some(&selected_node_id) = ui_state
        .logs_sidebar_nodes
        .get(ui_state.logs_sidebar_selected - 1)
    {
        let node_color = get_node_color(selected_node_id);
        Style::default().fg(node_color).add_modifier(Modifier::BOLD)
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    };

    // Create the logs widget
    let logs_paragraph = Paragraph::new(display_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title_text)
                .title_style(title_style),
        )
        .wrap(ratatui::widgets::Wrap { trim: false });

    frame.render_widget(logs_paragraph, logs_area);

    // Render scrollbar if needed
    if ui_state.total_log_lines > ui_state.log_viewport_height {
        let scrollbar_area = Rect {
            x: logs_area.x + logs_area.width.saturating_sub(1),
            y: logs_area.y + 1,
            width: 1,
            height: logs_area.height.saturating_sub(2),
        };

        ui_state.update_scrollbar_state();

        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            scrollbar_area,
            &mut ui_state.log_scrollbar_state,
        );
    }
}

/// Get a consistent color for a Pokemon name
fn get_pokemon_color(pokemon_name: &str) -> Color {
    // Use a simple hash to assign consistent colors
    // Available colors that work well for text
    let colors = [
        Color::Cyan,
        Color::Magenta,
        Color::Yellow,
        Color::Green,
        Color::Blue,
        Color::LightCyan,
        Color::LightMagenta,
        Color::LightYellow,
        Color::LightGreen,
        Color::LightBlue,
    ];

    // Simple hash: sum of character values
    let hash: usize = pokemon_name.chars().map(|c| c as usize).sum();
    colors[hash % colors.len()]
}

/// Create a colored log line
fn create_colored_log_line(
    entry: &LogEntry,
    show_node_name: bool,
    nodes: &HashMap<TuiNodeId, (String, NodeStatus, HashSet<NodeSpecialization>)>,
) -> Line<'static> {
    // Convert UTC timestamp to local time for display
    use chrono::{Local, TimeZone};
    let local_timestamp = Local.from_utc_datetime(&entry.timestamp.naive_utc());
    let timestamp = local_timestamp.format("%H:%M:%S%.3f");

    // Use tracing crate default colors
    let level_color = match entry.level {
        LogLevel::Error => Color::LightRed,
        LogLevel::Warn => Color::LightYellow,
        LogLevel::Info => Color::LightGreen,
        LogLevel::Debug => Color::LightBlue,
        LogLevel::Trace => Color::LightMagenta,
    };

    // Fixed-width formatting for perfect alignment
    let level_str = match entry.level {
        LogLevel::Error => "ERRO",
        LogLevel::Warn => "WARN",
        LogLevel::Info => "INFO",
        LogLevel::Debug => "DEBG",
        LogLevel::Trace => "TRAC",
    };

    let message = entry.message.clone();

    if show_node_name {
        // Extract Pokemon name from the execution_order
        let (node_display, node_color) = if entry.node_id == "main" {
            ("main".to_string(), Color::White)
        } else {
            // Use the execution_order to find the TuiNodeId with that execution order
            let execution_order = entry.execution_order;

            // Find the node with this execution_order in our nodes map
            let found_node = nodes
                .iter()
                .find(|(tui_id, _)| tui_id.execution_order == execution_order as u8);

            if let Some((tui_id, _)) = found_node {
                // Found the node, get its Pokemon name and color
                let pokemon_name = tui_id.pokemon_name();
                let color = get_pokemon_color(&pokemon_name);
                (pokemon_name, color)
            } else {
                // Node not found in our map, fallback to showing execution order
                (format!("node-{execution_order}"), Color::Gray)
            }
        };

        Line::from(vec![
            Span::styled(
                format!("{timestamp} "),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(format!("[{level_str}]"), Style::default().fg(level_color)),
            Span::raw(" "),
            Span::styled(format!("[{node_display}]"), Style::default().fg(node_color)),
            Span::raw(": "),
            Span::raw(message),
        ])
    } else {
        Line::from(vec![
            Span::styled(
                format!("{timestamp} "),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(format!("[{level_str}]"), Style::default().fg(level_color)),
            Span::raw(": "),
            Span::raw(message),
        ])
    }
}

/// Render logs sidebar with node selection
fn render_logs_sidebar(frame: &mut Frame, area: Rect, ui_state: &mut UiState) {
    // Store sidebar areas for mouse click detection
    ui_state.sidebar_area = area;

    // Render the border
    let border_block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Nodes ({}) ", ui_state.logs_sidebar_nodes.len()));
    frame.render_widget(&border_block, area);

    // Apply internal margins to the sidebar area
    let inner_area = border_block.inner(area);

    // Split the inner area to put debug at the bottom
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),    // Main list area
            Constraint::Length(1), // Debug area (1 line)
        ])
        .split(inner_area);
    let (main_area, debug_area) = (chunks[0], chunks[1]);

    // Store areas for mouse click detection
    ui_state.sidebar_main_area = main_area;
    ui_state.sidebar_debug_area = debug_area;

    // Create main sidebar items (All + nodes)
    let mut items = vec![
        // Empty line above "All"
        ListItem::new(Line::from("")),
        // "All" item with [`] shortcut
        ListItem::new(Line::from(vec![if ui_state.logs_sidebar_selected == 0
            && !ui_state.logs_sidebar_debug_selected
        {
            Span::styled(
                format!(" [`] Overview{}", " ".repeat(23 - 13)),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(" [`] Overview", Style::default().fg(Color::White))
        }])),
        // Empty line below "All"
        ListItem::new(Line::from("")),
    ];

    // Add node items
    for (i, &node_id) in ui_state.logs_sidebar_nodes.iter().enumerate() {
        let pokemon_name = node_id.full_pokemon_name();
        let node_color = get_node_color(node_id);
        let is_selected =
            ui_state.logs_sidebar_selected == i + 1 && !ui_state.logs_sidebar_debug_selected;

        // Get node status and determine icon and color
        let (status_icon, status_color) = if let Some((_, status, _)) = ui_state.nodes.get(&node_id)
        {
            let icon = get_status_icon(status);
            let color = match status {
                NodeStatus::NotStarted | NodeStatus::Stopped => Color::Gray,
                NodeStatus::Starting | NodeStatus::Stopping => Color::Yellow,
                NodeStatus::Running => Color::Green,
                NodeStatus::Failed(_) => Color::Red,
            };
            (icon, color)
        } else {
            ("?", Color::Gray) // Unknown status
        };

        // Format with number shortcut
        let prefix = if i < 9 {
            format!(" [{}] ", i + 1)
        } else {
            "     ".to_string()
        };

        let item = if is_selected {
            // When selected, show with background color
            // Need to pad to 25 to fill the entire width
            let content = format!("{prefix}{status_icon} {pokemon_name}");
            let padded_content = format!(
                "{}{}",
                content,
                " ".repeat(25_usize.saturating_sub(content.len()))
            );
            ListItem::new(Line::from(vec![Span::styled(
                padded_content,
                Style::default()
                    .fg(Color::White)
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )]))
        } else {
            // When not selected, show with colored status icon
            ListItem::new(Line::from(vec![
                Span::styled(prefix, Style::default().fg(Color::White)),
                Span::styled(status_icon, Style::default().fg(status_color)),
                Span::raw(" "),
                Span::styled(pokemon_name, Style::default().fg(node_color)),
            ]))
        };
        items.push(item);
    }

    // Render main list
    let main_list = List::new(items);
    frame.render_widget(main_list, main_area);

    // Render debug item separately
    let debug_item = if ui_state.logs_sidebar_debug_selected {
        Paragraph::new(Line::from(vec![Span::styled(
            format!(" [d] Debug{}", " ".repeat(23 - 10)),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]))
    } else {
        Paragraph::new(Line::from(vec![Span::styled(
            " [d] Debug",
            Style::default().fg(Color::Yellow),
        )]))
    };
    frame.render_widget(debug_item, debug_area);
}

/// Render footer
fn render_footer(frame: &mut Frame, area: Rect, shutting_down: bool, ui_state: &UiState) {
    let footer_text = if shutting_down {
        Line::from(vec![
            Span::styled(
                "Shutting down... ",
                Style::default()
                    .fg(Color::LightYellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("Press ", Style::default()),
            Span::styled(
                "Ctrl+C",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" to force quit", Style::default()),
        ])
    } else if let Some(msg) = &ui_state.message {
        Line::from(vec![Span::raw(msg)])
    } else {
        // Determine the action text based on selected node
        let (action_text, action_color) =
            if ui_state.logs_sidebar_debug_selected || ui_state.logs_sidebar_selected == 0 {
                // Debug or "All" selected - no specific node actions
                ("", Color::Green)
            } else if let Some(&selected_node_id) = ui_state
                .logs_sidebar_nodes
                .get(ui_state.logs_sidebar_selected - 1)
            {
                // Specific node selected - show context-aware action
                if let Some((_, status, _)) = ui_state.nodes.get(&selected_node_id) {
                    match status {
                        NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Failed(_) => {
                            ("start", Color::Green)
                        }
                        NodeStatus::Running | NodeStatus::Starting => ("stop", Color::Red),
                        NodeStatus::Stopping => ("stopping", Color::Yellow),
                    }
                } else {
                    ("start", Color::Green)
                }
            } else {
                ("", Color::Green)
            };

        let mut spans = vec![
            Span::styled(
                "Keys: ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("q", Style::default().fg(Color::Red)),
            Span::styled(":quit ", Style::default()),
            Span::styled("n", Style::default().fg(Color::Green)),
            Span::styled(":new node ", Style::default()),
        ];

        // Add context-aware start/stop/restart keys only if a specific node is selected
        if !action_text.is_empty()
            && !ui_state.logs_sidebar_debug_selected
            && ui_state.logs_sidebar_selected > 0
        {
            spans.extend(vec![
                Span::styled("s", Style::default().fg(action_color)),
                Span::styled(format!(":{action_text} "), Style::default()),
                Span::styled("r", Style::default().fg(Color::Blue)),
                Span::styled(":restart ", Style::default()),
            ]);
        }

        spans.extend(vec![
            Span::styled("l", Style::default().fg(Color::Yellow)),
            Span::styled(
                format!(":log level ({:?}) ", ui_state.log_level_filter),
                Style::default(),
            ),
            Span::styled("a", Style::default().fg(Color::LightMagenta)),
            Span::styled(":apps ", Style::default()),
            Span::styled("?", Style::default().fg(Color::White)),
            Span::styled(":help", Style::default()),
        ]);

        Line::from(spans)
    };

    let paragraph = Paragraph::new(footer_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(paragraph, area);
}

/// Render help overlay
fn render_help_overlay(frame: &mut Frame, area: Rect) {
    let help_text = vec![
        Line::from(vec![Span::styled(
            "Keyboard Shortcuts",
            Style::default().add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Navigation:",
            Style::default().add_modifier(Modifier::UNDERLINED),
        )]),
        Line::from("  ↑/↓       Navigate sidebar"),
        Line::from("  [`]       View all logs"),
        Line::from("  [1-9]     Jump to node by number"),
        Line::from("  [d]       View debug logs"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Node Control:",
            Style::default().add_modifier(Modifier::UNDERLINED),
        )]),
        Line::from("  n         Create new node"),
        Line::from("  s         Start selected node"),
        Line::from("  x         Stop selected node"),
        Line::from("  r         Restart selected node"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Log Control:",
            Style::default().add_modifier(Modifier::UNDERLINED),
        )]),
        Line::from("  PgUp/PgDn Scroll logs"),
        Line::from("  Home/End  Jump to start/end"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Other:",
            Style::default().add_modifier(Modifier::UNDERLINED),
        )]),
        Line::from("  h         Toggle this help"),
        Line::from("  q         Quit application"),
        Line::from(""),
        Line::from("Press any key to close this help"),
    ];

    let block = Block::default()
        .title(" Help ")
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::Black));

    let help_area = centered_rect(60, 70, area);
    frame.render_widget(Clear, help_area);

    let paragraph = Paragraph::new(help_text)
        .block(block)
        .alignment(Alignment::Left);

    frame.render_widget(paragraph, help_area);
}

/// Render node type selection modal
fn render_node_type_modal(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let specializations = [
        ("Bitcoin Mainnet", Color::Yellow),
        ("Bitcoin Testnet", Color::Yellow),
        ("Ethereum Mainnet", Color::Blue),
        ("Ethereum Holesky", Color::Blue),
        ("Ethereum Sepolia", Color::Blue),
        ("Radix Mainnet", Color::Green),
        ("Radix Stokenet", Color::Green),
    ];

    let mut modal_text = vec![
        Line::from("Select blockchain specializations for the new node:"),
        Line::from(""),
    ];

    // Add specialization options
    for (i, (spec_name, spec_color)) in specializations.iter().enumerate() {
        let is_highlighted = i == ui_state.node_modal_selected_index;
        let is_selected = ui_state
            .node_specializations_selected
            .get(i)
            .unwrap_or(&false);

        let checkbox = if *is_selected { "[✓]" } else { "[ ]" };
        let highlight_prefix = if is_highlighted { "> " } else { "  " };
        let highlight_suffix = if is_highlighted { " <" } else { "  " };

        let line = Line::from(vec![
            Span::styled(highlight_prefix, Style::default()),
            Span::styled(checkbox, Style::default().fg(Color::Cyan)),
            Span::raw(" "),
            Span::styled(*spec_name, Style::default().fg(*spec_color)),
            Span::styled(highlight_suffix, Style::default()),
        ]);

        modal_text.push(line);
    }

    // Add launch button
    modal_text.push(Line::from(""));
    let launch_highlighted = ui_state.node_modal_selected_index == specializations.len();
    let launch_prefix = if launch_highlighted { "> " } else { "  " };
    let launch_suffix = if launch_highlighted { " <" } else { "  " };

    let launch_line = Line::from(vec![
        Span::styled(launch_prefix, Style::default()),
        Span::styled(
            "Launch Node",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(launch_suffix, Style::default()),
    ]);
    modal_text.push(launch_line);

    modal_text.extend(vec![
        Line::from(""),
        Line::from("Up/Down: navigate, Space: toggle, Enter: launch/toggle, Esc: cancel"),
    ]);

    let popup_area = centered_rect(70, 70, area);

    frame.render_widget(Clear, popup_area);

    let paragraph = Paragraph::new(modal_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Create New Node ")
                .title_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .padding(Padding::uniform(1)),
        )
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Left);

    frame.render_widget(paragraph, popup_area);
}

/// Render application modal
fn render_application_modal(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let modal_area = centered_rect(70, 60, area);

    // Clear background
    frame.render_widget(Clear, modal_area);

    // Main block
    let main_block = Block::default()
        .title(" Application Manager ")
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::Black));

    let inner = main_block.inner(modal_area);
    frame.render_widget(main_block, modal_area);

    // Render different views based on app_manager_view
    match ui_state.app_manager_view {
        0 => render_application_list_view(frame, inner, ui_state),
        1 => render_application_details_view(frame, inner, ui_state),
        2 => render_add_origin_view(frame, inner, ui_state),
        3 => render_create_application_view(frame, inner, ui_state),
        _ => {}
    }
}

/// Render application list view
fn render_application_list_view(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Min(5),    // List
            Constraint::Length(3), // Result area
            Constraint::Length(2), // Help text
        ])
        .split(area);

    // Title
    let title_text = Line::from(vec![
        Span::styled(
            "Your Applications",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" ({})", ui_state.app_manager_applications.len()),
            Style::default().fg(Color::Gray),
        ),
    ]);
    let title_paragraph = Paragraph::new(title_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);
    frame.render_widget(title_paragraph, chunks[0]);

    // Applications list
    if ui_state.app_manager_applications.is_empty() {
        let empty_text = vec![
            Line::from(""),
            Line::from(vec![Span::styled(
                "No applications found",
                Style::default().fg(Color::Gray),
            )]),
            Line::from(""),
            Line::from(vec![Span::styled(
                "Press 'c' to create your first application",
                Style::default().fg(Color::Cyan),
            )]),
        ];
        let empty_paragraph = Paragraph::new(empty_text)
            .style(Style::default().fg(Color::White))
            .alignment(Alignment::Center);
        frame.render_widget(empty_paragraph, chunks[1]);
    } else {
        let mut app_items = Vec::new();
        for (i, app) in ui_state.app_manager_applications.iter().enumerate() {
            let is_selected = i == ui_state.app_manager_selected_index;

            let app_info = format!(
                "ID: {} | Origins: {} | Created: {}",
                app.id,
                app.allowed_origins.len(),
                app.created_at.format("%Y-%m-%d %H:%M:%S")
            );

            let styled_text = if is_selected {
                vec![Span::styled(
                    format!("  > {app_info}  "),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::White)
                        .add_modifier(Modifier::BOLD),
                )]
            } else {
                vec![Span::styled(
                    format!("    {app_info}  "),
                    Style::default().fg(Color::White),
                )]
            };

            app_items.push(ListItem::new(Line::from(styled_text)));
        }

        let apps_list = List::new(app_items)
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .title(" Applications ")
                    .title_style(Style::default().fg(Color::Yellow)),
            )
            .style(Style::default().fg(Color::White));

        frame.render_widget(apps_list, chunks[1]);
    }

    // Result area if there's a result
    if let Some(result) = &ui_state.app_manager_result {
        let result_paragraph = Paragraph::new(Line::from(vec![
            Span::styled("Result: ", Style::default().fg(Color::Gray)),
            Span::styled(result.clone(), Style::default().fg(Color::White)),
        ]))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .title(" Result ")
                .title_style(Style::default().fg(Color::Green)),
        )
        .wrap(ratatui::widgets::Wrap { trim: true });

        frame.render_widget(result_paragraph, chunks[2]);
    }

    // Help text
    let help_text = Line::from(vec![
        Span::styled(
            "↑↓",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":navigate  "),
        Span::styled(
            "Enter",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":view details  "),
        Span::styled(
            "c",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":create app  "),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":close"),
    ]);

    let help_paragraph = Paragraph::new(help_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(help_paragraph, chunks[3]);
}

/// Render application details view
fn render_application_details_view(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Min(5),    // Details
            Constraint::Length(2), // Help text
        ])
        .split(area);

    if let Some(app) = &ui_state.app_manager_selected_application {
        // Title
        let title_text = Line::from(vec![Span::styled(
            "Application Details",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]);
        let title_paragraph = Paragraph::new(title_text)
            .style(Style::default().fg(Color::White))
            .alignment(Alignment::Center);
        frame.render_widget(title_paragraph, chunks[0]);

        // Details
        let mut details_text = vec![
            Line::from(vec![
                Span::styled("ID: ", Style::default().fg(Color::Cyan)),
                Span::styled(app.id.to_string(), Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("Owner ID: ", Style::default().fg(Color::Cyan)),
                Span::styled(app.owner_id.to_string(), Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("Created: ", Style::default().fg(Color::Cyan)),
                Span::styled(
                    app.created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(""),
            Line::from(vec![Span::styled(
                format!("Allowed Origins ({}):", app.allowed_origins.len()),
                Style::default().fg(Color::Cyan),
            )]),
        ];

        if app.allowed_origins.is_empty() {
            details_text.push(Line::from(vec![Span::styled(
                "  (no origins configured)",
                Style::default().fg(Color::Gray),
            )]));
        } else {
            for origin in &app.allowed_origins {
                details_text.push(Line::from(vec![
                    Span::styled("  • ", Style::default().fg(Color::Green)),
                    Span::styled(origin.to_string(), Style::default().fg(Color::White)),
                ]));
            }
        }

        details_text.push(Line::from(""));
        details_text.push(Line::from(vec![Span::styled(
            format!("Linked HTTP Domains ({}):", app.linked_http_domains.len()),
            Style::default().fg(Color::Cyan),
        )]));

        if app.linked_http_domains.is_empty() {
            details_text.push(Line::from(vec![Span::styled(
                "  (no domains linked)",
                Style::default().fg(Color::Gray),
            )]));
        } else {
            for domain in &app.linked_http_domains {
                details_text.push(Line::from(vec![
                    Span::styled("  • ", Style::default().fg(Color::Blue)),
                    Span::styled(domain.to_string(), Style::default().fg(Color::White)),
                ]));
            }
        }

        let details_paragraph = Paragraph::new(details_text)
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .title(" Details ")
                    .title_style(Style::default().fg(Color::Yellow)),
            )
            .style(Style::default().fg(Color::White));

        frame.render_widget(details_paragraph, chunks[1]);
    }

    // Help text
    let help_text = Line::from(vec![
        Span::styled(
            "a",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":add origin  "),
        Span::styled(
            "Backspace",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":back to list  "),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":close"),
    ]);

    let help_paragraph = Paragraph::new(help_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(help_paragraph, chunks[2]);
}

/// Render add origin view
fn render_add_origin_view(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Length(4), // Input field with format hint
            Constraint::Min(2),    // Result area
            Constraint::Length(2), // Help text
        ])
        .split(area);

    // Title
    let title_text = Line::from(vec![Span::styled(
        "Add Origin",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )]);
    let title_paragraph = Paragraph::new(title_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);
    frame.render_widget(title_paragraph, chunks[0]);

    // Input field with format hint
    let input_block = Block::default()
        .title(" Origin URL (e.g., http://localhost:3000 or https://example.com) ")
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::Yellow));

    let input_text = Paragraph::new(format!("{}_", &ui_state.app_manager_origin_input))
        .block(input_block)
        .style(Style::default().fg(Color::White));

    frame.render_widget(input_text, chunks[1]);

    // Result area
    if let Some(result) = &ui_state.app_manager_result {
        let result_paragraph = Paragraph::new(Line::from(vec![
            Span::styled("Result: ", Style::default().fg(Color::Gray)),
            Span::styled(result.clone(), Style::default().fg(Color::White)),
        ]))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .title(" Result ")
                .title_style(Style::default().fg(Color::Green)),
        )
        .wrap(ratatui::widgets::Wrap { trim: true });

        frame.render_widget(result_paragraph, chunks[2]);
    }

    // Help text
    let help_text = Line::from(vec![
        Span::styled(
            "Enter",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":add origin  "),
        Span::styled(
            "Backspace",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":cancel  "),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":close"),
    ]);

    let help_paragraph = Paragraph::new(help_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(help_paragraph, chunks[3]);
}

/// Render create application view
fn render_create_application_view(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Length(5), // Create info
            Constraint::Min(2),    // Result area
            Constraint::Length(2), // Help text
        ])
        .split(area);

    // Title
    let title_text = Line::from(vec![Span::styled(
        "Create Application",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )]);
    let title_paragraph = Paragraph::new(title_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);
    frame.render_widget(title_paragraph, chunks[0]);

    // Create info
    let create_text = vec![
        Line::from(""),
        Line::from(vec![Span::styled(
            "A new application will be created with:",
            Style::default().fg(Color::Cyan),
        )]),
        Line::from(vec![
            Span::styled("  • ", Style::default().fg(Color::Green)),
            Span::styled(
                "Default name and settings",
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled("  • ", Style::default().fg(Color::Green)),
            Span::styled("Your owner ID", Style::default().fg(Color::White)),
        ]),
    ];

    let create_paragraph = Paragraph::new(create_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Left);
    frame.render_widget(create_paragraph, chunks[1]);

    // Result area
    if let Some(result) = &ui_state.app_manager_result {
        let result_paragraph = Paragraph::new(Line::from(vec![
            Span::styled("Result: ", Style::default().fg(Color::Gray)),
            Span::styled(result.clone(), Style::default().fg(Color::White)),
        ]))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .title(" Result ")
                .title_style(Style::default().fg(Color::Green)),
        )
        .wrap(ratatui::widgets::Wrap { trim: true });

        frame.render_widget(result_paragraph, chunks[2]);
    }

    // Help text
    let help_text = Line::from(vec![
        Span::styled(
            "Enter",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":create  "),
        Span::styled(
            "Backspace",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":cancel  "),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":close"),
    ]);

    let help_paragraph = Paragraph::new(help_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(help_paragraph, chunks[3]);
}

/// Render log level modal
fn render_log_level_modal(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let modal_area = centered_rect(40, 30, area);

    // Clear background
    frame.render_widget(Clear, modal_area);

    // Main block
    let block = Block::default()
        .title(" Select Log Level ")
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::Black));

    let inner = block.inner(modal_area);
    frame.render_widget(block, modal_area);

    // Log level options
    let levels = [
        ("ERROR", "Only show errors", 0),
        ("WARN", "Show warnings and errors", 1),
        ("INFO", "Show info, warnings, and errors", 2),
        ("DEBUG", "Show debug logs and above", 3),
        ("TRACE", "Show all logs including trace", 4),
    ];

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(1), // Title
            Constraint::Length(1), // Space
            Constraint::Length(1), // Error
            Constraint::Length(1), // Warn
            Constraint::Length(1), // Info
            Constraint::Length(1), // Debug
            Constraint::Length(1), // Trace
            Constraint::Length(1), // Space
            Constraint::Length(1), // Help
        ])
        .split(inner);

    // Title
    let title = Paragraph::new("Choose log level filter:")
        .style(Style::default().fg(Color::Yellow))
        .alignment(Alignment::Center);
    frame.render_widget(title, chunks[0]);

    // Render each level option
    for (i, (level, desc, idx)) in levels.iter().enumerate() {
        let is_selected = ui_state.log_level_modal_selected == *idx;
        let is_current = matches!(
            (ui_state.log_level_filter, idx),
            (LogLevel::Error, 0)
                | (LogLevel::Warn, 1)
                | (LogLevel::Info, 2)
                | (LogLevel::Debug, 3)
                | (LogLevel::Trace, 4)
        );

        let mut text = format!("  {level} - {desc}");
        if is_current {
            text.push_str(" (current)");
        }

        let style = if is_selected {
            Style::default().fg(Color::Black).bg(Color::White)
        } else if is_current {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::White)
        };

        let paragraph = Paragraph::new(text).style(style);
        frame.render_widget(paragraph, chunks[i + 2]);
    }

    // Help text
    let help = Paragraph::new("↑↓: Navigate | Enter: Select | Esc: Cancel")
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Center);
    frame.render_widget(help, chunks[8]);
}

/// Helper to create centered rect
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
