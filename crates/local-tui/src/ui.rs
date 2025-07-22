//! TUI interface components

use crate::{logs_viewer::LogReader, messages::TuiNodeId};

use proven_applications::Application;
use std::collections::{HashMap, HashSet};

use proven_local::NodeStatus;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation,
        ScrollbarState,
    },
};
use std::time::Instant;

/// Mouse state for dynamic capture switching
#[derive(Debug)]
pub struct MouseState {
    /// Whether mouse button is currently pressed
    pub is_down: bool,
    /// When the mouse button was pressed
    pub down_time: Option<Instant>,
    /// Whether mouse capture is currently enabled
    pub capture_enabled: bool,
}

impl MouseState {
    /// Create a new mouse state
    pub const fn new() -> Self {
        Self {
            is_down: false,
            down_time: None,
            capture_enabled: true, // Start with capture enabled
        }
    }
}

impl Default for MouseState {
    fn default() -> Self {
        Self::new()
    }
}

/// UI state management
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct UiState {
    /// Current scroll position (maintained by background thread, used for scrollbar display only)
    pub log_scroll: usize,
    /// Current viewport logs for display
    pub viewport_logs: Vec<crate::messages::LogEntry>,
    /// Total number of lines in the current log file
    pub total_log_lines: usize,
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
    /// Whether to show log level selection modal
    pub show_log_level_modal: bool,
    /// Selected index in log level modal (0-4 for Error, Warn, Info, Debug, Trace)
    pub log_level_modal_selected: usize,
    /// Whether to show RPC modal
    pub show_rpc_modal: bool,
    /// Selected tab in RPC modal (0 = Management, 1 = Application)
    pub rpc_modal_tab_selected: usize,
    /// Selected command in RPC modal
    pub rpc_modal_command_selected: usize,
    /// Result of last RPC command execution
    pub rpc_modal_result: Option<String>,
    /// Last applied node filter to prevent redundant calls
    last_applied_node_filter: Option<TuiNodeId>,
    /// Whether to show node type selection modal
    pub show_node_type_modal: bool,
    /// Selected specializations for new node,
    pub node_specializations_selected: Vec<bool>,
    /// Currently highlighted index in the specializations modal
    pub node_modal_selected_index: usize,
    /// Whether to show application manager modal
    pub show_application_manager_modal: bool,
    /// Current view in application manager (0 = list, 1 = details, 2 = add origin, 3 = create app)
    pub app_manager_view: usize,
    /// Selected application index in the list
    pub app_manager_selected_index: usize,
    /// List of applications owned by the user
    pub app_manager_applications: Vec<Application>,
    /// Currently selected application for details/operations
    pub app_manager_selected_application: Option<Application>,
    /// Input text for adding new origins
    pub app_manager_origin_input: String,
    /// Result message for application operations
    pub app_manager_result: Option<String>,
    /// Sidebar area for mouse click detection
    pub sidebar_area: ratatui::layout::Rect,
    /// Main sidebar list area (for Overview and nodes)
    pub sidebar_main_area: ratatui::layout::Rect,
    /// Debug area at bottom of sidebar  
    pub sidebar_debug_area: ratatui::layout::Rect,
    /// Logs area for mouse scroll detection
    pub logs_area: ratatui::layout::Rect,
    /// Mouse state for dynamic capture switching
    pub mouse_state: MouseState,
}

impl UiState {
    /// Create a new UI state
    #[must_use]
    pub fn new() -> Self {
        Self {
            log_scroll: 0,
            viewport_logs: Vec::new(),
            total_log_lines: 0,
            log_viewport_height: 0,
            log_scrollbar_state: ScrollbarState::default(),
            logs_sidebar_selected: 0,
            logs_sidebar_debug_selected: false,
            logs_sidebar_nodes: Vec::new(),
            show_help: false,
            show_log_level_modal: false,
            log_level_modal_selected: 2, // Default to Info (index 2)
            show_rpc_modal: false,
            rpc_modal_tab_selected: 0,
            rpc_modal_command_selected: 0,
            rpc_modal_result: None,
            last_applied_node_filter: None,
            show_node_type_modal: false,
            node_specializations_selected: vec![false; 7],
            node_modal_selected_index: 0,
            show_application_manager_modal: false,
            app_manager_view: 0,
            app_manager_selected_index: 0,
            app_manager_applications: Vec::new(),
            app_manager_selected_application: None,
            app_manager_origin_input: String::new(),
            app_manager_result: None,
            sidebar_area: ratatui::layout::Rect::default(),
            sidebar_main_area: ratatui::layout::Rect::default(),
            sidebar_debug_area: ratatui::layout::Rect::default(),
            logs_area: ratatui::layout::Rect::default(),
            mouse_state: MouseState::new(),
        }
    }

    /// Update scrollbar state based on current scroll position from background thread
    pub const fn update_scrollbar_state(&mut self) {
        let total_logs = self.total_log_lines;
        let viewport_height = self.log_viewport_height;

        if total_logs <= viewport_height {
            // All content fits in viewport, no scrolling needed
            self.log_scrollbar_state = self
                .log_scrollbar_state
                .content_length(total_logs)
                .viewport_content_length(viewport_height)
                .position(0);
        } else {
            // Content requires scrolling
            // For ratatui scrollbar: position 0 = top, position max = bottom
            // For our logs: log_scroll 0 = bottom, log_scroll max = top
            // So we need to invert: scrollbar_position = max_scroll - log_scroll
            let max_scroll = total_logs - viewport_height;
            let scrollbar_position = max_scroll.saturating_sub(self.log_scroll);

            // Set content_length to max_scroll + 1 to ensure scrollbar can reach the bottom
            // This makes the scrollbar's internal max position exactly equal to max_scroll
            self.log_scrollbar_state = self
                .log_scrollbar_state
                .content_length(max_scroll + 1)
                .viewport_content_length(1)
                .position(scrollbar_position);
        }
    }

    /// Update viewport logs and scroll state from background thread response
    pub fn update_viewport_logs(
        &mut self,
        logs: Vec<crate::messages::LogEntry>,
        total_lines: usize,
        scroll_position: usize,
    ) {
        self.viewport_logs = logs;
        self.total_log_lines = total_lines;
        self.log_scroll = scroll_position;

        // Update scrollbar state based on new scroll position
        self.update_scrollbar_state();
    }

    /// Update node filter only if it has changed (prevents redundant calls that cause auto-scroll jumping)
    pub fn update_node_filter_if_changed(
        &mut self,
        new_filter: Option<TuiNodeId>,
        log_reader: &LogReader,
    ) {
        if self.last_applied_node_filter != new_filter {
            self.last_applied_node_filter = new_filter;
            log_reader.set_node_filter(new_filter);
        }
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
        NodeStatus::NotStarted => "○", // Empty circle for not started
        NodeStatus::Running => "●",    // Filled circle for running
        NodeStatus::Starting => "◐",   // Half-filled circle for starting
        NodeStatus::Stopped | NodeStatus::Stopping => "◯", // Empty circle for stopping/stopped
        NodeStatus::Failed(_) => "✗",  // X for failed
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
            Color::Indexed(94),  // Bright blue (256-color)
            Color::Indexed(130), // Dark orange (256-color)
            Color::Indexed(97),  // Bright cyan (256-color)
            Color::Indexed(133), // Bright magenta (256-color)
            Color::Indexed(100), // Bright green (256-color)
            Color::Indexed(124), // Bright red (256-color)
        ];
        node_colors[(node_id.execution_order() - 1) as usize % node_colors.len()]
    }
}

/// Strip ANSI escape sequences from a string
fn strip_ansi_sequences(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            // Check if this is an ANSI escape sequence
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['

                // Skip until we find the end of the sequence (a letter)
                for next_ch in chars.by_ref() {
                    if next_ch.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                // Not an ANSI sequence, keep the character
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    result
}

/// Wrap text to fit within a specified width, breaking at word boundaries when possible
fn wrap_text(text: &str, max_width: usize) -> Vec<String> {
    if max_width == 0 {
        return vec![text.to_string()];
    }

    let mut lines = Vec::new();
    let mut current_line = String::new();

    for word in text.split_whitespace() {
        // If adding this word would exceed the width
        if !current_line.is_empty() && current_line.len() + 1 + word.len() > max_width {
            lines.push(current_line.clone());
            current_line.clear();
        }

        // If the word itself is longer than max_width, we need to break it
        if word.len() > max_width {
            // If we have content in current_line, push it first
            if !current_line.is_empty() {
                lines.push(current_line.clone());
                current_line.clear();
            }

            // Break the long word into chunks
            let mut remaining_word = word;
            while remaining_word.len() > max_width {
                let (chunk, rest) = remaining_word.split_at(max_width);
                lines.push(chunk.to_string());
                remaining_word = rest;
            }

            // Add the remaining part of the word
            if !remaining_word.is_empty() {
                current_line = remaining_word.to_string();
            }
        } else {
            // Add the word to current line
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(word);
        }
    }

    // Add the last line if it's not empty
    if !current_line.is_empty() {
        lines.push(current_line);
    }

    // If we have no lines, return the original text (edge case)
    if lines.is_empty() {
        lines.push(text.to_string());
    }

    lines
}

/// Create colored spans for a log entry with proper alignment and wrapping
#[allow(clippy::too_many_lines)]
pub fn create_colored_log_lines(
    entry: &'_ crate::messages::LogEntry,
    show_node_name: bool,
    max_width: u16,
) -> Vec<Line<'_>> {
    let timestamp = format!("{}", entry.timestamp.format("%H:%M:%S%.3f"));
    let node_name = entry.node_id.pokemon_name();

    // Get consistent node color
    let node_color = get_node_color(entry.node_id);

    // Use tracing crate default colors
    let level_color = match entry.level {
        crate::messages::LogLevel::Error => Color::LightRed,
        crate::messages::LogLevel::Warn => Color::LightYellow,
        crate::messages::LogLevel::Info => Color::LightGreen,
        crate::messages::LogLevel::Debug => Color::LightBlue,
        crate::messages::LogLevel::Trace => Color::LightMagenta,
    };

    // Fixed-width formatting for perfect alignment
    let level_str = match entry.level {
        crate::messages::LogLevel::Error => "ERRO",
        crate::messages::LogLevel::Warn => "WARN",
        crate::messages::LogLevel::Info => "INFO",
        crate::messages::LogLevel::Debug => "DEBG",
        crate::messages::LogLevel::Trace => "TRAC",
    };

    // Calculate the prefix length to determine how much space is left for the message
    let prefix_length = entry.target.as_ref().map_or_else(
        || {
            if show_node_name {
                // "HH:MM:SS.mmm [ERRO] [node_name]: "
                timestamp.len() + 1 + 1 + level_str.len() + 1 + 1 + 1 + node_name.len() + 2
            } else {
                // "HH:MM:SS.mmm [ERRO]: "
                timestamp.len() + 1 + 1 + level_str.len() + 2
            }
        },
        |target| {
            if show_node_name {
                // "HH:MM:SS.mmm [ERRO] [node_name] target: "
                timestamp.len()
                    + 1
                    + 1
                    + level_str.len()
                    + 1
                    + 1
                    + 1
                    + node_name.len()
                    + 1
                    + 1
                    + target.len()
                    + 2
            } else {
                // "HH:MM:SS.mmm [ERRO] target: "
                timestamp.len() + 1 + 1 + level_str.len() + 1 + 1 + target.len() + 2
            }
        },
    );

    // Clean the message
    let clean_message = strip_ansi_sequences(&entry.message);

    // Calculate available width for the message (accounting for borders and padding)
    #[allow(clippy::cast_possible_truncation)]
    let available_width = max_width
        .saturating_sub(prefix_length as u16)
        .saturating_sub(4); // Extra margin for safety

    // Split the message into lines that fit within the available width
    let wrapped_lines = wrap_text(&clean_message, available_width as usize);

    let mut result_lines = Vec::new();

    for (line_idx, wrapped_line) in wrapped_lines.iter().enumerate() {
        if line_idx == 0 {
            // First line includes the full prefix
            if let Some(target) = &entry.target {
                if show_node_name {
                    result_lines.push(Line::from(vec![
                        Span::styled(timestamp.clone(), Style::default().fg(Color::DarkGray)),
                        Span::raw(" "),
                        Span::styled(
                            "[",
                            Style::default()
                                .fg(level_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            level_str,
                            Style::default()
                                .fg(level_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            "]",
                            Style::default()
                                .fg(level_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(" "),
                        Span::styled("[", Style::default().fg(node_color)),
                        Span::styled(node_name.clone(), Style::default().fg(node_color)),
                        Span::styled("]", Style::default().fg(node_color)),
                        Span::raw(" "),
                        Span::styled(target.clone(), Style::default().fg(Color::DarkGray)),
                        Span::raw(": "),
                        Span::styled(wrapped_line.clone(), Style::default().fg(Color::White)),
                    ]));
                } else {
                    result_lines.push(Line::from(vec![
                        Span::styled(timestamp.clone(), Style::default().fg(Color::DarkGray)),
                        Span::raw(" "),
                        Span::styled(
                            "[",
                            Style::default()
                                .fg(level_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            level_str,
                            Style::default()
                                .fg(level_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            "]",
                            Style::default()
                                .fg(level_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(" "),
                        Span::styled(target.clone(), Style::default().fg(Color::DarkGray)),
                        Span::raw(": "),
                        Span::styled(wrapped_line.clone(), Style::default().fg(Color::White)),
                    ]));
                }
            } else if show_node_name {
                result_lines.push(Line::from(vec![
                    Span::styled(timestamp.clone(), Style::default().fg(Color::DarkGray)),
                    Span::raw(" "),
                    Span::styled(
                        "[",
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        level_str,
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        "]",
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(" "),
                    Span::styled("[", Style::default().fg(node_color)),
                    Span::styled(node_name.clone(), Style::default().fg(node_color)),
                    Span::styled("]", Style::default().fg(node_color)),
                    Span::raw(": "),
                    Span::styled(wrapped_line.clone(), Style::default().fg(Color::White)),
                ]));
            } else {
                result_lines.push(Line::from(vec![
                    Span::styled(timestamp.clone(), Style::default().fg(Color::DarkGray)),
                    Span::raw(" "),
                    Span::styled(
                        "[",
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        level_str,
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        "]",
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(": "),
                    Span::styled(wrapped_line.clone(), Style::default().fg(Color::White)),
                ]));
            }
        } else {
            // Continuation lines are indented to align with the message
            let indent = " ".repeat(prefix_length);
            result_lines.push(Line::from(vec![
                Span::styled(indent, Style::default().fg(Color::DarkGray)),
                Span::styled(wrapped_line.clone(), Style::default().fg(Color::White)),
            ]));
        }
    }

    result_lines
}

/// Main UI rendering function
pub fn render_ui<S: std::hash::BuildHasher>(
    frame: &mut Frame,
    ui_state: &mut UiState,
    nodes: &HashMap<
        TuiNodeId,
        (
            String,
            NodeStatus,
            HashSet<proven_topology::NodeSpecialization>,
        ),
        S,
    >,
    log_reader: &LogReader,
    shutting_down: bool,
) {
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

    // Render logs in the main area
    render_logs(frame, chunks[1], log_reader, ui_state, nodes);

    // Render footer at the bottom
    render_footer(frame, chunks[2], shutting_down, ui_state, nodes);

    // Render help overlay if requested
    if ui_state.show_help {
        render_help_overlay(frame, frame.area());
    }

    // Render log level modal if requested
    if ui_state.show_log_level_modal {
        render_log_level_modal(frame, frame.area(), ui_state, log_reader);
    }

    // Render RPC modal if requested
    if ui_state.show_rpc_modal {
        render_rpc_modal(frame, frame.area(), ui_state);
    }

    // Render node type selection modal if requested
    if ui_state.show_node_type_modal {
        render_node_type_modal(frame, frame.area(), ui_state);
    }

    // Render application manager modal if requested
    if ui_state.show_application_manager_modal {
        render_application_manager_modal(frame, frame.area(), ui_state);
    }
}

/// Render the header with title
fn render_header(frame: &mut Frame, area: ratatui::layout::Rect) {
    let header_text = Line::from(vec![Span::styled(
        "Proven Network - Local Debugger",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )]);

    let paragraph = Paragraph::new(header_text)
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(paragraph, area);
}

/// Render logs view
#[allow(clippy::too_many_lines)]
fn render_logs<S: std::hash::BuildHasher>(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    log_reader: &LogReader,
    ui_state: &mut UiState,
    nodes: &HashMap<
        TuiNodeId,
        (
            String,
            NodeStatus,
            HashSet<proven_topology::NodeSpecialization>,
        ),
        S,
    >,
) {
    // Update nodes with logs for sidebar - use NodeManager data instead of scanning log files
    let mut nodes_with_logs: Vec<TuiNodeId> = nodes
        .keys()
        .filter(|&&node_id| node_id != crate::messages::MAIN_THREAD_NODE_ID)
        .copied()
        .collect();

    // Sort by execution order to match overview screen
    nodes_with_logs.sort_by_key(|node_id| node_id.execution_order());
    ui_state.logs_sidebar_nodes = nodes_with_logs;

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
    render_logs_sidebar(frame, horizontal_chunks[0], ui_state, log_reader, nodes);

    // Render logs in the remaining space
    let logs_area = horizontal_chunks[1];

    // Store logs area for mouse scroll detection
    ui_state.logs_area = logs_area;

    // Determine what the node filter should be based on current UI state
    let desired_node_filter = if ui_state.logs_sidebar_debug_selected {
        // Show only debug (main thread) logs
        Some(crate::messages::MAIN_THREAD_NODE_ID)
    } else if ui_state.logs_sidebar_selected == 0 {
        // Show all logs
        None
    } else if let Some(&selected_node_id) = ui_state
        .logs_sidebar_nodes
        .get(ui_state.logs_sidebar_selected - 1)
    {
        // Show logs from specific node
        Some(selected_node_id)
    } else {
        // Invalid selection, show all logs
        None
    };

    // Update node filter only if it has changed (prevents redundant calls that cause jumping)
    ui_state.update_node_filter_if_changed(desired_node_filter, log_reader);

    // Process any responses from the background thread
    while let Some(response) = log_reader.try_get_response() {
        use crate::logs_viewer::LogResponse;
        match response {
            LogResponse::ViewportUpdate {
                logs,
                total_filtered_lines,
                scroll_position,
            } => {
                // Update logs and scroll position from background thread (single source of truth)
                ui_state.update_viewport_logs(logs, total_filtered_lines, scroll_position);
            }
            LogResponse::Error { message } => {
                // Log error but continue rendering
                tracing::debug!("Log reader error: {}", message);
            }
        }
    }

    // Update viewport height for scrolling calculations
    let content_height = logs_area.height.saturating_sub(2); // Account for borders
    let new_viewport_height = content_height as usize;

    // If viewport size changed, update the background thread
    if ui_state.log_viewport_height != new_viewport_height {
        ui_state.log_viewport_height = new_viewport_height;
        log_reader.update_viewport_size(ui_state.log_viewport_height);
    }

    // If we don't have any logs yet, request initial data
    if ui_state.viewport_logs.is_empty() && ui_state.total_log_lines == 0 {
        log_reader.request_initial_data();
    }

    // Determine whether to show node names in logs
    let show_node_names =
        ui_state.logs_sidebar_selected == 0 && !ui_state.logs_sidebar_debug_selected;

    // Update scrollbar state based on current scroll position
    ui_state.update_scrollbar_state();

    // Convert viewport logs to Lines for display with wrapping support
    let display_lines: Vec<Line> = ui_state
        .viewport_logs
        .iter()
        .flat_map(|entry| create_colored_log_lines(entry, show_node_names, logs_area.width))
        .collect();

    // Get current log level for title
    let current_level = log_reader.get_level_filter();
    let level_text = match current_level {
        crate::messages::LogLevel::Error => "ERROR",
        crate::messages::LogLevel::Warn => "WARN",
        crate::messages::LogLevel::Info => "INFO",
        crate::messages::LogLevel::Debug => "DEBUG",
        crate::messages::LogLevel::Trace => "TRACE",
    };

    // Create dynamic title based on selection
    let (title_text, title_style) = if ui_state.logs_sidebar_debug_selected {
        (
            format!(" Logs - Debug [{level_text}] "),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else if ui_state.logs_sidebar_selected == 0 {
        (
            format!(" Logs - All [{level_text}] "),
            Style::default().add_modifier(Modifier::BOLD),
        )
    } else if let Some(&selected_node_id) = ui_state
        .logs_sidebar_nodes
        .get(ui_state.logs_sidebar_selected - 1)
    {
        let pokemon_name = selected_node_id.full_pokemon_name();
        let node_color = get_node_color(selected_node_id);
        (
            format!(" Logs - {pokemon_name} [{level_text}] "),
            Style::default().fg(node_color).add_modifier(Modifier::BOLD),
        )
    } else {
        (
            format!(" Logs [{level_text}] "),
            Style::default().add_modifier(Modifier::BOLD),
        )
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
        let scrollbar_area = ratatui::layout::Rect {
            x: logs_area.x + logs_area.width.saturating_sub(1),
            y: logs_area.y + 1,
            width: 1,
            height: logs_area.height.saturating_sub(2),
        };

        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            scrollbar_area,
            &mut ui_state.log_scrollbar_state,
        );
    }
}

/// Render logs sidebar with node selection
#[allow(clippy::too_many_lines)]
fn render_logs_sidebar<S: std::hash::BuildHasher>(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    ui_state: &mut UiState,
    _log_reader: &LogReader,
    nodes: &HashMap<
        TuiNodeId,
        (
            String,
            NodeStatus,
            HashSet<proven_topology::NodeSpecialization>,
        ),
        S,
    >,
) {
    // Store sidebar areas for mouse click detection
    ui_state.sidebar_area = area;

    // Render the border
    let border_block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Nodes ({})", ui_state.logs_sidebar_nodes.len()));
    frame.render_widget(border_block, area);

    // Apply internal margins to the sidebar area
    let inner_area = area.inner(ratatui::layout::Margin {
        vertical: 1,
        horizontal: 1,
    });

    // Split the inner area to put debug at the bottom (always show debug)
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
                format!(" [`] Overview{}", " ".repeat(23 - 8)), // Pad to full width (23 - 8 chars)
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
    for (index, &node_id) in ui_state.logs_sidebar_nodes.iter().enumerate() {
        let list_index = index + 1; // +1 because "All" is at index 0
        let is_selected =
            ui_state.logs_sidebar_selected == list_index && !ui_state.logs_sidebar_debug_selected;
        let node_color = get_node_color(node_id);

        // Get node status and status icon
        let (status_icon, status_color) =
            if let Some((_, status, _specializations)) = nodes.get(&node_id) {
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

        // Show number shortcut for first 9 nodes
        let prefix = if index < 9 {
            format!("[{}] ", index + 1)
        } else {
            "    ".to_string()
        };

        let styled_text = if is_selected {
            // Calculate content length and pad to full width
            let content = format!(" {}{} {}", prefix, status_icon, node_id.full_pokemon_name());
            let padding_needed = if content.len() < 25 {
                25 - content.len()
            } else {
                0
            };
            let padded_content = format!("{}{}", content, " ".repeat(padding_needed));

            vec![Span::styled(
                padded_content,
                Style::default()
                    .fg(Color::White)
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )]
        } else {
            vec![
                Span::styled(" ", Style::default()),
                Span::styled(prefix, Style::default().fg(Color::White)),
                Span::styled(status_icon, Style::default().fg(status_color)),
                Span::styled(" ", Style::default()),
                Span::styled(node_id.full_pokemon_name(), Style::default().fg(node_color)),
            ]
        };

        items.push(ListItem::new(Line::from(styled_text)));
    }

    // Render the main list (All + nodes)
    let list = List::new(items).style(Style::default().fg(Color::White));
    frame.render_widget(list, main_area);

    // Render debug item at the bottom (always show)
    let debug_text = if ui_state.logs_sidebar_debug_selected {
        Line::from(vec![Span::styled(
            format!(" [d] Debug Logs{}", " ".repeat(23 - 10)), // Pad to full width (23 - 10 chars)
            Style::default()
                .bg(Color::White)
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )])
    } else {
        Line::from(vec![Span::styled(
            " [d] Debug Logs",
            Style::default().fg(Color::Yellow),
        )])
    };

    let debug_paragraph = Paragraph::new(debug_text).style(Style::default().fg(Color::White));
    frame.render_widget(debug_paragraph, debug_area);
}

/// Render footer with context-aware key hints
fn render_footer<S: std::hash::BuildHasher>(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    shutting_down: bool,
    ui_state: &UiState,
    nodes: &HashMap<
        TuiNodeId,
        (
            String,
            NodeStatus,
            HashSet<proven_topology::NodeSpecialization>,
        ),
        S,
    >,
) {
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
                if let Some((_, status, _specializations)) = nodes.get(&selected_node_id) {
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
            Span::styled("a", Style::default().fg(Color::LightMagenta)),
            Span::styled(":apps ", Style::default()),
            Span::styled("c", Style::default().fg(Color::LightMagenta)),
            Span::styled(":rpc ", Style::default()),
            Span::styled("l", Style::default().fg(Color::LightCyan)),
            Span::styled(":log-level ", Style::default()),
            Span::styled("?", Style::default().fg(Color::White)),
            Span::styled(":help", Style::default()),
        ]);

        Line::from(spans)
    };

    let paragraph = Paragraph::new(footer_text)
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(paragraph, area);
}

/// Render help overlay
fn render_help_overlay(frame: &mut Frame, area: ratatui::layout::Rect) {
    let help_text = vec![
        Line::from(Span::styled(
            "Proven Node TUI - Help",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Global Keys:"),
        Line::from("  q               - Graceful quit (stops all nodes)"),
        Line::from("  Ctrl+C          - Force quit (immediate exit)"),
        Line::from("  ?               - Toggle help"),
        Line::from(""),
        Line::from("Node Management:"),
        Line::from("  n               - Start new node"),
        Line::from("  s               - Start/stop selected node"),
        Line::from("  r               - Restart selected node"),
        Line::from(""),
        Line::from("Application Management:"),
        Line::from("  a               - Open Application Manager (view, create, manage apps)"),
        Line::from(""),
        Line::from("RPC Operations:"),
        Line::from("  c               - Open RPC modal (Management/Application commands)"),
        Line::from(""),
        Line::from("Navigation:"),
        Line::from("  Up/Down         - Navigate sidebar (select node)"),
        Line::from("  `               - Select Overview (in sidebar)"),
        Line::from("  1-9             - Select node in sidebar (quick access)"),
        Line::from("  d               - Select debug logs (in sidebar)"),
        Line::from(""),
        Line::from("Log Viewing:"),
        Line::from("  Alt+Up/Down     - Scroll logs (line by line)"),
        Line::from("  PageUp/PageDown - Scroll logs (page by page)"),
        Line::from("  Home/End        - Scroll to top/bottom of logs"),
        Line::from("  l               - Select log level filter"),
        Line::from(""),
        Line::from("Mouse Controls:"),
        Line::from("  Click sidebar   - Select node or debug logs"),
        Line::from("  Scroll wheel    - Scroll logs up/down"),
        Line::from("  Click and drag  - Text selection (auto-enabled after 250ms)"),
        Line::from(""),
        Line::from("Press ? or Esc to close this help"),
    ];

    let popup_area = centered_rect(60, 70, area);

    frame.render_widget(Clear, popup_area);

    let paragraph = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Help")
                .title_style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .style(Style::default().fg(Color::White));

    frame.render_widget(paragraph, popup_area);
}

/// Render log level selection modal
fn render_log_level_modal(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    ui_state: &UiState,
    log_reader: &LogReader,
) {
    let _current_level = log_reader.get_level_filter();

    let log_levels = [
        ("ERROR", crate::messages::LogLevel::Error, Color::LightRed),
        ("WARN", crate::messages::LogLevel::Warn, Color::LightYellow),
        ("INFO", crate::messages::LogLevel::Info, Color::LightGreen),
        ("DEBUG", crate::messages::LogLevel::Debug, Color::LightBlue),
        (
            "TRACE",
            crate::messages::LogLevel::Trace,
            Color::LightMagenta,
        ),
    ];

    let mut modal_text = vec![
        Line::from(Span::styled(
            "Log Level Filter",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Select a log level to filter logs:"),
        Line::from(""),
    ];

    for (i, (level_name, _level_enum, level_color)) in log_levels.iter().enumerate() {
        let is_selected = i == ui_state.log_level_modal_selected;

        let (prefix, suffix) = if is_selected {
            ("> ", " <")
        } else {
            ("  ", "  ")
        };

        let line = Line::from(vec![
            Span::styled(prefix, Style::default()),
            Span::styled(*level_name, Style::default().fg(*level_color)),
            Span::styled(suffix, Style::default()),
        ]);

        modal_text.push(line);
    }

    modal_text.extend(vec![
        Line::from(""),
        Line::from("Use Up/Down to navigate, Enter to select, Esc to cancel"),
    ]);

    let popup_area = centered_rect(40, 50, area);

    frame.render_widget(Clear, popup_area);

    let paragraph = Paragraph::new(modal_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Log Level Filter ")
                .title_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
        )
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(paragraph, popup_area);
}

/// Render RPC modal
#[allow(clippy::too_many_lines)]
fn render_rpc_modal(frame: &mut Frame, area: ratatui::layout::Rect, ui_state: &UiState) {
    let popup_area = centered_rect(70, 60, area);

    frame.render_widget(Clear, popup_area);

    // Create the main modal block
    let modal_block = Block::default()
        .borders(Borders::ALL)
        .title(" RPC Operations ")
        .title_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );

    frame.render_widget(modal_block, popup_area);

    // Split the modal into sections
    let inner_area = popup_area.inner(ratatui::layout::Margin {
        vertical: 1,
        horizontal: 2,
    });

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tab selection
            Constraint::Min(5),    // Commands
            Constraint::Length(3), // Result area (if there's a result)
            Constraint::Length(2), // Help text
        ])
        .split(inner_area);

    // Render tabs
    let tab_area = chunks[0];
    let tabs = vec![
        if ui_state.rpc_modal_tab_selected == 0 {
            Span::styled(
                " Management ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(" Management ", Style::default().fg(Color::White))
        },
        Span::raw(" "),
        if ui_state.rpc_modal_tab_selected == 1 {
            Span::styled(
                " Application ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(" Application ", Style::default().fg(Color::White))
        },
    ];

    let tabs_paragraph = Paragraph::new(Line::from(tabs))
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(tabs_paragraph, tab_area);

    // Render commands based on selected tab
    let commands_area = chunks[1];

    if ui_state.rpc_modal_tab_selected == 0 {
        // Management commands
        let management_commands = ["WhoAmI", "CreateApplication", "Identify", "Anonymize"];

        let mut command_items = Vec::new();

        for (i, command) in management_commands.iter().enumerate() {
            let is_selected = i == ui_state.rpc_modal_command_selected;

            let styled_text = if is_selected {
                vec![Span::styled(
                    format!("  > {command}  "),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::White)
                        .add_modifier(Modifier::BOLD),
                )]
            } else {
                vec![Span::styled(
                    format!("    {command}  "),
                    Style::default().fg(Color::White),
                )]
            };

            command_items.push(ListItem::new(Line::from(styled_text)));
        }

        let commands_list = List::new(command_items)
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .title(" Commands ")
                    .title_style(Style::default().fg(Color::Yellow)),
            )
            .style(Style::default().fg(Color::White));

        frame.render_widget(commands_list, commands_area);
    } else {
        // Application commands - under construction
        let under_construction = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "Application RPC is under construction",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::ITALIC),
            )),
            Line::from(""),
            Line::from("Coming soon: Execute code, list applications, etc."),
        ])
        .block(
            Block::default()
                .borders(Borders::TOP)
                .title(" Commands ")
                .title_style(Style::default().fg(Color::Yellow)),
        )
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);

        frame.render_widget(under_construction, commands_area);
    }

    // Render result area if there's a result
    if let Some(result) = &ui_state.rpc_modal_result {
        let result_area = chunks[2];

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

        frame.render_widget(result_paragraph, result_area);
    }

    // Render help text
    let help_area = chunks[3];
    let help_text = Line::from(vec![
        Span::styled(
            "Tab",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":switch  "),
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
        Span::raw(":execute  "),
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
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(help_paragraph, help_area);
}

/// Render node type selection modal
fn render_node_type_modal(frame: &mut Frame, area: ratatui::layout::Rect, ui_state: &UiState) {
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
            Span::styled(" ", Style::default()),
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
                .padding(ratatui::widgets::Padding::uniform(1)),
        )
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Left);

    frame.render_widget(paragraph, popup_area);
}

/// Render application manager modal
#[allow(clippy::too_many_lines)]
fn render_application_manager_modal(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    ui_state: &UiState,
) {
    let popup_area = centered_rect(80, 80, area);

    frame.render_widget(Clear, popup_area);

    // Create the main modal block
    let modal_block = Block::default()
        .borders(Borders::ALL)
        .title(" Application Manager ")
        .title_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );

    frame.render_widget(modal_block, popup_area);

    // Split the modal into sections
    let inner_area = popup_area.inner(ratatui::layout::Margin {
        vertical: 1,
        horizontal: 2,
    });

    match ui_state.app_manager_view {
        1 => render_application_details_view(frame, inner_area, ui_state),
        2 => render_add_origin_view(frame, inner_area, ui_state),
        3 => render_create_application_view(frame, inner_area, ui_state),
        _ => render_application_list_view(frame, inner_area, ui_state),
    }
}

/// Render application list view
#[allow(clippy::too_many_lines)]
fn render_application_list_view(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    ui_state: &UiState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title and instructions
            Constraint::Min(5),    // Application list
            Constraint::Length(3), // Result area (if there's a result)
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
        Span::raw(format!(
            " ({} applications)",
            ui_state.app_manager_applications.len()
        )),
    ]);
    let title_paragraph = Paragraph::new(title_text)
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);
    frame.render_widget(title_paragraph, chunks[0]);

    // Application list
    if ui_state.app_manager_applications.is_empty() {
        let empty_text = vec![
            Line::from(""),
            Line::from(Span::styled(
                "No applications found.",
                Style::default()
                    .fg(Color::Gray)
                    .add_modifier(Modifier::ITALIC),
            )),
            Line::from(""),
            Line::from(
                "Create a new application using the RPC modal (press 'c' → Management → CreateApplication)",
            ),
        ];
        let empty_paragraph = Paragraph::new(empty_text)
            .style(Style::default().fg(Color::White))
            .alignment(ratatui::layout::Alignment::Center);
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
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(help_paragraph, chunks[3]);
}

/// Render application details view
#[allow(clippy::too_many_lines)]
fn render_application_details_view(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    ui_state: &UiState,
) {
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
            .alignment(ratatui::layout::Alignment::Center);
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
        Span::raw(":back  "),
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
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(help_paragraph, chunks[2]);
}

/// Render add origin view
fn render_add_origin_view(frame: &mut Frame, area: ratatui::layout::Rect, ui_state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Length(4), // Input field
            Constraint::Length(3), // Result area (if there's a result)
            Constraint::Length(2), // Help text
        ])
        .split(area);

    // Title
    let title_text = Line::from(vec![Span::styled(
        "Add Allowed Origin",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )]);
    let title_paragraph = Paragraph::new(title_text)
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);
    frame.render_widget(title_paragraph, chunks[0]);

    // Input field
    let input_text = vec![
        Line::from("Enter the origin URL (e.g., https://example.com):"),
        Line::from(""),
        Line::from(vec![
            Span::styled("> ", Style::default().fg(Color::Cyan)),
            Span::styled(
                &ui_state.app_manager_origin_input,
                Style::default().fg(Color::White),
            ),
            Span::styled("█", Style::default().fg(Color::Gray)), // Cursor
        ]),
    ];

    let input_paragraph = Paragraph::new(input_text)
        .block(
            Block::default()
                .borders(Borders::TOP)
                .title(" Origin Input ")
                .title_style(Style::default().fg(Color::Yellow)),
        )
        .style(Style::default().fg(Color::White));

    frame.render_widget(input_paragraph, chunks[1]);

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
        Span::raw(":back  "),
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
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(help_paragraph, chunks[3]);
}

/// Render create application view
fn render_create_application_view(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    ui_state: &UiState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Length(6), // Instructions and info
            Constraint::Length(3), // Result area (if there's a result)
            Constraint::Length(2), // Help text
        ])
        .split(area);

    // Title
    let title_text = Line::from(vec![Span::styled(
        "Create New Application",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )]);
    let title_paragraph = Paragraph::new(title_text)
        .style(Style::default().fg(Color::White))
        .alignment(ratatui::layout::Alignment::Center);
    frame.render_widget(title_paragraph, chunks[0]);

    // Instructions and info
    let info_text = vec![
        Line::from("This will create a new application with the following defaults:"),
        Line::from(""),
        Line::from(vec![
            Span::styled("• ", Style::default().fg(Color::Cyan)),
            Span::styled("Owner: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                "Your authenticated identity",
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled("• ", Style::default().fg(Color::Cyan)),
            Span::styled("Allowed Origins: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                "None (you can add them later)",
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled("• ", Style::default().fg(Color::Cyan)),
            Span::styled("HTTP Domains: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                "None (you can link them later)",
                Style::default().fg(Color::White),
            ),
        ]),
    ];

    let info_paragraph = Paragraph::new(info_text)
        .block(
            Block::default()
                .borders(Borders::TOP)
                .title(" Application Details ")
                .title_style(Style::default().fg(Color::Yellow)),
        )
        .style(Style::default().fg(Color::White));

    frame.render_widget(info_paragraph, chunks[1]);

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
            "Enter",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":create application  "),
        Span::styled(
            "Backspace",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":back  "),
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
        .alignment(ratatui::layout::Alignment::Center);

    frame.render_widget(help_paragraph, chunks[3]);
}

/// Helper function to create a centered rectangle
fn centered_rect(
    percent_x: u16,
    percent_y: u16,
    r: ratatui::layout::Rect,
) -> ratatui::layout::Rect {
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
