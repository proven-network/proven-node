//! TUI interface components

use crate::{
    logs_viewer::LogReader,
    messages::{NodeId, NodeStatus},
};
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
use std::collections::HashMap;

/// UI state management
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct UiState {
    /// Scroll position for logs (0 = bottom/newest, higher = scrolled up towards older logs)
    pub log_scroll: usize,
    /// Cached logs for display (updated periodically, not on every render)
    pub cached_logs: Vec<crate::messages::LogEntry>,
    /// Last time logs were refreshed
    pub last_log_refresh: std::time::Instant,
    /// Whether auto-scroll is enabled (follows new logs)
    pub auto_scroll: bool,
    /// Viewport height for logs (set during render)
    pub log_viewport_height: usize,
    /// Scrollbar state for logs
    pub log_scrollbar_state: ScrollbarState,
    /// Logs sidebar selected index (0 = "All", 1+ = node indices)
    pub logs_sidebar_selected: usize,
    /// Whether debug is selected in the sidebar
    pub logs_sidebar_debug_selected: bool,
    /// List of nodes that have logs
    pub logs_sidebar_nodes: Vec<NodeId>,
    /// Whether to show help overlay
    pub show_help: bool,
    /// Whether to show log level selection modal
    pub show_log_level_modal: bool,
    /// Selected index in log level modal (0-4 for Error, Warn, Info, Debug, Trace)
    pub log_level_modal_selected: usize,
}

impl UiState {
    /// Create a new UI state
    #[must_use]
    pub fn new() -> Self {
        Self {
            log_scroll: 0,
            cached_logs: Vec::new(),
            last_log_refresh: std::time::Instant::now(),
            auto_scroll: true,
            log_viewport_height: 0,
            log_scrollbar_state: ScrollbarState::default(),
            logs_sidebar_selected: 0,
            logs_sidebar_debug_selected: false,
            logs_sidebar_nodes: Vec::new(),
            show_help: false,
            show_log_level_modal: false,
            log_level_modal_selected: 2, // Default to Info (index 2)
        }
    }

    /// Get the maximum scroll position based on log count and viewport height
    pub const fn max_scroll_position(&self) -> usize {
        if self.cached_logs.len() <= self.log_viewport_height {
            0
        } else {
            self.cached_logs.len() - self.log_viewport_height
        }
    }

    /// Check if currently at the bottom (newest logs)
    pub const fn is_at_bottom(&self) -> bool {
        self.log_scroll == 0
    }

    /// Scroll to bottom (newest logs)
    pub const fn scroll_to_bottom(&mut self) {
        self.log_scroll = 0;
        self.auto_scroll = true;
    }

    /// Scroll to top (oldest logs)
    pub const fn scroll_to_top(&mut self) {
        self.log_scroll = self.max_scroll_position();
        self.auto_scroll = false;
    }

    /// Scroll up (towards older logs)
    pub fn scroll_up(&mut self, amount: usize) {
        let max_scroll = self.max_scroll_position();
        self.log_scroll = (self.log_scroll + amount).min(max_scroll);
        self.auto_scroll = false;
    }

    /// Scroll down (towards newer logs)
    pub const fn scroll_down(&mut self, amount: usize) {
        if self.log_scroll >= amount {
            self.log_scroll -= amount;
        } else {
            self.log_scroll = 0;
        }

        // Re-enable auto-scroll if we've scrolled back to the bottom
        if self.log_scroll == 0 {
            self.auto_scroll = true;
        }
    }

    /// Update scrollbar state
    pub const fn update_scrollbar_state(&mut self) {
        let total_logs = self.cached_logs.len();
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

    /// Check if logs need refreshing (limit to 20 times per second)
    pub fn should_refresh_logs(&self) -> bool {
        self.last_log_refresh.elapsed().as_millis() > 50
    }

    /// Update cached logs and refresh timestamp
    pub fn update_cached_logs(&mut self, logs: Vec<crate::messages::LogEntry>) {
        let old_count = self.cached_logs.len();
        let was_at_bottom = self.is_at_bottom();

        self.cached_logs = logs;
        self.last_log_refresh = std::time::Instant::now();

        // Only auto-scroll if user was already at the bottom and new logs were added
        if self.auto_scroll && was_at_bottom && self.cached_logs.len() > old_count {
            self.log_scroll = 0;
        }

        // Update scrollbar state
        self.update_scrollbar_state();
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
fn get_node_color(node_id: NodeId) -> Color {
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

/// Create colored spans for a log entry with proper alignment
#[allow(clippy::too_many_lines)]
fn create_colored_log_line(entry: &crate::messages::LogEntry, show_node_name: bool) -> Line {
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

    if let Some(target) = &entry.target {
        if show_node_name {
            Line::from(vec![
                Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
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
                Span::styled(node_name, Style::default().fg(node_color)),
                Span::styled("]", Style::default().fg(node_color)),
                Span::raw(" "),
                Span::styled(target, Style::default().fg(Color::DarkGray)),
                Span::raw(": "),
                Span::styled(entry.message.clone(), Style::default().fg(Color::White)),
            ])
        } else {
            Line::from(vec![
                Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
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
                Span::styled(target, Style::default().fg(Color::DarkGray)),
                Span::raw(": "),
                Span::styled(entry.message.clone(), Style::default().fg(Color::White)),
            ])
        }
    } else if show_node_name {
        Line::from(vec![
            Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
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
            Span::styled(node_name, Style::default().fg(node_color)),
            Span::styled("]", Style::default().fg(node_color)),
            Span::raw(": "),
            Span::styled(entry.message.clone(), Style::default().fg(Color::White)),
        ])
    } else {
        Line::from(vec![
            Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
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
            Span::styled(entry.message.clone(), Style::default().fg(Color::White)),
        ])
    }
}

/// Main UI rendering function
pub fn render_ui<S: std::hash::BuildHasher>(
    frame: &mut Frame,
    ui_state: &mut UiState,
    nodes: &HashMap<NodeId, (String, NodeStatus), S>,
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

    // Extract cached logs to avoid borrowing issues
    let cached_logs = ui_state.cached_logs.clone();

    // Render logs in the main area
    render_logs(frame, chunks[1], &cached_logs, log_reader, ui_state, nodes);

    // Render footer at the bottom
    render_footer(frame, chunks[2], shutting_down);

    // Render help overlay if requested
    if ui_state.show_help {
        render_help_overlay(frame, frame.area());
    }

    // Render log level modal if requested
    if ui_state.show_log_level_modal {
        render_log_level_modal(frame, frame.area(), ui_state, log_reader);
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
    _cached_logs: &[crate::messages::LogEntry],
    log_reader: &LogReader,
    ui_state: &mut UiState,
    nodes: &HashMap<NodeId, (String, NodeStatus), S>,
) {
    // Update nodes with logs for sidebar - use NodeManager data instead of scanning log files
    let mut nodes_with_logs: Vec<NodeId> = nodes
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

    // Set the node filter on the log reader and get fresh logs
    if ui_state.logs_sidebar_debug_selected {
        // Show only debug (main thread) logs
        log_reader.set_node_filter(Some(crate::messages::MAIN_THREAD_NODE_ID));
    } else if ui_state.logs_sidebar_selected == 0 {
        // Show all logs
        log_reader.set_node_filter(None);
    } else if let Some(&selected_node_id) = ui_state
        .logs_sidebar_nodes
        .get(ui_state.logs_sidebar_selected - 1)
    {
        // Show logs from specific node
        log_reader.set_node_filter(Some(selected_node_id));
    } else {
        // Invalid selection, show all logs
        log_reader.set_node_filter(None);
    }

    let filtered_logs = log_reader
        .get_filtered_logs_blocking(std::time::Duration::from_millis(100))
        .into_iter()
        .collect::<Vec<_>>();

    // Determine whether to show node names in logs
    let show_node_names =
        ui_state.logs_sidebar_selected == 0 && !ui_state.logs_sidebar_debug_selected;

    // Convert LogEntry references to Lines for display
    let log_lines: Vec<Line> = filtered_logs
        .iter()
        .map(|entry| create_colored_log_line(entry, show_node_names))
        .collect();

    // Update viewport height for scrolling calculations
    let content_height = logs_area.height.saturating_sub(2); // Account for borders
    ui_state.log_viewport_height = content_height as usize;

    // Calculate which lines to display based on scroll position
    // Note: logs come in chronological order (index 0 = oldest, last index = newest)
    // We want to display with newest at bottom (terminal-like behavior)
    let total_lines = log_lines.len();
    let viewport_height = ui_state.log_viewport_height;

    let display_lines = if total_lines <= viewport_height {
        // All lines fit in viewport - show in chronological order (newest at bottom)
        log_lines
    } else {
        // Need to scroll - log_scroll of 0 means show newest (bottom)
        // log_scroll of max means show oldest (top)
        // Since we want newest at bottom, we need to calculate from the end
        let start_from_end = ui_state.log_scroll;
        let end_index = total_lines.saturating_sub(start_from_end);
        let start_index = end_index.saturating_sub(viewport_height);

        // Take slice in chronological order (newest will be at bottom of viewport)
        log_lines[start_index..end_index].to_vec()
    };

    // Update scrollbar state based on current scroll position
    ui_state.update_scrollbar_state();

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
    if total_lines > viewport_height {
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
    ui_state: &UiState,
    _log_reader: &LogReader,
    nodes: &HashMap<NodeId, (String, NodeStatus), S>,
) {
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
        let (status_icon, status_color) = if let Some((_, status)) = nodes.get(&node_id) {
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

/// Render footer with key hints
fn render_footer(frame: &mut Frame, area: ratatui::layout::Rect, shutting_down: bool) {
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
        Line::from(vec![
            Span::styled(
                "Keys: ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("q", Style::default().fg(Color::Red)),
            Span::styled(":quit ", Style::default()),
            Span::styled("s", Style::default().fg(Color::Green)),
            Span::styled(":start ", Style::default()),
            Span::styled("r", Style::default().fg(Color::Blue)),
            Span::styled(":refresh ", Style::default()),
            Span::styled("`", Style::default().fg(Color::Yellow)),
            Span::styled(":overview ", Style::default()),
            Span::styled("1-9", Style::default().fg(Color::Yellow)),
            Span::styled(":select ", Style::default()),
            Span::styled("d", Style::default().fg(Color::Magenta)),
            Span::styled(":debug ", Style::default()),
            Span::styled("a", Style::default().fg(Color::Cyan)),
            Span::styled(":auto-scroll ", Style::default()),
            Span::styled("l", Style::default().fg(Color::LightCyan)),
            Span::styled(":log-level ", Style::default()),
            Span::styled("?", Style::default().fg(Color::White)),
            Span::styled(":help", Style::default()),
        ])
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
        Line::from("  s               - Start new node"),
        Line::from("  r               - Refresh status"),
        Line::from(""),
        Line::from("Logs Navigation:"),
        Line::from("  `               - Select Overview (in sidebar)"),
        Line::from("  1-9             - Select node in sidebar (quick access)"),
        Line::from("  d               - Select debug logs (in sidebar)"),
        Line::from("  Up/Down         - Navigate sidebar (select node)"),
        Line::from("  Home/End        - Scroll to top/bottom of logs"),
        Line::from(""),
        Line::from("Log Viewing:"),
        Line::from("  Alt+Up/Down     - Scroll logs (line by line)"),
        Line::from("  PageUp/PageDown - Scroll logs (page by page)"),
        Line::from("  a               - Toggle auto-scroll"),
        Line::from("  l               - Select log level filter"),
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
