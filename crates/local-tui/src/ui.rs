//! TUI interface components

use crate::{
    logs::LogCollector,
    messages::{NodeId, NodeStatus},
};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Tabs,
    },
};
use std::collections::HashMap;

/// Current view mode
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewMode {
    /// Overview of all nodes
    Overview,
    /// Detailed view of a specific node
    NodeDetail(NodeId),
    /// Logs view
    Logs,
    /// Help screen
    Help,
}

/// UI state management
#[derive(Debug)]
pub struct UiState {
    /// Current view mode
    pub view_mode: ViewMode,
    /// Selected tab index
    pub selected_tab: usize,
    /// Whether to show help
    pub show_help: bool,
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
}

impl UiState {
    /// Create a new UI state
    #[must_use]
    pub fn new() -> Self {
        Self {
            view_mode: ViewMode::Overview,
            selected_tab: 0,
            show_help: false,
            log_scroll: 0,
            cached_logs: Vec::new(),
            last_log_refresh: std::time::Instant::now(),
            auto_scroll: true,
            log_viewport_height: 0,
            log_scrollbar_state: ScrollbarState::default(),
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
        self.cached_logs = logs;
        self.last_log_refresh = std::time::Instant::now();

        // If auto-scroll is enabled and new logs were added, stay at bottom
        if self.auto_scroll && self.cached_logs.len() > old_count {
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
fn create_colored_log_line(entry: &crate::messages::LogEntry) -> Line {
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
            Span::styled("[", Style::default().fg(node_color)),
            Span::styled(node_name, Style::default().fg(node_color)),
            Span::styled("]", Style::default().fg(node_color)),
            Span::raw(": "),
            Span::styled(entry.message.clone(), Style::default().fg(Color::White)),
        ])
    }
}

/// Render the main UI
pub fn render_ui<S: ::std::hash::BuildHasher>(
    frame: &mut Frame,
    ui_state: &mut UiState,
    nodes: &HashMap<NodeId, (String, NodeStatus), S>,
    log_collector: &LogCollector,
    shutting_down: bool,
) {
    let size = frame.area();

    // Create main layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header/tabs
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Footer/help
        ])
        .split(size);

    // Render header with tabs
    render_header(frame, chunks[0], ui_state);

    // Render main content based on view mode
    match ui_state.view_mode.clone() {
        ViewMode::Overview => render_overview(frame, chunks[1], nodes, shutting_down),
        ViewMode::NodeDetail(node_id) => render_node_detail(frame, chunks[1], node_id, nodes),
        ViewMode::Logs => {
            // Extract cached_logs to avoid borrowing conflicts
            let cached_logs = ui_state.cached_logs.clone();
            render_logs(frame, chunks[1], &cached_logs, log_collector, ui_state);
        }
        ViewMode::Help => render_help(frame, chunks[1]),
    }

    // Render footer
    render_footer(frame, chunks[2], shutting_down);

    // Render help overlay if needed
    if ui_state.show_help {
        render_help_overlay(frame, size);
    }
}

/// Render the header with tabs
fn render_header(frame: &mut Frame, area: ratatui::layout::Rect, ui_state: &UiState) {
    let tab_titles = vec!["Overview", "Logs", "Help"];
    let tabs = Tabs::new(tab_titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Proven Node TUI"),
        )
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .select(ui_state.selected_tab);

    frame.render_widget(tabs, area);
}

/// Render the node overview
fn render_overview<S: ::std::hash::BuildHasher>(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    nodes: &HashMap<NodeId, (String, NodeStatus), S>,
    shutting_down: bool,
) {
    if nodes.is_empty() {
        let help_text = Paragraph::new(vec![
            Line::from("No nodes running"),
            Line::from(""),
            Line::from(vec![
                Span::styled("Press ", Style::default()),
                Span::styled(
                    "'s'",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" to start a new node", Style::default()),
            ]),
            Line::from(vec![
                Span::styled("Press ", Style::default()),
                Span::styled(
                    "'q'",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Span::styled(" to quit", Style::default()),
            ]),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Node Overview"),
        )
        .style(Style::default().fg(Color::White));

        frame.render_widget(help_text, area);
        return;
    }

    let mut items = Vec::new();

    // Collect nodes into a vector and sort by execution order
    let mut sorted_nodes: Vec<_> = nodes.iter().collect();
    sorted_nodes.sort_by_key(|(node_id, _)| node_id.execution_order());

    for (node_id, (name, status)) in sorted_nodes {
        let (display_status, status_color) = match status {
            NodeStatus::Starting | NodeStatus::Stopping => (status.to_string(), Color::LightYellow),
            NodeStatus::Running => {
                if shutting_down {
                    ("Shutting down...".to_string(), Color::Gray)
                } else {
                    (status.to_string(), Color::LightGreen)
                }
            }
            NodeStatus::Stopped => (status.to_string(), Color::Gray),
            NodeStatus::Failed(_) => (status.to_string(), Color::LightRed),
        };

        let line = Line::from(vec![
            Span::styled(
                node_id.pokemon_name(),
                Style::default()
                    .fg(get_node_color(*node_id))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" | ", Style::default()),
            Span::styled(name, Style::default().fg(Color::White)),
            Span::styled(" | ", Style::default()),
            Span::styled(
                display_status,
                Style::default()
                    .fg(status_color)
                    .add_modifier(Modifier::BOLD),
            ),
        ]);

        items.push(ListItem::new(line));
    }

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Running Nodes"),
        )
        .style(Style::default().fg(Color::White));

    frame.render_widget(list, area);
}

/// Render node detail view
fn render_node_detail<S: ::std::hash::BuildHasher>(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    node_id: NodeId,
    nodes: &HashMap<NodeId, (String, NodeStatus), S>,
) {
    let content = if let Some((name, status)) = nodes.get(&node_id) {
        vec![
            Line::from(vec![
                Span::styled("Node ID: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(
                    node_id.pokemon_name(),
                    Style::default().fg(get_node_color(node_id)),
                ),
            ]),
            Line::from(vec![
                Span::styled("Name: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(name, Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!("{status}"),
                    Style::default().fg(match status {
                        NodeStatus::Running => Color::LightGreen,
                        NodeStatus::Failed(_) => Color::LightRed,
                        _ => Color::LightYellow,
                    }),
                ),
            ]),
            Line::from(""),
            Line::from("Available actions:"),
            Line::from("  [r] Restart node"),
            Line::from("  [Delete] Stop node"),
            Line::from("  [Esc] Back to overview"),
        ]
    } else {
        vec![
            Line::from("Node not found"),
            Line::from(""),
            Line::from("Press [Esc] to go back"),
        ]
    };

    let paragraph = Paragraph::new(content)
        .block(Block::default().borders(Borders::ALL).title("Node Details"))
        .style(Style::default().fg(Color::White));

    frame.render_widget(paragraph, area);
}

/// Render logs view
fn render_logs(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    cached_logs: &[crate::messages::LogEntry],
    log_collector: &LogCollector,
    ui_state: &mut UiState,
) {
    let logs = cached_logs;

    if logs.is_empty() {
        let paragraph = Paragraph::new("No logs available")
            .block(Block::default().borders(Borders::ALL).title("Logs"))
            .style(Style::default().fg(Color::Gray));

        frame.render_widget(paragraph, area);
        return;
    }

    // Create layout for logs and scrollbar
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(0),    // Logs area
            Constraint::Length(1), // Scrollbar
        ])
        .split(area);

    let logs_area = chunks[0];
    let scrollbar_area = chunks[1];

    // Calculate viewport height (subtract borders)
    let viewport_height = logs_area.height.saturating_sub(2) as usize;
    ui_state.log_viewport_height = viewport_height;

    // Update scrollbar state
    ui_state.update_scrollbar_state();

    // Terminal-like behavior: newest logs at bottom
    // Note: logs array comes from LogCollector with newest first (index 0 = newest)
    let total_logs = logs.len();

    // Calculate which logs to show based on scroll position
    let (start_idx, end_idx) = if total_logs <= viewport_height {
        // All logs fit in viewport - show all in correct order
        (0, total_logs)
    } else {
        // Need to scroll - calculate visible range
        // log_scroll = 0 means show newest (bottom), which are at the start of the array
        // log_scroll > 0 means scrolled up towards older logs
        let actual_scroll = ui_state.log_scroll.min(total_logs - viewport_height);
        (
            actual_scroll,
            (actual_scroll + viewport_height).min(total_logs),
        )
    };

    // Since logs array has newest first but we want newest at bottom of display,
    // we need to reverse the slice before displaying
    let items: Vec<ListItem> = logs[start_idx..end_idx]
        .iter()
        .rev() // Reverse so newest appears at bottom
        .map(|entry| {
            let formatted = create_colored_log_line(entry);
            ListItem::new(formatted)
        })
        .collect();

    let current_filter = log_collector.get_level_filter();
    let node_filter = log_collector.get_node_filter();

    let title = node_filter.map_or_else(
        || {
            let scroll_indicator = if ui_state.auto_scroll {
                " [Auto-scroll: ON]"
            } else {
                " [Auto-scroll: OFF]"
            };
            format!("Logs (Level: {current_filter}){scroll_indicator}")
        },
        |node_id| {
            let scroll_indicator = if ui_state.auto_scroll {
                " [Auto-scroll: ON]"
            } else {
                " [Auto-scroll: OFF]"
            };
            format!(
                "Logs (Level: {current_filter}, Node: {}){scroll_indicator}",
                node_id.pokemon_name()
            )
        },
    );

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .style(Style::default().fg(Color::White));

    frame.render_widget(list, logs_area);

    // Render scrollbar if there are more logs than can fit
    if total_logs > viewport_height {
        let scrollbar = Scrollbar::default()
            .orientation(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));

        frame.render_stateful_widget(scrollbar, scrollbar_area, &mut ui_state.log_scrollbar_state);
    }
}

/// Render help screen
fn render_help(frame: &mut Frame, area: ratatui::layout::Rect) {
    let help_text = vec![
        Line::from(Span::styled(
            "Proven Node TUI - Help",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Global Keys:"),
        Line::from("  q, Esc          - Graceful quit (stops all nodes)"),
        Line::from("  Ctrl+C          - Force quit (immediate exit)"),
        Line::from("  Tab             - Switch between tabs"),
        Line::from("  ?               - Toggle help"),
        Line::from(""),
        Line::from("Node Management:"),
        Line::from("  s               - Start new node"),
        Line::from("  r               - Refresh status"),
        Line::from("  Enter           - View node details"),
        Line::from(""),
        Line::from("Logs:"),
        Line::from("  Up/Down         - Scroll logs (line by line)"),
        Line::from("  PageUp/PageDown - Scroll logs (page by page)"),
        Line::from("  Home/End        - Jump to top/bottom"),
        Line::from("  a               - Toggle auto-scroll"),
        Line::from("  f               - Filter by log level"),
        Line::from("  n               - Filter by node"),
        Line::from("  c               - Clear logs"),
        Line::from(""),
        Line::from("Node Details:"),
        Line::from("  r               - Restart node"),
        Line::from("  Delete          - Stop node"),
        Line::from("  Esc             - Back to overview"),
    ];

    let paragraph = Paragraph::new(help_text)
        .block(Block::default().borders(Borders::ALL).title("Help"))
        .style(Style::default().fg(Color::White));

    frame.render_widget(paragraph, area);
}

/// Render footer with key hints
fn render_footer(frame: &mut Frame, area: ratatui::layout::Rect, shutting_down: bool) {
    let footer_text = if shutting_down {
        Line::from(vec![
            Span::styled(
                "⏳ SHUTTING DOWN NODES... ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD | Modifier::SLOW_BLINK),
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
                "[q]",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" Quit ", Style::default()),
            Span::styled(
                "[s]",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" Start Node ", Style::default()),
            Span::styled(
                "[↑↓]",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" Scroll ", Style::default()),
            Span::styled(
                "[a]",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" Auto-scroll ", Style::default()),
            Span::styled(
                "[?]",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" Help", Style::default()),
        ])
    };

    let paragraph = Paragraph::new(footer_text)
        .block(Block::default().borders(Borders::ALL))
        .style(Style::default().fg(Color::White));

    frame.render_widget(paragraph, area);
}

/// Render help overlay
fn render_help_overlay(frame: &mut Frame, area: ratatui::layout::Rect) {
    let popup_area = centered_rect(80, 60, area);
    frame.render_widget(Clear, popup_area);
    render_help(frame, popup_area);
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
