//! TUI interface components for SQL-based logging

use crate::{
    logs_viewer::DisplayLogEntry,
    messages::{LogLevel, TuiNodeId},
};

use proven_local::NodeStatus;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
        Tabs,
    },
};
use std::collections::HashMap;

/// UI State management
pub struct UiState {
    /// Currently active tab
    active_tab: usize,
    /// Log level filter
    log_level_filter: LogLevel,
    /// Node filter (None = all nodes)
    node_filter: Option<TuiNodeId>,
    /// Scroll position for logs
    log_scroll: usize,
    /// Scroll position for nodes
    node_scroll: usize,
    /// Node information
    nodes: HashMap<TuiNodeId, NodeStatus>,
    /// Selected node for operations
    selected_node: Option<TuiNodeId>,
}

impl UiState {
    /// Create new UI state
    pub fn new() -> Self {
        Self {
            active_tab: 0,
            log_level_filter: LogLevel::Info,
            node_filter: None,
            log_scroll: 0,
            node_scroll: 0,
            nodes: HashMap::new(),
            selected_node: None,
        }
    }

    /// Get the currently active tab index
    pub fn active_tab(&self) -> usize {
        self.active_tab
    }

    /// Check if logs tab is active
    pub fn is_log_tab_active(&self) -> bool {
        self.active_tab == 1
    }

    /// Move to next tab
    pub fn next_tab(&mut self) {
        self.active_tab = (self.active_tab + 1) % 3;
    }

    /// Move to previous tab
    pub fn previous_tab(&mut self) {
        self.active_tab = if self.active_tab == 0 {
            2
        } else {
            self.active_tab - 1
        };
    }

    /// Set active tab directly
    pub fn set_active_tab(&mut self, tab: usize) {
        if tab < 3 {
            self.active_tab = tab;
        }
    }

    /// Scroll up in the active view
    pub fn scroll_up(&mut self) {
        match self.active_tab {
            0 => self.node_scroll = self.node_scroll.saturating_sub(1),
            1 => self.log_scroll = self.log_scroll.saturating_sub(1),
            _ => {}
        }
    }

    /// Scroll down in the active view
    pub fn scroll_down(&mut self) {
        match self.active_tab {
            0 => self.node_scroll = self.node_scroll.saturating_add(1),
            1 => self.log_scroll = self.log_scroll.saturating_add(1),
            _ => {}
        }
    }

    /// Page up
    pub fn page_up(&mut self) {
        match self.active_tab {
            0 => self.node_scroll = self.node_scroll.saturating_sub(10),
            1 => self.log_scroll = self.log_scroll.saturating_sub(10),
            _ => {}
        }
    }

    /// Page down
    pub fn page_down(&mut self) {
        match self.active_tab {
            0 => self.node_scroll = self.node_scroll.saturating_add(10),
            1 => self.log_scroll = self.log_scroll.saturating_add(10),
            _ => {}
        }
    }

    /// Scroll to top
    pub fn scroll_to_top(&mut self) {
        match self.active_tab {
            0 => self.node_scroll = 0,
            1 => self.log_scroll = 0,
            _ => {}
        }
    }

    /// Scroll to bottom
    pub fn scroll_to_bottom(&mut self) {
        match self.active_tab {
            0 => self.node_scroll = usize::MAX / 2, // Will be clamped
            1 => self.log_scroll = usize::MAX / 2,  // Will be clamped
            _ => {}
        }
    }

    /// Increase log level filter
    pub fn increase_log_level(&mut self) {
        self.log_level_filter = match self.log_level_filter {
            LogLevel::Error => LogLevel::Warn,
            LogLevel::Warn => LogLevel::Info,
            LogLevel::Info => LogLevel::Debug,
            LogLevel::Debug => LogLevel::Trace,
            LogLevel::Trace => LogLevel::Trace,
        };
    }

    /// Decrease log level filter
    pub fn decrease_log_level(&mut self) {
        self.log_level_filter = match self.log_level_filter {
            LogLevel::Trace => LogLevel::Debug,
            LogLevel::Debug => LogLevel::Info,
            LogLevel::Info => LogLevel::Warn,
            LogLevel::Warn => LogLevel::Error,
            LogLevel::Error => LogLevel::Error,
        };
    }

    /// Get current log level filter
    pub fn get_log_level_filter(&self) -> LogLevel {
        self.log_level_filter
    }

    /// Toggle node filter
    pub fn toggle_node_filter(&mut self) {
        if self.node_filter.is_some() {
            self.node_filter = None;
        } else if let Some(node) = self.selected_node {
            self.node_filter = Some(node);
        }
    }

    /// Get current node filter
    pub fn get_node_filter(&self) -> Option<TuiNodeId> {
        self.node_filter
    }

    /// Add a node
    pub fn add_node(&mut self, node_id: TuiNodeId, status: NodeStatus) {
        self.nodes.insert(node_id, status);
        if self.selected_node.is_none() {
            self.selected_node = Some(node_id);
        }
    }

    /// Update node status
    pub fn update_node_status(&mut self, node_id: TuiNodeId, status: NodeStatus) {
        self.nodes.insert(node_id, status);
    }

    /// Get selected node
    pub fn get_selected_node(&self) -> Option<TuiNodeId> {
        self.selected_node
    }
}

/// Main UI rendering function for SQL-based logs
pub fn render_ui(frame: &mut Frame, ui_state: &UiState, logs: &[DisplayLogEntry]) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Header
            Constraint::Length(3), // Tabs
            Constraint::Min(0),    // Content
            Constraint::Length(3), // Status bar
        ])
        .split(frame.area());

    // Header
    let header = Paragraph::new("Proven Node TUI - SQL Logging")
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default());
    frame.render_widget(header, chunks[0]);

    // Tabs
    let tab_titles = vec!["Nodes", "Logs", "Help"];
    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL))
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .select(ui_state.active_tab);
    frame.render_widget(tabs, chunks[1]);

    // Content area
    match ui_state.active_tab {
        0 => render_nodes_tab(frame, chunks[2], ui_state),
        1 => render_logs_tab(frame, chunks[2], ui_state, logs),
        2 => render_help_tab(frame, chunks[2]),
        _ => {}
    }

    // Status bar
    render_status_bar(frame, chunks[3], ui_state);
}

/// Render the nodes tab
fn render_nodes_tab(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let nodes_block = Block::default().title("Nodes").borders(Borders::ALL);

    let items: Vec<ListItem> = ui_state
        .nodes
        .iter()
        .map(|(node_id, status)| {
            let status_color = match status {
                NodeStatus::NotStarted => Color::Gray,
                NodeStatus::Starting => Color::Yellow,
                NodeStatus::Running => Color::Green,
                NodeStatus::Stopping => Color::Yellow,
                NodeStatus::Stopped => Color::Red,
                NodeStatus::Failed(_) => Color::Red,
            };

            let selected = ui_state.selected_node == Some(*node_id);
            let prefix = if selected { "> " } else { "  " };

            ListItem::new(Line::from(vec![
                Span::raw(prefix),
                Span::styled(
                    format!("{}: ", node_id.display_name()),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(format!("{:?}", status), Style::default().fg(status_color)),
            ]))
        })
        .collect();

    let nodes_list = List::new(items)
        .block(nodes_block)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol("> ");

    frame.render_widget(nodes_list, area);
}

/// Render the logs tab
fn render_logs_tab(frame: &mut Frame, area: Rect, ui_state: &UiState, logs: &[DisplayLogEntry]) {
    let logs_block = Block::default()
        .title(format!(
            "Logs - Level: {:?} {}",
            ui_state.log_level_filter,
            if let Some(node) = ui_state.node_filter {
                format!("- Node: {}", node.display_name())
            } else {
                String::new()
            }
        ))
        .borders(Borders::ALL);

    let items: Vec<ListItem> = logs
        .iter()
        .map(|log| {
            let level_color = match log.level {
                LogLevel::Error => Color::Red,
                LogLevel::Warn => Color::Yellow,
                LogLevel::Info => Color::Green,
                LogLevel::Debug => Color::Blue,
                LogLevel::Trace => Color::Magenta,
            };

            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("[{}]", log.timestamp.format("%H:%M:%S%.3f")),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("[{:5}]", format!("{:?}", log.level)),
                    Style::default().fg(level_color),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("[{}]", log.node_id.display_name()),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(" "),
                Span::raw(&log.message),
            ]))
        })
        .collect();

    let logs_list = List::new(items).block(logs_block);

    frame.render_widget(logs_list, area);

    // Scrollbar
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(Some("↑"))
        .end_symbol(Some("↓"));

    let mut scrollbar_state = ScrollbarState::new(logs.len()).position(ui_state.log_scroll);

    frame.render_stateful_widget(
        scrollbar,
        area.inner(ratatui::layout::Margin {
            vertical: 1,
            horizontal: 0,
        }),
        &mut scrollbar_state,
    );
}

/// Render the help tab
fn render_help_tab(frame: &mut Frame, area: Rect) {
    let help_text = vec![
        "Keyboard Shortcuts:",
        "",
        "General:",
        "  q, Q, Ctrl+C - Quit",
        "  Tab          - Next tab",
        "  Shift+Tab    - Previous tab",
        "",
        "Navigation:",
        "  ↑/↓          - Scroll up/down",
        "  Page Up/Down - Scroll page",
        "  Home/End     - Go to top/bottom",
        "",
        "Nodes Tab:",
        "  s, S         - Start a new node",
        "  x, X         - Stop selected node",
        "",
        "Logs Tab:",
        "  ←/→          - Decrease/increase log level",
        "  n, N         - Toggle node filter",
        "",
        "Mouse:",
        "  Click tabs   - Switch tabs",
        "  Scroll       - Scroll content",
    ];

    let help = Paragraph::new(help_text.join("\n"))
        .block(Block::default().title("Help").borders(Borders::ALL))
        .style(Style::default().fg(Color::White));

    frame.render_widget(help, area);
}

/// Render the status bar
fn render_status_bar(frame: &mut Frame, area: Rect, ui_state: &UiState) {
    let status_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(34),
            Constraint::Percentage(33),
        ])
        .split(area);

    // Left: Node count
    let node_count = Paragraph::new(format!("Nodes: {}", ui_state.nodes.len()))
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(node_count, status_chunks[0]);

    // Center: Current filter
    let filter_info = Paragraph::new(format!(
        "Filter: {:?} {}",
        ui_state.log_level_filter,
        if let Some(node) = ui_state.node_filter {
            format!("({})", node.display_name())
        } else {
            "(all)".to_string()
        }
    ))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(filter_info, status_chunks[1]);

    // Right: Help
    let help_hint = Paragraph::new("Press 'q' to quit, Tab to switch tabs")
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(help_hint, status_chunks[2]);
}
