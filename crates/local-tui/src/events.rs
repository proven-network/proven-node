//! Event handling for the TUI

use crate::messages::{NodeCommand, TuiMessage};
use crossterm::event::{self, KeyCode, KeyEvent, KeyModifiers};
use proven_governance_mock::MockGovernance;
use std::{sync::Arc, sync::mpsc, thread, time::Duration};
use tracing::info;

/// Events that can occur in the TUI
#[derive(Debug, Clone)]
pub enum Event {
    /// Terminal input event (keyboard, mouse, etc.)
    Input(crossterm::event::Event),
    /// Timer tick for regular updates
    Tick,
    /// Message from async tasks
    Message(Box<TuiMessage>),
    /// Resize the terminal
    Resize(u16, u16),
}

/// Result of handling a key event
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEventResult {
    /// Continue normal operation
    Continue,
    /// Graceful shutdown initiated
    GracefulShutdown,
    /// Force quit requested (immediate exit)
    ForceQuit,
}

/// Handles events and manages the event loop
pub struct EventHandler {
    sender: mpsc::Sender<Event>,
    receiver: mpsc::Receiver<Event>,
    command_sender: mpsc::Sender<NodeCommand>,
    governance: Arc<MockGovernance>,
}

impl EventHandler {
    /// Create a new event handler
    #[must_use]
    pub fn new(command_sender: mpsc::Sender<NodeCommand>, governance: Arc<MockGovernance>) -> Self {
        let (sender, receiver) = mpsc::channel();

        Self {
            sender,
            receiver,
            command_sender,
            governance,
        }
    }

    /// Get a sender for external tasks to send messages
    #[must_use]
    pub fn get_sender(&self) -> mpsc::Sender<Event> {
        self.sender.clone()
    }

    /// Start listening for terminal events
    pub fn listen_for_terminal_events(&self) {
        let sender = self.sender.clone();

        thread::spawn(move || {
            loop {
                // Poll for events with a timeout
                if let Ok(available) = event::poll(Duration::from_millis(100)) {
                    if available {
                        if let Ok(event) = event::read() {
                            if sender.send(Event::Input(event)).is_err() {
                                break;
                            }
                        }
                    }
                }

                // Send periodic tick events (reduced frequency for better performance)
                if sender.send(Event::Tick).is_err() {
                    break;
                }

                std::thread::sleep(Duration::from_millis(100));
            }
        });
    }

    /// Start listening for messages from node operations
    pub fn listen_for_messages(&self, message_receiver: mpsc::Receiver<TuiMessage>) {
        let sender = self.sender.clone();

        thread::spawn(move || {
            while let Ok(message) = message_receiver.recv() {
                if sender.send(Event::Message(Box::new(message))).is_err() {
                    break;
                }
            }
        });
    }

    /// Receive the next event with a timeout (blocking)
    pub fn next_blocking(&self, timeout: Duration) -> Option<Event> {
        self.receiver.recv_timeout(timeout).ok()
    }

    /// Trigger a shutdown (called by signal handler)
    pub fn trigger_shutdown(&self) {
        let _ = self.command_sender.send(NodeCommand::Shutdown);
    }

    /// Handle keyboard input and return the result of the key processing
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue processing the key event or sending commands.
    pub fn handle_key_event(
        &self,
        key: KeyEvent,
        shutting_down: bool,
    ) -> anyhow::Result<KeyEventResult> {
        match key.code {
            // Graceful quit the application
            KeyCode::Char('q') | KeyCode::Esc => {
                if !shutting_down {
                    info!("Initiating graceful shutdown of all nodes...");
                    let _ = self.command_sender.send(NodeCommand::Shutdown);
                    return Ok(KeyEventResult::GracefulShutdown);
                }
                // If already shutting down, do nothing
                return Ok(KeyEventResult::Continue);
            }

            // Force quit with Ctrl+C (immediate exit)
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                info!("Force quit requested - shutting down immediately");
                let _ = self.command_sender.send(NodeCommand::Shutdown);
                return Ok(KeyEventResult::ForceQuit);
            }

            // Start a new node with 's' (only if not shutting down)
            KeyCode::Char('s') => {
                if !shutting_down {
                    self.start_new_node()?;
                }
            }

            // Refresh status with 'r' (only if not shutting down)
            KeyCode::Char('r') => {
                if !shutting_down {
                    let _ = self.command_sender.send(NodeCommand::GetStatus);
                }
            }

            _ => {}
        }

        Ok(KeyEventResult::Continue)
    }

    /// Start a new node with default configuration'
    fn start_new_node(&self) -> anyhow::Result<()> {
        use crate::node_id::NodeId;

        let id = NodeId::new();
        let name = id.display_name(); // Use the new display_name method

        // Let NodeManager handle all the configuration creation, governance registration,
        // and first-node logic through its create_node_config function
        let command = NodeCommand::StartNode {
            id,
            name,
            config: None, // Let NodeManager create the config
        };

        self.command_sender
            .send(command)
            .map_err(|e| anyhow::anyhow!("Failed to send start command: {e}"))?;

        Ok(())
    }
}
