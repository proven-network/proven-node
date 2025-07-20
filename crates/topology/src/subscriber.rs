//! Topology change subscription mechanism

use crate::Node;
use tokio::sync::watch;

/// Subscription to topology changes
pub struct TopologySubscription {
    receiver: watch::Receiver<Vec<Node>>,
}

impl TopologySubscription {
    /// Create a new subscription from a receiver
    pub(crate) fn new(receiver: watch::Receiver<Vec<Node>>) -> Self {
        Self { receiver }
    }

    /// Wait for the next topology change
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.receiver.changed().await
    }

    /// Get the current topology
    pub fn current(&self) -> Vec<Node> {
        self.receiver.borrow().clone()
    }

    /// Borrow the current topology
    pub fn borrow(&self) -> watch::Ref<'_, Vec<Node>> {
        self.receiver.borrow()
    }
}

/// Manages subscriptions to topology changes
pub(crate) struct TopologyBroadcaster {
    sender: watch::Sender<Vec<Node>>,
}

impl TopologyBroadcaster {
    /// Create a new broadcaster with initial topology
    pub fn new(initial: Vec<Node>) -> Self {
        let (sender, _) = watch::channel(initial);
        Self { sender }
    }

    /// Update the topology and notify all subscribers
    pub fn update(&self, topology: Vec<Node>) {
        // Only send if there are active receivers
        if self.sender.receiver_count() > 0 {
            let _ = self.sender.send(topology);
        }
    }

    /// Create a new subscription
    pub fn subscribe(&self) -> TopologySubscription {
        TopologySubscription::new(self.sender.subscribe())
    }
}
