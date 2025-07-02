//! Node identifier implementation

use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::{LazyLock, Mutex};

/// Global state for tracking used Pokemon IDs and execution order
static USED_POKEMON_IDS: LazyLock<Mutex<HashSet<u8>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static NEXT_EXECUTION_ORDER: LazyLock<Mutex<u8>> = LazyLock::new(|| Mutex::new(1));

/// Special node ID for main/TUI thread (non-node threads)
pub const MAIN_THREAD_NODE_ID: NodeId = NodeId {
    execution_order: 0,
    pokemon_id: 255,
};

/// Node identifier consisting of execution order and pokemon ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    /// Execution order (1, 2, 3, etc.) - used for naming and coloring  
    pub execution_order: u8,
    /// Pokemon ID (0-149 for Kanto Pokemon) - used for pokemon names
    pub pokemon_id: u8,
}

impl NodeId {
    /// Create a new `NodeId` with incremental execution order and random pokemon
    pub fn new() -> Self {
        let mut used_pokemon_ids = USED_POKEMON_IDS.lock().unwrap();
        let mut next_order = NEXT_EXECUTION_ORDER.lock().unwrap();

        // If all 150 Pokemon are used, start reusing them
        if used_pokemon_ids.len() >= 150 {
            used_pokemon_ids.clear();
        }

        // Find an unused Pokemon ID
        let mut rng = rand::thread_rng();
        let pokemon_id = loop {
            let candidate = rng.gen_range(0..150);
            if !used_pokemon_ids.contains(&candidate) {
                used_pokemon_ids.insert(candidate);
                break candidate;
            }
        };

        drop(used_pokemon_ids);

        let execution_order = *next_order;

        // Increment execution order, wrapping at 255 to avoid overflow
        *next_order = next_order.wrapping_add(1);
        if *next_order == 0 {
            *next_order = 1; // Skip 0 as it's reserved for main thread
        }

        drop(next_order);

        Self {
            execution_order,
            pokemon_id,
        }
    }

    /// Create a `NodeId` with specific values (useful for testing)
    pub const fn with_values(execution_order: u8, pokemon_id: u8) -> Self {
        Self {
            execution_order,
            pokemon_id,
        }
    }

    /// Check if this is the first node created (`execution_order` == 1)
    pub const fn is_first_node(self) -> bool {
        self.execution_order == 1
    }

    /// Get the full pokemon name for this node
    pub fn full_pokemon_name(self) -> String {
        full_pokemon_name_from_id(self.pokemon_id)
    }

    /// Get the pokemon name for this node
    pub fn pokemon_name(self) -> String {
        pokemon_name_from_id(self.pokemon_id)
    }

    /// Get the execution order for naming and coloring
    pub const fn execution_order(self) -> u8 {
        self.execution_order
    }

    /// Get the pokemon ID
    pub const fn pokemon_id(self) -> u8 {
        self.pokemon_id
    }

    /// Get a display name in the format "node-X" where X is the execution order
    pub fn display_name(self) -> String {
        if self == MAIN_THREAD_NODE_ID {
            "main".to_string()
        } else {
            format!("node-{}", self.execution_order)
        }
    }

    /// Reset the global state (useful for testing)
    #[cfg(test)]
    pub fn reset_global_state() {
        let mut used_pokemon_ids = USED_POKEMON_IDS.lock().unwrap();
        let mut next_order = NEXT_EXECUTION_ORDER.lock().unwrap();
        used_pokemon_ids.clear();
        drop(used_pokemon_ids);
        *next_order = 1;
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == MAIN_THREAD_NODE_ID {
            write!(f, "main")
        } else {
            write!(f, "{}({})", self.execution_order, self.pokemon_name())
        }
    }
}

/// Get Pokemon name from pokemon ID
pub fn full_pokemon_name_from_id(pokemon_id: u8) -> String {
    // Special case for main/TUI thread
    if pokemon_id == 255 {
        return "main".to_string();
    }

    let kanto_pokemon = pokemon_rs::get_generation("Kanto", Some("en"));
    let index = (pokemon_id as usize) % kanto_pokemon.len();

    kanto_pokemon[index].to_string()
}

/// Get Pokemon name from pokemon ID
pub fn pokemon_name_from_id(pokemon_id: u8) -> String {
    // Clean up the name: replace spaces with dashes, keep only letters and dashes
    full_pokemon_name_from_id(pokemon_id)
        .to_lowercase()
        .replace(' ', "-")
        .chars()
        .filter(|c| c.is_ascii_alphabetic() || *c == '-')
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;

    #[test]
    #[serial]
    fn test_node_id_creation() {
        NodeId::reset_global_state();

        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let node3 = NodeId::new();

        // Execution orders should be sequential
        assert_eq!(node1.execution_order(), 1);
        assert_eq!(node2.execution_order(), 2);
        assert_eq!(node3.execution_order(), 3);

        // Pokemon IDs should be different
        assert_ne!(node1.pokemon_id(), node2.pokemon_id());
        assert_ne!(node2.pokemon_id(), node3.pokemon_id());
        assert_ne!(node1.pokemon_id(), node3.pokemon_id());
    }

    #[test]
    #[serial]
    fn test_first_node_detection() {
        NodeId::reset_global_state();

        let node1 = NodeId::new();
        let node2 = NodeId::new();

        assert!(node1.is_first_node());
        assert!(!node2.is_first_node());
    }

    #[test]
    #[serial]
    fn test_display_name() {
        NodeId::reset_global_state();

        let node1 = NodeId::new();
        let node2 = NodeId::new();

        assert_eq!(node1.display_name(), "node-1");
        assert_eq!(node2.display_name(), "node-2");
        assert_eq!(MAIN_THREAD_NODE_ID.display_name(), "main");
    }

    #[test]
    #[serial]
    fn test_pokemon_names() {
        let node = NodeId::with_values(1, 0);
        let name = node.pokemon_name();

        // Should be a valid pokemon name (bulbasaur for ID 0)
        assert!(!name.is_empty());
        assert!(name.chars().all(|c| c.is_ascii_alphabetic() || c == '-'));
    }

    #[test]
    #[serial]
    fn test_execution_order_wrapping() {
        NodeId::reset_global_state();

        // Set the execution order to near the limit
        {
            let mut next_order = NEXT_EXECUTION_ORDER.lock().unwrap();
            *next_order = 254;
        }

        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let node3 = NodeId::new();

        assert_eq!(node1.execution_order(), 254);
        assert_eq!(node2.execution_order(), 255);
        assert_eq!(node3.execution_order(), 1); // Should wrap to 1, skipping 0
    }

    #[test]
    #[serial]
    fn test_pokemon_id_reuse() {
        NodeId::reset_global_state();

        // Create more nodes than available Pokemon (150)
        let mut nodes = Vec::new();
        for _ in 0..151 {
            nodes.push(NodeId::new());
        }

        // After 150 nodes, pokemon IDs should start being reused
        let pokemon_ids: HashSet<u8> = nodes.iter().map(|n| n.pokemon_id()).collect();

        // We should have at most 150 unique pokemon IDs, but the 151st node
        // should reuse an ID, so we might have less than 151 unique IDs
        assert!(pokemon_ids.len() <= 150);
    }

    #[test]
    fn test_copy_trait() {
        let node = NodeId::new();
        let copied_node = node; // This should work since NodeId is Copy

        assert_eq!(node, copied_node);

        // Original should still be usable
        let _ = node.pokemon_name();
        let _ = copied_node.pokemon_name();
    }

    #[test]
    fn test_main_thread_constant() {
        assert_eq!(MAIN_THREAD_NODE_ID.execution_order(), 0);
        assert_eq!(MAIN_THREAD_NODE_ID.pokemon_id(), 255);
        assert_eq!(MAIN_THREAD_NODE_ID.display_name(), "main");
        assert_eq!(MAIN_THREAD_NODE_ID.pokemon_name(), "main");
    }

    #[test]
    #[serial]
    fn test_with_values_constructor() {
        let node = NodeId::with_values(42, 25);
        assert_eq!(node.execution_order(), 42);
        assert_eq!(node.pokemon_id(), 25);
        assert_eq!(node.display_name(), "node-42");
    }

    #[test]
    #[serial]
    fn test_debug_and_display() {
        let node = NodeId::with_values(5, 10);

        // Debug should show the struct fields
        let debug_str = format!("{node:?}");
        assert!(debug_str.contains("execution_order"));
        assert!(debug_str.contains("pokemon_id"));

        // Display should show execution order and pokemon name
        let display_str = format!("{node}");
        assert!(display_str.contains('5'));
        assert!(!display_str.is_empty());
    }
}
