//! Extensions for working with tracing spans and node context

use tracing::{Level, Span, span};

/// Extension trait for creating spans with node context
pub trait NodeSpanExt {
    /// Create a span for a specific node
    fn for_node(node_id: impl std::fmt::Display) -> Span;
}

impl NodeSpanExt for Span {
    fn for_node(node_id: impl std::fmt::Display) -> Span {
        // We encode the node_id in the span name using a convention
        // Format: "node:{node_id}"
        span!(Level::INFO, "node", node_id = %node_id)
    }
}

/// Helper to extract node_id from a span name
pub fn extract_node_id_from_span_name(span_name: &str) -> Option<String> {
    span_name.strip_prefix("node:").map(|s| s.to_string())
}
