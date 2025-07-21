//! Logging context for structured data

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

fn default_component() -> &'static str {
    "unknown"
}

/// Lightweight context that can be attached to loggers
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Context {
    /// Node ID (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<Cow<'static, str>>,

    /// Component name
    #[serde(default = "default_component")]
    pub component: &'static str,

    /// Additional fields (kept minimal for performance)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl Context {
    /// Create a new context for a component
    #[inline]
    pub const fn new(component: &'static str) -> Self {
        Self {
            node_id: None,
            component,
            trace_id: None,
        }
    }

    /// Set node ID
    #[inline]
    pub fn with_node_id(mut self, node_id: impl Into<Cow<'static, str>>) -> Self {
        self.node_id = Some(node_id.into());
        self
    }

    /// Set trace ID
    #[inline]
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Merge with another context (other takes precedence)
    pub fn merge(&self, other: &Context) -> Context {
        Context {
            node_id: other.node_id.clone().or_else(|| self.node_id.clone()),
            component: if other.component != "unknown" {
                other.component
            } else {
                self.component
            },
            trace_id: other.trace_id.clone().or_else(|| self.trace_id.clone()),
        }
    }
}
