//! Types and utilities for working with bundle manifests
//!
//! This module contains the types that define the structure of a bundle manifest,
//! which is used to describe a code package and its dependencies.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Information about a handler parameter
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ParameterInfo {
    /// The name of the parameter
    pub name: String,
    /// The TypeScript type of the parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Whether this parameter is optional
    pub optional: bool,
    /// Default value for the parameter if provided
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
}

/// Information about a handler function
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HandlerInfo {
    /// The name of the handler function
    pub name: String,
    /// The type of handler: "rpc" | "http" | "event" | "schedule"
    pub r#type: String,
    /// Parameters accepted by this handler
    pub parameters: Vec<ParameterInfo>,
    /// Handler configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
}

/// An executable module in the bundle
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExecutableModule {
    /// Module specifier (e.g., "<file:///src/handlers.ts>")
    pub specifier: String,
    /// Source code content
    pub content: String,
    /// Exported handlers (empty if no handlers)
    pub handlers: Vec<HandlerInfo>,
    /// Module imports for dependency resolution
    pub imports: Vec<String>,
}

/// Optional build metadata (for tooling/debugging)
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BuildMetadata {
    /// ISO 8601 timestamp of when the bundle was created
    pub created_at: String,
    /// Build mode: "development" or "production"
    pub mode: String,
    /// Version of the bundler plugin that created this manifest
    pub plugin_version: String,
}

/// A complete bundle manifest containing all information about a code package
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BundleManifest {
    /// Unique identifier for this manifest
    pub id: String,
    /// Version of the manifest format
    pub version: String,
    /// All executable modules in the bundle
    pub modules: Vec<ExecutableModule>,
    /// Runtime dependencies (NPM packages)
    pub dependencies: HashMap<String, String>,
    /// Optional build metadata (for tooling/debugging)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BuildMetadata>,
}
