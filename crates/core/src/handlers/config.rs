use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};
use proven_governance::{Node, NodeSpecialization};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{Context, Result};

/// Config information about the node
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node ID
    pub id: String,
    /// Specializations enabled on this node
    pub specializations: Vec<String>,
}

/// Handler for config endpoints
pub struct ConfigHandler;

impl ConfigHandler {
    /// Create a new router with config endpoints
    pub fn router(context: Arc<Context>) -> Router {
        Router::new()
            .route("/config", get(get_config))
            .with_state(context)
    }
}

/// Get the node configuration
async fn get_config(State(context): State<Arc<Context>>) -> Result<Json<NodeConfig>> {
    // Get node information from the governance source
    let governance = context.governance.clone();
    let topology = governance.get_topology().await.map_err(|e| {
        error!("Failed to get topology: {:?}", e);
        crate::Error::Governance
    })?;

    // Get node ID from the context - in real implementation, this would be from the attestation
    let node_id = context.node_id.clone();

    // Find our node in the topology
    let node = topology
        .into_iter()
        .find(|n| n.id == node_id)
        .ok_or_else(|| {
            error!("Node with ID {} not found in topology", node_id);
            crate::Error::Governance
        })?;

    // Convert specializations to strings
    let specialization_strings = node
        .specializations
        .iter()
        .map(|s| match s {
            NodeSpecialization::RadixMainnet => "RadixMainnet".to_string(),
            NodeSpecialization::RadixStokenet => "RadixStokenet".to_string(),
        })
        .collect();

    Ok(Json(NodeConfig {
        id: node.id,
        specializations: specialization_strings,
    }))
}
