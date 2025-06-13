mod execute;
mod execute_hash;
mod who_am_i;

use crate::rpc::context::RpcContext;
use async_trait::async_trait;

/// Trait for RPC commands that can be executed
#[async_trait]
pub trait RpcCommand {
    /// The response type for this command
    type Response;

    /// Execute the command with the given context
    async fn execute<AM, RM>(&self, context: &mut RpcContext<AM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        RM: proven_runtime::RuntimePoolManagement;
}

// Re-export all command types
pub use execute::{ExecuteCommand, ExecuteResponse};
pub use execute_hash::{ExecuteHashCommand, ExecuteHashResponse};
pub use who_am_i::{WhoAmICommand, WhoAmIResponse};
