mod anonymize;
mod execute;
mod execute_hash;
mod identify;
mod who_am_i;

use crate::rpc::context::RpcContext;
use async_trait::async_trait;

/// Trait for RPC commands that can be executed
#[async_trait]
pub trait RpcCommand {
    /// The response type for this command
    type Response;

    /// Execute the command with the given context
    async fn execute<AM, IM, RM>(&self, context: &mut RpcContext<AM, IM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        RM: proven_runtime::RuntimePoolManagement;
}

// Re-export all command types
pub use anonymize::{AnonymizeCommand, AnonymizeResponse};
pub use execute::{ExecuteCommand, ExecuteResponse};
pub use execute_hash::{ExecuteHashCommand, ExecuteHashResponse};
pub use identify::{IdentifyCommand, IdentifyResponse};
pub use who_am_i::{WhoAmICommand, WhoAmIResponse};
