mod add_allowed_origin;
mod anonymize;
mod create_application;
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
    async fn execute<AM, IM, SM, RM>(
        &self,
        context: &mut RpcContext<AM, IM, SM, RM>,
    ) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        SM: proven_sessions::SessionManagement,
        RM: proven_runtime::RuntimePoolManagement;
}

// Re-export all command types
pub use add_allowed_origin::{AddAllowedOriginCommand, AddAllowedOriginResponse};
pub use anonymize::{AnonymizeCommand, AnonymizeResponse};
pub use create_application::{CreateApplicationCommand, CreateApplicationResponse};
pub use execute::{ExecuteCommand, ExecuteResponse};
pub use execute_hash::{ExecuteHashCommand, ExecuteHashResponse};
pub use identify::{IdentifyCommand, IdentifyResponse};
pub use who_am_i::{WhoAmICommand, WhoAmIResponse};
