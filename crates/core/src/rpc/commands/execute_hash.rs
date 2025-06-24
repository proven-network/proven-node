use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use proven_runtime::{ExecutionRequest, ExecutionResult, HandlerSpecifier};

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

type Args = Vec<serde_json::Value>;
type ModuleHash = String;
type HandlerSpecifierString = String;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteHashCommand {
    pub args: Args,
    pub handler_specifier: HandlerSpecifierString,
    pub module_hash: ModuleHash,
}

#[derive(Debug, Serialize)]
pub enum ExecuteHashResponse {
    BadHandlerSpecifier,
    Failure(String),
    HashUnknown,
    Success(ExecutionResult),
}

#[async_trait]
impl RpcCommand for ExecuteHashCommand {
    type Response = ExecuteHashResponse;

    async fn execute<AM, IM, SM, RM>(
        &self,
        context: &mut RpcContext<AM, IM, SM, RM>,
    ) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        SM: proven_sessions::SessionManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        let handler_specifier = match HandlerSpecifier::parse(&self.handler_specifier) {
            Ok(hs) => hs,
            Err(_) => return ExecuteHashResponse::BadHandlerSpecifier,
        };

        let execution_request = ExecutionRequest::Rpc {
            application_id: context.application_id.clone(),
            args: self.args.clone(),
            handler_specifier,
            session: context.session.clone(),
        };

        match context
            .runtime_pool_manager
            .execute_prehashed(self.module_hash.clone(), execution_request)
            .await
        {
            Ok(result) => ExecuteHashResponse::Success(result),
            Err(proven_runtime::Error::HashUnknown) => ExecuteHashResponse::HashUnknown,
            Err(e) => ExecuteHashResponse::Failure(format!("{e:?}")),
        }
    }
}
