use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use proven_code_package::CodePackage;
use proven_runtime::{ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader};

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

type Args = Vec<serde_json::Value>;
type Module = String;
type HandlerSpecifierString = String;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteCommand {
    pub args: Args,
    pub handler_specifier: HandlerSpecifierString,
    pub module: Module,
}

#[derive(Debug, Serialize)]
pub enum ExecuteResponse {
    BadHandlerSpecifier,
    Failure(String),
    Success(ExecutionResult),
}

#[async_trait]
impl RpcCommand for ExecuteCommand {
    type Response = ExecuteResponse;

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
            Err(_) => return ExecuteResponse::BadHandlerSpecifier,
        };

        let execution_request = ExecutionRequest::Rpc {
            application_id: context.application_id.clone(),
            args: self.args.clone(),
            handler_specifier,
            session: context.session.clone(),
        };

        match context
            .runtime_pool_manager
            .execute(
                ModuleLoader::new(CodePackage::from_str(&self.module).unwrap()),
                execution_request,
            )
            .await
        {
            Ok(result) => ExecuteResponse::Success(result),
            Err(e) => ExecuteResponse::Failure(format!("{e:?}")),
        }
    }
}
