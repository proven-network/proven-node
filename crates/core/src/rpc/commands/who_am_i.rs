use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use proven_identity::WhoAmI;

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhoAmICommand;

#[derive(Debug, Serialize)]
pub struct WhoAmIResponse(pub WhoAmI);

#[async_trait]
impl RpcCommand for WhoAmICommand {
    type Response = WhoAmIResponse;

    async fn execute<AM, RM>(&self, context: &mut RpcContext<AM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        WhoAmIResponse(context.session.clone().into())
    }
}
