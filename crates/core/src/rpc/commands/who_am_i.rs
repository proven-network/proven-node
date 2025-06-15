use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use proven_identity::WhoAmI;

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhoAmICommand;

#[async_trait]
impl RpcCommand for WhoAmICommand {
    type Response = WhoAmI;

    async fn execute<AM, IM, RM>(&self, context: &mut RpcContext<AM, IM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        context.session.clone().into()
    }
}
