use proven_applications::ApplicationManagement;
use proven_identity::{IdentityManagement, Session};
use proven_runtime::RuntimePoolManagement;
use uuid::Uuid;

/// Context object that holds all shared state and dependencies for RPC commands
pub struct RpcContext<AM, IM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    RM: RuntimePoolManagement,
{
    pub application_id: Uuid,
    pub _application_manager: AM,
    pub identity_manager: IM,
    pub runtime_pool_manager: RM,
    pub session: Session,
}

impl<AM, IM, RM> RpcContext<AM, IM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_id: Uuid,
        application_manager: AM,
        identity_manager: IM,
        runtime_pool_manager: RM,
        session: Session,
    ) -> Self {
        Self {
            application_id,
            _application_manager: application_manager,
            identity_manager,
            runtime_pool_manager,
            session,
        }
    }
}
