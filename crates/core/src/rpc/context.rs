use proven_applications::ApplicationManagement;
use proven_identity::Session;
use proven_runtime::RuntimePoolManagement;

/// Context object that holds all shared state and dependencies for RPC commands
pub struct RpcContext<AM, RM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
{
    pub application_id: String,
    pub _application_manager: AM,
    pub runtime_pool_manager: RM,
    pub session: Session,
}

impl<AM, RM> RpcContext<AM, RM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_id: String,
        application_manager: AM,
        runtime_pool_manager: RM,
        session: Session,
    ) -> Self {
        Self {
            application_id,
            _application_manager: application_manager,
            runtime_pool_manager,
            session,
        }
    }
}
