use proven_applications::ApplicationManagement;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::{Session, SessionManagement};
use uuid::Uuid;

/// Context for RPC commands - works with any session type
#[derive(Clone)]
pub struct RpcContext<AM, IM, SM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
    RM: RuntimePoolManagement,
{
    pub application_manager: AM,
    pub identity_manager: IM,
    pub sessions_manager: SM,
    pub runtime_pool_manager: RM,
    pub session: Session,
}

impl<AM, IM, SM, RM> RpcContext<AM, IM, SM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_manager: AM,
        identity_manager: IM,
        sessions_manager: SM,
        runtime_pool_manager: RM,
        session: Session,
    ) -> Self {
        Self {
            application_manager,
            identity_manager,
            sessions_manager,
            runtime_pool_manager,
            session,
        }
    }

    /// Get the `application_id` if this is an application session
    pub fn application_id(&self) -> Option<Uuid> {
        match &self.session {
            Session::Application(app_session) => Some(*app_session.application_id()),
            Session::Management(_) => None,
        }
    }
}
