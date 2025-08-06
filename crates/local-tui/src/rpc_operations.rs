//! RPC operations for the TUI using LocalCluster's RpcClient

use anyhow::Result;
use proven_applications::Application;
use proven_local_cluster::{RpcClient, UserIdentity};
use proven_util::Origin;
use std::cell::RefCell;
use std::str::FromStr;
use tokio::runtime::Runtime;
use tracing::info;

/// Handles RPC operations for the TUI
pub struct RpcOperations {
    /// Tokio runtime for async operations
    runtime: Runtime,
    /// Persistent RPC client for all operations
    client: RefCell<RpcClient>,
    /// Current node URL
    current_url: RefCell<Option<String>>,
}

impl Default for RpcOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcOperations {
    /// Create new RPC operations handler
    pub fn new() -> Self {
        let runtime = Runtime::new().expect("Failed to create tokio runtime");

        // Create a persistent RPC client with TUI user identity
        let mut client = RpcClient::new();
        let identity = UserIdentity::new("tui-user");
        client.set_identity(identity);

        Self {
            runtime,
            client: RefCell::new(client),
            current_url: RefCell::new(None),
        }
    }

    /// Ensure the client is connected to the given URL
    async fn ensure_session(
        client: &mut RpcClient,
        current_url: &mut Option<String>,
        node_url: &str,
    ) -> Result<()> {
        // Only reinitialize if URL changed
        if current_url.as_deref() != Some(node_url) {
            client.initialize_session(node_url).await?;
            client.identify().await?;
            *current_url = Some(node_url.to_string());
        }
        Ok(())
    }

    /// Execute WhoAmI command
    pub fn who_am_i(&self, node_url: &str) -> Result<String> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;
            let response = client.who_am_i().await?;

            // Handle the enum response
            match response {
                proven_core::WhoAmIResponse::Anonymous { session_id, .. } => {
                    Ok(format!("Session: {session_id}\nIdentified: No"))
                }
                proven_core::WhoAmIResponse::Identified {
                    session_id,
                    identity,
                    ..
                } => Ok(format!(
                    "Session: {session_id}\nIdentified: Yes\nIdentity: {identity:?}"
                )),
                proven_core::WhoAmIResponse::Failure(err) => {
                    Err(anyhow::anyhow!("WhoAmI failed: {}", err))
                }
            }
        })
    }

    /// Create an application
    pub fn create_application(
        &self,
        node_url: &str,
        name: &str,
        _origin: &str, // Origin is not used in the current API
    ) -> Result<Application> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            // Ensure session
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;

            // Create application (returns just the UUID)
            let app_id = client.create_application(name).await?;

            info!("Created application: {} ({})", name, app_id);

            // Fetch the created application details
            let applications = client.list_applications_by_owner().await?;
            applications
                .into_iter()
                .find(|app| app.id == app_id)
                .ok_or_else(|| anyhow::anyhow!("Failed to find created application"))
        })
    }

    /// Add allowed origin to an application  
    pub fn add_allowed_origin(
        &self,
        node_url: &str,
        app_id: &str,
        _origin: &str, // Not supported in current API
    ) -> Result<String> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            // Ensure session
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;

            // Parse app ID
            let _app_id: uuid::Uuid = app_id
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid application ID"))?;

            // Note: add_allowed_origin returns () in the current API
            // client.add_allowed_origin(app_id, origin).await?;

            Ok("Origin management not yet implemented".to_string())
        })
    }

    /// List applications by owner
    pub fn list_applications(&self, node_url: &str) -> Result<Vec<Application>> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            // Ensure session
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;

            // List applications (returns Vec<Application>)
            let applications = client.list_applications_by_owner().await?;

            Ok(applications)
        })
    }

    /// Add origin to application
    pub fn add_origin(&self, node_url: &str, app_id: &str, origin_str: &str) -> Result<()> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            // Ensure session
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;

            // Parse app ID
            let app_uuid: uuid::Uuid = app_id
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid application ID"))?;

            // Parse origin
            let origin = Origin::from_str(origin_str)
                .map_err(|e| anyhow::anyhow!("Invalid origin: {}", e))?;

            // Add the origin
            client
                .add_allowed_origin(app_uuid, origin)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to add origin: {}", e))?;

            Ok(())
        })
    }

    /// Identify the current session
    pub fn identify(&self, node_url: &str, user_identity: UserIdentity) -> Result<String> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            // Set identity on client
            client.set_identity(user_identity);

            // Ensure session
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;

            // Identify (returns ())
            client.identify().await?;

            Ok("Successfully identified".to_string())
        })
    }

    /// Anonymize the current session
    pub fn anonymize(&self, node_url: &str) -> Result<String> {
        let mut client = self.client.borrow_mut();
        let mut current_url = self.current_url.borrow_mut();

        self.runtime.block_on(async {
            // Ensure session
            Self::ensure_session(&mut client, &mut current_url, node_url).await?;

            // Anonymize (returns ())
            client.anonymize().await?;

            Ok("Successfully anonymized".to_string())
        })
    }
}
