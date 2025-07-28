//! SQL store implementation using engine streams.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proven_engine::{Client, StreamConfig};
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::{
    connection::Connection, coordination::LeadershipCoordinator, error::Error, service::SqlService,
};

/// Global registry to track running SQL services by stream name.
/// This ensures that only one service runs per stream name within the same program.
static RUNNING_SERVICES: std::sync::LazyLock<Arc<Mutex<HashSet<String>>>> =
    std::sync::LazyLock::new(|| Arc::new(Mutex::new(HashSet::new())));

/// Default leadership configuration
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);
const DEFAULT_RENEWAL_INTERVAL: Duration = Duration::from_secs(10);

/// SQL store that uses engine streams.
#[derive(Clone)]
pub struct SqlEngineStore {
    /// Engine client
    client: Arc<Client>,
    /// Base name for streams
    name: String,
    /// Migrations to apply
    migrations: Vec<String>,
    /// Leadership lease duration
    lease_duration: Duration,
    /// Leadership renewal interval
    renewal_interval: Duration,
}

impl SqlEngineStore {
    /// Create a new SQL engine store.
    #[must_use]
    pub fn new(client: Arc<Client>, name: &str) -> Self {
        Self {
            client,
            name: name.to_string(),
            migrations: Vec::new(),
            lease_duration: DEFAULT_LEASE_DURATION,
            renewal_interval: DEFAULT_RENEWAL_INTERVAL,
        }
    }

    /// Set the migrations to apply.
    #[must_use]
    pub fn with_migrations(mut self, migrations: Vec<String>) -> Self {
        self.migrations = migrations;
        self
    }

    /// Set the leadership lease duration.
    #[must_use]
    pub const fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Set the leadership renewal interval.
    #[must_use]
    pub const fn with_renewal_interval(mut self, interval: Duration) -> Self {
        self.renewal_interval = interval;
        self
    }

    /// Get the command stream name.
    fn command_stream(&self) -> String {
        format!("{}.sql.commands", self.name)
    }

    /// Get the leadership stream name.
    fn leadership_stream(&self) -> String {
        format!("{}.sql.leadership", self.name)
    }
}

#[async_trait]
impl SqlStore for SqlEngineStore {
    type Error = Error;
    type Connection = Connection;

    async fn connect<Q: Clone + Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error> {
        let command_stream = self.command_stream();
        let leadership_stream = self.leadership_stream();

        // Create streams if they don't exist
        let command_stream_exists = match self.client.get_stream_info(&command_stream).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !command_stream_exists {
            info!("Creating SQL command stream: {}", command_stream);
            self.client
                .create_stream(command_stream.clone(), StreamConfig::default())
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;
        }

        let leadership_stream_exists = match self.client.get_stream_info(&leadership_stream).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !leadership_stream_exists {
            info!("Creating SQL leadership stream: {}", leadership_stream);
            self.client
                .create_stream(leadership_stream.clone(), StreamConfig::default())
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;
        }

        // Give streams a moment to be fully created
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check if service is already running
        let mut running_services = RUNNING_SERVICES.lock().await;
        let service_key = format!("{}:{}", self.client.node_id(), command_stream);

        if running_services.contains(&service_key) {
            debug!(
                "SQL service already running for stream '{}'",
                command_stream
            );
        } else {
            // We need to start the service
            info!("Starting SQL service for stream '{}'", command_stream);

            // Create leadership coordinator
            let leadership = Arc::new(LeadershipCoordinator::new(
                self.client.clone(),
                leadership_stream,
                self.client.node_id().clone(),
                self.lease_duration,
                self.renewal_interval,
            ));

            // Start leadership monitoring
            let leadership_clone = leadership;
            let client_clone = self.client.clone();
            let command_stream_clone = command_stream.clone();
            let _service_key_clone = service_key.clone();
            let all_migrations = self
                .migrations
                .iter()
                .cloned()
                .chain(migrations.iter().map(|m| m.clone().into()))
                .collect::<Vec<String>>();

            tokio::spawn(async move {
                let mut sql_service: Option<SqlService> = None;
                let mut was_leader = false;

                loop {
                    let is_leader = leadership_clone.is_leader().await;

                    if is_leader && !was_leader {
                        // Became leader - start SQL service
                        info!("Became SQL leader, starting service");

                        match SqlService::new().await {
                            Ok(mut service) => {
                                service.start(
                                    client_clone.clone(),
                                    command_stream_clone.clone(),
                                    all_migrations.clone(),
                                );
                                sql_service = Some(service);
                                was_leader = true;
                            }
                            Err(e) => {
                                error!("Failed to create SQL service: {}", e);
                            }
                        }
                    } else if !is_leader && was_leader {
                        // Lost leadership - stop SQL service
                        info!("Lost SQL leadership, stopping service");
                        if let Some(service) = sql_service.take() {
                            service.stop().await;
                        }
                        was_leader = false;
                    }

                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });

            running_services.insert(service_key);
            debug!("Registered SQL service for stream '{}'", command_stream);
        }

        drop(running_services);

        // Create connection
        let connection = Connection::new(self.client.clone(), command_stream);

        Ok(connection)
    }
}

/// Single-scoped SQL store
#[derive(Clone)]
pub struct SqlEngineStore1 {
    /// Engine client
    client: Arc<Client>,
    /// Base name for streams
    name: String,
    /// Migrations to apply
    migrations: Vec<String>,
    /// Leadership lease duration
    lease_duration: Duration,
    /// Leadership renewal interval
    renewal_interval: Duration,
}

impl SqlEngineStore1 {
    /// Create a new single-scoped SQL engine store.
    #[must_use]
    pub fn new(client: Arc<Client>, name: &str) -> Self {
        Self {
            client,
            name: name.to_string(),
            migrations: Vec::new(),
            lease_duration: DEFAULT_LEASE_DURATION,
            renewal_interval: DEFAULT_RENEWAL_INTERVAL,
        }
    }
}

#[async_trait]
impl SqlStore1 for SqlEngineStore1 {
    type Error = Error;
    type Connection = Connection;
    type Scoped = SqlEngineStore;

    fn scope<S>(&self, scope: S) -> Self::Scoped
    where
        S: AsRef<str> + Copy + Send,
    {
        SqlEngineStore {
            client: self.client.clone(),
            name: format!("{}.{}", self.name, scope.as_ref()),
            migrations: self.migrations.clone(),
            lease_duration: self.lease_duration,
            renewal_interval: self.renewal_interval,
        }
    }
}

/// Double-scoped SQL store
#[derive(Clone)]
pub struct SqlEngineStore2 {
    /// Engine client
    client: Arc<Client>,
    /// Base name for streams
    name: String,
    /// Migrations to apply
    migrations: Vec<String>,
    /// Leadership lease duration
    lease_duration: Duration,
    /// Leadership renewal interval
    renewal_interval: Duration,
}

impl SqlEngineStore2 {
    /// Create a new double-scoped SQL engine store.
    #[must_use]
    pub fn new(client: Arc<Client>, name: &str) -> Self {
        Self {
            client,
            name: name.to_string(),
            migrations: Vec::new(),
            lease_duration: DEFAULT_LEASE_DURATION,
            renewal_interval: DEFAULT_RENEWAL_INTERVAL,
        }
    }
}

#[async_trait]
impl SqlStore2 for SqlEngineStore2 {
    type Error = Error;
    type Connection = Connection;
    type Scoped = SqlEngineStore1;

    fn scope<S>(&self, scope: S) -> Self::Scoped
    where
        S: AsRef<str> + Copy + Send,
    {
        SqlEngineStore1 {
            client: self.client.clone(),
            name: format!("{}.{}", self.name, scope.as_ref()),
            migrations: self.migrations.clone(),
            lease_duration: self.lease_duration,
            renewal_interval: self.renewal_interval,
        }
    }
}

/// Triple-scoped SQL store
#[derive(Clone)]
pub struct SqlEngineStore3 {
    /// Engine client
    client: Arc<Client>,
    /// Base name for streams
    name: String,
    /// Migrations to apply
    migrations: Vec<String>,
    /// Leadership lease duration
    lease_duration: Duration,
    /// Leadership renewal interval
    renewal_interval: Duration,
}

impl SqlEngineStore3 {
    /// Create a new triple-scoped SQL engine store.
    #[must_use]
    pub fn new(client: Arc<Client>, name: &str) -> Self {
        Self {
            client,
            name: name.to_string(),
            migrations: Vec::new(),
            lease_duration: DEFAULT_LEASE_DURATION,
            renewal_interval: DEFAULT_RENEWAL_INTERVAL,
        }
    }
}

#[async_trait]
impl SqlStore3 for SqlEngineStore3 {
    type Error = Error;
    type Connection = Connection;
    type Scoped = SqlEngineStore2;

    fn scope<S>(&self, scope: S) -> Self::Scoped
    where
        S: AsRef<str> + Copy + Send,
    {
        SqlEngineStore2 {
            client: self.client.clone(),
            name: format!("{}.{}", self.name, scope.as_ref()),
            migrations: self.migrations.clone(),
            lease_duration: self.lease_duration,
            renewal_interval: self.renewal_interval,
        }
    }
}
