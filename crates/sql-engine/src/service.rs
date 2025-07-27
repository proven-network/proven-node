//! SQL service that processes commands from the stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use libsql::Transaction as LibsqlTransaction;
use proven_engine::{Client, StoredMessage};
use proven_storage::LogIndex;
use tempfile::NamedTempFile;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    error::Error,
    handle_request::handle_request,
    pool::{ConnectionPool, PoolConfig},
    request::Request,
    response::Response,
};

/// Metadata keys for request/response correlation
const REQUEST_ID_KEY: &str = "request_id";
const RESPONSE_TO_KEY: &str = "response_to";
const REQUEST_TYPE_KEY: &str = "request_type";

/// Request wrapper for commands sent via stream
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamRequest {
    /// Unique request ID for correlation
    pub id: Uuid,
    /// The actual SQL request
    pub request: Request,
    /// Node that sent the request (for logging)
    pub from_node: String,
}

/// Response wrapper for responses sent via stream
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamResponse {
    /// Request ID this is responding to
    pub request_id: Uuid,
    /// The actual response
    pub response: Response,
}

/// SQL service that processes commands
pub struct SqlService {
    /// Connection pool
    pool: Arc<ConnectionPool>,
    /// Applied migrations
    applied_migrations: Arc<Mutex<Vec<String>>>,
    /// Whether migrations have been applied
    migrations_applied: Arc<Mutex<bool>>,
    /// Active transactions
    transactions: Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
    /// Background task handle
    handle: Option<JoinHandle<()>>,
    /// Shutdown channel
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl SqlService {
    /// Create a new SQL service.
    pub async fn new() -> Result<Self, Error> {
        // Create temporary database path
        let db_path = NamedTempFile::new()?.into_temp_path().to_path_buf();

        // Create connection pool with default config
        let pool = ConnectionPool::new(db_path.clone(), PoolConfig::default()).await?;

        // The connection pool will handle creating the database file and enabling WAL mode
        // Migrations table will be created when migrations are applied

        Ok(Self {
            pool: Arc::new(pool),
            applied_migrations: Arc::new(Mutex::new(Vec::new())),
            migrations_applied: Arc::new(Mutex::new(false)),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            handle: None,
            shutdown_tx: None,
        })
    }

    /// Start the service with the given client and stream name.
    pub fn start(&mut self, client: Arc<Client>, command_stream: String, migrations: Vec<String>) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let pool = Arc::clone(&self.pool);
        let applied_migrations = Arc::clone(&self.applied_migrations);
        let migrations_applied = Arc::clone(&self.migrations_applied);
        let transactions = Arc::clone(&self.transactions);

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::run_service_loop(
                client,
                command_stream,
                pool,
                applied_migrations,
                migrations_applied,
                transactions,
                migrations,
                shutdown_rx,
            )
            .await
            {
                error!("SQL service error: {}", e);
            }
        });

        self.handle = Some(handle);
    }

    /// Stop the service.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Check if migrations have been applied.
    #[allow(dead_code)]
    pub async fn migrations_applied(&self) -> bool {
        *self.migrations_applied.lock().await
    }

    /// Run the service loop.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::cognitive_complexity)]
    async fn run_service_loop(
        client: Arc<Client>,
        command_stream: String,
        pool: Arc<ConnectionPool>,
        applied_migrations: Arc<Mutex<Vec<String>>>,
        migrations_applied: Arc<Mutex<bool>>,
        transactions: Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
        migrations: Vec<String>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Error> {
        info!("Starting SQL service for stream '{}'", command_stream);

        // Apply migrations first
        Self::apply_migrations(&pool, &applied_migrations, &migrations_applied, migrations).await?;

        // Track last processed sequence
        let mut last_sequence = 0u64;

        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                info!("SQL service shutting down");
                break;
            }

            // Read next batch of messages
            let start_seq = LogIndex::new(last_sequence + 1).unwrap();
            let count = 10; // Process up to 10 at a time

            match client
                .read_from_stream(command_stream.clone(), start_seq, count)
                .await
            {
                Ok(messages) => {
                    if messages.is_empty() {
                        // No new messages, wait a bit
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    for message in messages {
                        last_sequence = message.sequence.get();

                        // Process the message
                        if let Err(e) = Self::process_message(
                            &client,
                            &command_stream,
                            &pool,
                            &transactions,
                            message,
                        )
                        .await
                        {
                            error!("Failed to process message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read command stream: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(())
    }

    /// Apply migrations to the database.
    #[allow(clippy::cognitive_complexity)]
    async fn apply_migrations(
        pool: &Arc<ConnectionPool>,
        applied_migrations: &Arc<Mutex<Vec<String>>>,
        migrations_applied: &Arc<Mutex<bool>>,
        migrations: Vec<String>,
    ) -> Result<(), Error> {
        let mut applied = applied_migrations.lock().await;

        for migration in migrations {
            if !applied.contains(&migration) {
                debug!("Applying migration: {}", migration);

                // Use handle_request to apply migration
                let response = handle_request(
                    pool,
                    &Arc::new(Mutex::new(HashMap::new())),
                    Request::Migrate(migration.clone()),
                )
                .await;

                match response {
                    Response::Migrate(needed_to_run) => {
                        if needed_to_run {
                            applied.push(migration.clone());
                            info!("Applied migration successfully");
                        } else {
                            debug!("Migration already applied");
                        }
                    }
                    Response::Failed(e) => {
                        error!("Failed to apply migration: {:?}", e);
                        return Err(Error::Libsql(e));
                    }
                    _ => {
                        error!("Unexpected response from migration");
                        return Err(Error::Stream(
                            "Unexpected response from migration".to_string(),
                        ));
                    }
                }
            }
        }

        drop(applied);

        *migrations_applied.lock().await = true;
        info!("All migrations applied successfully");
        Ok(())
    }

    /// Process a single command message.
    async fn process_message(
        client: &Arc<Client>,
        command_stream: &str,
        pool: &Arc<ConnectionPool>,
        transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
        message: StoredMessage,
    ) -> Result<(), Error> {
        // Check if this is a request
        let headers: HashMap<String, String> = message
            .data
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if headers.get(REQUEST_TYPE_KEY).map(String::as_str) == Some("sql_request") {
            // Deserialize request
            let stream_request: StreamRequest =
                ciborium::de::from_reader(&message.data.payload[..])
                    .map_err(|e| Error::Deserialization(e.to_string()))?;

            debug!(
                "Processing SQL request {:?} from node {}",
                stream_request.id, stream_request.from_node
            );

            // Process the SQL request
            let response = handle_request(pool, transactions, stream_request.request).await;

            // Send response if needed
            if let Some(request_id) = headers.get(REQUEST_ID_KEY) {
                let stream_response = StreamResponse {
                    request_id: Uuid::parse_str(request_id)
                        .map_err(|e| Error::Deserialization(e.to_string()))?,
                    response,
                };

                // Serialize response
                let mut payload = Vec::new();
                ciborium::ser::into_writer(&stream_response, &mut payload)?;

                // Create response metadata
                let mut response_metadata = HashMap::new();
                response_metadata.insert(RESPONSE_TO_KEY.to_string(), request_id.clone());
                response_metadata.insert(REQUEST_TYPE_KEY.to_string(), "sql_response".to_string());

                // Publish response
                let mut message = proven_engine::Message::new(payload);
                for (k, v) in response_metadata {
                    message = message.with_header(k, v);
                }
                client
                    .publish_to_stream(command_stream.to_string(), vec![message])
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;
            }

            // Delete the processed message
            client
                .delete_message(command_stream.to_string(), message.sequence)
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;
        }

        Ok(())
    }
}
