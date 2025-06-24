//! Manages database of all currently deployed applications.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod application;
mod error;
mod events;

pub use application::Application;
use bytes::Bytes;
pub use error::Error;

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_store::Store;
use uuid::Uuid;

static CREATE_APPLICATIONS_SQL: &str = include_str!("../sql/01_create_applications.sql");

/// Options for creating a new application.
pub struct CreateApplicationOptions {
    /// Owner's identity ID.
    pub owner_identity_id: Uuid,
}

/// Trait for managing applications.
#[async_trait]
pub trait ApplicationManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// The store type.
    type Store: Store<
            Application,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >;

    /// The SQL store type.
    type SqlStore: SqlStore;

    /// Create a new application manager.
    fn new(store: Self::Store, sql_store: Self::SqlStore) -> Self;

    /// Create a new application.
    async fn create_application(
        &self,
        options: CreateApplicationOptions,
    ) -> Result<Application, Error>;

    /// Get an application by its ID.
    async fn get_application(&self, application_id: Uuid) -> Result<Option<Application>, Error>;
}

/// Manages database of all currently deployed applications.
#[derive(Clone)]
pub struct ApplicationManager<S, SS>
where
    S: Store<
            Application,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    SS: SqlStore,
{
    store: S,
    sql_store: SS,
    sql_connection: Arc<OnceCell<SS::Connection>>,
}

impl<S, SS> ApplicationManager<S, SS>
where
    S: Store<
            Application,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    SS: SqlStore,
{
    /// Get or create the cached SQL connection
    async fn get_sql_connection(&self) -> Result<SS::Connection, Error> {
        if let Some(connection) = self.sql_connection.get() {
            return Ok(connection.clone());
        }

        let connection = self
            .sql_store
            .connect(vec![CREATE_APPLICATIONS_SQL])
            .await
            .map_err(|e| Error::SqlStore(e.to_string()))?;

        // Try to set the connection, but if another thread beat us to it, use that one
        match self.sql_connection.set(connection.clone()) {
            Ok(()) => Ok(connection),
            Err(_) => {
                // Another thread set it first, use the one that was set
                Ok(self.sql_connection.get().unwrap().clone())
            }
        }
    }
}

#[async_trait]
impl<S, SS> ApplicationManagement for ApplicationManager<S, SS>
where
    S: Store<
            Application,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    SS: SqlStore,
{
    type Store = S;

    type SqlStore = SS;

    fn new(store: Self::Store, sql_store: Self::SqlStore) -> Self {
        Self {
            store,
            sql_store,
            sql_connection: Arc::new(OnceCell::new()),
        }
    }

    async fn create_application(
        &self,
        CreateApplicationOptions { owner_identity_id }: CreateApplicationOptions,
    ) -> Result<Application, Error> {
        let application_id = Uuid::new_v4();

        let connection = self.get_sql_connection().await?;

        let now = chrono::Utc::now().timestamp();
        connection
            .execute(
                "INSERT INTO applications (id, owner_identity_id, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
                vec![
                    SqlParam::Blob(Bytes::from(application_id.into_bytes().to_vec())),
                    SqlParam::Blob(Bytes::from(owner_identity_id.into_bytes().to_vec())),
                    SqlParam::Integer(now),
                    SqlParam::Integer(now),
                ],
            )
            .await
            .map_err(|e| Error::SqlStore(e.to_string()))?;

        Ok(Application {
            id: application_id,
            owner_identity_id,
        })
    }

    async fn get_application(&self, application_id: Uuid) -> Result<Option<Application>, Error> {
        if let Some(application) = self
            .store
            .get(application_id.to_string())
            .await
            .map_err(|e| Error::Store(e.to_string()))?
        {
            return Ok(Some(application));
        }

        let connection = self.get_sql_connection().await?;

        let mut rows = connection
            .query(
                r"
                    SELECT owner_identity_id FROM applications WHERE id = ?1
                "
                .trim(),
                vec![SqlParam::Blob(Bytes::from(
                    application_id.into_bytes().to_vec(),
                ))],
            )
            .await
            .map_err(|e| Error::SqlStore(e.to_string()))?;

        // Early return if no rows found
        let Some(first_row) = rows.next().await else {
            return Ok(None);
        };

        // Get owner identity ID from first row
        let owner_identity_id = match &first_row[0] {
            SqlParam::Blob(blob) => Uuid::from_slice(&blob.as_ref()).unwrap(),
            SqlParam::BlobWithName(_, blob) => Uuid::from_slice(&blob.as_ref()).unwrap(),
            _ => unreachable!(),
        };

        Ok(Some(Application {
            id: application_id,
            owner_identity_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_messaging::stream::Stream;
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        service::MemoryServiceOptions,
        stream::{MemoryStream, MemoryStreamOptions},
    };
    use proven_sql_streamed::{Request, StreamedSqlStore};
    use proven_store_memory::MemoryStore;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    type DeserializeError = ciborium::de::Error<std::io::Error>;
    type SerializeError = ciborium::ser::Error<std::io::Error>;
    type TestStore = MemoryStore<Application, DeserializeError, SerializeError>;
    type TestSqlStore = StreamedSqlStore<
        MemoryStream<Request, DeserializeError, SerializeError>,
        MemoryStore<bytes::Bytes>,
    >;
    type TestApplicationManager = ApplicationManager<TestStore, TestSqlStore>;

    async fn create_test_manager() -> TestApplicationManager {
        let store = MemoryStore::new();
        // Use a unique stream name for each test to avoid conflicts
        let stream_name = format!("test_applications_{}", Uuid::new_v4());
        let stream = MemoryStream::new(stream_name, MemoryStreamOptions);
        let sql_store = StreamedSqlStore::new(
            stream,
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryStore::new(),
        );

        ApplicationManager::new(store, sql_store)
    }

    #[tokio::test]
    async fn test_create_application() {
        let result = timeout(Duration::from_secs(5), async {
            let manager = create_test_manager().await;
            let owner_identity_id = Uuid::new_v4();

            let options = CreateApplicationOptions { owner_identity_id };

            let result = manager.create_application(options).await;
            assert!(result.is_ok());

            let application = result.unwrap();
            assert_eq!(application.owner_identity_id, owner_identity_id);
            assert!(!application.id.is_nil());
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_get_application_from_sql() {
        let result = timeout(Duration::from_secs(5), async {
            let manager = create_test_manager().await;
            let owner_identity_id = Uuid::new_v4();

            // Create an application
            let options = CreateApplicationOptions { owner_identity_id };
            let created_app = manager.create_application(options).await.unwrap();

            // Retrieve the application by ID
            let retrieved_app = manager.get_application(created_app.id).await.unwrap();
            assert!(retrieved_app.is_some());

            let retrieved_app = retrieved_app.unwrap();
            assert_eq!(retrieved_app.id, created_app.id);
            assert_eq!(retrieved_app.owner_identity_id, owner_identity_id);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_get_nonexistent_application() {
        let result = timeout(Duration::from_secs(5), async {
            let manager = create_test_manager().await;
            let nonexistent_id = Uuid::new_v4();

            let result = manager.get_application(nonexistent_id).await.unwrap();
            assert!(result.is_none());
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_get_application_from_cache() {
        let result = timeout(Duration::from_secs(5), async {
            let manager = create_test_manager().await;
            let app_id = Uuid::new_v4();
            let owner_identity_id = Uuid::new_v4();

            // Manually add application to the store cache
            let application = Application {
                id: app_id,
                owner_identity_id,
            };

            manager
                .store
                .put(app_id.to_string(), application.clone())
                .await
                .unwrap();

            // Retrieve the application - should come from cache, not SQL
            let retrieved_app = manager.get_application(app_id).await.unwrap();
            assert!(retrieved_app.is_some());

            let retrieved_app = retrieved_app.unwrap();
            assert_eq!(retrieved_app.id, app_id);
            assert_eq!(retrieved_app.owner_identity_id, owner_identity_id);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_sql_connection_reuse() {
        let result = timeout(Duration::from_secs(5), async {
            let manager = create_test_manager().await;
            let owner_identity_id1 = Uuid::new_v4();
            let owner_identity_id2 = Uuid::new_v4();

            // Create multiple applications to ensure connection reuse
            let options1 = CreateApplicationOptions {
                owner_identity_id: owner_identity_id1,
            };
            let options2 = CreateApplicationOptions {
                owner_identity_id: owner_identity_id2,
            };

            let app1 = manager.create_application(options1).await.unwrap();
            let app2 = manager.create_application(options2).await.unwrap();

            // Verify both applications were created successfully
            assert_ne!(app1.id, app2.id);
            assert_eq!(app1.owner_identity_id, owner_identity_id1);
            assert_eq!(app2.owner_identity_id, owner_identity_id2);

            // Verify we can retrieve both applications
            let retrieved_app1 = manager.get_application(app1.id).await.unwrap().unwrap();
            let retrieved_app2 = manager.get_application(app2.id).await.unwrap().unwrap();

            assert_eq!(retrieved_app1.id, app1.id);
            assert_eq!(retrieved_app2.id, app2.id);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_concurrent_application_creation() {
        let result = timeout(Duration::from_secs(15), async {
            // Create one manager to be shared across all concurrent operations
            let manager = create_test_manager().await;

            // Create multiple applications concurrently using the same manager
            let mut handles = Vec::new();
            for _i in 0..5 {
                let manager_clone = manager.clone();
                let handle = tokio::spawn(async move {
                    let owner_identity_id = Uuid::new_v4();
                    let options = CreateApplicationOptions { owner_identity_id };
                    manager_clone.create_application(options).await
                });
                handles.push(handle);
            }

            // Wait for all tasks to complete
            let mut results = Vec::new();
            for handle in handles {
                let result = handle.await.unwrap();
                assert!(result.is_ok());
                results.push(result.unwrap());
            }

            // Verify all applications have unique IDs
            let mut ids: Vec<Uuid> = results.iter().map(|app| app.id).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), 5); // All IDs should be unique
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
