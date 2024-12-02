//! Manages database of all currently deployed applications.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod application;
mod error;

pub use application::Application;
pub use error::{Error, Result};

use async_trait::async_trait;
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use uuid::Uuid;

static CREATE_APPLICATIONS_SQL: &str = include_str!("../sql/create_applications.sql");
static CREATE_DAPP_DEFININITIONS_SQL: &str = include_str!("../sql/create_dapp_definition.sql");

/// Options for creating a new application.
pub struct CreateApplicationOptions {
    /// Owner's identity address on Radix Network.
    pub owner_identity_address: String,

    /// dApp definition addresses on Radix Network.
    pub dapp_definition_addresses: Vec<String>,
}

/// Trait for managing applications.
#[async_trait]
pub trait ApplicationManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the store.
    type SqlStore: SqlStore;

    /// Create a new application manager.
    async fn new(applications_store: Self::SqlStore) -> Result<Self, Self::SqlStore>;

    /// Create a new application.
    async fn create_application(
        &self,
        options: CreateApplicationOptions,
    ) -> Result<Application, Self::SqlStore>;

    /// Get an application by its ID.
    async fn get_application(
        &self,
        application_id: String,
    ) -> Result<Option<Application>, Self::SqlStore>;
}

/// Manages database of all currently deployed applications.
#[derive(Clone)]
pub struct ApplicationManager<AS: SqlStore> {
    connection: AS::Connection,
}

#[async_trait]
impl<S> ApplicationManagement for ApplicationManager<S>
where
    S: SqlStore,
{
    type SqlStore = S;

    async fn new(applications_store: Self::SqlStore) -> Result<Self, S> {
        let connection = applications_store
            .connect(vec![CREATE_APPLICATIONS_SQL, CREATE_DAPP_DEFININITIONS_SQL])
            .await
            .map_err(Error::SqlStore)?;

        Ok(Self { connection })
    }

    async fn create_application(
        &self,
        CreateApplicationOptions {
            owner_identity_address,
            dapp_definition_addresses,
        }: CreateApplicationOptions,
    ) -> Result<Application, S> {
        let application_id = Uuid::new_v4().to_string();

        self.connection
            .execute(
                "INSERT INTO applications (id, owner_identity) VALUES (?1, ?2)",
                vec![
                    SqlParam::Text(application_id.clone()),
                    SqlParam::Text(owner_identity_address.clone()),
                ],
            )
            .await
            .map_err(Error::SqlStore)?;

        self.connection
            .execute_batch(
                "INSERT INTO dapps (application_id, dapp_definition_address) VALUES (?1, ?2)",
                dapp_definition_addresses
                    .iter()
                    .map(|dapp_definition_address| {
                        vec![
                            SqlParam::Text(application_id.clone()),
                            SqlParam::Text(dapp_definition_address.clone()),
                        ]
                    })
                    .collect(),
            )
            .await
            .map_err(Error::SqlStore)?;

        Ok(Application {
            id: application_id,
            owner_identity_address,
            dapp_definition_addresses,
        })
    }

    async fn get_application(&self, application_id: String) -> Result<Option<Application>, S> {
        let rows = self
            .connection
            .query(
                r"
                    SELECT * FROM applications
                    JOIN dapps ON applications.id = dapps.application_id
                    WHERE id = ?1
                "
                .trim(),
                vec![SqlParam::Text(application_id.clone())],
            )
            .await
            .map_err(Error::SqlStore)?;

        if rows.is_empty() {
            return Ok(None);
        }

        let application = Application {
            id: application_id,
            owner_identity_address: rows.row(0).unwrap().get_text(1).unwrap().to_string(),
            dapp_definition_addresses: rows
                .iter()
                .map(|row| row.get_text(3).unwrap().to_string())
                .collect(),
        };

        Ok(Some(application))
    }
}
