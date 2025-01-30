//! Manages database of all currently deployed applications.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod application;
mod error;

pub use application::Application;
pub use error::Error;

use async_trait::async_trait;
use futures::StreamExt;
use proven_sql::{SqlConnection, SqlParam, SqlStore, SqlStore1};
use proven_store::Store;
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
    /// The store type.
    type Store: Store<
        Application,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >;

    /// The SQL store type.
    type SqlStore: SqlStore1;

    /// Create a new application manager.
    fn new(store: Self::Store, sql_store: Self::SqlStore) -> Self;

    /// Create a new application.
    async fn create_application(
        &self,
        options: CreateApplicationOptions,
    ) -> Result<
        Application,
        Error<
            <Self::Store as Store<
                Application,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >>::Error,
            <Self::SqlStore as SqlStore1>::Error,
        >,
    >;

    /// Get an application by its ID.
    async fn get_application(
        &self,
        application_id: String,
    ) -> Result<
        Option<Application>,
        Error<
            <Self::Store as Store<
                Application,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >>::Error,
            <Self::SqlStore as SqlStore1>::Error,
        >,
    >;
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
    SS: SqlStore1,
{
    store: S,
    sql_store: SS,
}

#[async_trait]
impl<S, SS> ApplicationManagement for ApplicationManager<S, SS>
where
    S: Store<
        Application,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
    SS: SqlStore1,
{
    type Store = S;

    type SqlStore = SS;

    fn new(store: Self::Store, sql_store: Self::SqlStore) -> Self {
        Self { store, sql_store }
    }

    async fn create_application(
        &self,
        CreateApplicationOptions {
            owner_identity_address,
            dapp_definition_addresses,
        }: CreateApplicationOptions,
    ) -> Result<Application, Error<S::Error, SS::Error>> {
        let application_id = Uuid::new_v4().to_string();

        let connection = self
            .sql_store
            .scope(&application_id)
            .connect(vec![CREATE_APPLICATIONS_SQL, CREATE_DAPP_DEFININITIONS_SQL])
            .await
            .map_err(Error::SqlStore)?;

        connection
            .execute(
                "INSERT INTO applications (id, owner_identity) VALUES (?1, ?2)",
                vec![
                    SqlParam::Text(application_id.clone()),
                    SqlParam::Text(owner_identity_address.clone()),
                ],
            )
            .await
            .map_err(Error::SqlStore)?;

        connection
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

    async fn get_application(
        &self,
        application_id: String,
    ) -> Result<Option<Application>, Error<S::Error, SS::Error>> {
        if let Some(application) = self
            .store
            .get(application_id.clone())
            .await
            .map_err(Error::Store)?
        {
            return Ok(Some(application));
        }

        let connection = self
            .sql_store
            .scope(&application_id)
            .connect(vec![CREATE_APPLICATIONS_SQL, CREATE_DAPP_DEFININITIONS_SQL])
            .await
            .map_err(Error::SqlStore)?;

        let mut rows = connection
            .query(
                r"
                    SELECT owner_identity, dapps.dapp_definition_address FROM applications
                    JOIN dapps ON applications.id = dapps.application_id
                    WHERE id = ?1
                "
                .trim(),
                vec![SqlParam::Text(application_id.clone())],
            )
            .await
            .map_err(Error::SqlStore)?;

        // Early return if no rows found
        let Some(first_row) = rows.next().await else {
            return Ok(None);
        };

        // Get owner from first row
        let owner_identity = match &first_row[0] {
            SqlParam::Text(text) => text.clone(),
            _ => unreachable!(),
        };

        // Collect all dapp addresses including from first row
        let mut dapp_addresses = Vec::new();

        // Add first row's dapp address
        if let SqlParam::Text(addr) = &first_row[1] {
            dapp_addresses.push(addr.clone());
        }

        // Add remaining rows' dapp addresses
        while let Some(row) = rows.next().await {
            if let SqlParam::Text(addr) = &row[1] {
                dapp_addresses.push(addr.clone());
            }
        }

        Ok(Some(Application {
            id: application_id,
            owner_identity_address: owner_identity,
            dapp_definition_addresses: dapp_addresses,
        }))
    }
}
