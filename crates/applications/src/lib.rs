mod application;
mod error;

pub use application::Application;
pub use error::{Error, Result};

use async_trait::async_trait;
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use uuid::Uuid;

static CREATE_APPLICATIONS_SQL: &str = include_str!("../sql/create_applications.sql");
static CREATE_DAPP_DEFININITIONS_SQL: &str = include_str!("../sql/create_dapp_definition.sql");

pub struct CreateApplicationOptions {
    pub owner_identity_address: String,
    pub dapp_definition_addresses: Vec<String>,
}

#[async_trait]
pub trait ApplicationManagement: Clone + Send + Sync + 'static {
    type SqlStore: SqlStore;

    async fn new(
        applications_store: Self::SqlStore,
    ) -> Result<Self, <Self::SqlStore as SqlStore>::Error>;

    async fn create_application(
        &self,
        options: CreateApplicationOptions,
    ) -> Result<Application, <Self::SqlStore as SqlStore>::Error>;

    async fn get_application(
        &self,
        application_id: String,
    ) -> Result<Option<Application>, <Self::SqlStore as SqlStore>::Error>;
}

#[derive(Clone)]
pub struct ApplicationManager<AS: SqlStore> {
    connection: AS::Connection,
}

#[async_trait]
impl<AS> ApplicationManagement for ApplicationManager<AS>
where
    AS: SqlStore,
{
    type SqlStore = AS;

    async fn new(
        applications_store: Self::SqlStore,
    ) -> Result<Self, <Self::SqlStore as SqlStore>::Error> {
        let connection = applications_store
            .connect(vec![CREATE_APPLICATIONS_SQL, CREATE_DAPP_DEFININITIONS_SQL])
            .await?;

        Ok(ApplicationManager { connection })
    }

    async fn create_application(
        &self,
        CreateApplicationOptions {
            owner_identity_address,
            dapp_definition_addresses,
        }: CreateApplicationOptions,
    ) -> Result<Application, <Self::SqlStore as SqlStore>::Error> {
        let application_id = Uuid::new_v4().to_string();

        self.connection
            .execute(
                "INSERT INTO applications (id, owner_identity) VALUES (?1, ?2)",
                vec![
                    SqlParam::Text(application_id.clone()),
                    SqlParam::Text(owner_identity_address.clone()),
                ],
            )
            .await?;

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
            .await?;

        Ok(Application {
            id: application_id,
            owner_identity_address,
            dapp_definition_addresses,
        })
    }

    async fn get_application(
        &self,
        application_id: String,
    ) -> Result<Option<Application>, <Self::SqlStore as SqlStore>::Error> {
        let rows = self
            .connection
            .query(
                r#"
                    SELECT * FROM applications
                    JOIN dapps ON applications.id = dapps.application_id
                    WHERE id = ?1
                "#
                .trim(),
                vec![SqlParam::Text(application_id.clone())],
            )
            .await?;

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
