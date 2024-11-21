mod application;

pub use application::Application;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::Store;
use radix_common::network::NetworkDefinition;

#[async_trait]
pub trait ApplicationManagement: Clone + Send + Sync {
    type AS: Store;

    fn new(applications_store: Self::AS, network_definition: NetworkDefinition) -> Self;

    async fn create_application(&self) -> Result<Application>;

    async fn get_application(&self, application_id: String) -> Result<Option<Application>>;
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ApplicationStoreError,
}

#[derive(Clone)]
pub struct ApplicationManager<AS: Store> {
    applications_store: AS,
    network_definition: NetworkDefinition,
}

impl<AS> ApplicationManager<AS>
where
    AS: Store,
{
    fn application_store_key(&self, application_id: String) -> String {
        format!("{}-{}", self.network_definition.id, application_id)
    }
}

#[async_trait]
impl<AS> ApplicationManagement for ApplicationManager<AS>
where
    AS: Store + Send + Sync,
{
    type AS = AS;

    fn new(applications_store: Self::AS, network_definition: NetworkDefinition) -> Self {
        ApplicationManager {
            applications_store,
            network_definition,
        }
    }

    async fn create_application(&self) -> Result<Application> {
        let application = Application {
            application_id: self.application_store_key("".to_string()),
            owner_identity_address: "".to_string(),
            dapp_definition_addresses: vec![],
        };

        let bytes: Bytes = application
            .clone()
            .try_into()
            .map_err(|_| Error::ApplicationStoreError)?;

        self.applications_store
            .put(application.application_id.clone(), bytes)
            .await
            .map_err(|_| Error::ApplicationStoreError)?;

        Ok(application)
    }

    async fn get_application(&self, application_id: String) -> Result<Option<Application>> {
        match self
            .applications_store
            .get(self.application_store_key(application_id))
            .await
        {
            Ok(Some(bytes)) => Application::try_from(bytes)
                .map(Some)
                .map_err(|_| Error::ApplicationStoreError),
            Ok(None) => Ok(None),
            Err(_) => Err(Error::ApplicationStoreError),
        }
    }
}
