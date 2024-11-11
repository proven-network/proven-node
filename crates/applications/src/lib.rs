use std::vec;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::Store;
use radix_common::network::NetworkDefinition;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Application {
    pub application_id: String,
    pub owner_identity_address: String,
    pub dapp_definition_addresses: Vec<String>,
}

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

        let application_cbor = serde_cbor::to_vec(&application).unwrap();
        self.applications_store
            .put(
                application.application_id.clone(),
                Bytes::from(application_cbor),
            )
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
            Ok(application_opt) => match application_opt {
                Some(application_cbor) => {
                    let application: Application =
                        serde_cbor::from_slice(&application_cbor).unwrap();
                    Ok(Some(application))
                }
                None => Ok(None),
            },
            Err(_) => Err(Error::ApplicationStoreError),
        }
    }
}
