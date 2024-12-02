use std::future::IntoFuture;

use crate::error::Error::{AddressHasNoOwnerKeys, AddressNotFound};
use crate::error::Result;

use proven_radix_gateway_sdk::generated::model::{
    ResourceAggregationLevel::Vault, StateEntityDetailsOptIns,
};
use proven_radix_gateway_sdk::Client;

pub struct GatewayService {
    client: Client,
}

impl GatewayService {
    pub fn new(
        gateway_url: &str,
        dapp_definition: String,
        application_name: String,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::new(gateway_url, Some(application_name), Some(dapp_definition))?,
        })
    }

    pub async fn get_entity_owner_keys(&self, address: String) -> Result<String> {
        let opt_ins = StateEntityDetailsOptIns {
            ancestor_identities: None,
            component_royalty_config: None,
            component_royalty_vault_balance: None,
            dapp_two_way_links: None,
            explicit_metadata: Some(vec!["owner_keys".to_string()]),
            native_resource_details: None,
            non_fungible_include_nfids: None,
            package_royalty_vault_balance: None,
        };

        self.client
            .get_inner_client()
            .state_entity_details(&[address.as_str()], Vault, opt_ins)
            .into_future()
            .await?
            .items
            .first()
            .ok_or_else(|| AddressNotFound)
            .and_then(|item| {
                item.metadata
                    .items
                    .iter()
                    .find(|item| item.key == "owner_keys")
                    .ok_or_else(|| AddressHasNoOwnerKeys)
                    .map(|item| item.value.raw_hex.clone())
            })
    }
}
