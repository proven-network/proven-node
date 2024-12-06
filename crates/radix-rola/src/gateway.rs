use crate::error::Error::{AddressHasNoOwnerKeys, AddressNotFound};
use crate::error::Result;

use proven_radix_gateway_sdk::types::{
    Address, StateEntityDetailsOptIns, StateEntityDetailsRequest,
};
use proven_radix_gateway_sdk::{build_client, Client};

pub struct GatewayService {
    client: Client,
}

impl GatewayService {
    pub fn new(gateway_url: &str, dapp_definition: &str, application_name: &str) -> Self {
        Self {
            client: build_client(gateway_url, Some(dapp_definition), Some(application_name)),
        }
    }

    pub async fn get_entity_owner_keys(&self, address: String) -> Result<String> {
        let opt_ins = StateEntityDetailsOptIns {
            ancestor_identities: false,
            component_royalty_config: false,
            component_royalty_vault_balance: false,
            dapp_two_way_links: false,
            explicit_metadata: vec!["owner_keys".to_string()],
            native_resource_details: false,
            non_fungible_include_nfids: false,
            package_royalty_vault_balance: false,
        };

        let request = StateEntityDetailsRequest::builder()
            .addresses(vec![Address(address)])
            .opt_ins(opt_ins);

        self.client
            .state_entity_details()
            .body(request)
            .send()
            .await?
            .items
            .first()
            .ok_or_else(|| AddressNotFound)?
            .metadata
            .items
            .iter()
            .find(|item| item.key == "owner_keys")
            .ok_or_else(|| AddressHasNoOwnerKeys)
            .map(|item| item.value.raw_hex.clone().0)
    }
}
