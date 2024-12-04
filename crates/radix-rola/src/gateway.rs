use crate::error::Error::{AddressHasNoOwnerKeys, AddressNotFound};
use crate::error::Result;

use proven_radix_gateway_sdk::types::{
    Address, StateEntityDetailsOptIns, StateEntityDetailsRequest,
};
use proven_radix_gateway_sdk::Client;

pub struct GatewayService {
    client: Client,
}

impl GatewayService {
    pub fn new(gateway_url: &str, dapp_definition: &str, application_name: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "User-Agent",
            "proven-radix-gateway-sdk/0.1.0".parse().unwrap(),
        );

        headers.insert("Rdx-App-Dapp-Definition", dapp_definition.parse().unwrap());
        headers.insert("Rdx-App-Name", application_name.parse().unwrap());

        let client = Client::new_with_client(
            gateway_url,
            reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .unwrap(),
        );

        Self { client }
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
