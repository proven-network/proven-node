mod challenge;
mod error;
mod gateway;
mod util;

pub use challenge::*;
use error::Error::{InvalidProof, InvalidPublicKey};
pub use error::{Error, Result};
use util::*;

use radix_common::network::NetworkDefinition;

#[derive(Debug)]
pub struct Rola {
    application_name: String,
    dapp_definition_address: String,
    expected_origin: String,
    gateway_url: String,
    network_definition: NetworkDefinition,
}

impl Rola {
    pub fn new(
        network_definition: NetworkDefinition,
        gateway_url: String,
        dapp_definition_address: String,
        expected_origin: String,
        application_name: String,
    ) -> Self {
        Self {
            application_name,
            dapp_definition_address,
            expected_origin,
            gateway_url,
            network_definition,
        }
    }

    pub async fn verify_signed_challenge(&self, signed_challenge: SignedChallenge) -> Result<()> {
        let verify_proof = verify_proof_factory(signed_challenge.proof.clone());

        let check_ledger_for_key_address_match = || async {
            gateway::GatewayService::new(
                self.gateway_url.clone(),
                self.dapp_definition_address.clone(),
                self.application_name.clone(),
            )
            .ok()
            .unwrap()
            .get_entity_owner_keys(signed_challenge.clone().address)
            .await
            .ok()
            .and_then(|owner_keys| {
                if owner_keys.to_uppercase().contains(
                    get_owner_key_part_for_public_key(&signed_challenge.proof.public_key)
                        .to_uppercase()
                        .as_str(),
                ) {
                    Some(())
                } else {
                    None
                }
            })
        };

        create_signature_message_hash(
            signed_challenge.challenge.clone(),
            self.dapp_definition_address.clone(),
            self.expected_origin.clone(),
        )
        .and_then(|signature_message| verify_proof(&signature_message).ok())
        .ok_or(InvalidProof)?;

        match check_ledger_for_key_address_match().await {
            Some(_) => Ok(()),
            None => {
                match get_virtual_address_for_public_key(
                    &signed_challenge,
                    &self.network_definition,
                ) {
                    Ok(virtual_address) => match virtual_address == signed_challenge.address {
                        true => Ok(()),
                        false => Err(InvalidPublicKey),
                    },
                    Err(_) => Err(InvalidPublicKey),
                }
            }
        }
    }
}
