//! Rust port of the Radix ROLA example code.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::result_large_err)]

mod challenge;
mod error;
mod gateway;
mod util;

pub use challenge::*;
use error::Error::{InvalidProof, InvalidPublicKey};
pub use error::{Error, Result};
use util::{
    create_signature_message_hash, get_owner_key_part_for_public_key,
    get_virtual_address_for_public_key, verify_proof_factory,
};

use radix_common::network::NetworkDefinition;
use tracing::error;

/// Client for the Radix On Ledger Authenticator.
#[derive(Debug)]
pub struct Rola<'a> {
    application_name: String,
    dapp_definition_address: String,
    expected_origin: String,
    gateway_url: &'a str,
    network_definition: &'a NetworkDefinition,
}

/// Options for configuring `Rola`.
pub struct RolaOptions<'a> {
    /// The name of the application.
    pub application_name: &'a str,

    /// The address of the dApp definition.
    pub dapp_definition_address: &'a str,

    /// The expected origin of the request.
    pub expected_origin: &'a str,

    /// The URL of the Radix Gateway.
    pub gateway_url: &'a str,

    /// The network definition.
    pub network_definition: &'a NetworkDefinition,
}

impl<'a> Rola<'a> {
    /// Creates a new `Rola` instance with the given options.
    #[must_use]
    pub fn new(
        RolaOptions {
            application_name,
            dapp_definition_address,
            expected_origin,
            gateway_url,
            network_definition,
        }: RolaOptions<'a>,
    ) -> Self {
        Self {
            application_name: application_name.to_string(),
            dapp_definition_address: dapp_definition_address.to_string(),
            expected_origin: expected_origin.to_string(),
            gateway_url,
            network_definition,
        }
    }

    /// Verifies a signed challenge.
    ///
    /// # Errors
    ///
    /// This function will return an error if the proof is invalid, the public key is invalid,
    /// or if there is an error communicating with the gateway service.
    pub async fn verify_signed_challenge(&self, signed_challenge: SignedChallenge) -> Result<()> {
        let verify_proof = verify_proof_factory(signed_challenge.proof.clone());

        let check_ledger_for_key_address_match = || async {
            gateway::GatewayService::new(
                self.gateway_url,
                &self.dapp_definition_address,
                &self.application_name,
            )
            .get_entity_owner_keys(signed_challenge.clone().address)
            .await
            .map_err(|e| {
                error!("Error getting entity owner keys: {:?}", e);
                e
            })
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
            &self.dapp_definition_address,
            &self.expected_origin,
        )
        .and_then(|signature_message| verify_proof(&signature_message).ok())
        .ok_or(InvalidProof)?;

        match check_ledger_for_key_address_match().await {
            Some(()) => Ok(()),
            None => {
                match get_virtual_address_for_public_key(&signed_challenge, self.network_definition)
                {
                    Ok(virtual_address) => {
                        if virtual_address == signed_challenge.address {
                            Ok(())
                        } else {
                            Err(InvalidPublicKey)
                        }
                    }
                    Err(_) => Err(InvalidPublicKey),
                }
            }
        }
    }
}
