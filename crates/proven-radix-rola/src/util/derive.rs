use crate::{Proof, Result, SignedChallenge};

use radix_common::crypto::{HasPublicKeyHash, IsPublicKeyHash, PublicKey};
use radix_common::network::NetworkDefinition;

pub fn get_owner_key_part_for_public_key(public_key: &PublicKey) -> String {
    let hash = public_key.get_hash();
    let hash_bytes = hash.get_hash_bytes();
    let len = hash_bytes.len();
    let last_bytes = &hash_bytes[len.saturating_sub(29)..];

    hex::encode(last_bytes)
}

pub fn get_virtual_address_for_public_key(
    SignedChallenge {
        proof: Proof { public_key, .. },
        r#type: entity_type,
        ..
    }: &SignedChallenge,
    network_definition: &NetworkDefinition,
) -> Result<String> {
    let hash = public_key.get_hash();
    let hash_bytes = hash.get_hash_bytes();
    let len = hash_bytes.len();
    let last_bytes = &hash_bytes[len.saturating_sub(29)..];

    let entity_prefix = match entity_type {
        crate::Type::Account => "account",
        crate::Type::Persona => "identiy",
    };

    let hrp = bech32::Hrp::parse(
        format!("{}_{}", entity_prefix, network_definition.hrp_suffix).as_str(),
    )?;

    match public_key {
        PublicKey::Secp256k1(_) => {
            let mut bytes = vec![0xD1];
            bytes.extend_from_slice(last_bytes);
            Ok(bech32::encode::<bech32::Bech32m>(hrp, bytes.as_slice())?)
        }
        PublicKey::Ed25519(_) => {
            let mut bytes = vec![0x51];
            bytes.extend_from_slice(last_bytes);
            Ok(bech32::encode::<bech32::Bech32m>(hrp, bytes.as_slice())?)
        }
    }
}
