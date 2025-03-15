use radix_common::crypto::{Hash, hash};

pub fn create_signature_message_hash(
    challenge: String,
    dapp_definition_address: &str,
    origin: &str,
) -> Option<Hash> {
    let prefix = b"R";
    let length_of_dapp_def_address: u8 = dapp_definition_address.len().try_into().ok()?;
    let dapp_def_address_buffer = dapp_definition_address.as_bytes();
    let origin_buffer = origin.as_bytes();
    let challenge = hex::decode(challenge).ok()?;
    let challenge_buffer = challenge.as_slice();

    let message_buffer = [
        prefix,
        challenge_buffer,
        &length_of_dapp_def_address.to_le_bytes(),
        dapp_def_address_buffer,
        origin_buffer,
    ]
    .concat();

    Some(hash(message_buffer))
}
