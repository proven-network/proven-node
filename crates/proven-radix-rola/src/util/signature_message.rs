use radix_common::crypto::{hash, Hash};

pub fn create_signature_message_hash(
    challenge: String,
    dapp_definition_address: String,
    origin: String,
) -> Option<Hash> {
    let prefix = b"R";
    let length_of_dapp_def_address = dapp_definition_address.len() as u8;
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
