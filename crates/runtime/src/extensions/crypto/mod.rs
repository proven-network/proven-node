#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use bytes::Bytes;
use deno_core::{extension, op2};
use ed25519_dalek::ed25519::signature::Signer;
use ed25519_dalek::SigningKey;

pub enum Key {
    Ed25519(SigningKey),
}

#[derive(Default)]
pub struct CryptoState {
    pub keys: Vec<Key>,
}

impl CryptoState {
    pub fn generate_new_ed25519(&mut self) -> u32 {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        self.keys.push(Key::Ed25519(signing_key));
        (self.keys.len() - 1).try_into().unwrap()
    }

    #[allow(dead_code)]
    pub fn load_existing_key(&mut self, key: Key) -> u32 {
        self.keys.push(key);
        (self.keys.len() - 1).try_into().unwrap()
    }
}

#[op2(fast)]
pub fn op_generate_ed25519(#[state] state: &mut CryptoState) -> u32 {
    state.generate_new_ed25519()
}

#[op2]
#[buffer]
pub fn op_get_public_key(#[state] state: &CryptoState, key_id: u32) -> Vec<u8> {
    match &state.keys[key_id as usize] {
        Key::Ed25519(signing_key) => signing_key.verifying_key().to_bytes().to_vec(),
    }
}

#[allow(clippy::needless_pass_by_value)]
#[op2]
#[buffer]
pub fn op_sign_bytes(
    #[state] state: &CryptoState,
    key_id: u32,
    #[buffer(copy)] bytes_to_sign: Bytes,
) -> Vec<u8> {
    match &state.keys[key_id as usize] {
        Key::Ed25519(signing_key) => {
            let signature = signing_key.sign(bytes_to_sign.as_ref());
            signature.to_bytes().into()
        }
    }
}

#[op2]
#[buffer]
pub fn op_sign_string(
    #[state] state: &CryptoState,
    key_id: u32,
    #[string] string_to_sign: &str,
) -> Vec<u8> {
    match &state.keys[key_id as usize] {
        Key::Ed25519(signing_key) => {
            let signature = signing_key.sign(string_to_sign.as_bytes());
            signature.to_bytes().into()
        }
    }
}

extension!(
    crypto_ext,
    ops = [op_generate_ed25519, op_get_public_key, op_sign_bytes, op_sign_string],
    esm_entry_point = "proven:crypto",
    esm = [ dir "src/extensions/crypto", "proven:crypto" = "crypto.js" ],
    docs = "Functions for key management and signing"
);
