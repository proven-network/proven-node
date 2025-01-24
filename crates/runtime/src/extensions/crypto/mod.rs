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

    pub fn get_key(&self, key_id: u32) -> &Key {
        &self.keys[key_id as usize]
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
#[string]
pub fn op_get_curve_name(#[state] state: &CryptoState, key_id: u32) -> String {
    match &state.keys[key_id as usize] {
        Key::Ed25519(_) => "Ed25519".to_string(),
    }
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
    ops = [
        op_generate_ed25519,
        op_get_curve_name,
        op_get_public_key,
        op_sign_bytes,
        op_sign_string
    ],
    esm_entry_point = "proven:crypto",
    esm = [ dir "src/extensions/crypto", "proven:crypto" = "crypto.js" ],
    docs = "Functions for key management and signing"
);

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, HandlerSpecifier, RuntimeOptions, Worker};

    use ed25519_dalek::Verifier;
    use radix_transactions::model::{RawNotarizedTransaction, TransactionPayload};

    #[tokio::test]
    async fn test_ed25519_signing() {
        let runtime_options = RuntimeOptions::for_test_code("crypto/test_ed25519_signing", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_array());
        let (verifying_key_bytes, signature_bytes): (Vec<u8>, Vec<u8>) =
            execution_result.deserialize_output().unwrap();

        // Check that the signature is valid
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(
            &verifying_key_bytes
                .try_into()
                .expect("slice with incorrect length"),
        )
        .unwrap();
        let signature = ed25519_dalek::Signature::from_bytes(
            &signature_bytes
                .try_into()
                .expect("slice with incorrect length"),
        );

        let message = "Hello, world!";
        assert!(verifying_key.verify(message.as_bytes(), &signature).is_ok());
    }

    #[tokio::test]
    async fn test_ed25519_signing_radix_transaction() {
        let runtime_options =
            RuntimeOptions::for_test_code("crypto/test_ed25519_signing_radix_transaction", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let result = result.unwrap();
        let output = result.output.as_array().unwrap();

        // First element is compiled notorized transaction in hex
        assert!(output[0].is_string());
        let raw_transaction: RawNotarizedTransaction =
            hex::decode(output[0].as_str().unwrap()).unwrap().into();

        let parse_result =
            radix_transactions::model::NotarizedTransactionV1::from_raw(&raw_transaction);

        assert!(parse_result.is_ok());

        // Second element is notary public key in hex
        assert!(output[1].is_string());

        // Third element is signer public key in hex
        assert!(output[2].is_string());

        // TODO: Check signatures
    }

    #[tokio::test]
    async fn test_ed25519_storage() {
        let mut runtime_options =
            RuntimeOptions::for_test_code("crypto/test_ed25519_storage", "save");
        let mut worker = Worker::new(runtime_options.clone()).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request.clone()).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
        let execution_result = result.unwrap();
        let bytes_vec: Vec<u8> = execution_result.deserialize_output().unwrap();

        let verifying_key =
            ed25519_dalek::VerifyingKey::from_bytes(bytes_vec.as_slice().try_into().unwrap())
                .unwrap();

        // Reuse options to ensure the same application kv store is used. Just change the handler name.
        runtime_options.handler_specifier =
            HandlerSpecifier::parse("file:///main.ts#load").unwrap();
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        // Check that the signature is valid
        let execution_result = result.unwrap();
        let bytes_vec: Vec<u8> = execution_result.deserialize_output().unwrap();
        let signature =
            ed25519_dalek::Signature::from_bytes(bytes_vec.as_slice().try_into().unwrap());

        let message = "Hello, world!";
        assert!(verifying_key.verify(message.as_bytes(), &signature).is_ok());
    }
}
