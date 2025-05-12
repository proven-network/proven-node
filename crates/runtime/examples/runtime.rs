use proven_runtime::{
    Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RpcEndpoints,
    Runtime, RuntimeOptions,
};

use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_code_package::CodePackage;
use proven_identity::{Identity, LedgerIdentity, RadixIdentityDetails, Session};
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
use serde_json::json;
use tempfile::tempdir;

fn main() -> Result<(), Error> {
    let module_loader = ModuleLoader::new(
        CodePackage::from_str(
            r#"
            import { run } from "@proven-network/handler";
            import { getCurrentAccounts, getCurrentIdentity } from "proven:session";

            export const handler = run(async (a, b) => {
                const userId = getCurrentIdentity();
                const accounts = getCurrentAccounts();
                console.info("Current identity: " + userId);
                console.log("Current accounts: " + accounts);

                return a + b;
            });
        "#,
        )
        .unwrap(),
    );

    let mut runtime = Runtime::new(RuntimeOptions {
        application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
        application_store: MemoryStore2::new(),
        file_system_store: MemoryStore::new(),
        module_loader,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
        personal_store: MemoryStore3::new(),
        radix_nft_verifier: MockRadixNftVerifier::new(),
        rpc_endpoints: RpcEndpoints::external(),
    })?;

    let random_signing_key = SigningKey::generate(&mut rand::thread_rng());
    let random_verifying_key = VerifyingKey::from(&SigningKey::generate(&mut rand::thread_rng()));

    let request = ExecutionRequest::Rpc {
        application_id: "application_id".to_string(),
        args: vec![json!(10), json!(20)],
        handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
        session: Session::Identified {
            identity: Identity {
                identity_id: "identity_id".to_string(),
                ledger_identities: vec![LedgerIdentity::Radix(RadixIdentityDetails {
                    account_addresses: vec!["my_account_1".to_string(), "my_account_2".to_string()],
                    dapp_definition_address: "dapp_definition_address".to_string(),
                    expected_origin: "origin".to_string(),
                    identity_address: "my_identity".to_string(),
                })],
                passkeys: vec![],
            },
            ledger_identity: LedgerIdentity::Radix(RadixIdentityDetails {
                account_addresses: vec!["my_account_1".to_string(), "my_account_2".to_string()],
                dapp_definition_address: "dapp_definition_address".to_string(),
                expected_origin: "origin".to_string(),
                identity_address: "my_identity".to_string(),
            }),
            origin: "origin".to_string(),
            session_id: "session_id".to_string(),
            signing_key: random_signing_key,
            verifying_key: random_verifying_key,
        },
    };

    let result = runtime.execute(request)?;

    assert!(matches!(result, ExecutionResult::Ok { .. }));

    println!("Result: {:?}", result);

    Ok(())
}
