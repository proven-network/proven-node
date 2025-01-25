use proven_runtime::{
    Error, ExecutionRequest, HandlerSpecifier, ModuleLoader, Runtime, RuntimeOptions,
};

use proven_code_package::CodePackage;
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore2, MemoryStore3};
use radix_common::network::NetworkDefinition;
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
        application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
        application_store: MemoryStore2::new(),
        module_loader,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        personal_store: MemoryStore3::new(),
        radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
        radix_network_definition: NetworkDefinition::stokenet(),
        radix_nft_verifier: MockRadixNftVerifier::new(),
    })?;

    let request = ExecutionRequest::Rpc {
        accounts: vec!["my_account_1".to_string(), "my_account_2".to_string()],
        args: vec![json!(10), json!(20)],
        dapp_definition_address: "dapp_definition_address".to_string(),
        handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
        identity: "my_identity".to_string(),
    };

    let result = runtime.execute(request)?;

    assert!(result.output.is_number());
    let output = result.output.as_i64().unwrap();
    assert_eq!(output, 30);

    println!("Result: {:?}", result);

    Ok(())
}
