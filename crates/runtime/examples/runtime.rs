#![allow(clippy::result_large_err)]

use proven_runtime::{
    Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RpcEndpoints,
    Runtime, RuntimeOptions,
};

use proven_code_package::CodePackage;
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

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

    let request = ExecutionRequest::Rpc {
        application_id: Uuid::max(),
        args: vec![json!(10), json!(20)],
        handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
    };

    let result = runtime.execute(request)?;

    assert!(matches!(result, ExecutionResult::Ok { .. }));

    println!("Result: {:?}", result);

    Ok(())
}
