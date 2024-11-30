use proven_runtime::{Error, ExecutionRequest, Runtime, RuntimeOptions};

use proven_sql_direct::DirectSqlStore;
use proven_store_memory::MemoryStore;
use serde_json::json;
use tempfile::tempdir;

fn main() -> Result<(), Error> {
    let user_module = r#"
        import { getCurrentAccounts, getCurrentIdentity } from "proven:sessions";
        import { runWithOptions } from "proven:run";

        export const handler = runWithOptions(async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }, { timeout: 2000 });

        export const handler2 = runWithOptions(async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }, { timeout: 2 });

        export const something = 69;
        export default 42;
        export const nothing = 0;

        export const handler3 = runWithOptions(async function (a, b) {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }, { timeout: 3 });
    "#;

    let mut runtime = Runtime::new(RuntimeOptions {
        application_sql_store: DirectSqlStore::new(tempdir().unwrap().into_path()),
        application_store: MemoryStore::new(),
        gateway_origin: "https://stokenet.radixdlt.com".to_string(),
        handler_name: Some("handler".to_string()),
        module: user_module.to_string(),
        nft_sql_store: DirectSqlStore::new(tempdir().unwrap().into_path()),
        nft_store: MemoryStore::new(),
        personal_sql_store: DirectSqlStore::new(tempdir().unwrap().into_path()),
        personal_store: MemoryStore::new(),
    })?;

    let request = ExecutionRequest {
        accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
        args: vec![json!(10), json!(20)],
        dapp_definition_address: "dapp_definition_address".to_string(),
        identity: Some("identity".to_string()),
    };

    let result = runtime.execute(request)?;

    assert!(result.output.is_number());
    let output = result.output.as_i64().unwrap();
    assert_eq!(output, 30);

    println!("Result: {:?}", result);

    Ok(())
}
