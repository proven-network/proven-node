use proven_runtime::{Context, ExecutionRequest, Pool};

use std::sync::Arc;

use futures::future::join_all;
use rustyscript::Error;
use serde_json::json;

use std::panic;
use std::process;

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        eprintln!("Panic occurred: {:?}", panic_info);
        orig_hook(panic_info);
        process::exit(1);
    }));

    let user_module = r#"
        import { getCurrentAccounts, getCurrentIdentity } from "proven:sessions";

        export const handler = async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }
    "#
    .to_string();

    let pool = Pool::new(25);
    let mut handles = vec![];

    for i in 0..1000 {
        let pool = Arc::clone(&pool);
        let user_module = user_module.clone();
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest {
                context: Context {
                    identity: Some("identity".to_string()),
                    accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
                },
                handler_name: "handler".to_string(),
                args: vec![json!(10), json!(20)],
            };

            match pool.execute(user_module, request).await.await {
                Ok(result) => match result {
                    Ok(result) => {
                        assert!(result.output.is_number());
                        let output = result.output.as_i64().unwrap();
                        assert_eq!(output, 30);
                        println!("[{i}] Result: {:?}", result);
                    }
                    Err(e) => eprintln!("Error in result: {:?}", e),
                },
                Err(e) => eprintln!("Error executing request: {:?}", e),
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    join_all(handles).await;

    Ok(())
}
