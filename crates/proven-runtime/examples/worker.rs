use std::sync::Arc;

use futures::future::join_all;
use proven_runtime::{Context, ExecutionRequest, Worker};
use rustyscript::Error;
use serde_json::json;
use tokio::{sync::Mutex, time::Instant};

static EXECUTIONS: usize = 10000;

#[tokio::main]
async fn main() -> Result<(), Error> {
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

    let worker = Arc::new(Mutex::new(Worker::new(user_module)));
    let mut handles = vec![];

    for _ in 0..EXECUTIONS {
        let worker = Arc::clone(&worker);
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest {
                context: Context {
                    identity: Some("identity".to_string()),
                    accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
                },
                handler_name: "handler".to_string(),
                args: vec![json!(10), json!(20)],
            };

            let result = worker.lock().await.execute(request).await.unwrap();

            assert!(result.output.is_number());
            let output = result.output.as_i64().unwrap();
            assert_eq!(output, 30);
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let start = Instant::now();
    join_all(handles).await;
    let duration = start.elapsed();
    println!("{} tasks completed in {:?}", EXECUTIONS, duration);

    Ok(())
}
