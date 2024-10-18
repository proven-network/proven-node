use proven_runtime::{Context, ExecutionRequest, Pool};

use std::sync::Arc;

use rustyscript::Error;
use serde_json::json;
use tokio::time::{sleep, Duration, Instant};

#[tokio::main(worker_threads = 8)]
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

    let pool = Pool::new(50).await;
    let mut handles = vec![];

    for batch in 0..10 {
        for i in 0..100 {
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

                let start = Instant::now();
                let result = pool.execute(user_module, request).await;
                let duration = start.elapsed();

                match result {
                    Ok(result) => {
                        assert!(result.output.is_number());
                        let output = result.output.as_i64().unwrap();
                        assert_eq!(output, 30);
                        println!(
                            "[{}] (Thread ID: {:?}) Result: {:?} executed in {:?}, total time: {:?}",
                            batch * 100 + i + 1,
                            std::thread::current().id(),
                            output,
                            result.duration,
                            duration
                        );
                    }
                    Err(e) => eprintln!("Error in result: {:?}", e),
                }
            });
            handles.push(handle);
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Wait for all tasks to complete
    futures::future::try_join_all(handles).await.unwrap();

    Ok(())
}
