#![allow(clippy::result_large_err)]

use proven_runtime::{
    Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, Pool, PoolOptions,
    RpcEndpoints,
};

use std::sync::Arc;

use proven_code_package::CodePackage;
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

static EXECUTIONS: usize = 100;
static NUM_WORKERS: u32 = 40;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = Pool::new(PoolOptions {
        application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
        application_store: MemoryStore2::new(),
        file_system_store: MemoryStore::new(),
        max_workers: NUM_WORKERS,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
        personal_store: MemoryStore3::new(),
        radix_nft_verifier: MockRadixNftVerifier::new(),
        rpc_endpoints: RpcEndpoints::external(),
    })
    .await;

    let mut handles = vec![];
    let durations = Arc::new(Mutex::new(vec![]));

    let base_script = r#"
        import { run } from "@proven-network/handler";
        import { getCurrentAccounts, getCurrentIdentity } from "proven:session";

        export const handler = run(async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        });
    "#;

    for i in 0..EXECUTIONS {
        // Add the invocation number to every 2nd script to make it unique
        // This tests a mix of unique and duplicate scripts
        let module_loader = if i % 2 == 0 {
            ModuleLoader::new(
                CodePackage::from_str(&format!("// Invocation: {}\n{}", i, base_script))
                    .await
                    .unwrap(),
            )
        } else {
            ModuleLoader::new(CodePackage::from_str(base_script).await.unwrap())
        };

        let pool = Arc::clone(&pool);
        let durations = Arc::clone(&durations);
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest::Rpc {
                application_id: Uuid::max(),
                args: vec![json!(10), json!(20)],
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
            };

            match pool.execute(module_loader, request).await {
                Ok(ExecutionResult::Ok { duration, .. }) => {
                    durations.lock().await.push(duration);
                }
                _ => {
                    panic!("Execution failed");
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let start = Instant::now();
    futures::future::join_all(handles).await;
    let duration = start.elapsed();
    println!("{} tasks completed in {:?}", EXECUTIONS, duration);

    let durations = durations.lock().await;
    let mut durations_vec: Vec<_> = durations.iter().cloned().collect();
    durations_vec.sort();

    let min_duration = durations_vec.first().unwrap();
    let max_duration = durations_vec.last().unwrap();
    let average_duration = durations_vec.iter().sum::<std::time::Duration>() / EXECUTIONS as u32;
    let median_duration = if EXECUTIONS % 2 == 0 {
        (durations_vec[EXECUTIONS / 2 - 1] + durations_vec[EXECUTIONS / 2]) / 2
    } else {
        durations_vec[EXECUTIONS / 2]
    };
    let executions_per_second = EXECUTIONS as f64 / duration.as_secs_f64();
    println!("Min execution time: {:?}", min_duration);
    println!("Max execution time: {:?}", max_duration);
    println!("Average execution time: {:?}", average_duration);
    println!("Median execution time: {:?}", median_duration);
    println!(
        "Executions per second: {}",
        (executions_per_second * 100.0).round() / 100.0
    );

    Ok(())
}
