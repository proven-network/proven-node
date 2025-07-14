#![allow(clippy::result_large_err)]

use proven_runtime::{
    ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RpcEndpoints,
    RuntimeOptions, Worker,
};

use std::sync::Arc;

use futures::future::join_all;
use proven_code_package::CodePackage;
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
use rustyscript::Error;
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

static EXECUTIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Error> {
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
        .await
        .unwrap(),
    );

    let worker = Arc::new(Mutex::new(
        Worker::new(RuntimeOptions {
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
        })
        .await
        .unwrap(),
    ));
    let mut handles = vec![];
    let durations = Arc::new(Mutex::new(vec![]));

    for _ in 0..EXECUTIONS {
        let worker = Arc::clone(&worker);
        let durations = Arc::clone(&durations);
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest::Rpc {
                application_id: Uuid::max(),
                args: vec![json!(10), json!(20)],
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
            };

            let start = Instant::now();
            let result = worker.lock().await.execute(request).await.unwrap();
            let duration = start.elapsed();
            durations.lock().await.push(duration);

            assert!(matches!(result, ExecutionResult::Ok { .. }));
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let start = Instant::now();
    join_all(handles).await;
    let duration = start.elapsed();
    println!("{EXECUTIONS} tasks completed in {duration:?}");

    let durations = durations.lock().await;
    let mut durations_vec: Vec<_> = durations.iter().cloned().collect();
    durations_vec.sort();

    let min_duration = durations_vec.first().unwrap();
    let max_duration = durations_vec.last().unwrap();
    let average_duration = durations_vec.iter().sum::<std::time::Duration>() / EXECUTIONS as u32;
    let median_duration = if EXECUTIONS.is_multiple_of(2) {
        (durations_vec[EXECUTIONS / 2 - 1] + durations_vec[EXECUTIONS / 2]) / 2
    } else {
        durations_vec[EXECUTIONS / 2]
    };

    println!("Min execution time: {min_duration:?}");
    println!("Max execution time: {max_duration:?}");
    println!("Average execution time: {average_duration:?}");
    println!("Median execution time: {median_duration:?}");

    Ok(())
}
