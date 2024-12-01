use crate::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};

use std::marker::PhantomData;
use std::thread;

use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Worker<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    sender: mpsc::Sender<WorkerRequest>,
    _marker: PhantomData<AS>,
    _marker2: PhantomData<PS>,
    _marker3: PhantomData<NS>,
    _marker4: PhantomData<ASS>,
    _marker5: PhantomData<PSS>,
    _marker6: PhantomData<NSS>,
}

type WorkerRequest = (
    ExecutionRequest,
    oneshot::Sender<Result<ExecutionResult, Error>>,
);

/// A worker that handles execution requests using a runtime. Can be run async in tokio.
///
/// # Type Parameters
/// - `AS`: Application Store type implementing `Store2`.
/// - `PS`: Personal Store type implementing `Store3`.
/// - `NS`: NFT Store type implementing `Store3`.
///
/// # Example
/// ```rust
/// use proven_runtime::{
///     Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions, Worker,
/// };
/// use proven_sql_direct::DirectSqlStore;
/// use proven_store_memory::MemoryStore;
/// use radix_common::network::NetworkDefinition;
/// use serde_json::json;
/// use tempfile::tempdir;
///
/// #[tokio::main]
/// async fn main() {
///     let mut worker = Worker::new(RuntimeOptions {
///         application_sql_store: DirectSqlStore::new(tempdir().unwrap().into_path()),
///         application_store: MemoryStore::new(),
///         handler_name: Some("handler".to_string()),
///         module: "export const handler = (a, b) => a + b;".to_string(),
///         nft_sql_store: DirectSqlStore::new(tempdir().unwrap().into_path()),
///         nft_store: MemoryStore::new(),
///         personal_sql_store: DirectSqlStore::new(tempdir().unwrap().into_path()),
///         personal_store: MemoryStore::new(),
///         radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
///         radix_network_definition: NetworkDefinition::stokenet(),
///     })
///     .await
///     .expect("Failed to create worker");
///
///     worker
///         .execute(ExecutionRequest {
///             accounts: None,
///             args: vec![json!(10), json!(20)],
///             dapp_definition_address: "dapp_definition_address".to_string(),
///             identity: None,
///         })
///         .await;
/// }
/// ```
impl<AS, PS, NS, ASS, PSS, NSS> Worker<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    /// Creates a new worker with the given runtime options and stores.
    ///
    /// # Parameters
    /// - `runtime_options`: The runtime options to use.
    /// - `application_store`: The application store to use.
    /// - `personal_store`: The personal store to use.
    /// - `nft_store`: The NFT store to use.
    ///
    /// # Returns
    /// The created worker.
    pub async fn new(
        runtime_options: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS>,
    ) -> Result<Self, Error> {
        let (sender, mut receiver) = mpsc::channel::<WorkerRequest>(1);

        let (error_sender, error_reciever) = oneshot::channel();

        thread::spawn(move || match Runtime::new(runtime_options) {
            Ok(mut runtime) => {
                error_sender.send(None).unwrap();

                while let Some((request, responder)) = receiver.blocking_recv() {
                    let result = runtime.execute(request);
                    responder.send(result).unwrap();
                }
            }
            Err(e) => {
                error_sender.send(Some(e)).unwrap();
            }
        });

        if let Some(e) = error_reciever.await.unwrap() {
            return Err(e);
        }

        Ok(Self {
            sender,
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
            _marker4: PhantomData,
            _marker5: PhantomData,
            _marker6: PhantomData,
        })
    }

    /// Executes the given request and awaits the result.
    ///
    /// # Parameters
    /// - `request`: The execution request to execute.
    ///
    /// # Returns
    /// An a result containing the execution result.
    pub async fn execute(&mut self, request: ExecutionRequest) -> Result<ExecutionResult, Error> {
        let (sender, reciever) = oneshot::channel();
        let worker_request = (request, sender);

        if (self.sender.send(worker_request).await).is_err() {
            eprintln!("Failed to send request to worker");
        }

        let response = reciever.await;

        if response.is_err() {
            eprintln!("Failed to receive response from worker");
        }

        response.unwrap()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use proven_sql_direct::DirectSqlStore;
    use proven_store_memory::MemoryStore;
    use radix_common::network::NetworkDefinition;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_runtime_options(
        script: &str,
        handler_name: Option<String>,
    ) -> RuntimeOptions<
        MemoryStore,
        MemoryStore,
        MemoryStore,
        DirectSqlStore,
        DirectSqlStore,
        DirectSqlStore,
    > {
        let mut temp_application_sql = tempdir().unwrap().into_path();
        temp_application_sql.push("application.db");
        let mut temp_nft_sql = tempdir().unwrap().into_path();
        temp_nft_sql.push("nft.db");
        let mut temp_personal_sql = tempdir().unwrap().into_path();
        temp_personal_sql.push("personal.db");

        RuntimeOptions {
            application_sql_store: DirectSqlStore::new(temp_application_sql),
            application_store: MemoryStore::new(),
            handler_name,
            module: script.to_string(),
            nft_sql_store: DirectSqlStore::new(temp_nft_sql),
            nft_store: MemoryStore::new(),
            personal_sql_store: DirectSqlStore::new(temp_personal_sql),
            personal_store: MemoryStore::new(),
            radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            radix_network_definition: NetworkDefinition::stokenet(),
        }
    }

    #[tokio::test]
    async fn test_worker_execute_in_tokio() {
        let runtime_options = create_runtime_options(
            "export const test = (a, b) => a + b;",
            Some("test".to_string()),
        );
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        };
        let result = worker.execute(request).await;

        assert!(result.is_ok());
    }
}
