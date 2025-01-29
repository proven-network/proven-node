use crate::file_system::StoredEntry;
use crate::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};

use std::marker::PhantomData;
use std::thread;

use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store, Store2, Store3};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// A worker that handles execution requests using a runtime. Can be run async in tokio.
///
/// # Type Parameters
/// - `AS`: Application Store type implementing `Store2`.
/// - `PS`: Personal Store type implementing `Store3`.
/// - `NS`: NFT Store type implementing `Store3`.
///
/// # Example
/// ```rust
/// use proven_code_package::CodePackage;
/// use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
/// use proven_runtime::{
///     Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, Runtime,
///     RuntimeOptions, Worker,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
/// use radix_common::network::NetworkDefinition;
/// use serde_json::json;
/// use tempfile::tempdir;
///
/// #[tokio::main]
/// async fn main() {
///     let code_package =
///         CodePackage::from_str("export const handler = (a, b) => a + b;").unwrap();
///
///     let mut worker = Worker::new(RuntimeOptions {
///         application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
///         application_store: MemoryStore2::new(),
///         file_system_store: MemoryStore::new(),
///         module_loader: ModuleLoader::new(code_package),
///         nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///         nft_store: MemoryStore3::new(),
///         personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///         personal_store: MemoryStore3::new(),
///         radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
///         radix_network_definition: NetworkDefinition::stokenet(),
///         radix_nft_verifier: MockRadixNftVerifier::new(),
///     })
///     .await
///     .expect("Failed to create worker");
///
///     worker
///         .execute(ExecutionRequest::Rpc {
///             accounts: vec![],
///             args: vec![json!(10), json!(20)],
///             dapp_definition_address: "dapp_definition_address".to_string(),
///             handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
///             identity: "my_identity".to_string(),
///         })
///         .await;
/// }
/// ```
#[allow(clippy::type_complexity)]
pub struct Worker<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
    RNV: RadixNftVerifier,
{
    sender: mpsc::Sender<WorkerRequest>,
    _marker: PhantomData<(AS, PS, NS, ASS, PSS, NSS, FSS, RNV)>,
}

type WorkerRequest = (
    ExecutionRequest,
    oneshot::Sender<Result<ExecutionResult, Error>>,
);

impl<AS, PS, NS, ASS, PSS, NSS, FSS, RNV> Worker<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
    RNV: RadixNftVerifier,
{
    /// Creates a new `Worker` with the given runtime options.
    ///
    /// # Errors
    /// This function will return an error if the runtime initialization fails.
    ///
    /// # Panics
    /// This function will panic if it fails to send or receive messages through the channels.
    pub async fn new(
        runtime_options: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>,
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
        })
    }

    /// Executes the given request and awaits the result.
    ///
    /// # Parameters
    /// - `request`: The execution request to execute.
    ///
    /// # Returns
    /// A result containing the execution result.
    ///
    /// # Errors
    /// This function will return an error if it fails to send or receive messages through the channels.
    ///
    /// # Panics
    /// This function will panic if it fails to unwrap the response.
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

    use crate::HandlerSpecifier;

    use serde_json::json;

    #[tokio::test]
    async fn test_worker_execute_in_tokio() {
        let runtime_options = RuntimeOptions::for_test_code("test_runtime_execute");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }
}
