use crate::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};

use std::marker::PhantomData;
use std::thread;

use proven_store::Store1;
use proven_store::Store2;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Worker<AS: Store1, PS: Store2, NS: Store2> {
    sender: mpsc::Sender<WorkerRequest>,
    _marker: PhantomData<AS>,
    _marker2: PhantomData<PS>,
    _marker3: PhantomData<NS>,
}

type WorkerRequest = (
    ExecutionRequest,
    oneshot::Sender<Result<ExecutionResult, Error>>,
);

/// A worker that handles execution requests using a runtime. Can be run async in tokio.
///
/// # Type Parameters
/// - `AS`: Application Store type implementing `Store1`.
/// - `PS`: Personal Store type implementing `Store2`.
/// - `NS`: NFT Store type implementing `Store2`.
///
/// # Example
/// ```rust
/// use proven_runtime::{
///     Context, Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions, Worker,
/// };
/// use proven_store_memory::MemoryStore;
/// use serde_json::json;
///
/// #[tokio::main]
/// async fn main() {
///     let mut worker = Worker::new(RuntimeOptions {
///         application_store: MemoryStore::new(),
///         max_heap_mbs: 10,
///         module: "export const test = (a, b) => a + b;".to_string(),
///         nft_store: MemoryStore::new(),
///         personal_store: MemoryStore::new(),
///         timeout_millis: 1000,
///     })
///     .await
///     .expect("Failed to create worker");
///
///     worker
///         .execute(ExecutionRequest {
///             context: Context {
///                 dapp_definition_address: "dapp_definition_address".to_string(),
///                 identity: None,
///                 accounts: None,
///             },
///             handler_name: "test".to_string(),
///             args: vec![json!(10), json!(20)],
///         })
///         .await;
/// }
/// ```
impl<AS: Store1, PS: Store2, NS: Store2> Worker<AS, PS, NS> {
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
    pub async fn new(runtime_options: RuntimeOptions<AS, PS, NS>) -> Result<Self, Error> {
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
    use crate::Context;

    use proven_store_memory::MemoryStore;
    use serde_json::json;

    fn create_runtime_options(
        script: &str,
    ) -> RuntimeOptions<MemoryStore, MemoryStore, MemoryStore> {
        RuntimeOptions {
            application_store: MemoryStore::new(),
            max_heap_mbs: 10,
            module: script.to_string(),
            nft_store: MemoryStore::new(),
            personal_store: MemoryStore::new(),
            timeout_millis: 1000,
        }
    }

    #[tokio::test]
    async fn test_worker_execute_in_tokio() {
        let runtime_options = create_runtime_options("export const test = (a, b) => a + b;");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            context: Context {
                dapp_definition_address: "dapp_definition_address".to_string(),
                identity: None,
                accounts: None,
            },
            handler_name: "test".to_string(),
            args: vec![json!(10), json!(20)],
        };
        let result = worker.execute(request).await;

        assert!(result.is_ok());
    }
}
