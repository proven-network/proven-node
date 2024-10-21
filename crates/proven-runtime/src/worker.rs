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
/// # Methods
/// - `new`: Creates a new worker with the given runtime options and stores.
/// - `execute`: Sends an execution request to the worker and awaits the result.
///
/// # Example
/// ```rust
/// use proven_runtime::{Context, Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions, Worker};
/// use proven_store_memory::MemoryStore;
/// use serde_json::json;
///
/// #[tokio::main]
/// async fn main() {
///     let runtime_options = RuntimeOptions {
///         max_heap_mbs: 10,
///         module: "export const test = (a, b) => a + b;".to_string(),
///         timeout_millis: 1000,
///     };
///     let application_store = MemoryStore::new();
///     let personal_store = MemoryStore::new();
///     let nft_store = MemoryStore::new();
///     let request = ExecutionRequest {
///         context: Context {
///             dapp_definition_address: "dapp_definition_address".to_string(),
///             identity: None,
///             accounts: None,
///         },
///         handler_name: "test".to_string(),
///         args: vec![json!(10), json!(20)],
///     };
///     let mut worker = Worker::new(runtime_options, application_store, personal_store, nft_store);
///     let result = worker.execute(request).await;
/// }
/// ```
impl<AS: Store1, PS: Store2, NS: Store2> Worker<AS, PS, NS> {
    /// Creates a new worker with the given runtime options and stores.
    ///
    /// # Type Parameters
    /// - `AS`: Application Store type implementing `Store1`.
    /// - `PS`: Personal Store type implementing `Store2`.
    /// - `NS`: NFT Store type implementing `Store2`.
    ///
    /// # Parameters
    /// - `runtime_options`: The runtime options to use.
    /// - `application_store`: The application store to use.
    /// - `personal_store`: The personal store to use.
    /// - `nft_store`: The NFT store to use.
    ///
    /// # Returns
    /// The new worker.
    pub fn new(
        runtime_options: RuntimeOptions,
        application_store: AS,
        personal_store: PS,
        nft_store: NS,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<WorkerRequest>(1);

        thread::spawn(move || {
            let mut runtime = Runtime::new(
                runtime_options,
                application_store,
                personal_store,
                nft_store,
            )
            .unwrap();

            while let Some((request, responder)) = receiver.blocking_recv() {
                let result = runtime.execute(request);
                responder.send(result).unwrap();
            }
        });

        Self {
            sender,
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
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

    #[tokio::test]
    async fn test_worker_execute_in_tokio() {
        let runtime_options = RuntimeOptions {
            max_heap_mbs: 10,
            module: "export const test = (a, b) => a + b;".to_string(),
            timeout_millis: 1000,
        };
        let application_store = MemoryStore::new();
        let personal_store = MemoryStore::new();
        let nft_store = MemoryStore::new();

        let mut worker = Worker::new(
            runtime_options,
            application_store,
            personal_store,
            nft_store,
        );

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
