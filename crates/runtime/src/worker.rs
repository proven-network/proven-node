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
/// use ed25519_dalek::{SigningKey, VerifyingKey};
/// use proven_code_package::CodePackage;
/// use proven_identity::{Identity, LedgerIdentity, RadixIdentityDetails, Session};
/// use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
/// use proven_runtime::{
///     Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RpcEndpoints,
///     Runtime, RuntimeOptions, Worker,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
/// use serde_json::json;
/// use tempfile::tempdir;
/// use uuid::Uuid;
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
///         radix_nft_verifier: MockRadixNftVerifier::new(),
///         rpc_endpoints: RpcEndpoints::external(),
///     })
///     .await
///     .expect("Failed to create worker");
///
///     worker
///         .execute(ExecutionRequest::Rpc {
///             application_id: "application_id".to_string(),
///             args: vec![json!(10), json!(20)],
///             handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
///             session: Session::Identified {
///                 identity: Identity {
///                     identity_id: "identity_id".to_string(),
///                     ledger_identities: vec![LedgerIdentity::Radix(RadixIdentityDetails {
///                         account_addresses: vec![
///                             "my_account_1".to_string(),
///                             "my_account_2".to_string(),
///                         ],
///                         dapp_definition_address: "dapp_definition_address".to_string(),
///                         expected_origin: "origin".to_string(),
///                         identity_address: "my_identity".to_string(),
///                     })],
///                     passkeys: vec![],
///                 },
///                 ledger_identity: LedgerIdentity::Radix(RadixIdentityDetails {
///                     account_addresses: vec![
///                         "my_account_1".to_string(),
///                         "my_account_2".to_string(),
///                     ],
///                     dapp_definition_address: "dapp_definition_address".to_string(),
///                     expected_origin: "origin".to_string(),
///                     identity_address: "my_identity".to_string(),
///                 }),
///                 origin: "origin".to_string(),
///                 session_id: Uuid::new_v4(),
///                 signing_key: SigningKey::generate(&mut rand::thread_rng()),
///                 verifying_key: VerifyingKey::from(&SigningKey::generate(
///                     &mut rand::thread_rng(),
///                 )),
///             },
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

    #[tokio::test]
    async fn test_worker_execute_in_tokio() {
        let runtime_options = RuntimeOptions::for_test_code("test_runtime_execute");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);
        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }
}
