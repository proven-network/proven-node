use crate::file_system::StoredEntry;
use crate::{
    Error, ExecutionRequest, ExecutionResult, ModuleLoader, Result, RpcEndpoints, RuntimeOptions,
    Worker,
};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store, Store2, Store3};
use radix_common::network::NetworkDefinition;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{Duration, Instant, sleep};

type WorkerMap<AS, PS, NS, ASS, PSS, NSS, FSS, RNV> =
    HashMap<String, Vec<Worker<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>>>;
type SharedWorkerMap<AS, PS, NS, ASS, PSS, NSS, FSS, RNV> =
    Arc<Mutex<WorkerMap<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>>>;
type LastUsedMap = Arc<Mutex<HashMap<String, Instant>>>;

type SendChannel = oneshot::Sender<Result<ExecutionResult>>;

type QueueItem = (ModuleLoader, ExecutionRequest, SendChannel);
type QueueSender = mpsc::Sender<QueueItem>;
type QueueReceiver = mpsc::Receiver<QueueItem>;

/// Options for a new `Pool`.
pub struct PoolOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
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
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

    /// Store used for file-system virtualisation.
    pub file_system_store: FSS,

    /// Max pool workers.
    pub max_workers: u32,

    /// NFT-scoped SQL store.
    pub nft_sql_store: NSS,

    /// NFT-scoped KV store.
    pub nft_store: NS,

    /// Persona-scoped SQL store.
    pub personal_sql_store: PSS,

    /// Persona-scoped KV store.
    pub personal_store: PS,

    /// Network definition for Radix Network.
    pub radix_network_definition: NetworkDefinition,

    /// Verifier for checking NFT ownership on the Radix Network.
    pub radix_nft_verifier: RNV,

    /// RPC endpoints for the runtime.
    pub rpc_endpoints: RpcEndpoints,
}

/// A pool of workers that can execute tasks concurrently.
///
/// The `Pool` struct manages a set of workers that can execute tasks concurrently. It maintains
/// a queue of tasks and distributes them to available workers. If the number of workers exceeds
/// the maximum allowed, tasks are queued until a worker becomes available.
///
/// # Type Parameters
///
/// * `AS`: The type of the application store, which must implement the `Store2` trait.
/// * `PS`: The type of the personal store, which must implement the `Store3` trait.
/// * `NS`: The type of the NFT store, which must implement the `Store3` trait.
///
/// # Example
///
/// ```rust
/// use ed25519_dalek::{SigningKey, VerifyingKey};
/// use proven_code_package::CodePackage;
/// use proven_identity::{Identity, LedgerIdentity, RadixIdentityDetails, Session};
/// use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
/// use proven_runtime::{
///     Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, Pool, PoolOptions,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
/// use radix_common::network::NetworkDefinition;
/// use serde_json::json;
/// use tempfile::tempdir;
///
/// #[tokio::main]
/// async fn main() {
///     let pool = Pool::new(PoolOptions {
///         application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
///         application_store: MemoryStore2::new(),
///         file_system_store: MemoryStore::new(),
///         max_workers: 10,
///         nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///         nft_store: MemoryStore3::new(),
///         personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///         personal_store: MemoryStore3::new(),
///         radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
///         radix_network_definition: NetworkDefinition::stokenet(),
///         radix_nft_verifier: MockRadixNftVerifier::new(),
///     })
///     .await;
///
///     let code_package =
///         CodePackage::from_str("export const handler = (a, b) => a + b;").unwrap();
///
///     let request = ExecutionRequest::Rpc {
///         application_id: "application_id".to_string(),
///         args: vec![json!(10), json!(20)],
///         handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
///         session: Session::Identified {
///             identity: Identity {
///                 identity_id: "identity_id".to_string(),
///                 ledger_identities: vec![LedgerIdentity::Radix(RadixIdentityDetails {
///                     account_addresses: vec![
///                         "my_account_1".to_string(),
///                         "my_account_2".to_string(),
///                     ],
///                     dapp_definition_address: "dapp_definition_address".to_string(),
///                     expected_origin: "origin".to_string(),
///                     identity_address: "my_identity".to_string(),
///                 })],
///                 passkeys: vec![],
///             },
///             ledger_identity: LedgerIdentity::Radix(RadixIdentityDetails {
///                 account_addresses: vec!["my_account_1".to_string(), "my_account_2".to_string()],
///                 dapp_definition_address: "dapp_definition_address".to_string(),
///                 expected_origin: "origin".to_string(),
///                 identity_address: "my_identity".to_string(),
///             }),
///             origin: "origin".to_string(),
///             session_id: "session_id".to_string(),
///             signing_key: SigningKey::generate(&mut rand::thread_rng()),
///             verifying_key: VerifyingKey::from(&SigningKey::generate(&mut rand::thread_rng())),
///         },
///     };
///
///     pool.execute(ModuleLoader::new(code_package), request).await;
/// }
/// ```
pub struct Pool<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
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
    application_sql_store: ASS,
    application_store: AS,
    file_system_store: FSS,
    known_hashes: Arc<Mutex<HashMap<String, ModuleLoader>>>,
    last_killed: Arc<Mutex<Option<Instant>>>,
    last_used: LastUsedMap,
    max_workers: u32,
    nft_sql_store: NSS,
    nft_store: NS,
    overflow_processor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    overflow_queue: Arc<Mutex<VecDeque<QueueItem>>>,
    personal_sql_store: PSS,
    personal_store: PS,
    rpc_endpoints: RpcEndpoints,
    radix_network_definition: NetworkDefinition,
    radix_nft_verifier: RNV,
    queue_processor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    queue_sender: QueueSender,
    total_workers: AtomicU32,
    try_kill_interval: Duration,
    workers: SharedWorkerMap<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>,
}

impl<AS, PS, NS, ASS, PSS, NSS, FSS, RNV> Pool<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
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
    /// Creates a new `Pool`.
    pub async fn new(
        PoolOptions {
            application_sql_store,
            application_store,
            file_system_store,
            max_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_network_definition,
            radix_nft_verifier,
            rpc_endpoints,
        }: PoolOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>,
    ) -> Arc<Self> {
        rustyscript::init_platform(max_workers, true);

        let (queue_sender, queue_receiver) = mpsc::channel(max_workers as usize * 10);

        let pool = Arc::new(Self {
            application_sql_store,
            application_store,
            file_system_store,
            known_hashes: Arc::new(Mutex::new(HashMap::new())),
            last_killed: Arc::new(Mutex::new(Some(Instant::now()))),
            last_used: Arc::new(Mutex::new(HashMap::new())),
            max_workers,
            nft_sql_store,
            nft_store,
            overflow_processor: Arc::new(Mutex::new(None)),
            overflow_queue: Arc::new(Mutex::new(VecDeque::new())),
            personal_sql_store,
            personal_store,
            queue_processor: Arc::new(Mutex::new(None)),
            queue_sender,
            radix_network_definition,
            radix_nft_verifier,
            rpc_endpoints,
            total_workers: AtomicU32::new(0),
            try_kill_interval: Duration::from_millis(20),
            workers: Arc::new(Mutex::new(HashMap::new())),
        });

        Arc::clone(&pool)
            .start_queue_processor(queue_receiver)
            .await;
        Arc::clone(&pool).start_overflow_processor().await;

        pool
    }

    async fn start_queue_processor(self: Arc<Self>, mut queue_receiver: QueueReceiver) {
        let pool = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            'outer: while let Some((module_loader, request, sender)) = queue_receiver.recv().await {
                let code_package_hash = module_loader.code_package_hash();

                // --- Check for existing worker ---
                let worker_opt = {
                    let mut worker_map = pool.workers.lock().await;
                    if let Some(workers) = worker_map.get_mut(&code_package_hash) {
                        workers.pop()
                    } else {
                        None
                    }
                };

                if let Some(mut worker) = worker_opt {
                    // Found existing worker, spawn execution
                    let pool_clone = Arc::clone(&pool);
                    tokio::spawn(async move {
                        let result = worker.execute(request).await;

                        // Return worker
                        let mut worker_map = pool_clone.workers.lock().await;
                        worker_map
                            .entry(code_package_hash.clone())
                            .or_default()
                            .push(worker);
                        drop(worker_map);
                        pool_clone
                            .last_used
                            .lock()
                            .await
                            .insert(code_package_hash, Instant::now());

                        // Send result back (ignore error)
                        let _ = sender.send(result);
                    });
                    continue 'outer; // Continue processing queue immediately
                }

                // --- Try to create new worker ---
                if pool.total_workers.load(Ordering::SeqCst) < pool.max_workers
                    || pool.kill_idle_worker(&code_package_hash).await
                {
                    pool.total_workers.fetch_add(1, Ordering::SeqCst);

                    let worker_result =
                        Worker::<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>::new(RuntimeOptions {
                            application_sql_store: pool.application_sql_store.clone(),
                            application_store: pool.application_store.clone(),
                            file_system_store: pool.file_system_store.clone(),
                            module_loader: module_loader.clone(),
                            nft_sql_store: pool.nft_sql_store.clone(),
                            nft_store: pool.nft_store.clone(),
                            personal_sql_store: pool.personal_sql_store.clone(),
                            personal_store: pool.personal_store.clone(),
                            rpc_endpoints: pool.rpc_endpoints.clone(),
                            radix_network_definition: pool.radix_network_definition.clone(),
                            radix_nft_verifier: pool.radix_nft_verifier.clone(),
                        })
                        .await;

                    match worker_result {
                        Ok(mut worker) => {
                            // Created worker successfully, spawn execution
                            let pool_clone = Arc::clone(&pool);
                            tokio::spawn(async move {
                                let result = worker.execute(request).await;

                                if matches!(
                                    result,
                                    Err(Error::RuntimeError(rustyscript::Error::HeapExhausted))
                                ) {
                                    pool_clone.total_workers.fetch_sub(1, Ordering::SeqCst);
                                    // Don't return worker
                                } else {
                                    // Return worker
                                    let mut worker_map = pool_clone.workers.lock().await;
                                    worker_map
                                        .entry(code_package_hash.clone())
                                        .or_default()
                                        .push(worker);
                                    drop(worker_map);
                                    pool_clone
                                        .last_used
                                        .lock()
                                        .await
                                        .insert(code_package_hash.clone(), Instant::now());
                                }

                                // Insert into known_hashes (assuming creation means it's new or needed)
                                pool_clone
                                    .known_hashes
                                    .lock()
                                    .await
                                    .insert(code_package_hash, module_loader);

                                // Send result back (ignore error)
                                let _ = sender.send(result);
                            });
                            continue 'outer; // Continue processing queue immediately
                        }
                        Err(e) => {
                            // Handles HeapExhausted and others
                            pool.total_workers.fetch_sub(1, Ordering::SeqCst);
                            // Send error back immediately
                            let _ = sender.send(Err(e));
                            continue 'outer;
                        }
                    }
                } else {
                    // Could not find or create worker, queue request (pushes to overflow if needed)
                    pool.queue_request(module_loader, request, sender, true)
                        .await;
                }
            }
        });

        self.queue_processor.lock().await.replace(handle);
    }

    async fn start_overflow_processor(self: Arc<Self>) {
        let pool = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            let duration = Duration::from_millis(100);
            loop {
                sleep(duration).await;
                let items_to_requeue = {
                    let mut overflow_queue = pool.overflow_queue.lock().await;
                    let mut items = VecDeque::with_capacity(overflow_queue.len());
                    while let Some(item) = overflow_queue.pop_front() {
                        items.push_back(item);
                    }
                    items
                };
                for (runtime_options, request, tx) in items_to_requeue {
                    pool.queue_request(runtime_options, request, tx, true).await;
                }
            }
        });
        self.overflow_processor.lock().await.replace(handle);
    }

    /// Executes a request using the specified runtime options.
    ///
    /// # Arguments
    ///
    /// * `runtime_options` - The options to configure the runtime.
    /// * `request` - The execution request to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` containing the execution result or an error.
    ///
    /// # Errors
    ///
    /// This function will return an error if the execution fails.
    ///
    /// # Panics
    ///
    /// This function will panic if the receiver is dropped before the result is received.
    pub async fn execute(
        self: Arc<Self>,
        module_loader: ModuleLoader,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        let code_package_hash = module_loader.code_package_hash();
        let (sender, receiver) = oneshot::channel();

        // --- Check for existing worker ---
        let worker_opt = {
            let mut worker_map = self.workers.lock().await;
            if let Some(workers) = worker_map.get_mut(&code_package_hash) {
                workers.pop()
            } else {
                None
            }
        };

        if let Some(mut worker) = worker_opt {
            // Found existing worker, spawn execution
            let pool_clone = Arc::clone(&self);
            tokio::spawn(async move {
                let result = worker.execute(request).await;

                // Return worker
                let mut worker_map = pool_clone.workers.lock().await;
                worker_map
                    .entry(code_package_hash.clone())
                    .or_default()
                    .push(worker);
                drop(worker_map);
                pool_clone
                    .last_used
                    .lock()
                    .await
                    .insert(code_package_hash, Instant::now());

                // Send result back (ignore error)
                let _ = sender.send(result);
            });
        } else {
            // --- Try to create new worker ---
            if self.total_workers.load(Ordering::SeqCst) < self.max_workers
                || self.maybe_kill_idle_worker(&code_package_hash).await
            {
                self.total_workers.fetch_add(1, Ordering::SeqCst);

                let worker_result =
                    Worker::<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>::new(RuntimeOptions {
                        application_sql_store: self.application_sql_store.clone(),
                        application_store: self.application_store.clone(),
                        file_system_store: self.file_system_store.clone(),
                        module_loader: module_loader.clone(),
                        nft_sql_store: self.nft_sql_store.clone(),
                        nft_store: self.nft_store.clone(),
                        personal_sql_store: self.personal_sql_store.clone(),
                        personal_store: self.personal_store.clone(),
                        radix_network_definition: self.radix_network_definition.clone(),
                        radix_nft_verifier: self.radix_nft_verifier.clone(),
                        rpc_endpoints: self.rpc_endpoints.clone(),
                    })
                    .await;

                match worker_result {
                    Ok(mut worker) => {
                        // Created worker successfully, spawn execution
                        let pool_clone = Arc::clone(&self);
                        tokio::spawn(async move {
                            let result = worker.execute(request).await;

                            if matches!(
                                result,
                                Err(Error::RuntimeError(rustyscript::Error::HeapExhausted))
                            ) {
                                pool_clone.total_workers.fetch_sub(1, Ordering::SeqCst);
                                // Don't return worker
                            } else {
                                // Return worker
                                let mut worker_map = pool_clone.workers.lock().await;
                                worker_map
                                    .entry(code_package_hash.clone())
                                    .or_default()
                                    .push(worker);
                                drop(worker_map);
                                pool_clone
                                    .last_used
                                    .lock()
                                    .await
                                    .insert(code_package_hash.clone(), Instant::now());
                            }

                            // Insert into known_hashes
                            pool_clone
                                .known_hashes
                                .lock()
                                .await
                                .insert(code_package_hash, module_loader);

                            // Send result back (ignore error)
                            let _ = sender.send(result);
                        });
                    }
                    Err(e) => {
                        // Handles HeapExhausted and others
                        self.total_workers.fetch_sub(1, Ordering::SeqCst);
                        // Send error back immediately
                        let _ = sender.send(Err(e));
                    }
                }
            } else {
                // Could not find or create worker, queue request
                self.queue_request(module_loader, request, sender, false)
                    .await;
            }
        }

        // Await the result from the spawned task or the queued task
        receiver
            .await
            .unwrap_or_else(|_| Err(Error::ChannelCommunicationError))
    }

    /// Executes a request using precomputed hash options.
    ///
    /// # Arguments
    ///
    /// * `options_hash` - The precomputed hash of the runtime options.
    /// * `request` - The execution request to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` containing the execution result or an error.
    ///
    /// # Errors
    ///
    /// This function will return an error if the execution fails or if the hash is unknown.
    ///
    /// # Panics
    ///
    /// This function will panic if the receiver is dropped before the result is received.
    pub async fn execute_prehashed(
        self: Arc<Self>,
        code_package_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        let (sender, receiver) = oneshot::channel();

        // Need module loader first to check worker map or create worker
        let known_hashes_guard = self.known_hashes.lock().await;
        let module_loader_opt = known_hashes_guard.get(&code_package_hash).cloned();
        drop(known_hashes_guard);

        if module_loader_opt.is_none() {
            return Err(Error::HashUnknown);
        }
        let module_loader = module_loader_opt.unwrap();

        // --- Check for existing worker ---
        let worker_opt = {
            let mut worker_map = self.workers.lock().await;
            if let Some(workers) = worker_map.get_mut(&code_package_hash) {
                workers.pop()
            } else {
                None
            }
        };

        if let Some(mut worker) = worker_opt {
            // Found existing worker, spawn execution
            let pool_clone = Arc::clone(&self);
            tokio::spawn(async move {
                let result = worker.execute(request).await;

                // Return worker
                let mut worker_map = pool_clone.workers.lock().await;
                worker_map
                    .entry(code_package_hash.clone())
                    .or_default()
                    .push(worker);
                drop(worker_map);
                pool_clone
                    .last_used
                    .lock()
                    .await
                    .insert(code_package_hash, Instant::now());

                // Send result back (ignore error)
                let _ = sender.send(result);
            });
        } else {
            // --- Try to create new worker ---
            if self.total_workers.load(Ordering::SeqCst) < self.max_workers
                || self.maybe_kill_idle_worker(&code_package_hash).await
            {
                self.total_workers.fetch_add(1, Ordering::SeqCst);

                let worker_result =
                    Worker::<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>::new(RuntimeOptions {
                        application_sql_store: self.application_sql_store.clone(),
                        application_store: self.application_store.clone(),
                        file_system_store: self.file_system_store.clone(),
                        module_loader: module_loader.clone(),
                        nft_sql_store: self.nft_sql_store.clone(),
                        nft_store: self.nft_store.clone(),
                        personal_sql_store: self.personal_sql_store.clone(),
                        personal_store: self.personal_store.clone(),
                        radix_network_definition: self.radix_network_definition.clone(),
                        radix_nft_verifier: self.radix_nft_verifier.clone(),
                        rpc_endpoints: self.rpc_endpoints.clone(),
                    })
                    .await;

                match worker_result {
                    Ok(mut worker) => {
                        // Created worker successfully, spawn execution
                        let pool_clone = Arc::clone(&self);
                        tokio::spawn(async move {
                            let result = worker.execute(request).await;

                            if matches!(
                                result,
                                Err(Error::RuntimeError(rustyscript::Error::HeapExhausted))
                            ) {
                                pool_clone.total_workers.fetch_sub(1, Ordering::SeqCst);
                                // Don't return worker
                            } else {
                                // Return worker
                                let mut worker_map = pool_clone.workers.lock().await;
                                worker_map
                                    .entry(code_package_hash.clone())
                                    .or_default()
                                    .push(worker);
                                drop(worker_map);
                                pool_clone
                                    .last_used
                                    .lock()
                                    .await
                                    .insert(code_package_hash.clone(), Instant::now());
                            }

                            // No need to insert into known_hashes, it was already there

                            // Send result back (ignore error)
                            let _ = sender.send(result);
                        });
                    }
                    Err(e) => {
                        // Handles HeapExhausted and others
                        self.total_workers.fetch_sub(1, Ordering::SeqCst);
                        // Send error back immediately
                        let _ = sender.send(Err(e));
                    }
                }
            } else {
                // Could not find or create worker, queue request
                // Need to pass the original module_loader again
                self.queue_request(module_loader, request, sender, false)
                    .await;
            }
        }

        // Await the result from the spawned task or the queued task
        receiver
            .await
            .unwrap_or_else(|_| Err(Error::ChannelCommunicationError))
    }

    async fn queue_request(
        &self,
        module_loader: ModuleLoader,
        request: ExecutionRequest,
        tx: SendChannel,
        queue_front: bool,
    ) {
        match self.queue_sender.try_reserve() {
            Ok(permit) => {
                permit.send((module_loader, request, tx));
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                if queue_front {
                    self.overflow_queue
                        .lock()
                        .await
                        .push_front((module_loader, request, tx));
                } else {
                    self.overflow_queue
                        .lock()
                        .await
                        .push_back((module_loader, request, tx));
                }
            }
            Err(e) => {
                eprintln!("Failed to reserve slot in queue: {e:?}");
                // Ensure we send an error back if queueing fails
                let _ = tx.send(Err(Error::ChannelCommunicationError));
            }
        }
    }

    async fn maybe_kill_idle_worker(&self, needed_hash: &str) -> bool {
        let mut last_killed_guard = self.last_killed.lock().await;
        if let Some(last_killed) = last_killed_guard.as_ref() {
            if last_killed.elapsed() < self.try_kill_interval {
                drop(last_killed_guard);
                return false;
            }
        }
        *last_killed_guard = Some(Instant::now());
        drop(last_killed_guard);
        self.kill_idle_worker(needed_hash).await
    }

    async fn kill_idle_worker(&self, needed_hash: &str) -> bool {
        let last_used = self.last_used.lock().await.clone();

        // Get and sort LRU entries
        let mut last_used_entries: Vec<_> =
            last_used.iter().map(|(k, v)| (k.clone(), *v)).collect();
        last_used_entries.sort_by_key(|&(_, time)| time);
        drop(last_used); // Drop clone early

        let mut workers = self.workers.lock().await; // Lock workers map once

        for (module_key, _) in last_used_entries {
            // *** Check if this is the hash we currently need ***
            if module_key == needed_hash {
                continue; // Skip killing this worker type, check next LRU
            }

            if let Some(worker_list) = workers.get_mut(&module_key) {
                if worker_list.pop().is_some() {
                    let was_empty = worker_list.is_empty(); // Check before potential remove below
                    if was_empty {
                        workers.remove(&module_key);
                        // Also remove from last_used map (need to re-lock unfortunately)
                        self.last_used.lock().await.remove(&module_key);
                    }
                    // drop(workers); // Drop map lock before fetch_sub?
                    self.total_workers.fetch_sub(1, Ordering::SeqCst);
                    return true; // Found and killed a *different* idle worker
                }
                // If list was empty, remove it
                workers.remove(&module_key);
                self.last_used.lock().await.remove(&module_key);
            } else {
                // If not in workers map, remove stale last_used entry
                self.last_used.lock().await.remove(&module_key);
            }
        }
        false // No suitable idle worker found to kill
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
    use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
    use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
    use tempfile::tempdir;

    #[allow(clippy::type_complexity)]
    fn create_pool_options() -> PoolOptions<
        MemoryStore2,
        MemoryStore3,
        MemoryStore3,
        DirectSqlStore2,
        DirectSqlStore3,
        DirectSqlStore3,
        MemoryStore<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        MockRadixNftVerifier,
    > {
        PoolOptions {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::new(),
            max_workers: 10,
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            personal_store: MemoryStore3::new(),
            radix_network_definition: NetworkDefinition::stokenet(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
            rpc_endpoints: RpcEndpoints::external(),
        }
    }

    #[tokio::test]
    async fn test_pool_creation() {
        let pool = Pool::new(create_pool_options()).await;

        assert_eq!(pool.max_workers, 10);
        assert_eq!(pool.total_workers.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_execute() {
        let pool = Pool::new(create_pool_options()).await;

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        let result = pool
            .execute(
                ModuleLoader::from_test_code("test_runtime_execute"),
                request,
            )
            .await;
        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_execute_prehashed() {
        let pool = Pool::new(create_pool_options()).await;

        let module_loader = ModuleLoader::from_test_code("test_runtime_execute");

        let code_package_hash = module_loader.code_package_hash();

        pool.known_hashes
            .lock()
            .await
            .insert(code_package_hash.clone(), module_loader);

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        let result = pool.execute_prehashed(code_package_hash, request).await;
        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_queue_request() {
        let pool = Pool::new(create_pool_options()).await;

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        let (tx, rx) = oneshot::channel();

        pool.queue_request(
            ModuleLoader::from_test_code("test_runtime_execute"),
            request,
            tx,
            false,
        )
        .await;
        assert!(rx.await.is_ok());
    }

    #[tokio::test]
    async fn test_kill_idle_worker() {
        let pool = Pool::new(create_pool_options()).await;

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        let pool_clone = Arc::clone(&pool);
        let _ = pool_clone
            .execute(
                ModuleLoader::from_test_code("test_runtime_execute"),
                request,
            )
            .await;

        let killed = pool.kill_idle_worker("test_runtime_execute").await;
        assert!(killed);
    }
}
