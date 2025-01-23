use crate::{Error, ExecutionRequest, ExecutionResult, Result, RuntimeOptions, Worker};

use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use deno_core::url::Url;
use deno_graph::ModuleGraph;
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use radix_common::network::NetworkDefinition;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep, Duration, Instant};

type WorkerMap<AS, PS, NS, ASS, PSS, NSS, RNV> =
    HashMap<String, Vec<Worker<AS, PS, NS, ASS, PSS, NSS, RNV>>>;
type SharedWorkerMap<AS, PS, NS, ASS, PSS, NSS, RNV> =
    Arc<Mutex<WorkerMap<AS, PS, NS, ASS, PSS, NSS, RNV>>>;
type LastUsedMap = Arc<Mutex<HashMap<String, Instant>>>;

type SendChannel = oneshot::Sender<Result<ExecutionResult>>;

type QueueItem = (PoolRuntimeOptions, ExecutionRequest, SendChannel);
type QueueSender = mpsc::Sender<QueueItem>;
type QueueReceiver = mpsc::Receiver<QueueItem>;

/// Options for a new `Pool`.
pub struct PoolOptions<AS, PS, NS, ASS, PSS, NSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    RNV: RadixNftVerifier,
{
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

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

    /// Origin for Radix Network gateway.
    pub radix_gateway_origin: String,

    /// Network definition for Radix Network.
    pub radix_network_definition: NetworkDefinition,

    /// Verifier for checking NFT ownership on the Radix Network.
    pub radix_nft_verifier: RNV,
}

/// Runtime options to instantiate a runtime in the pool.
#[derive(Clone)]
pub struct PoolRuntimeOptions {
    /// Name of the handler function.
    pub handler_name: Option<String>,

    /// The graph of modules to load.
    pub module_graph: ModuleGraph,

    /// The root module to look for the handler in.
    pub module_root: Url,
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
/// use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
/// use proven_runtime::{
///     create_module_graph, Error, ExecutionRequest, ExecutionResult, Pool, PoolOptions,
///     PoolRuntimeOptions,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore2, MemoryStore3};
/// use radix_common::network::NetworkDefinition;
/// use serde_json::json;
/// use tempfile::tempdir;
///
/// #[tokio::main]
/// async fn main() {
///     let pool = Pool::new(PoolOptions {
///         application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
///         application_store: MemoryStore2::new(),
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
///     let (module_root, module_graph) =
///         create_module_graph("export const handler = (a, b) => a + b;");
///
///     let runtime_options = PoolRuntimeOptions {
///         handler_name: Some("handler".to_string()),
///         module_graph,
///         module_root,
///     };
///
///     let request = ExecutionRequest::Rpc {
///         accounts: vec![],
///         args: vec![json!(10), json!(20)],
///         dapp_definition_address: "dapp_definition_address".to_string(),
///         identity: "my_identity".to_string(),
///     };
///
///     pool.execute(runtime_options, request).await;
/// }
/// ```
pub struct Pool<AS, PS, NS, ASS, PSS, NSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    RNV: RadixNftVerifier,
{
    application_sql_store: ASS,
    application_store: AS,
    known_hashes: Arc<Mutex<HashMap<String, PoolRuntimeOptions>>>,
    last_killed: Arc<Mutex<Option<Instant>>>,
    last_used: LastUsedMap,
    max_workers: u32,
    nft_sql_store: NSS,
    nft_store: NS,
    overflow_processor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    overflow_queue: Arc<Mutex<VecDeque<QueueItem>>>,
    personal_sql_store: PSS,
    personal_store: PS,
    radix_gateway_origin: String,
    radix_network_definition: NetworkDefinition,
    radix_nft_verifier: RNV,
    queue_processor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    queue_sender: QueueSender,
    total_workers: AtomicU32,
    try_kill_interval: Duration,
    workers: SharedWorkerMap<AS, PS, NS, ASS, PSS, NSS, RNV>,
}

impl<AS, PS, NS, ASS, PSS, NSS, RNV> Pool<AS, PS, NS, ASS, PSS, NSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    RNV: RadixNftVerifier,
{
    /// Creates a new `Pool`.
    pub async fn new(
        PoolOptions {
            application_sql_store,
            application_store,
            max_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin,
            radix_network_definition,
            radix_nft_verifier,
        }: PoolOptions<AS, PS, NS, ASS, PSS, NSS, RNV>,
    ) -> Arc<Self> {
        rustyscript::init_platform(max_workers, true);

        let (queue_sender, queue_receiver) = mpsc::channel(max_workers as usize * 10);

        let pool = Arc::new(Self {
            application_sql_store,
            application_store,
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
            radix_gateway_origin,
            radix_network_definition,
            radix_nft_verifier,
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
            'outer: while let Some((runtime_options, request, sender)) = queue_receiver.recv().await
            {
                let options_hash = hash_options(&runtime_options).unwrap();
                let mut worker_map = pool.workers.lock().await;

                if let Some(workers) = worker_map.get_mut(&options_hash) {
                    if let Some(mut worker) = workers.pop() {
                        drop(worker_map); // Unlock the worker map

                        let result = worker.execute(request).await;

                        let mut worker_map = pool.workers.lock().await;
                        worker_map
                            .entry(options_hash.clone())
                            .or_default()
                            .push(worker);

                        drop(worker_map);

                        pool.last_used
                            .lock()
                            .await
                            .insert(options_hash, Instant::now());

                        sender.send(result).unwrap();
                        continue 'outer;
                    }
                }
                drop(worker_map);

                if pool.total_workers.load(Ordering::SeqCst) < pool.max_workers
                    || pool.kill_idle_worker().await
                {
                    pool.total_workers.fetch_add(1, Ordering::SeqCst);

                    match Worker::<AS, PS, NS, ASS, PSS, NSS, RNV>::new(RuntimeOptions {
                        application_sql_store: pool.application_sql_store.clone(),
                        application_store: pool.application_store.clone(),
                        handler_name: runtime_options.handler_name.clone(),
                        module_graph: runtime_options.module_graph.clone(),
                        module_root: runtime_options.module_root.clone(),
                        nft_sql_store: pool.nft_sql_store.clone(),
                        nft_store: pool.nft_store.clone(),
                        personal_sql_store: pool.personal_sql_store.clone(),
                        personal_store: pool.personal_store.clone(),
                        radix_gateway_origin: pool.radix_gateway_origin.clone(),
                        radix_network_definition: pool.radix_network_definition.clone(),
                        radix_nft_verifier: pool.radix_nft_verifier.clone(),
                    })
                    .await
                    {
                        Ok(mut worker) => {
                            let result = worker.execute(request).await;

                            if matches!(
                                result,
                                Err(Error::RustyScript(rustyscript::Error::HeapExhausted))
                            ) {
                                // Remove the worker from the pool if the heap is exhausted (can't recover)
                                pool.total_workers.fetch_sub(1, Ordering::SeqCst);
                            } else {
                                pool.workers
                                    .lock()
                                    .await
                                    .entry(options_hash.clone())
                                    .or_default()
                                    .push(worker);
                            }

                            pool.last_used
                                .lock()
                                .await
                                .insert(options_hash.clone(), Instant::now());

                            pool.known_hashes
                                .lock()
                                .await
                                .insert(options_hash.clone(), runtime_options);

                            sender.send(result).unwrap();
                            continue 'outer;
                        }
                        Err(Error::RustyScript(rustyscript::Error::HeapExhausted)) => {
                            // Remove the worker from the pool if the heap is exhausted (can't recover)
                            pool.total_workers.fetch_sub(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            sender.send(Err(e)).unwrap();
                            continue 'outer;
                        }
                    }
                } else {
                    pool.queue_request(runtime_options, request, sender, true)
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
                let mut overflow_queue = self.overflow_queue.lock().await;
                while let Some((runtime_options, request, tx)) = overflow_queue.pop_front() {
                    self.queue_request(runtime_options, request, tx, true).await;
                }
            }
        });
        pool.overflow_processor.lock().await.replace(handle);
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
        runtime_options: PoolRuntimeOptions,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        let options_hash = hash_options(&runtime_options)?;
        let (sender, reciever) = oneshot::channel();

        let mut worker_map = self.workers.lock().await;

        if let Some(workers) = worker_map.get_mut(&options_hash) {
            if let Some(mut worker) = workers.pop() {
                drop(worker_map); // Unlock the worker map

                let result = worker.execute(request).await;

                let mut worker_map = self.workers.lock().await;
                worker_map
                    .entry(options_hash.clone())
                    .or_default()
                    .push(worker);

                drop(worker_map);

                self.last_used
                    .lock()
                    .await
                    .insert(options_hash.to_string(), Instant::now());

                return result;
            }
        }
        drop(worker_map);

        if self.total_workers.load(Ordering::SeqCst) < self.max_workers
            || self.maybe_kill_idle_worker().await
        {
            self.total_workers.fetch_add(1, Ordering::SeqCst);

            let mut worker = Worker::<AS, PS, NS, ASS, PSS, NSS, RNV>::new(RuntimeOptions {
                application_sql_store: self.application_sql_store.clone(),
                application_store: self.application_store.clone(),
                handler_name: runtime_options.handler_name.clone(),
                module_graph: runtime_options.module_graph.clone(),
                module_root: runtime_options.module_root.clone(),
                nft_sql_store: self.nft_sql_store.clone(),
                nft_store: self.nft_store.clone(),
                personal_sql_store: self.personal_sql_store.clone(),
                personal_store: self.personal_store.clone(),
                radix_gateway_origin: self.radix_gateway_origin.clone(),
                radix_network_definition: self.radix_network_definition.clone(),
                radix_nft_verifier: self.radix_nft_verifier.clone(),
            })
            .await?;
            let result = worker.execute(request).await;

            if matches!(
                result,
                Err(Error::RustyScript(rustyscript::Error::HeapExhausted))
            ) {
                // Remove the worker from the pool if the heap is exhausted (can't recover)
                self.total_workers.fetch_sub(1, Ordering::SeqCst);
            } else {
                self.workers
                    .lock()
                    .await
                    .entry(options_hash.to_string())
                    .or_default()
                    .push(worker);
            }

            self.last_used
                .lock()
                .await
                .insert(options_hash.to_string(), Instant::now());

            self.known_hashes
                .lock()
                .await
                .insert(options_hash.clone(), runtime_options);

            result
        } else {
            self.queue_request(runtime_options, request, sender, false)
                .await;
            reciever.await.unwrap()
        }
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
        options_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        let known_hashes = self.known_hashes.lock().await;
        if let Some(runtime_options) = known_hashes.get(&options_hash) {
            let runtime_options = runtime_options.clone();
            let (sender, reciever) = oneshot::channel();

            let mut worker_map = self.workers.lock().await;

            if let Some(workers) = worker_map.get_mut(&options_hash) {
                if let Some(mut worker) = workers.pop() {
                    drop(worker_map); // Unlock the worker map

                    let result = worker.execute(request).await;

                    let mut worker_map = self.workers.lock().await;
                    worker_map
                        .entry(options_hash.clone())
                        .or_default()
                        .push(worker);

                    drop(worker_map);

                    self.last_used
                        .lock()
                        .await
                        .insert(options_hash.to_string(), Instant::now());

                    return result;
                }
            }
            drop(worker_map);

            if self.total_workers.load(Ordering::SeqCst) < self.max_workers
                || self.maybe_kill_idle_worker().await
            {
                self.total_workers.fetch_add(1, Ordering::SeqCst);

                let mut worker = Worker::<AS, PS, NS, ASS, PSS, NSS, RNV>::new(RuntimeOptions {
                    application_sql_store: self.application_sql_store.clone(),
                    application_store: self.application_store.clone(),
                    handler_name: runtime_options.handler_name.clone(),
                    module_graph: runtime_options.module_graph.clone(),
                    module_root: runtime_options.module_root.clone(),
                    nft_sql_store: self.nft_sql_store.clone(),
                    nft_store: self.nft_store.clone(),
                    personal_sql_store: self.personal_sql_store.clone(),
                    personal_store: self.personal_store.clone(),
                    radix_gateway_origin: self.radix_gateway_origin.clone(),
                    radix_network_definition: self.radix_network_definition.clone(),
                    radix_nft_verifier: self.radix_nft_verifier.clone(),
                })
                .await?;
                let result = worker.execute(request).await;

                if matches!(
                    result,
                    Err(Error::RustyScript(rustyscript::Error::HeapExhausted))
                ) {
                    // Remove the worker from the pool if the heap is exhausted (can't recover)
                    self.total_workers.fetch_sub(1, Ordering::SeqCst);
                } else {
                    self.workers
                        .lock()
                        .await
                        .entry(options_hash.to_string())
                        .or_default()
                        .push(worker);
                }

                self.last_used
                    .lock()
                    .await
                    .insert(options_hash.to_string(), Instant::now());

                result
            } else {
                self.queue_request(runtime_options, request, sender, false)
                    .await;
                reciever.await.unwrap()
            }
        } else {
            Err(Error::HashUnknown)
        }
    }

    async fn queue_request(
        &self,
        runtime_options: PoolRuntimeOptions,
        request: ExecutionRequest,
        tx: SendChannel,
        queue_front: bool,
    ) {
        match self.queue_sender.try_reserve() {
            Ok(permit) => {
                permit.send((runtime_options, request, tx));
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                if queue_front {
                    self.overflow_queue
                        .lock()
                        .await
                        .push_front((runtime_options, request, tx));
                } else {
                    self.overflow_queue
                        .lock()
                        .await
                        .push_back((runtime_options, request, tx));
                }
            }
            Err(e) => {
                eprintln!("Failed to reserve slot in queue: {e:?}");
            }
        }
    }

    async fn maybe_kill_idle_worker(&self) -> bool {
        // Only allow killing workers every `try_kill_interval` duration
        let mut last_killed_guard = self.last_killed.lock().await;
        if let Some(last_killed) = last_killed_guard.as_ref() {
            if last_killed.elapsed() < self.try_kill_interval {
                drop(last_killed_guard);
                return false;
            }
            *last_killed_guard = Some(Instant::now());
            drop(last_killed_guard);
        }

        self.kill_idle_worker().await
    }

    async fn kill_idle_worker(&self) -> bool {
        let last_used = self.last_used.lock().await.clone();

        // Find the module type that was used the least recently
        let oldest_module = last_used
            .iter()
            .min_by_key(|entry| entry.1)
            .map(|(module, _)| module.clone());

        if let Some(module) = oldest_module {
            let mut workers = self.workers.lock().await;
            if let Some(worker_list) = workers.get_mut(&module) {
                if worker_list.pop().is_some() {
                    // Remove the module from the pool if there are no workers left
                    if worker_list.is_empty() {
                        workers.remove(&module);
                        self.last_used.lock().await.remove(&module);
                    }

                    drop(workers);

                    self.total_workers.fetch_sub(1, Ordering::SeqCst);

                    return true;
                }
                workers.remove(&module);
                drop(workers);
            }
        }

        false
    }
}

/// Computes a hash for the given runtime options.
///
/// # Arguments
///
/// * `options` - The runtime options to hash.
///
/// # Returns
///
/// A `Result` containing the hash string or an error.
///
/// # Errors
///
/// This function will return an error if the options cannot be hashed.
pub fn hash_options(options: &PoolRuntimeOptions) -> Result<String> {
    let mut hasher = Sha256::new();

    // Concatenate module and handler_name - newline separated
    // TODO: Need to go back to hashing content. Quick fix for graph update.
    let mut data = format!("{}\n", options.module_root.as_str());
    write!(
        &mut data,
        "{}",
        options
            .handler_name
            .clone()
            .unwrap_or_else(|| "<DEFAULT>".to_string())
    )?;

    // Hash the concatenated string
    hasher.update(data);
    let result = hasher.finalize();

    // Convert the hash result to a hexadecimal string
    let hash_string = format!("{result:x}");

    Ok(hash_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::create_test_module_graph;

    use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
    use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
    use proven_store_memory::{MemoryStore2, MemoryStore3};
    use serde_json::json;
    use tempfile::tempdir;

    fn create_pool_options() -> PoolOptions<
        MemoryStore2,
        MemoryStore3,
        MemoryStore3,
        DirectSqlStore2,
        DirectSqlStore3,
        DirectSqlStore3,
        MockRadixNftVerifier,
    > {
        PoolOptions {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            max_workers: 10,
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            personal_store: MemoryStore3::new(),
            radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            radix_network_definition: NetworkDefinition::stokenet(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
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

        let (module_root, module_graph) = create_test_module_graph("test_runtime_execute", "test");

        let runtime_options = PoolRuntimeOptions {
            handler_name: Some("test".to_string()),
            module_graph,
            module_root,
        };

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = pool.execute(runtime_options, request).await;
        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_execute_prehashed() {
        let pool = Pool::new(create_pool_options()).await;

        let (module_root, module_graph) = create_test_module_graph("test_runtime_execute", "test");

        let runtime_options = PoolRuntimeOptions {
            handler_name: Some("test".to_string()),
            module_graph,
            module_root,
        };

        let options_hash = hash_options(&runtime_options).unwrap();
        pool.known_hashes
            .lock()
            .await
            .insert(options_hash.clone(), runtime_options.clone());

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = pool.execute_prehashed(options_hash, request).await;
        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_queue_request() {
        let pool = Pool::new(create_pool_options()).await;

        let (module_root, module_graph) = create_test_module_graph("test_runtime_execute", "test");

        let runtime_options = PoolRuntimeOptions {
            handler_name: Some("test".to_string()),
            module_graph,
            module_root,
        };

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let (tx, rx) = oneshot::channel();

        pool.queue_request(runtime_options, request, tx, false)
            .await;
        assert!(rx.await.is_ok());
    }

    #[tokio::test]
    async fn test_kill_idle_worker() {
        let pool = Pool::new(create_pool_options()).await;

        let (module_root, module_graph) = create_test_module_graph("test_runtime_execute", "test");

        let runtime_options = PoolRuntimeOptions {
            handler_name: Some("test".to_string()),
            module_graph,
            module_root,
        };

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let pool_clone = Arc::clone(&pool);
        let _ = pool_clone.execute(runtime_options.clone(), request).await;

        let killed = pool.kill_idle_worker().await;
        assert!(killed);
    }
}
