use crate::{Error, ExecutionRequest, ExecutionResult, RuntimeOptions, Worker};

use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep, Duration, Instant};

type WorkerMap = HashMap<String, Vec<Worker>>;
type SharedWorkerMap = Arc<Mutex<WorkerMap>>;
type LastUsedMap = Arc<Mutex<HashMap<String, Instant>>>;

type SendChannel = oneshot::Sender<Result<ExecutionResult, Error>>;

type QueueItem = (RuntimeOptions, ExecutionRequest, SendChannel);
type QueueSender = mpsc::Sender<QueueItem>;
type QueueReceiver = mpsc::Receiver<QueueItem>;

pub struct Pool {
    workers: SharedWorkerMap,
    known_hashes: Arc<Mutex<HashMap<String, RuntimeOptions>>>,
    max_workers: usize,
    total_workers: AtomicUsize,
    last_used: LastUsedMap,
    queue_sender: QueueSender,
    overflow_queue: Arc<Mutex<VecDeque<QueueItem>>>,
    queue_processor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    overflow_processor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    try_kill_interval: Duration,
    last_killed: Arc<Mutex<Option<Instant>>>,
}

impl Pool {
    pub async fn new(max_workers: usize) -> Arc<Self> {
        let (queue_sender, queue_receiver) = mpsc::channel(max_workers * 10);

        let pool = Arc::new(Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            known_hashes: Arc::new(Mutex::new(HashMap::new())),
            max_workers,
            total_workers: AtomicUsize::new(0),
            last_used: Arc::new(Mutex::new(HashMap::new())),
            queue_sender,
            overflow_queue: Arc::new(Mutex::new(VecDeque::new())),
            queue_processor: Arc::new(Mutex::new(None)),
            overflow_processor: Arc::new(Mutex::new(None)),
            try_kill_interval: Duration::from_millis(20),
            last_killed: Arc::new(Mutex::new(Some(Instant::now()))),
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
                let options_hash = hash_options(&runtime_options);
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
                            .insert(options_hash, Instant::now());

                        sender.send(result).unwrap();
                        continue 'outer;
                    } else {
                        drop(worker_map);
                    }
                } else {
                    drop(worker_map);
                }

                if self.total_workers.load(Ordering::SeqCst) < self.max_workers
                    || self.kill_idle_worker().await
                {
                    self.total_workers.fetch_add(1, Ordering::SeqCst);

                    let mut worker = Worker::new(runtime_options.clone());
                    let result = worker.execute(request).await;

                    if let Err(Error::RustyScript(rustyscript::Error::HeapExhausted)) = result {
                        // Remove the worker from the pool if the heap is exhausted (can't recover)
                        self.total_workers.fetch_sub(1, Ordering::SeqCst);
                    } else {
                        self.workers
                            .lock()
                            .await
                            .entry(options_hash.clone())
                            .or_default()
                            .push(worker);
                    }

                    self.last_used
                        .lock()
                        .await
                        .insert(options_hash.clone(), Instant::now());

                    self.known_hashes
                        .lock()
                        .await
                        .insert(options_hash.clone(), runtime_options);

                    sender.send(result).unwrap();
                    continue 'outer;
                } else {
                    self.queue_request(runtime_options, request, sender, true)
                        .await;
                }
            }
        });

        pool.queue_processor.lock().await.replace(handle);
    }

    async fn start_overflow_processor(self: Arc<Self>) {
        let pool = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            let duration = Duration::from_millis(100);
            loop {
                sleep(duration).await;
                let mut overflow_queue = self.overflow_queue.lock().await;
                while let Some((runtime_options, request, tx)) = overflow_queue.pop_front() {
                    drop(overflow_queue);
                    self.queue_request(runtime_options, request, tx, true).await;
                    overflow_queue = self.overflow_queue.lock().await;
                }
            }
        });
        pool.overflow_processor.lock().await.replace(handle);
    }

    pub async fn execute(
        self: Arc<Self>,
        runtime_options: RuntimeOptions,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult, Error> {
        let options_hash = hash_options(&runtime_options);
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
            } else {
                drop(worker_map);
            }
        } else {
            drop(worker_map);
        }

        if self.total_workers.load(Ordering::SeqCst) < self.max_workers
            || self.maybe_kill_idle_worker().await
        {
            self.total_workers.fetch_add(1, Ordering::SeqCst);

            let mut worker = Worker::new(runtime_options.clone());
            let result = worker.execute(request).await;

            if let Err(Error::RustyScript(rustyscript::Error::HeapExhausted)) = result {
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

    pub async fn execute_prehashed(
        self: Arc<Self>,
        options_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult, Error> {
        match self.known_hashes.lock().await.get(&options_hash) {
            Some(runtime_options) => {
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
                    } else {
                        drop(worker_map);
                    }
                } else {
                    drop(worker_map);
                }

                if self.total_workers.load(Ordering::SeqCst) < self.max_workers
                    || self.maybe_kill_idle_worker().await
                {
                    self.total_workers.fetch_add(1, Ordering::SeqCst);

                    let mut worker = Worker::new(runtime_options.clone());
                    let result = worker.execute(request).await;

                    if let Err(Error::RustyScript(rustyscript::Error::HeapExhausted)) = result {
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
            }
            None => Err(Error::HashUnknown),
        }
    }

    async fn queue_request(
        &self,
        runtime_options: RuntimeOptions,
        request: ExecutionRequest,
        tx: SendChannel,
        queue_front: bool,
    ) {
        match self.queue_sender.try_reserve() {
            Ok(permit) => {
                permit.send((runtime_options, request, tx));
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
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
                eprintln!("Failed to reserve slot in queue: {:?}", e);
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
            } else {
                *last_killed_guard = Some(Instant::now());
                drop(last_killed_guard);
            }
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
                } else {
                    workers.remove(&module);
                    drop(workers);
                }
            }
        }

        false
    }
}

pub fn hash_options(options: &RuntimeOptions) -> String {
    let mut hasher = Sha256::new();

    // Concatenate module, timeout, and max_heap_size
    let mut data = options.module.clone();
    writeln!(&mut data, "{:?}", options.timeout_millis).unwrap();
    write!(&mut data, "{:?}", options.max_heap_mbs).unwrap();

    // Hash the concatenated string
    hasher.update(data);
    let result = hasher.finalize();

    // Convert the hash result to a hexadecimal string
    let hash_string = format!("{:x}", result);

    hash_string
}
