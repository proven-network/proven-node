use crate::{ExecutionRequest, ExecutionResult, Worker};

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rustyscript::Error;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep, Duration, Instant};

type WorkerMap = HashMap<String, Vec<Worker>>;
type SharedWorkerMap = Arc<Mutex<WorkerMap>>;
type LastUsedMap = Arc<Mutex<HashMap<String, Instant>>>;

type SendChannel = oneshot::Sender<Result<ExecutionResult, Error>>;

type QueueItem = (
    String,           // module
    ExecutionRequest, // request
    SendChannel,      // tx
);
type QueueSender = mpsc::Sender<QueueItem>;
type QueueReceiver = mpsc::Receiver<QueueItem>;

pub struct Pool {
    workers: SharedWorkerMap,
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
            'outer: while let Some((module, request, sender)) = queue_receiver.recv().await {
                let mut worker_map = self.workers.lock().await;

                if let Some(workers) = worker_map.get_mut(&module) {
                    if let Some(mut worker) = workers.pop() {
                        drop(worker_map); // Unlock the worker map

                        let result = worker.execute(request).await;

                        let mut worker_map = self.workers.lock().await;
                        worker_map.entry(module.clone()).or_default().push(worker);

                        drop(worker_map);

                        self.last_used
                            .lock()
                            .await
                            .insert(module.to_string(), Instant::now());

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

                    let mut worker = Worker::new(module.to_string());
                    let result = worker.execute(request).await;

                    if let Err(rustyscript::Error::HeapExhausted) = result {
                        // Remove the worker from the pool if the heap is exhausted (can't recover)
                        self.total_workers.fetch_sub(1, Ordering::SeqCst);
                    } else {
                        self.workers
                            .lock()
                            .await
                            .entry(module.to_string())
                            .or_default()
                            .push(worker);
                    }

                    self.last_used
                        .lock()
                        .await
                        .insert(module.to_string(), Instant::now());

                    sender.send(result).unwrap();
                    continue 'outer;
                } else {
                    self.queue_request(module, request, sender, true).await;
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
                while let Some((module, request, tx)) = overflow_queue.pop_front() {
                    drop(overflow_queue);
                    self.queue_request(module, request, tx, true).await;
                    overflow_queue = self.overflow_queue.lock().await;
                }
            }
        });
        pool.overflow_processor.lock().await.replace(handle);
    }

    pub async fn execute(
        self: Arc<Self>,
        module: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult, Error> {
        let (sender, reciever) = oneshot::channel();

        let mut worker_map = self.workers.lock().await;

        if let Some(workers) = worker_map.get_mut(&module) {
            if let Some(mut worker) = workers.pop() {
                drop(worker_map); // Unlock the worker map

                let result = worker.execute(request).await;

                let mut worker_map = self.workers.lock().await;
                worker_map.entry(module.clone()).or_default().push(worker);

                drop(worker_map);

                self.last_used
                    .lock()
                    .await
                    .insert(module.to_string(), Instant::now());

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

            let mut worker = Worker::new(module.to_string());
            let result = worker.execute(request).await;

            if let Err(rustyscript::Error::HeapExhausted) = result {
                // Remove the worker from the pool if the heap is exhausted (can't recover)
                self.total_workers.fetch_sub(1, Ordering::SeqCst);
            } else {
                self.workers
                    .lock()
                    .await
                    .entry(module.to_string())
                    .or_default()
                    .push(worker);
            }

            self.last_used
                .lock()
                .await
                .insert(module.to_string(), Instant::now());

            result
        } else {
            self.queue_request(module, request, sender, false).await;
            reciever.await.unwrap()
        }
    }

    async fn queue_request(
        &self,
        module: String,
        request: ExecutionRequest,
        tx: SendChannel,
        queue_front: bool,
    ) {
        match self.queue_sender.try_reserve() {
            Ok(permit) => {
                permit.send((module, request, tx));
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if queue_front {
                    self.overflow_queue
                        .lock()
                        .await
                        .push_front((module, request, tx));
                } else {
                    self.overflow_queue
                        .lock()
                        .await
                        .push_back((module, request, tx));
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
