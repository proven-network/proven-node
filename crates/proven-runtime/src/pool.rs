use crate::{ExecutionRequest, ExecutionResult, Worker};

use rustyscript::Error;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep, Duration, Instant};

type WorkerMap = HashMap<String, Vec<Arc<Mutex<Worker>>>>;
type SharedWorkerMap = Arc<Mutex<WorkerMap>>;

type SendChannel = oneshot::Sender<Result<ExecutionResult, Error>>;
type RecvChannel = oneshot::Receiver<Result<ExecutionResult, Error>>;

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
    total_workers: Arc<Mutex<usize>>,
    queue_sender: QueueSender,
    overflow_queue: Arc<Mutex<VecDeque<QueueItem>>>,
}

impl Pool {
    pub fn new(max_workers: usize) -> Arc<Self> {
        let (queue_sender, queue_receiver) = mpsc::channel(max_workers * 10);

        let pool = Arc::new(Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            max_workers,
            total_workers: Arc::new(Mutex::new(0)),
            queue_sender,
            overflow_queue: Arc::new(Mutex::new(VecDeque::new())),
        });

        Arc::clone(&pool).start_queue_processor(queue_receiver);
        Arc::clone(&pool).start_overflow_processor();

        pool
    }

    fn start_queue_processor(self: Arc<Self>, mut queue_receiver: QueueReceiver) {
        tokio::spawn(async move {
            'outer: while let Some((module, request, tx)) = queue_receiver.recv().await {
                let worker_map = self.workers.lock().await;
                let existing_workers = worker_map.get(&module);
                if let Some(workers) = existing_workers {
                    for worker in workers.iter() {
                        if worker.try_lock().is_ok() {
                            let result = worker.lock().await.execute(request).await;
                            let _ = tx.send(result);
                            continue 'outer;
                        }
                    }
                }
                // Release worker_map lock
                drop(worker_map);

                let mut total_workers = self.total_workers.lock().await;

                if *total_workers < self.max_workers || self.remove_idle_worker().await {
                    *total_workers += 1;
                    drop(total_workers);

                    let worker = Worker::new(module.clone());
                    let result = worker.lock().await.execute(request).await;
                    self.workers
                        .lock()
                        .await
                        .entry(module.to_string())
                        .or_default()
                        .push(worker);
                    let _ = tx.send(result);
                } else {
                    drop(total_workers);
                    self.queue_request(module.to_string(), request, tx, true)
                        .await;
                }
            }
        });
    }

    fn start_overflow_processor(self: Arc<Self>) {
        tokio::spawn(async move {
            let duration = Duration::from_millis(100);
            loop {
                sleep(duration).await;
                let mut overflow_queue = self.overflow_queue.lock().await;
                while let Some((module, request, tx)) = overflow_queue.pop_front() {
                    self.queue_request(module, request, tx, true).await;
                }
            }
        });
    }

    pub async fn execute(
        self: Arc<Self>,
        module: String,
        request: ExecutionRequest,
    ) -> RecvChannel {
        let (tx, rx) = oneshot::channel();

        let worker_map = self.workers.lock().await;
        let existing_workers = worker_map.get(&module);
        if let Some(workers) = existing_workers {
            for worker in workers.iter() {
                if worker.try_lock().is_ok() {
                    let result = worker.lock().await.execute(request).await;
                    let _ = tx.send(result);
                    return rx;
                }
            }
        }
        // Release worker_map lock
        drop(worker_map);

        let mut total_workers = self.total_workers.lock().await;

        if *total_workers < self.max_workers || self.remove_idle_worker().await {
            *total_workers += 1;
            drop(total_workers);

            let worker = Worker::new(module.to_string());
            let result = worker.lock().await.execute(request).await;
            self.workers
                .lock()
                .await
                .entry(module.to_string())
                .or_default()
                .push(worker);

            let _ = tx.send(result);
        } else {
            drop(total_workers);
            self.queue_request(module.to_string(), request, tx, false)
                .await;
        }

        rx
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

    async fn remove_idle_worker(&self) -> bool {
        let mut workers = self.workers.lock().await;

        let oldest_worker = {
            let mut current_oldest: Option<(String, usize, Instant)> = None;
            for (module, workers) in workers.iter() {
                for (index, worker) in workers.iter().enumerate() {
                    let last_used = worker.lock().await.last_used();
                    if current_oldest.is_none() || last_used < current_oldest.as_ref().unwrap().2 {
                        current_oldest.replace((module.clone(), index, last_used));
                    }
                }
            }

            current_oldest
        };

        if let Some((module, index, _)) = oldest_worker {
            // Remove the worker from the pool
            workers.get_mut(&module).unwrap().remove(index);

            // Remove the module from the pool if there are no workers left
            if workers.get(&module).unwrap().is_empty() {
                workers.remove(&module);
            }

            let mut workers = self.total_workers.lock().await;
            *workers -= 1;

            true
        } else {
            false
        }
    }
}
