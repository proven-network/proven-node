use crate::{ExecutionRequest, ExecutionResult, Worker};

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rustyscript::Error;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};

type WorkerMap = HashMap<String, Vec<Worker>>;
type SharedWorkerMap = Arc<Mutex<WorkerMap>>;
type LastUsedMap = Arc<Mutex<HashMap<String, Instant>>>;

pub struct Pool {
    workers: SharedWorkerMap,
    max_workers: usize,
    total_workers: AtomicUsize,
    last_used: LastUsedMap,
}

impl Pool {
    pub async fn new(max_workers: usize) -> Arc<Self> {
        Arc::new(Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            max_workers,
            total_workers: AtomicUsize::new(0),
            last_used: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn execute(
        self: Arc<Self>,
        module: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult, Error> {
        let retry_delay = Duration::from_millis(10);

        loop {
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
                || self.remove_idle_worker().await
            {
                self.total_workers.fetch_add(1, Ordering::SeqCst);

                let mut worker = Worker::new(module.to_string());
                let result = worker.execute(request).await;

                self.workers
                    .lock()
                    .await
                    .entry(module.to_string())
                    .or_default()
                    .push(worker);

                self.last_used
                    .lock()
                    .await
                    .insert(module.to_string(), Instant::now());

                return result;
            } else {
                sleep(retry_delay).await;
                tokio::task::yield_now().await;
            }
        }
    }

    async fn remove_idle_worker(&self) -> bool {
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
