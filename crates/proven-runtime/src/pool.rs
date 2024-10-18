use crate::{ExecutionRequest, ExecutionResult, Worker};

use rustyscript::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};

type WorkerMap = HashMap<String, Vec<Worker>>;
type SharedWorkerMap = Arc<Mutex<WorkerMap>>;
type LastUsedMap = Arc<Mutex<HashMap<String, Instant>>>;

pub struct Pool {
    workers: SharedWorkerMap,
    max_workers: usize,
    total_workers: Arc<Mutex<usize>>,
    last_used: LastUsedMap,
}

impl Pool {
    pub async fn new(max_workers: usize) -> Arc<Self> {
        Arc::new(Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            max_workers,
            total_workers: Arc::new(Mutex::new(0)),
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

                    // Re-add the worker to the map
                    let mut worker_map = self.workers.lock().await;
                    worker_map.entry(module.clone()).or_default().push(worker);
                    drop(worker_map);

                    // Update the last used time for the module
                    let mut last_used = self.last_used.lock().await;
                    last_used.insert(module.clone(), Instant::now());
                    drop(last_used);

                    return result;
                } else {
                    drop(worker_map);
                }
            } else {
                drop(worker_map);
            }

            let mut total_workers = self.total_workers.lock().await;

            if *total_workers < self.max_workers || self.remove_idle_worker().await {
                *total_workers += 1;
                drop(total_workers);

                let mut worker = Worker::new(module.to_string());
                let result = worker.execute(request).await;

                self.workers
                    .lock()
                    .await
                    .entry(module.to_string())
                    .or_default()
                    .push(worker);

                // Update the last used time for the module
                let mut last_used = self.last_used.lock().await;
                last_used.insert(module.to_string(), Instant::now());
                drop(last_used);

                return result;
            } else {
                drop(total_workers);
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
            // Remove a worker from the pool
            let mut workers = self.workers.lock().await;
            if let Some(worker_list) = workers.get_mut(&module) {
                if worker_list.pop().is_some() {
                    // Remove the module from the pool if there are no workers left
                    if worker_list.is_empty() {
                        workers.remove(&module);
                    }
                    drop(workers);
                    // Update the last used time for the module
                    let mut last_used = self.last_used.lock().await;
                    last_used.insert(module.clone(), Instant::now());
                    drop(last_used);

                    let mut total_workers = self.total_workers.lock().await;
                    *total_workers -= 1;
                    drop(total_workers);

                    return true;
                } else {
                    drop(workers);
                }
            }
        }

        false
    }
}
