use crate::{ExecutionRequest, ExecutionResult, Runtime};

use std::sync::Arc;
use std::thread;

use rustyscript::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub struct Worker {
    sender: mpsc::Sender<WorkerRequest>,
    last_used: Instant,
}

pub enum WorkerRequest {
    Execute {
        request: ExecutionRequest,
        response: oneshot::Sender<Result<ExecutionResult, Error>>,
    },
}

impl Worker {
    pub fn new(module: String) -> Arc<Mutex<Self>> {
        let (sender, mut receiver) = mpsc::channel(1);

        thread::spawn(move || {
            let mut runtime = Runtime::new(&module).unwrap();

            while let Some(request) = receiver.blocking_recv() {
                match request {
                    WorkerRequest::Execute { request, response } => {
                        let result = runtime.execute(request);
                        let _ = response.send(result);
                    }
                }
            }
        });

        Arc::new(Mutex::new(Self {
            sender,
            last_used: Instant::now(),
        }))
    }

    pub async fn execute(&mut self, request: ExecutionRequest) -> Result<ExecutionResult, Error> {
        self.last_used = Instant::now();
        let (response_tx, response_rx) = oneshot::channel();
        let request = WorkerRequest::Execute {
            request,
            response: response_tx,
        };

        if (self.sender.send(request).await).is_err() {
            eprintln!("Failed to send request to worker");
        }

        let response = response_rx.await;

        if response.is_err() {
            eprintln!("Failed to receive response from worker");
        }

        response.unwrap()
    }

    pub fn last_used(&self) -> Instant {
        self.last_used
    }
}
