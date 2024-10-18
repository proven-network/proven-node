use crate::{ExecutionRequest, ExecutionResult, Runtime};

use std::thread;

use rustyscript::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Worker {
    sender: mpsc::Sender<WorkerRequest>,
}

pub enum WorkerRequest {
    Execute {
        request: ExecutionRequest,
        sender: oneshot::Sender<Result<ExecutionResult, Error>>,
    },
}

impl Worker {
    pub fn new(module: String) -> Self {
        let (sender, mut receiver) = mpsc::channel(1);

        thread::spawn(move || {
            let mut runtime = Runtime::new(&module).unwrap();

            while let Some(request) = receiver.blocking_recv() {
                match request {
                    WorkerRequest::Execute {
                        request,
                        sender: response,
                    } => {
                        let result = runtime.execute(request);
                        let _ = response.send(result);
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn execute(&mut self, request: ExecutionRequest) -> Result<ExecutionResult, Error> {
        let (sender, reciever) = oneshot::channel();
        let worker_request = WorkerRequest::Execute { request, sender };

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
