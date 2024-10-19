use crate::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};

use std::marker::PhantomData;
use std::thread;

use proven_store::Store1;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Worker<AS: Store1> {
    sender: mpsc::Sender<WorkerRequest>,
    _marker: PhantomData<AS>,
}

pub enum WorkerRequest {
    Execute {
        request: ExecutionRequest,
        sender: oneshot::Sender<Result<ExecutionResult, Error>>,
    },
}

impl<AS: Store1> Worker<AS> {
    pub fn new(runtime_options: RuntimeOptions, application_store: AS) -> Self {
        let (sender, mut receiver) = mpsc::channel(1);

        thread::spawn(move || {
            let mut runtime = Runtime::new(runtime_options, application_store).unwrap();

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

        Self {
            sender,
            _marker: PhantomData,
        }
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
