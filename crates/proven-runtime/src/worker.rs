use crate::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};

use std::marker::PhantomData;
use std::thread;

use proven_store::Store1;
use proven_store::Store2;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Worker<AS: Store1, PS: Store2, NS: Store2> {
    sender: mpsc::Sender<WorkerRequest>,
    _marker: PhantomData<AS>,
    _marker2: PhantomData<PS>,
    _marker3: PhantomData<NS>,
}

pub enum WorkerRequest {
    Execute {
        request: ExecutionRequest,
        sender: oneshot::Sender<Result<ExecutionResult, Error>>,
    },
}

impl<AS: Store1, PS: Store2, NS: Store2> Worker<AS, PS, NS> {
    pub fn new(
        runtime_options: RuntimeOptions,
        application_store: AS,
        personal_store: PS,
        nft_store: NS,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(1);

        thread::spawn(move || {
            let mut runtime = Runtime::new(
                runtime_options,
                application_store,
                personal_store,
                nft_store,
            )
            .unwrap();

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
            _marker2: PhantomData,
            _marker3: PhantomData,
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
