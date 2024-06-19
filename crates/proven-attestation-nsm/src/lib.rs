mod error;

pub use error::{Error, Result};

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_nitro_enclaves_nsm_api::api::{Request, Response};
use aws_nitro_enclaves_nsm_api::driver::{nsm_exit, nsm_init, nsm_process_request};
use proven_attestation::{AttestationParams, Attestor};
use serde_bytes::ByteBuf;
use tracing::error;

#[derive(Debug)]
pub struct NsmAttestor {
    fd: Arc<Mutex<i32>>,
}

impl NsmAttestor {
    pub fn new() -> Self {
        Self {
            fd: Arc::new(Mutex::new(nsm_init())),
        }
    }

    fn process_request(&self, req: Request) -> Result<Response> {
        let fd = self.fd.lock().unwrap();
        match nsm_process_request(*fd, req) {
            Response::Error(err) => {
                error!("Request error: {:?}", err);
                Err(Error::RequestError(err))
            }
            resp => Ok(resp),
        }
    }
}

#[async_trait]
impl Attestor for NsmAttestor {
    type AE = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Vec<u8>> {
        let req = Request::Attestation {
            nonce: params.nonce.map(ByteBuf::from),
            public_key: params.public_key.map(ByteBuf::from),
            user_data: params.user_data.map(ByteBuf::from),
        };

        match self.process_request(req)? {
            Response::Attestation { document } => Ok(document),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn secure_random(&self) -> Result<Vec<u8>> {
        match self.process_request(Request::GetRandom {})? {
            Response::GetRandom { random } => Ok(random),
            _ => Err(Error::UnexpectedResponse),
        }
    }
}

impl Clone for NsmAttestor {
    fn clone(&self) -> Self {
        Self {
            fd: Arc::clone(&self.fd),
        }
    }
}

impl Default for NsmAttestor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for NsmAttestor {
    fn drop(&mut self) {
        let fd = self.fd.lock().unwrap();
        nsm_exit(*fd);
    }
}
