mod error;

pub use error::{Error, Result};

use async_trait::async_trait;
use aws_nitro_enclaves_nsm_api::api::{Request, Response};
use proven_attestation::{AttestationParams, Attestor};
use serde_bytes::ByteBuf;

pub struct NsmAttestor {
    fd: i32,
}

impl NsmAttestor {
    pub fn new() -> Self {
        Self {
            fd: aws_nitro_enclaves_nsm_api::driver::nsm_init(),
        }
    }

    fn process_request(&self, req: Request) -> Result<Response> {
        match aws_nitro_enclaves_nsm_api::driver::nsm_process_request(self.fd, req) {
            Response::Error(err) => Err(Error::RequestError(err)),
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
            user_data: params.user_data.map(ByteBuf::from),
            public_key: params.public_key.map(ByteBuf::from),
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

impl Default for NsmAttestor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for NsmAttestor {
    fn drop(&mut self) {
        aws_nitro_enclaves_nsm_api::driver::nsm_exit(self.fd);
    }
}
