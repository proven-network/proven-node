use crate::{Error, Result};

use aws_nitro_enclaves_nsm_api::api::{Request, Response};
use aws_nitro_enclaves_nsm_api::driver::{nsm_exit, nsm_init, nsm_process_request};
use proven_attestation::AttestationParams;
use serde_bytes::ByteBuf;

#[derive(Debug)]
pub struct Nsm {
    fd: i32,
}

impl Nsm {
    pub fn new() -> Self {
        Self { fd: nsm_init() }
    }

    pub fn get_random(&self) -> Result<Vec<u8>> {
        match self.process_request(Request::GetRandom {})? {
            Response::GetRandom { random } => Ok(random),

            _ => Err(Error::UnexpectedResponse),
        }
    }

    pub fn attestation(&self, params: &AttestationParams) -> Result<Vec<u8>> {
        let req = Request::Attestation {
            nonce: params.nonce.cloned().map(ByteBuf::from),
            user_data: params.user_data.cloned().map(ByteBuf::from),
            public_key: params.public_key.cloned().map(ByteBuf::from),
        };

        match self.process_request(req)? {
            Response::Attestation { document } => Ok(document),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    fn process_request(&self, req: Request) -> Result<Response> {
        match nsm_process_request(self.fd, req) {
            Response::Error(err) => Err(err.into()),
            resp => Ok(resp),
        }
    }
}

impl Drop for Nsm {
    fn drop(&mut self) {
        nsm_exit(self.fd);
    }
}
