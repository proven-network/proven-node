//! Implementation of attestation using the Nitro Security Module.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::{Error, Result};

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nsm_nitro_enclave_utils::api::nsm::{Request, Response};
use nsm_nitro_enclave_utils::driver::Driver;
use nsm_nitro_enclave_utils::driver::nitro::Nitro;
use proven_attestation::{AttestationParams, Attestor, Pcrs};
use serde_bytes::ByteBuf;
use tokio::sync::Mutex;

/// Attestor implementation using the Nitro Security Module.
#[derive(Clone)]
pub struct NsmAttestor {
    driver: Arc<Mutex<Nitro>>,
    pcrs: Pcrs,
}

impl NsmAttestor {
    /// Create a new `NsmAttestor`.
    #[must_use]
    pub fn new() -> Result<Self> {
        let driver = nsm_nitro_enclave_utils::driver::nitro::Nitro::init();

        let pcr0 = match driver.process_request(Request::DescribePCR { index: 0 }) {
            Response::DescribePCR { data, .. } => data,
            Response::Error(e) => return Err(Error::from(e)),
            _ => return Err(Error::InvalidResponse),
        };

        let pcr1 = match driver.process_request(Request::DescribePCR { index: 1 }) {
            Response::DescribePCR { data, .. } => data,
            Response::Error(e) => return Err(Error::from(e)),
            _ => return Err(Error::InvalidResponse),
        };

        let pcr2 = match driver.process_request(Request::DescribePCR { index: 2 }) {
            Response::DescribePCR { data, .. } => data,
            Response::Error(e) => return Err(Error::from(e)),
            _ => return Err(Error::InvalidResponse),
        };

        let pcr3 = match driver.process_request(Request::DescribePCR { index: 3 }) {
            Response::DescribePCR { data, .. } => data,
            Response::Error(e) => return Err(Error::from(e)),
            _ => return Err(Error::InvalidResponse),
        };

        let pcr4 = match driver.process_request(Request::DescribePCR { index: 4 }) {
            Response::DescribePCR { data, .. } => data,
            Response::Error(e) => return Err(Error::from(e)),
            _ => return Err(Error::InvalidResponse),
        };

        let pcr8 = match driver.process_request(Request::DescribePCR { index: 8 }) {
            Response::DescribePCR { data, .. } => data,
            Response::Error(e) => return Err(Error::from(e)),
            _ => return Err(Error::InvalidResponse),
        };

        let pcrs = Pcrs {
            pcr0: pcr0.into(),
            pcr1: pcr1.into(),
            pcr2: pcr2.into(),
            pcr3: pcr3.into(),
            pcr4: pcr4.into(),
            pcr8: pcr8.into(),
        };

        Ok(Self {
            driver: Arc::new(Mutex::new(driver)),
            pcrs,
        })
    }
}

#[async_trait]
impl Attestor for NsmAttestor {
    type Error = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Bytes> {
        let driver = self.driver.lock().await;

        let attestation = driver.process_request(Request::Attestation {
            nonce: params.nonce.map(ByteBuf::from),
            public_key: params.public_key.map(ByteBuf::from),
            user_data: params.user_data.map(ByteBuf::from),
        });

        match attestation {
            Response::Attestation { document } => Ok(document.into()),
            Response::Error(e) => Err(Error::from(e)),
            _ => Err(Error::InvalidResponse),
        }
    }

    async fn pcrs(&self) -> Result<Pcrs> {
        Ok(self.pcrs.clone())
    }

    async fn secure_random(&self) -> Result<Bytes> {
        let driver = self.driver.lock().await;

        let random = driver.process_request(Request::GetRandom);

        match random {
            Response::GetRandom { random } => Ok(random.into()),
            Response::Error(e) => Err(Error::from(e)),
            _ => Err(Error::InvalidResponse),
        }
    }
}
