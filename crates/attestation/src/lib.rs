use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

pub struct AttestationParams {
    pub nonce: Option<Bytes>,
    pub user_data: Option<Bytes>,
    pub public_key: Option<Bytes>,
}

#[async_trait]
pub trait Attestor: Clone + Send + Sync + 'static {
    type AE: Debug + Error + Send + Sync;
    async fn attest(&self, params: AttestationParams) -> Result<Bytes, Self::AE>;
    async fn secure_random(&self) -> Result<Bytes, Self::AE>;
}
