use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

pub struct AttestationParams {
    pub nonce: Option<Vec<u8>>,
    pub user_data: Option<Vec<u8>>,
    pub public_key: Option<Vec<u8>>,
}

#[async_trait]
pub trait Attestor: Clone + Send + Sync {
    type AE: Debug + Error + Send + Sync;
    async fn attest(&self, params: AttestationParams) -> Result<Vec<u8>, Self::AE>;
    async fn secure_random(&self) -> Result<Vec<u8>, Self::AE>;
}
