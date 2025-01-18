//! A mock implementation of the Radix NFT verifier.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use proven_radix_nft_verifier::{RadixNftVerificationResult, RadixNftVerifier};
use tokio::sync::Mutex;

mod error;
pub use error::RadixNftVerifierErrorMock;

/// A mock implementation of the `RadixNftVerifier` trait. Used for testing.
#[derive(Clone, Debug, Default)]
pub struct RadixNftVerifierMock {
    ownership_map: Arc<Mutex<HashMap<String, String>>>,
}

impl RadixNftVerifierMock {
    /// Creates a new instance of `RadixNftVerifierMock`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ownership_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Inserts a record of NFT ownership into the mock verifier.
    pub async fn insert_ownership<A, R, N>(&mut self, account: A, resource_address: R, nft_id: N)
    where
        A: Clone + Into<String> + Send + Sync,
        R: Clone + Into<String> + Send + Sync,
        N: Clone + Into<String> + Send + Sync,
    {
        self.ownership_map.lock().await.insert(
            format!("{}:{}", resource_address.into(), nft_id.into()),
            account.into(),
        );
    }
}

#[async_trait]
impl RadixNftVerifier for RadixNftVerifierMock {
    type Error = RadixNftVerifierErrorMock;

    async fn verify_ownership<A, R, N>(
        &self,
        accounts: &[A],
        resource_address: R,
        nft_id: N,
    ) -> Result<RadixNftVerificationResult, Self::Error>
    where
        A: Clone + Into<String> + Send + Sync,
        R: Clone + Into<String> + Send + Sync,
        N: Clone + Into<String> + Send + Sync,
    {
        let current_owner = self
            .ownership_map
            .lock()
            .await
            .get(&format!("{}:{}", resource_address.into(), nft_id.into()))
            .cloned();

        current_owner.map_or(
            Ok(RadixNftVerificationResult::NftDoesNotExist),
            |current_owner| {
                if accounts
                    .iter()
                    .any(|account| account.clone().into() == current_owner)
                {
                    Ok(RadixNftVerificationResult::Owned(current_owner))
                } else {
                    Ok(RadixNftVerificationResult::NotOwned(current_owner))
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_verify_ownership_when_owned() {
        let mut verifier = RadixNftVerifierMock::new();
        verifier
            .insert_ownership(
                "account123".to_string(),
                "resource456".to_string(),
                "nft789".to_string(),
            )
            .await;

        let result = verifier
            .verify_ownership(&["account123"], "resource456", "nft789")
            .await
            .unwrap();

        assert!(matches!(
            result,
            RadixNftVerificationResult::Owned(owner) if owner == "account123"
        ));
    }

    #[tokio::test]
    async fn test_verify_ownership_when_not_owned() {
        let mut verifier = RadixNftVerifierMock::new();
        verifier
            .insert_ownership(
                "account123".to_string(),
                "resource456".to_string(),
                "nft789".to_string(),
            )
            .await;

        let result = verifier
            .verify_ownership(&["different_account"], "resource456", "nft789")
            .await
            .unwrap();

        assert!(matches!(
            result,
            RadixNftVerificationResult::NotOwned(owner) if owner == "account123"
        ));
    }

    #[tokio::test]
    async fn test_verify_ownership_when_nft_does_not_exist() {
        let verifier = RadixNftVerifierMock::new();

        let result = verifier
            .verify_ownership(&["account123"], "resource456", "nft789")
            .await
            .unwrap();

        assert!(matches!(
            result,
            RadixNftVerificationResult::NftDoesNotExist
        ));
    }
}
