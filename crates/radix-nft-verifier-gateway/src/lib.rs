//! An implementation of the NFT verifier which uses the Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use async_trait::async_trait;
use proven_radix_gateway_sdk::types::{NonFungibleId, StateNonFungibleLocationRequest};
use proven_radix_gateway_sdk::{build_client, Client};
use proven_radix_nft_verifier::{RadixNftVerificationResult, RadixNftVerifier};

mod error;
pub use error::Error;

/// A gateway-based implementation of the `RadixNftVerifier` trait.
#[derive(Clone, Debug)]
pub struct GatewayRadixNftVerifier {
    client: Client,
}

impl GatewayRadixNftVerifier {
    /// Creates a new instance of `GatewayRadixNftVerifier`.
    #[must_use]
    pub fn new(radix_gateway_origin: &str) -> Self {
        let client = build_client(radix_gateway_origin, None, None);

        Self { client }
    }
}

#[async_trait]
impl RadixNftVerifier for GatewayRadixNftVerifier {
    type Error = Error;

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
        let body = StateNonFungibleLocationRequest::builder()
            .resource_address(resource_address.into())
            .non_fungible_ids(vec![NonFungibleId(nft_id.into())]);

        match self.client.non_fungible_location().body(body).send().await {
            Ok(response) => match response.non_fungible_ids.first() {
                Some(location) => {
                    let owner_address = if let Some(address) =
                        location.owning_vault_global_ancestor_address.clone()
                    {
                        address.to_string()
                    } else {
                        return Ok(RadixNftVerificationResult::NftDoesNotExist);
                    };

                    if accounts
                        .iter()
                        .any(|account| account.clone().into() == owner_address)
                    {
                        return Ok(RadixNftVerificationResult::Owned(owner_address));
                    }

                    return Ok(RadixNftVerificationResult::NotOwned(owner_address));
                }
                None => Ok(RadixNftVerificationResult::NftDoesNotExist),
            },
            Err(e) => Err(Error::Gateway(e)),
        }
    }
}
