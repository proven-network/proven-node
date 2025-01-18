use proven_radix_nft_verifier::RadixNftVerifierError;
use proven_store::StoreError;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error<SE, VE>
where
    SE: StoreError,
    VE: RadixNftVerifierError,
{
    #[error(transparent)]
    Store(SE),

    #[error(transparent)]
    Verification(VE),
}
