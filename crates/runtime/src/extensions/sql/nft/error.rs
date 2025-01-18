use proven_radix_nft_verifier::RadixNftVerifierError;
use proven_sql::SqlStoreError;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error<SE, VE>
where
    SE: SqlStoreError,
    VE: RadixNftVerifierError,
{
    #[error(transparent)]
    SqlStore(SE),

    #[error(transparent)]
    Verification(VE),
}
