pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub enum Error {
    Attestation,
    Cbor,
    ChallengeStore,
    SessionStore,
    SignedChallengeInvalid,
}
