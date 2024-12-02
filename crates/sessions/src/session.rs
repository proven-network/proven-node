use crate::Error;

use std::marker::PhantomData;

use bytes::Bytes;
use proven_attestation::Attestor;
use proven_store::Store1;
use serde::{Deserialize, Serialize};

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    /// The account addresses.
    pub account_addresses: Vec<String>,

    /// The dApp definition address.
    pub dapp_definition_address: String,

    /// The expected origin of future requests.
    pub expected_origin: String,

    /// The identity address.
    pub identity_address: String,

    /// The session ID.
    pub session_id: String,

    /// The node signing key.
    pub signing_key: Vec<u8>,

    /// The node verifying key.
    pub verifying_key: Vec<u8>,
}

impl<A, CS, SS> From<MarkedSession<A, CS, SS>> for Session
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    fn from(marked: MarkedSession<A, CS, SS>) -> Self {
        Self {
            account_addresses: marked.account_addresses,
            dapp_definition_address: marked.dapp_definition_address,
            expected_origin: marked.expected_origin,
            identity_address: marked.identity_address,
            session_id: marked.session_id,
            signing_key: marked.signing_key,
            verifying_key: marked.verifying_key,
        }
    }
}

/// A session that has been marked with traits (crate internal).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarkedSession<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    pub session_id: String,
    pub signing_key: Vec<u8>,
    pub verifying_key: Vec<u8>,
    pub dapp_definition_address: String,
    pub expected_origin: String,
    pub identity_address: String,
    pub account_addresses: Vec<String>,
    #[serde(skip)]
    pub _marker: PhantomData<A>,
    #[serde(skip)]
    pub _marker2: PhantomData<CS>,
    #[serde(skip)]
    pub _marker3: PhantomData<SS>,
}

impl<A, CS, SS> MarkedSession<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    pub const fn new(
        session_id: String,
        signing_key: Vec<u8>,
        verifying_key: Vec<u8>,
        dapp_definition_address: String,
        expected_origin: String,
        identity_address: String,
        account_addresses: Vec<String>,
    ) -> Self {
        Self {
            session_id,
            signing_key,
            verifying_key,
            dapp_definition_address,
            expected_origin,
            identity_address,
            account_addresses,
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }
}

impl<A, CS, SS> TryFrom<Bytes> for MarkedSession<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    type Error = Error<A, CS, SS>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref()).map_err(std::convert::Into::into)
    }
}

impl<A, CS, SS> TryInto<Bytes> for MarkedSession<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    type Error = Error<A, CS, SS>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
