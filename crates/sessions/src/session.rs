use crate::Error;

use std::marker::PhantomData;

use bytes::Bytes;
use proven_store::Store1;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub signing_key: Vec<u8>,
    pub verifying_key: Vec<u8>,
    pub dapp_definition_address: String,
    pub expected_origin: String,
    pub identity_address: String,
    pub account_addresses: Vec<String>,
}

impl<CS: Store1, SS: Store1> From<MarkedSession<CS, SS>> for Session {
    fn from(marked: MarkedSession<CS, SS>) -> Self {
        Self {
            session_id: marked.session_id,
            signing_key: marked.signing_key,
            verifying_key: marked.verifying_key,
            dapp_definition_address: marked.dapp_definition_address,
            expected_origin: marked.expected_origin,
            identity_address: marked.identity_address,
            account_addresses: marked.account_addresses,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MarkedSession<CS: Store1, SS: Store1> {
    pub session_id: String,
    pub signing_key: Vec<u8>,
    pub verifying_key: Vec<u8>,
    pub dapp_definition_address: String,
    pub expected_origin: String,
    pub identity_address: String,
    pub account_addresses: Vec<String>,
    #[serde(skip)]
    pub _marker: PhantomData<CS>,
    #[serde(skip)]
    pub _marker2: PhantomData<SS>,
}

impl<CS: Store1, SS: Store1> MarkedSession<CS, SS> {
    pub fn new(
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
        }
    }
}

impl<CS: Store1, SS: Store1> TryFrom<Bytes> for MarkedSession<CS, SS> {
    type Error = Error<CS::Error, SS::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref()).map_err(|e| e.into())
    }
}

impl<CS: Store1, SS: Store1> TryInto<Bytes> for MarkedSession<CS, SS> {
    type Error = Error<CS::Error, SS::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
