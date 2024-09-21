use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::key_value_store_data`].

On request success, this will return a [`StateKeyValueStoreDataResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValueStoreDataRequest {
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub key_value_store_address: String,
    pub keys: Vec<StateKeyValueStoreDataRequestKeyItem>,
}
impl KeyValueStoreDataRequest {}
impl FluentRequest<'_, KeyValueStoreDataRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, KeyValueStoreDataRequest> {
    type Output = crate::Result<StateKeyValueStoreDataResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/key-value-store/data";
            let mut r = self.client.client.post(url);
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            r = r
                .json(
                    json!(
                        { "key_value_store_address" : self.params.key_value_store_address
                        }
                    ),
                );
            r = r.json(json!({ "keys" : self.params.keys }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
