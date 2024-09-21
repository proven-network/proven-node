use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::non_fungible_location`].

On request success, this will return a [`StateNonFungibleLocationResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct NonFungibleLocationRequest {
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub non_fungible_ids: Vec<String>,
    pub resource_address: String,
}
impl NonFungibleLocationRequest {}
impl FluentRequest<'_, NonFungibleLocationRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, NonFungibleLocationRequest> {
    type Output = crate::Result<StateNonFungibleLocationResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/non-fungible/location";
            let mut r = self.client.client.post(url);
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            r = r.json(json!({ "non_fungible_ids" : self.params.non_fungible_ids }));
            r = r.json(json!({ "resource_address" : self.params.resource_address }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
