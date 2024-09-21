use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::non_fungible_data`].

On request success, this will return a [`StateNonFungibleDataResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct NonFungibleDataRequest {
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub non_fungible_ids: Vec<String>,
    pub resource_address: String,
}
impl NonFungibleDataRequest {}
impl FluentRequest<'_, NonFungibleDataRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, NonFungibleDataRequest> {
    type Output = crate::Result<StateNonFungibleDataResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/non-fungible/data";
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
