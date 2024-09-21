use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::state_entity_details`].

On request success, this will return a [`StateEntityDetailsResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct StateEntityDetailsRequest {
    pub addresses: Vec<String>,
    pub aggregation_level: ResourceAggregationLevel,
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub opt_ins: StateEntityDetailsOptIns,
}
impl StateEntityDetailsRequest {}
impl FluentRequest<'_, StateEntityDetailsRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, StateEntityDetailsRequest> {
    type Output = crate::Result<StateEntityDetailsResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/entity/details";
            let mut r = self.client.client.post(url);
            r = r.json(json!({ "addresses" : self.params.addresses }));
            r = r.json(json!({ "aggregation_level" : self.params.aggregation_level }));
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            r = r.json(json!({ "opt_ins" : self.params.opt_ins }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
