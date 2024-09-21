use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::entity_non_fungibles_page`].

On request success, this will return a [`StateEntityNonFungiblesPageResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct EntityNonFungiblesPageRequest {
    pub address: String,
    pub aggregation_level: ResourceAggregationLevel,
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub cursor: Option<String>,
    pub limit_per_page: Option<i64>,
    pub opt_ins: StateEntityNonFungiblesPageRequestOptIns,
}
impl EntityNonFungiblesPageRequest {}
impl FluentRequest<'_, EntityNonFungiblesPageRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
    ///Set the value of the cursor field.
    pub fn cursor(mut self, cursor: &str) -> Self {
        self.params.cursor = Some(cursor.to_owned());
        self
    }
    ///Set the value of the limit_per_page field.
    pub fn limit_per_page(mut self, limit_per_page: i64) -> Self {
        self.params.limit_per_page = Some(limit_per_page);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, EntityNonFungiblesPageRequest> {
    type Output = crate::Result<StateEntityNonFungiblesPageResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/entity/page/non-fungibles/";
            let mut r = self.client.client.post(url);
            r = r.json(json!({ "address" : self.params.address }));
            r = r.json(json!({ "aggregation_level" : self.params.aggregation_level }));
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.cursor {
                r = r.json(json!({ "cursor" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.limit_per_page {
                r = r.json(json!({ "limit_per_page" : unwrapped }));
            }
            r = r.json(json!({ "opt_ins" : self.params.opt_ins }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
