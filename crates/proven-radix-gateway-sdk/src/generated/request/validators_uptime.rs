use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::validators_uptime`].

On request success, this will return a [`ValidatorsUptimeResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatorsUptimeRequest {
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub from_ledger_state: Option<LedgerStateSelector>,
    pub validator_addresses: Vec<String>,
}
impl ValidatorsUptimeRequest {}
impl FluentRequest<'_, ValidatorsUptimeRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
    ///Set the value of the from_ledger_state field.
    pub fn from_ledger_state(mut self, from_ledger_state: LedgerStateSelector) -> Self {
        self.params.from_ledger_state = Some(from_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, ValidatorsUptimeRequest> {
    type Output = crate::Result<ValidatorsUptimeResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/statistics/validators/uptime";
            let mut r = self.client.client.post(url);
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.from_ledger_state {
                r = r.json(json!({ "from_ledger_state" : unwrapped }));
            }
            r = r
                .json(
                    json!({ "validator_addresses" : self.params.validator_addresses }),
                );
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
