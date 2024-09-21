use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::account_lockers_touched_at`].

On request success, this will return a [`StateAccountLockersTouchedAtResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountLockersTouchedAtRequest {
    pub account_lockers: Vec<AccountLockerAddress>,
    pub at_ledger_state: Option<LedgerStateSelector>,
}
impl AccountLockersTouchedAtRequest {}
impl FluentRequest<'_, AccountLockersTouchedAtRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture
for FluentRequest<'a, AccountLockersTouchedAtRequest> {
    type Output = crate::Result<StateAccountLockersTouchedAtResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/account-lockers/touched-at";
            let mut r = self.client.client.post(url);
            r = r.json(json!({ "account_lockers" : self.params.account_lockers }));
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
