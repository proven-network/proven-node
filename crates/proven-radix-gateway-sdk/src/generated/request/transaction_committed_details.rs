use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::transaction_committed_details`].

On request success, this will return a [`TransactionCommittedDetailsResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionCommittedDetailsRequest {
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub intent_hash: String,
    pub opt_ins: TransactionDetailsOptIns,
}
impl TransactionCommittedDetailsRequest {}
impl FluentRequest<'_, TransactionCommittedDetailsRequest> {
    ///Set the value of the at_ledger_state field.
    pub fn at_ledger_state(mut self, at_ledger_state: LedgerStateSelector) -> Self {
        self.params.at_ledger_state = Some(at_ledger_state);
        self
    }
}
impl<'a> ::std::future::IntoFuture
for FluentRequest<'a, TransactionCommittedDetailsRequest> {
    type Output = crate::Result<TransactionCommittedDetailsResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/transaction/committed-details";
            let mut r = self.client.client.post(url);
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            r = r.json(json!({ "intent_hash" : self.params.intent_hash }));
            r = r.json(json!({ "opt_ins" : self.params.opt_ins }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
