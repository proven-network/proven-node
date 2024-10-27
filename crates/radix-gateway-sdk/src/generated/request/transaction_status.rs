use crate::generated::model::*;
use crate::generated::FluentRequest;
use crate::generated::LowLevelClient;
use httpclient::InMemoryResponseExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
/**You should use this struct via [`LowLevelClient::transaction_status`].

On request success, this will return a [`TransactionStatusResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionStatusRequest {
    pub intent_hash: String,
}
impl TransactionStatusRequest {}
impl FluentRequest<'_, TransactionStatusRequest> {}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, TransactionStatusRequest> {
    type Output = crate::Result<TransactionStatusResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/transaction/status";
            let mut r = self.client.client.post(url);
            r = r.json(json!({ "intent_hash" : self.params.intent_hash }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
