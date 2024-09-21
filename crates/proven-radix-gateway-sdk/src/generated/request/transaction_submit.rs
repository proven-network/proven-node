use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::transaction_submit`].

On request success, this will return a [`TransactionSubmitResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionSubmitRequest {
    pub notarized_transaction_hex: String,
}
impl TransactionSubmitRequest {}
impl FluentRequest<'_, TransactionSubmitRequest> {}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, TransactionSubmitRequest> {
    type Output = crate::Result<TransactionSubmitResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/transaction/submit";
            let mut r = self.client.client.post(url);
            r = r
                .json(
                    json!(
                        { "notarized_transaction_hex" : self.params
                        .notarized_transaction_hex }
                    ),
                );
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
