use crate::generated::model::*;
use crate::generated::FluentRequest;
use crate::generated::LowLevelClient;
use httpclient::InMemoryResponseExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
/**You should use this struct via [`LowLevelClient::transaction_construction`].

On request success, this will return a [`TransactionConstructionResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionConstructionRequest {}
impl TransactionConstructionRequest {}
impl FluentRequest<'_, TransactionConstructionRequest> {}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, TransactionConstructionRequest> {
    type Output = crate::Result<TransactionConstructionResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/transaction/construction";
            let mut r = self.client.client.post(url);
            r = r.set_query(self.params);
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
