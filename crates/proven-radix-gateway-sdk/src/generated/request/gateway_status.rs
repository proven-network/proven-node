use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::gateway_status`].

On request success, this will return a [`GatewayStatusResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct GatewayStatusRequest {}
impl GatewayStatusRequest {}
impl FluentRequest<'_, GatewayStatusRequest> {}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, GatewayStatusRequest> {
    type Output = crate::Result<GatewayStatusResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/status/gateway-status";
            let mut r = self.client.client.post(url);
            r = r.set_query(self.params);
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
