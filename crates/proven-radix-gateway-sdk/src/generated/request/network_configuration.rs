use crate::generated::model::*;
use crate::generated::FluentRequest;
use crate::generated::LowLevelClient;
use httpclient::InMemoryResponseExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
/**You should use this struct via [`LowLevelClient::network_configuration`].

On request success, this will return a [`NetworkConfigurationResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkConfigurationRequest {}
impl NetworkConfigurationRequest {}
impl FluentRequest<'_, NetworkConfigurationRequest> {}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, NetworkConfigurationRequest> {
    type Output = crate::Result<NetworkConfigurationResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/status/network-configuration";
            let mut r = self.client.client.post(url);
            r = r.set_query(self.params);
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
