use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::resource_holders_page`].

On request success, this will return a [`ResourceHoldersResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceHoldersPageRequest {
    pub cursor: Option<String>,
    pub limit_per_page: Option<i64>,
    pub resource_address: String,
}
impl ResourceHoldersPageRequest {}
impl FluentRequest<'_, ResourceHoldersPageRequest> {
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
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, ResourceHoldersPageRequest> {
    type Output = crate::Result<ResourceHoldersResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/extensions/resource-holders/page";
            let mut r = self.client.client.post(url);
            if let Some(ref unwrapped) = self.params.cursor {
                r = r.json(json!({ "cursor" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.limit_per_page {
                r = r.json(json!({ "limit_per_page" : unwrapped }));
            }
            r = r.json(json!({ "resource_address" : self.params.resource_address }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
