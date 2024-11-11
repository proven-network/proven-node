use crate::generated::model::*;
use crate::generated::FluentRequest;
use crate::generated::LowLevelClient;
use httpclient::InMemoryResponseExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
/**You should use this struct via [`LowLevelClient::entity_non_fungible_ids_page`].

On request success, this will return a [`StateEntityNonFungibleIdsPageResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct EntityNonFungibleIdsPageRequest {
    pub address: String,
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub cursor: Option<String>,
    pub limit_per_page: Option<i64>,
    pub resource_address: String,
    pub vault_address: String,
}
impl EntityNonFungibleIdsPageRequest {}
impl FluentRequest<'_, EntityNonFungibleIdsPageRequest> {
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
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, EntityNonFungibleIdsPageRequest> {
    type Output = crate::Result<StateEntityNonFungibleIdsPageResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/state/entity/page/non-fungible-vault/ids";
            let mut r = self.client.client.post(url);
            r = r.json(json!({ "address" : self.params.address }));
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.cursor {
                r = r.json(json!({ "cursor" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.limit_per_page {
                r = r.json(json!({ "limit_per_page" : unwrapped }));
            }
            r = r.json(json!({ "resource_address" : self.params.resource_address }));
            r = r.json(json!({ "vault_address" : self.params.vault_address }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}