use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::account_deposit_pre_validation`].

On request success, this will return a [`AccountDepositPreValidationResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountDepositPreValidationRequest {
    pub account_address: String,
    pub badge: TransactionAccountDepositPreValidationAuthorizedDepositorBadge,
    pub resource_addresses: Vec<String>,
}
impl AccountDepositPreValidationRequest {}
impl FluentRequest<'_, AccountDepositPreValidationRequest> {}
impl<'a> ::std::future::IntoFuture
for FluentRequest<'a, AccountDepositPreValidationRequest> {
    type Output = crate::Result<AccountDepositPreValidationResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/transaction/account-deposit-pre-validation";
            let mut r = self.client.client.post(url);
            r = r.json(json!({ "account_address" : self.params.account_address }));
            r = r.json(json!({ "badge" : self.params.badge }));
            r = r.json(json!({ "resource_addresses" : self.params.resource_addresses }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
