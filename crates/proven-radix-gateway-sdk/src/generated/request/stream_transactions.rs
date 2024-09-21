use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::stream_transactions`].

On request success, this will return a [`StreamTransactionsResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamTransactionsRequest {
    pub accounts_with_manifest_owner_method_calls: Vec<String>,
    pub accounts_without_manifest_owner_method_calls: Vec<String>,
    pub affected_global_entities_filter: Vec<String>,
    pub at_ledger_state: Option<LedgerStateSelector>,
    pub cursor: Option<String>,
    pub events_filter: Vec<StreamTransactionsRequestEventFilterItem>,
    pub from_ledger_state: Option<LedgerStateSelector>,
    pub kind_filter: String,
    pub limit_per_page: Option<i64>,
    pub manifest_accounts_deposited_into_filter: Vec<String>,
    pub manifest_accounts_withdrawn_from_filter: Vec<String>,
    pub manifest_badges_presented_filter: Vec<String>,
    pub manifest_class_filter: serde_json::Value,
    pub manifest_resources_filter: Vec<String>,
    pub opt_ins: TransactionDetailsOptIns,
    pub order: String,
}
impl StreamTransactionsRequest {}
pub struct StreamTransactionsRequired<'a> {
    pub accounts_with_manifest_owner_method_calls: &'a [&'a str],
    pub accounts_without_manifest_owner_method_calls: &'a [&'a str],
    pub affected_global_entities_filter: &'a [&'a str],
    pub events_filter: Vec<StreamTransactionsRequestEventFilterItem>,
    pub kind_filter: &'a str,
    pub manifest_accounts_deposited_into_filter: &'a [&'a str],
    pub manifest_accounts_withdrawn_from_filter: &'a [&'a str],
    pub manifest_badges_presented_filter: &'a [&'a str],
    pub manifest_class_filter: serde_json::Value,
    pub manifest_resources_filter: &'a [&'a str],
    pub opt_ins: TransactionDetailsOptIns,
    pub order: &'a str,
}
impl<'a> StreamTransactionsRequired<'a> {}
impl FluentRequest<'_, StreamTransactionsRequest> {
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
    ///Set the value of the from_ledger_state field.
    pub fn from_ledger_state(mut self, from_ledger_state: LedgerStateSelector) -> Self {
        self.params.from_ledger_state = Some(from_ledger_state);
        self
    }
    ///Set the value of the limit_per_page field.
    pub fn limit_per_page(mut self, limit_per_page: i64) -> Self {
        self.params.limit_per_page = Some(limit_per_page);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, StreamTransactionsRequest> {
    type Output = crate::Result<StreamTransactionsResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/stream/transactions";
            let mut r = self.client.client.post(url);
            r = r
                .json(
                    json!(
                        { "accounts_with_manifest_owner_method_calls" : self.params
                        .accounts_with_manifest_owner_method_calls }
                    ),
                );
            r = r
                .json(
                    json!(
                        { "accounts_without_manifest_owner_method_calls" : self.params
                        .accounts_without_manifest_owner_method_calls }
                    ),
                );
            r = r
                .json(
                    json!(
                        { "affected_global_entities_filter" : self.params
                        .affected_global_entities_filter }
                    ),
                );
            if let Some(ref unwrapped) = self.params.at_ledger_state {
                r = r.json(json!({ "at_ledger_state" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.cursor {
                r = r.json(json!({ "cursor" : unwrapped }));
            }
            r = r.json(json!({ "events_filter" : self.params.events_filter }));
            if let Some(ref unwrapped) = self.params.from_ledger_state {
                r = r.json(json!({ "from_ledger_state" : unwrapped }));
            }
            r = r.json(json!({ "kind_filter" : self.params.kind_filter }));
            if let Some(ref unwrapped) = self.params.limit_per_page {
                r = r.json(json!({ "limit_per_page" : unwrapped }));
            }
            r = r
                .json(
                    json!(
                        { "manifest_accounts_deposited_into_filter" : self.params
                        .manifest_accounts_deposited_into_filter }
                    ),
                );
            r = r
                .json(
                    json!(
                        { "manifest_accounts_withdrawn_from_filter" : self.params
                        .manifest_accounts_withdrawn_from_filter }
                    ),
                );
            r = r
                .json(
                    json!(
                        { "manifest_badges_presented_filter" : self.params
                        .manifest_badges_presented_filter }
                    ),
                );
            r = r
                .json(
                    json!(
                        { "manifest_class_filter" : self.params.manifest_class_filter }
                    ),
                );
            r = r
                .json(
                    json!(
                        { "manifest_resources_filter" : self.params
                        .manifest_resources_filter }
                    ),
                );
            r = r.json(json!({ "opt_ins" : self.params.opt_ins }));
            r = r.json(json!({ "order" : self.params.order }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
