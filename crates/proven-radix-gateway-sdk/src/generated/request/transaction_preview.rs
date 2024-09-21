use serde_json::json;
use crate::generated::model::*;
use crate::generated::FluentRequest;
use serde::{Serialize, Deserialize};
use httpclient::InMemoryResponseExt;
use crate::generated::LowLevelClient;
/**You should use this struct via [`LowLevelClient::transaction_preview`].

On request success, this will return a [`TransactionPreviewResponse`].*/
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionPreviewRequest {
    pub blobs_hex: Option<Vec<String>>,
    pub end_epoch_exclusive: i64,
    pub flags: serde_json::Value,
    pub manifest: String,
    pub nonce: i64,
    pub notary_is_signatory: Option<bool>,
    pub notary_public_key: Option<PublicKey>,
    pub signer_public_keys: Vec<PublicKey>,
    pub start_epoch_inclusive: i64,
    pub tip_percentage: i64,
}
impl TransactionPreviewRequest {}
pub struct TransactionPreviewRequired<'a> {
    pub end_epoch_exclusive: i64,
    pub flags: serde_json::Value,
    pub manifest: &'a str,
    pub nonce: i64,
    pub signer_public_keys: Vec<PublicKey>,
    pub start_epoch_inclusive: i64,
    pub tip_percentage: i64,
}
impl<'a> TransactionPreviewRequired<'a> {}
impl FluentRequest<'_, TransactionPreviewRequest> {
    ///Set the value of the blobs_hex field.
    pub fn blobs_hex(
        mut self,
        blobs_hex: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        self.params.blobs_hex = Some(
            blobs_hex.into_iter().map(|s| s.as_ref().to_owned()).collect(),
        );
        self
    }
    ///Set the value of the notary_is_signatory field.
    pub fn notary_is_signatory(mut self, notary_is_signatory: bool) -> Self {
        self.params.notary_is_signatory = Some(notary_is_signatory);
        self
    }
    ///Set the value of the notary_public_key field.
    pub fn notary_public_key(mut self, notary_public_key: PublicKey) -> Self {
        self.params.notary_public_key = Some(notary_public_key);
        self
    }
}
impl<'a> ::std::future::IntoFuture for FluentRequest<'a, TransactionPreviewRequest> {
    type Output = crate::Result<TransactionPreviewResponse>;
    type IntoFuture = ::futures::future::BoxFuture<'a, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let url = "/transaction/preview";
            let mut r = self.client.client.post(url);
            if let Some(ref unwrapped) = self.params.blobs_hex {
                r = r.json(json!({ "blobs_hex" : unwrapped }));
            }
            r = r
                .json(
                    json!({ "end_epoch_exclusive" : self.params.end_epoch_exclusive }),
                );
            r = r.json(json!({ "flags" : self.params.flags }));
            r = r.json(json!({ "manifest" : self.params.manifest }));
            r = r.json(json!({ "nonce" : self.params.nonce }));
            if let Some(ref unwrapped) = self.params.notary_is_signatory {
                r = r.json(json!({ "notary_is_signatory" : unwrapped }));
            }
            if let Some(ref unwrapped) = self.params.notary_public_key {
                r = r.json(json!({ "notary_public_key" : unwrapped }));
            }
            r = r.json(json!({ "signer_public_keys" : self.params.signer_public_keys }));
            r = r
                .json(
                    json!(
                        { "start_epoch_inclusive" : self.params.start_epoch_inclusive }
                    ),
                );
            r = r.json(json!({ "tip_percentage" : self.params.tip_percentage }));
            let res = r.await?;
            res.json().map_err(|e| crate::Error::LowLevel(e.into()))
        })
    }
}
