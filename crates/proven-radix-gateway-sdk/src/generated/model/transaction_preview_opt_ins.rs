use serde::{Deserialize, Serialize};
///A set of flags to configure the response of the transaction preview.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionPreviewOptIns {
    /**This flag controls whether the preview response will include a Radix Engine Toolkit serializable
    receipt or not. If not provided, this defaults to `false` and no toolkit receipt is provided in
    the response.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub radix_engine_toolkit_receipt: Option<bool>,
}
impl std::fmt::Display for TransactionPreviewOptIns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
