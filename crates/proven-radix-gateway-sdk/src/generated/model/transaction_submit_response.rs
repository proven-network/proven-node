use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionSubmitResponse {
    ///Is true if the transaction is a duplicate of an existing pending transaction.
    pub duplicate: bool,
}
impl std::fmt::Display for TransactionSubmitResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
