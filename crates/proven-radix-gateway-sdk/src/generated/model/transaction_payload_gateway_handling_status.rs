use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionPayloadGatewayHandlingStatus {
    HandlingSubmission,
    Concluded,
}
