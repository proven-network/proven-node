use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ResourceAggregationLevel {
    Global,
    Vault,
}
