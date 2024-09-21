use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NetworkConfigurationResponse {
    ///The logical id of the network
    pub network_id: i64,
    ///The logical name of the network
    pub network_name: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub well_known_addresses: serde_json::Value,
}
impl std::fmt::Display for NetworkConfigurationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
