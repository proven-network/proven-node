use serde::{Deserialize, Serialize};
use std::time::Duration;

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Route {
    pub rid: Option<u64>,
    pub remote_id: Option<String>,
    pub remote_name: Option<String>,
    pub did_solicit: Option<bool>,
    pub is_configured: Option<bool>,
    pub ip: Option<String>,
    pub port: Option<u16>,
    pub start: Option<String>,
    pub last_activity: Option<String>,
    #[serde(with = "crate::serde_duration")]
    pub rtt: Option<Duration>,
    #[serde(with = "crate::serde_duration")]
    pub uptime: Option<Duration>,
    #[serde(with = "crate::serde_duration")]
    pub idle: Option<Duration>,
    pub pending_size: Option<u64>,
    pub in_msgs: Option<u64>,
    pub out_msgs: Option<u64>,
    pub in_bytes: Option<u64>,
    pub out_bytes: Option<u64>,
    pub subscriptions: Option<u32>,
    pub account: Option<String>,
    pub compression: Option<String>,
}

/// Represents the routes information (Routez) from the NATS server.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Routez {
    pub server_id: String,
    pub server_name: String,
    pub now: String,
    pub num_routes: u32,
    pub routes: Vec<Route>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_sample_routez() {
        let json_str = include_str!("../../test_data/sample_routez.json");
        let result: Result<Routez, serde_json::Error> = serde_json::from_str(json_str);
        assert!(result.is_ok());
    }
}
