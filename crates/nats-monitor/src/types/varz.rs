use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, from_value};

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzLeaf {
    pub host: String,
    pub port: u16,
    pub auth_timeout: f32,
    pub tls_timeout: f32,
    pub tls_required: bool,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzMqtt {
    pub host: String,
    pub port: u16,
    pub no_auth_user: String,
    pub tls_timeout: f32,
    pub ack_wait: u64,
    pub max_ack_pending: u32,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzWebsocket {
    pub host: String,
    pub port: u16,
    pub no_auth_user: String,
    pub handshake_timeout: u64,
    pub compression: bool,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzJetstreamConfig {
    pub max_memory: u64,
    pub max_storage: u64,
    pub store_dir: Option<String>,
    pub sync_interval: Option<u64>,
    pub sync_always: Option<bool>,
    pub compress_ok: Option<bool>,
    pub unique_tag: Option<String>,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzJetstreamStatsApi {
    pub total: u64,
    pub errors: u64,
    pub inflight: Option<u64>,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzJetstreamStats {
    pub memory: u64,
    pub storage: u64,
    pub reserved_memory: u64,
    pub reserved_storage: u64,
    pub accounts: u32,
    pub ha_assets: u32,
    pub api: VarzJetstreamStatsApi,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzJetstream {
    pub config: VarzJetstreamConfig,
    pub stats: VarzJetstreamStats,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzSlowConsumerStats {
    pub clients: u32,
    pub routes: u32,
    pub gateways: u32,
    pub leafs: u32,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Varz {
    pub server_id: String,
    pub server_name: String,
    pub version: String,
    pub proto: i32,
    pub git_commit: Option<String>,
    pub go: String,
    pub host: String,
    pub port: u32,
    pub auth_required: Option<bool>,
    pub tls_required: Option<bool>,
    pub tls_verify: Option<bool>,
    pub tls_ocsp_peer_verify: Option<bool>,
    pub ip: Option<String>,
    pub connect_urls: Option<Vec<String>>,
    pub ws_connect_urls: Option<Vec<String>>,
    pub max_connections: u32,
    pub max_subscriptions: Option<u32>,
    pub ping_interval: u64,
    pub ping_max: u32,
    pub http_host: String,
    pub http_port: u16,
    pub http_base_path: String,
    pub https_port: u16,
    pub auth_timeout: f32,
    pub max_control_line: u32,
    pub max_payload: u32,
    pub max_pending: u32,
    // TODO: Find the structure of these fields
    pub cluster: serde_json::Value, // Using serde_json::Value for unspecified structure
    pub gateway: serde_json::Value, // Using serde_json::Value for unspecified structure
    #[serde(deserialize_with = "deserialize_option_leaf")]
    pub leaf: Option<VarzLeaf>,
    #[serde(deserialize_with = "deserialize_option_mqtt")]
    pub mqtt: Option<VarzMqtt>,
    #[serde(deserialize_with = "deserialize_option_websocket")]
    pub websocket: Option<VarzWebsocket>,
    #[serde(deserialize_with = "deserialize_option_jetstream")]
    pub jetstream: Option<VarzJetstream>,
    pub tls_timeout: f32,
    pub write_deadline: u64,
    pub start: String,
    pub now: String,
    pub uptime: String,
    pub mem: u32,
    pub cores: f32,
    pub gomaxprocs: u8,
    pub cpu: f64,
    pub connections: u32,
    pub total_connections: u32,
    pub routes: u32,
    pub remotes: u32,
    pub leafnodes: u32,
    pub in_msgs: u64,
    pub out_msgs: u64,
    pub in_bytes: u64,
    pub out_bytes: u64,
    pub slow_consumers: u32,
    pub subscriptions: u32,
    pub http_req_stats: HashMap<String, u64>,
    pub config_load_time: String,
    pub system_account: Option<String>,
    pub slow_consumer_stats: VarzSlowConsumerStats,
}

fn deserialize_option_leaf<'de, D>(deserializer: D) -> Result<Option<VarzLeaf>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    match val {
        Value::Object(ref map) if map.is_empty() => Ok(None),
        _ => from_value(val).map(Some).map_err(serde::de::Error::custom),
    }
}

fn deserialize_option_mqtt<'de, D>(deserializer: D) -> Result<Option<VarzMqtt>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    match val {
        Value::Object(ref map) if map.is_empty() => Ok(None),
        _ => from_value(val).map(Some).map_err(serde::de::Error::custom),
    }
}

fn deserialize_option_websocket<'de, D>(deserializer: D) -> Result<Option<VarzWebsocket>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    match val {
        Value::Object(ref map) if map.is_empty() => Ok(None),
        _ => from_value(val).map(Some).map_err(serde::de::Error::custom),
    }
}

fn deserialize_option_jetstream<'de, D>(deserializer: D) -> Result<Option<VarzJetstream>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    match val {
        Value::Object(ref map) if map.is_empty() => Ok(None),
        _ => from_value(val).map(Some).map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_sample_varz() {
        let json_str = include_str!("../../test_data/sample_varz.json");
        let result: Result<Varz, serde_json::Error> = serde_json::from_str(json_str);
        assert!(result.is_ok());
    }
}
