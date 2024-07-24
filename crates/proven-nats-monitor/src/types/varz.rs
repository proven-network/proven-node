use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{from_value, Value};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzLeaf {
    pub host: String,
    pub port: u16,
    pub auth_timeout: f32,
    pub tls_timeout: f32,
    pub tls_required: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzMqtt {
    pub host: String,
    pub port: u16,
    pub no_auth_user: String,
    pub tls_timeout: f32,
    pub ack_wait: u64,
    pub max_ack_pending: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzWebsocket {
    pub host: String,
    pub port: u16,
    pub no_auth_user: String,
    pub handshake_timeout: u64,
    pub compression: bool,
}

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzJetstreamStatsApi {
    pub total: u64,
    pub errors: u64,
    pub inflight: Option<u64>,
}

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzJetstream {
    pub config: VarzJetstreamConfig,
    pub stats: VarzJetstreamStats,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VarzSlowConsumerStats {
    pub clients: u32,
    pub routes: u32,
    pub gateways: u32,
    pub leafs: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Varz {
    pub server_id: String,
    pub server_name: String,
    pub version: String,
    pub proto: i32,
    pub git_commit: String,
    pub go: String,
    pub host: String,
    pub port: u32,
    pub auth_required: bool,
    pub max_connections: u32,
    pub max_subscriptions: u32,
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
    pub system_account: String,
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
    fn test_deserialize_sample_json() {
        let json_str = r#"
        {
            "server_id": "NBYQVR2I5KUG5I6FN2YNFAPP5GVWF6V7QYGFK2VSKQDOY5ZYC2Y7QAZO",
            "server_name": "us-south-nats-demo",
            "version": "2.10.18",
            "proto": 1,
            "git_commit": "57d23ac",
            "go": "go1.22.5",
            "host": "0.0.0.0",
            "port": 4222,
            "auth_required": true,
            "max_connections": 250000,
            "max_subscriptions": 200000,
            "ping_interval": 120000000000,
            "ping_max": 2,
            "http_host": "0.0.0.0",
            "http_port": 0,
            "http_base_path": "",
            "https_port": 8222,
            "auth_timeout": 6,
            "max_control_line": 4096,
            "max_payload": 1048576,
            "max_pending": 67108864,
            "cluster": {},
            "gateway": {},
            "leaf": {
                "host": "0.0.0.0",
                "port": 7422,
                "auth_timeout": 6,
                "tls_timeout": 5,
                "tls_required": true
            },
            "mqtt": {
                "host": "0.0.0.0",
                "port": 1883,
                "no_auth_user": "demo-user",
                "tls_timeout": 5,
                "ack_wait": 60000000000,
                "max_ack_pending": 1024
            },
            "websocket": {
                "host": "0.0.0.0",
                "port": 8443,
                "no_auth_user": "demo-user",
                "handshake_timeout": 5000000000,
                "compression": true
            },
            "jetstream": {
                "config": {
                    "max_memory": 10737418240,
                    "max_storage": 440234147840,
                    "store_dir": "/var/jetstream/jetstream",
                    "sync_interval": 120000000000,
                    "compress_ok": true
                },
                "stats": {
                    "memory": 885,
                    "storage": 39445695,
                    "reserved_memory": 0,
                    "reserved_storage": 2695403737,
                    "accounts": 1,
                    "ha_assets": 0,
                    "api": {
                        "total": 427113,
                        "errors": 983
                    }
                }
            },
            "tls_timeout": 5,
            "write_deadline": 10000000000,
            "start": "2024-07-17T14:48:20.61769644Z",
            "now": "2024-07-24T09:04:58.442675859Z",
            "uptime": "6d18h16m37s",
            "mem": 85266432,
            "cores": 16,
            "gomaxprocs": 16,
            "cpu": 0,
            "connections": 50,
            "total_connections": 128187,
            "routes": 0,
            "remotes": 0,
            "leafnodes": 0,
            "in_msgs": 4121415,
            "out_msgs": 2141909,
            "in_bytes": 10181237941,
            "out_bytes": 11132504247,
            "slow_consumers": 105,
            "subscriptions": 1318,
            "http_req_stats": {
                "/": 9851,
                "/accountz": 55,
                "/accstatz": 41,
                "/connz": 7962,
                "/gatewayz": 27,
                "/healthz": 2024,
                "/jsz": 1072,
                "/leafz": 51,
                "/routez": 63,
                "/subsz": 69,
                "/varz": 10039
            },
            "config_load_time": "2024-07-17T14:48:20.61769644Z",
            "system_account": "$SYS",
            "slow_consumer_stats": {
                "clients": 105,
                "routes": 0,
                "gateways": 0,
                "leafs": 0
            }
        }"#;

        let result: Result<Varz, serde_json::Error> = serde_json::from_str(json_str);
        assert!(result.is_ok());
    }
}
