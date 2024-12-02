use serde::{Deserialize, Serialize};

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConnzConnectionTlsPeerCert {
    pub subject: Option<String>,
    pub spki_sha256: Option<String>,
    pub cert_sha256: Option<String>,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConnzConnectionSubDetail {
    pub account: Option<String>,
    pub subject: String,
    pub qgroup: Option<String>,
    pub sid: String,
    pub msgs: i64,
    pub max: Option<i64>,
    pub cid: u64,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConnzConnection {
    pub cid: u64,
    pub kind: String,
    #[serde(rename = "type")]
    pub conn_type: String,
    pub ip: String,
    pub port: u16,
    pub start: String,
    pub last_activity: String,
    pub rtt: String,
    pub uptime: String,
    pub idle: String,
    pub pending_bytes: u64,
    pub in_msgs: u64,
    pub out_msgs: u64,
    pub in_bytes: u64,
    pub out_bytes: u64,
    pub subscriptions: u32,
    pub name: Option<String>,
    pub lang: Option<String>,
    pub version: Option<String>,
    pub tls_version: Option<String>,
    pub tls_cipher_suite: Option<String>,
    pub tls_peer_certs: Option<Vec<ConnzConnectionTlsPeerCert>>,
    pub tls_first: Option<bool>,
    pub authorized_user: Option<String>,
    pub account: Option<String>,
    pub subscriptions_list: Option<Vec<String>>,
    pub subscriptions_list_detail: Option<Vec<ConnzConnectionSubDetail>>,
    pub jwt: Option<String>,
    pub issuer_key: Option<String>,
    pub name_tag: Option<String>,
    pub tags: Option<Vec<String>>,
    pub mqtt_client: Option<String>,
}

/// Represents the connection information (Connz) from the NATS server.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Connz {
    pub server_id: String,
    pub now: String,
    pub num_connections: u32,
    pub total: u32,
    pub offset: u32,
    pub limit: u32,
    pub connections: Vec<ConnzConnection>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_sample_connz() {
        let json_str = include_str!("../../test_data/sample_connz.json");
        let result: Result<Connz, serde_json::Error> = serde_json::from_str(json_str);
        assert!(result.is_ok());
    }
}
