use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConnzConnectionTlsPeerCert {
    pub subject: Option<String>,
    pub spki_sha256: Option<String>,
    pub cert_sha256: Option<String>,
}

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
        let json_str = r#"
        {
            "server_id": "NBYQVR2I5KUG5I6FN2YNFAPP5GVWF6V7QYGFK2VSKQDOY5ZYC2Y7QAZO",
            "now": "2024-07-26T10:43:25.602275695Z",
            "num_connections": 48,
            "total": 48,
            "offset": 0,
            "limit": 1024,
            "connections": [
                {
                "cid": 1280,
                "kind": "Client",
                "type": "nats",
                "ip": "198.211.99.91",
                "port": 52968,
                "start": "2024-07-17T14:48:20.74205633Z",
                "last_activity": "2024-07-26T10:43:08.520440368Z",
                "rtt": "42.868726ms",
                "uptime": "8d19h55m4s",
                "idle": "17s",
                "pending_bytes": 0,
                "in_msgs": 50060,
                "out_msgs": 50178,
                "in_bytes": 20716640,
                "out_bytes": 20772124,
                "subscriptions": 1,
                "lang": "nats.js",
                "version": "2.18.0",
                "tls_version": "1.3",
                "tls_cipher_suite": "TLS_AES_128_GCM_SHA256"
                },
                {
                "cid": 1287,
                "kind": "Client",
                "type": "nats",
                "ip": "3.69.240.25",
                "port": 37404,
                "start": "2024-07-17T14:48:20.842136268Z",
                "last_activity": "2024-07-17T14:48:21.113895023Z",
                "rtt": "120.359199ms",
                "uptime": "8d19h55m4s",
                "idle": "8d19h55m4s",
                "pending_bytes": 0,
                "in_msgs": 0,
                "out_msgs": 0,
                "in_bytes": 0,
                "out_bytes": 0,
                "subscriptions": 1,
                "lang": "python3",
                "version": "2.2.0"
                }
            ]
        }"#;

        let result: Result<Connz, serde_json::Error> = serde_json::from_str(json_str);
        assert!(result.is_ok());
    }
}
