use serde::Deserialize;

/// Auth gateway definition in the network config file
#[derive(Debug, Deserialize)]
pub struct ConfigAuthGateway {
    pub primary: String,
    pub alternates: Vec<String>,
}

/// Node definition in the topology file
#[derive(Debug, Deserialize)]
pub struct ConfigNode {
    pub origin: String,
    pub public_key: String,
    pub specializations: Vec<String>,
}

/// Node definition in the topology file
#[derive(Debug, Deserialize)]
pub struct ConfigVersion {
    pub pcr0: String,
    pub pcr1: String,
    pub pcr2: String,
}

/// Network config file
#[derive(Debug, Deserialize)]
pub struct Config {
    pub auth_gateways: ConfigAuthGateway,
    pub topology: Vec<ConfigNode>,
    pub versions: Vec<ConfigVersion>,
}
