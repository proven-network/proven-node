use serde::Deserialize;

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

#[derive(Debug, Deserialize)]
pub struct Config {
    pub topology: Vec<ConfigNode>,
    pub versions: Vec<ConfigVersion>,
}
