use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GatewayInfoResponseReleaseInfo {
    ///Image tag that is currently deployed to the Gateway API.
    pub image_tag: String,
    ///The Open API Schema version that was used to generate the API models.
    pub open_api_schema_version: String,
    ///The release that is currently deployed to the Gateway API.
    pub release_version: String,
}
impl std::fmt::Display for GatewayInfoResponseReleaseInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
