use serde::{Serialize, Deserialize};
///Check detailed [OptIns](#section/Using-endpoints-with-opt-in-features) documentation for more details
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityFungiblesPageRequestOptIns {
    ///allows specifying explicitly metadata properties which should be returned in response, limited to max 20 items.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_metadata: Option<Vec<String>>,
}
impl std::fmt::Display for StateEntityFungiblesPageRequestOptIns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
