use serde::{Serialize, Deserialize};
use super::{AccountAuthorizedDepositorsResponseItem, ResultSetCursorMixin};
///Account resource preferences collection
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountAuthorizedDepositorsCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<AccountAuthorizedDepositorsResponseItem>,
}
impl std::fmt::Display for AccountAuthorizedDepositorsCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for AccountAuthorizedDepositorsCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for AccountAuthorizedDepositorsCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
