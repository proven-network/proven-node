use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ManifestClass {
    General,
    Transfer,
    PoolContribution,
    PoolRedemption,
    ValidatorStake,
    ValidatorUnstake,
    ValidatorClaim,
    AccountDepositSettingsUpdate,
}
