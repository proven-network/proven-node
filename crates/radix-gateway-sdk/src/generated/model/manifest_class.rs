use serde::{Deserialize, Serialize};
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
