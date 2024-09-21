use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NativeResourceKind {
    Xrd,
    PackageOwnerBadge,
    AccountOwnerBadge,
    IdentityOwnerBadge,
    ValidatorOwnerBadge,
    #[serde(rename = "Secp256k1SignatureResource")]
    Secp256K1SignatureResource,
    Ed25519SignatureResource,
    GlobalCallerResource,
    PackageOfDirectCallerResource,
    SystemExecutionResource,
    ValidatorLiquidStakeUnit,
    ValidatorClaimNft,
    OneResourcePoolUnit,
    TwoResourcePoolUnit,
    MultiResourcePoolUnit,
    AccessControllerRecoveryBadge,
}
