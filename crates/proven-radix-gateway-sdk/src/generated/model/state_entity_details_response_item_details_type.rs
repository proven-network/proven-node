use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StateEntityDetailsResponseItemDetailsType {
    FungibleResource,
    NonFungibleResource,
    FungibleVault,
    NonFungibleVault,
    Package,
    Component,
}
