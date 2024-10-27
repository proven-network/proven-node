use radix_common::crypto::{Ed25519PublicKey, PublicKey, Secp256k1PublicKey};
use serde::{Deserialize, Deserializer};

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Curve {
    Curve25519,
    Secp256k1,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Account,
    Persona,
}

#[derive(Clone, Debug)]
pub struct Proof {
    pub curve: Curve,
    pub public_key: PublicKey,
    pub signature: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedChallenge {
    pub address: String,
    pub challenge: String,
    pub proof: Proof,
    pub r#type: Type,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawProof {
    curve: String,
    public_key: String,
    signature: String,
}

impl TryFrom<RawProof> for Proof {
    type Error = Box<dyn std::error::Error>;

    fn try_from(raw: RawProof) -> Result<Self, Self::Error> {
        let curve = match raw.curve.as_str() {
            "curve25519" => Curve::Curve25519,
            "secp256k1" => Curve::Secp256k1,
            _ => return Err("Invalid curve".into()),
        };

        let bytes = hex::decode(&raw.public_key)?;

        let public_key = match curve {
            Curve::Curve25519 => {
                let key = Ed25519PublicKey::try_from(bytes.as_slice())?;
                PublicKey::Ed25519(key)
            }
            Curve::Secp256k1 => {
                let key = Secp256k1PublicKey::try_from(bytes.as_slice())?;
                PublicKey::Secp256k1(key)
            }
        };

        Ok(Proof {
            curve,
            public_key,
            signature: raw.signature,
        })
    }
}

impl<'de> Deserialize<'de> for Proof {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawProof::deserialize(deserializer)?;
        Proof::try_from(raw).map_err(serde::de::Error::custom)
    }
}
