use radix_common::crypto::{Ed25519PublicKey, PublicKey, Secp256k1PublicKey};
use serde::{Deserialize, Deserializer};

/// A curve used to generate a public key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Curve {
    /// The Curve25519 curve.
    Curve25519,

    /// The Secp256k1 curve.
    Secp256k1,
}

/// The type of an entity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    /// An account.
    Account,

    /// A persona.
    Persona,
}

/// A proof of ownership.
#[derive(Clone, Debug)]
pub struct Proof {
    /// The curve used to generate the public key.
    pub curve: Curve,

    /// The public key.
    pub public_key: PublicKey,

    /// The signature.
    pub signature: String,
}

/// A signed challenge.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedChallenge {
    /// The address of the entity.
    pub address: String,

    /// The challenge.
    pub challenge: String,

    /// The proof.
    pub proof: Proof,

    /// The type of the entity.
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

        Ok(Self {
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
        Self::try_from(raw).map_err(serde::de::Error::custom)
    }
}
