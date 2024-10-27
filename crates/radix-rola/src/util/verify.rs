use std::str::FromStr;

use crate::error::Error::{CurveDoesNotMatchKey, FailedVerification};
use crate::error::Result;
use crate::{Curve, Proof};

use radix_common::crypto::{
    verify_ed25519, verify_secp256k1, Ed25519Signature, Hash, PublicKey, Secp256k1Signature,
};

pub fn verify_proof_factory(
    Proof {
        public_key,
        curve,
        signature,
        ..
    }: Proof,
) -> impl Fn(&Hash) -> Result<()> {
    move |signature_message: &Hash| -> Result<()> {
        match (&curve, public_key) {
            (Curve::Curve25519, PublicKey::Ed25519(public_key)) => {
                let signature = Ed25519Signature::from_str(signature.as_str())?;

                match verify_ed25519(signature_message, &public_key, &signature) {
                    true => Ok(()),
                    false => Err(FailedVerification),
                }
            }
            (Curve::Secp256k1, PublicKey::Secp256k1(public_key)) => {
                let signature = Secp256k1Signature::from_str(signature.as_str())?;

                match verify_secp256k1(signature_message, &public_key, &signature) {
                    true => Ok(()),
                    false => Err(FailedVerification),
                }
            }
            _ => Err(CurveDoesNotMatchKey),
        }
    }
}
