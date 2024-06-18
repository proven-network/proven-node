mod error;

pub use error::{Error, Result};

use aws_config::Region;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{KeyEncryptionMechanism, RecipientInfo};
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use proven_attestation::{AttestationParams, Attestor};
use proven_attestation_nsm::NsmAttestor;
use rand::rngs::OsRng;
use rsa::oaep::Oaep;
use rsa::pkcs8::EncodePublicKey;
use rsa::{RsaPrivateKey, RsaPublicKey};
use sha2::Sha256;

pub struct Kms {
    client: aws_sdk_kms::Client,
    key_id: String,
    nsm_attestor: NsmAttestor,
}

impl Kms {
    pub async fn new(key_id: String, region: String) -> Self {
        let config = aws_config::from_env()
            .region(Region::new(region))
            .load()
            .await;

        Self {
            client: aws_sdk_kms::Client::new(&config),
            key_id,
            nsm_attestor: NsmAttestor::new(),
        }
    }

    pub async fn encrypt(&self, plaintext: Vec<u8>) -> Result<Vec<u8>> {
        let ciphertext = self
            .client
            .encrypt()
            .plaintext(Blob::new(plaintext))
            .key_id(&self.key_id)
            .send()
            .await
            .map_err(|e| Error::Kms(e.into()))
            .map(|output| output.ciphertext_blob.unwrap().into_inner())?;

        Ok(ciphertext)
    }

    pub async fn decrypt(&self, ciphertext: Vec<u8>) -> Result<Vec<u8>> {
        let (private_key, public_key) = Self::generate_keypair().await?;

        let attestation_document = self
            .nsm_attestor
            .attest(AttestationParams {
                nonce: None,
                public_key: Some(public_key.to_public_key_der()?.to_vec()),
                user_data: None,
            })
            .await?;

        let recipient = RecipientInfo::builder()
            .key_encryption_algorithm(KeyEncryptionMechanism::RsaesOaepSha256)
            .attestation_document(Blob::new(attestation_document))
            .build();

        let base64_ciphertext_for_recipient = self
            .client
            .decrypt()
            .ciphertext_blob(Blob::new(ciphertext))
            .recipient(recipient)
            .key_id(&self.key_id)
            .send()
            .await
            .map_err(|e| Error::Kms(e.into()))
            .map(|output| output.ciphertext_for_recipient.unwrap())?;

        base64
            .decode(base64_ciphertext_for_recipient.into_inner())
            .map(|ciphertext_for_recipient| {
                let plaintext = private_key.decrypt(
                    Oaep::new_with_mgf_hash::<Sha256, Sha256>(),
                    &ciphertext_for_recipient,
                )?;

                Ok(plaintext)
            })?
    }

    async fn generate_keypair() -> Result<(RsaPrivateKey, RsaPublicKey)> {
        let mut rng = OsRng;

        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits)?;
        let public_key = RsaPublicKey::from(&private_key);

        Ok((private_key, public_key))
    }
}
