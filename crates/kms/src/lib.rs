//! Manages encryption/decryption of plain/cipher text using AWS KMS keys which
//! are scoped to the EIF PCRs.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::result_large_err)]

mod error;
mod pkcs7;

use pkcs7::ContentInfo;

pub use error::{Error, Result};

use aws_config::Region;
use aws_sdk_kms::Client;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{KeyEncryptionMechanism, RecipientInfo};
use bytes::Bytes;
use proven_attestation::{AttestationParams, Attestor};
use rand::rngs::OsRng;
use rsa::pkcs8::EncodePublicKey;
use rsa::{RsaPrivateKey, RsaPublicKey};

/// Options for the KMS client.
pub struct KmsOptions<A>
where
    A: Attestor,
{
    /// The attestor to use for attestation.
    pub attestor: A,

    /// The ID of the KMS key to use.
    pub key_id: String,

    /// The region of the KMS key.
    pub region: String,
}

/// Manages encryption/decryption of plain/cipher text using AWS KMS keys.
pub struct Kms<A>
where
    A: Attestor,
{
    attestor: A,
    client: Client,
    key_id: String,
}

impl<A> Kms<A>
where
    A: Attestor,
{
    /// Creates a new instance of `Kms`.
    pub async fn new(
        KmsOptions {
            attestor,
            key_id,
            region,
        }: KmsOptions<A>,
    ) -> Self {
        let config = aws_config::from_env()
            .region(Region::new(region))
            .load()
            .await;

        Self {
            attestor,
            client: Client::new(&config),
            key_id,
        }
    }

    /// Encrypts the given plaintext using the AWS KMS key.
    ///
    /// # Arguments
    ///
    /// * `plaintext` - The plaintext data to be encrypted.
    ///
    /// # Returns
    ///
    /// A `Result` containing the encrypted ciphertext as `Bytes` on success.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The encryption operation fails
    /// - The KMS service returns no ciphertext blob
    pub async fn encrypt(&self, plaintext: Bytes) -> Result<Bytes> {
        let response = self
            .client
            .encrypt()
            .plaintext(Blob::new(plaintext))
            .key_id(&self.key_id)
            .send()
            .await
            .map_err(|e| Error::Kms(e.into()))?;

        let blob = response.ciphertext_blob.ok_or(Error::MissingCiphertext)?;

        Ok(Bytes::from(blob.into_inner()))
    }

    /// Decrypts the given ciphertext using the AWS KMS key.
    ///
    /// # Arguments
    ///
    /// * `ciphertext` - The ciphertext data to be decrypted.
    ///
    /// # Returns
    ///
    /// A `Result` containing the decrypted plaintext as `Bytes` on success.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The decryption operation fails
    /// - The KMS service returns no ciphertext for recipient
    /// - Key pair generation fails
    /// - NSM attestation fails
    /// - Content info parsing or decryption fails
    pub async fn decrypt(&self, ciphertext: Bytes) -> Result<Bytes> {
        let (private_key, public_key) = Self::generate_keypair()?;

        let attestation_document = self
            .attestor
            .attest(AttestationParams {
                nonce: None,
                public_key: Some(Bytes::from(public_key.to_public_key_der()?.to_vec())),
                user_data: None,
            })
            .await
            .map_err(|e| Error::Attestation(e.to_string()))?;

        let recipient = RecipientInfo::builder()
            .key_encryption_algorithm(KeyEncryptionMechanism::RsaesOaepSha256)
            .attestation_document(Blob::new(attestation_document))
            .build();

        let response = self
            .client
            .decrypt()
            .ciphertext_blob(Blob::new(ciphertext))
            .recipient(recipient)
            .key_id(&self.key_id)
            .send()
            .await
            .map_err(|e| Error::Kms(e.into()))?;

        let ciphertext = response
            .ciphertext_for_recipient
            .ok_or(Error::MissingCiphertext)?
            .into_inner();

        let content_info = ContentInfo::parse_ber(ciphertext.as_slice())?;
        let plaintext = Bytes::from(content_info.decrypt_content(&private_key)?);

        Ok(plaintext)
    }

    fn generate_keypair() -> Result<(RsaPrivateKey, RsaPublicKey)> {
        let mut rng = OsRng;

        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits)?;
        let public_key = RsaPublicKey::from(&private_key);

        Ok((private_key, public_key))
    }
}
