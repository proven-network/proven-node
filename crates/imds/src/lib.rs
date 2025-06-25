//! Helper crate to interact with the Instance Metadata Service. Verifies all
//! recieved data via embedded public certificates.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod pem;

pub use error::{Error, Result};
use pem::REGION_PEMS;

use aws_lc_rs::signature;
use aws_lc_rs::signature::UnparsedPublicKey;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::Client;
use serde::Deserialize;
use x509_parser::prelude::*;

static IMDS_BASE_URL: &str = "http://169.254.169.254";
static IMDS_IDENTITY_PATH: &str = "/latest/dynamic/instance-identity/document";
static IMDS_TOKEN_PATH: &str = "/latest/api/token";
static IMDS_VERIFICATION_PATH: &str = "/latest/dynamic/instance-identity/signature";

/// The instance identity document.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityDocument {
    /// The AWS account ID of the instance.
    pub account_id: String,

    /// The CPU architecture of the instance.
    pub architecture: String,

    /// The availability zone of the instance.
    pub availability_zone: String,

    /// The image ID of the instance.
    pub image_id: String,

    /// The instance ID of the instance.
    pub instance_id: String,

    /// The instance type of the instance.
    pub instance_type: String,

    /// The kernel ID of the instance (if applicable).
    pub kernel_id: Option<String>,

    /// The pending time of the instance.
    pub pending_time: String,

    /// The private IP address of the instance.
    pub private_ip: String,

    /// The RAM disk ID of the instance (if applicable).
    pub ramdisk_id: Option<String>,

    /// The region of the instance.
    pub region: String,

    /// The version of the instance identity document.
    pub version: String,
}

/// The IMDS client.
pub struct Imds {
    client: Client,
    token: String,
}

impl Imds {
    /// Creates a new instance of the IMDS client.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The HTTP request to obtain the token fails
    /// - The response cannot be parsed as text
    pub async fn new() -> Result<Self> {
        let client = Client::new();

        let token_response = client
            .put(format!("{IMDS_BASE_URL}{IMDS_TOKEN_PATH}"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await?;

        let token = token_response.text().await?;

        Ok(Self { client, token })
    }

    /// Retrieves and verifies the instance identity document.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The HTTP request to obtain the identity document fails
    /// - The HTTP request to obtain the verification document fails
    /// - The verification document cannot be decoded from base64
    /// - The identity document cannot be deserialized
    /// - The public key cannot be parsed
    /// - The signature verification fails
    pub async fn get_verified_identity_document(&self) -> Result<IdentityDocument> {
        let identity_document = self.get_from_endpoint(IMDS_IDENTITY_PATH).await?;
        let verification_document = self
            .get_from_endpoint(IMDS_VERIFICATION_PATH)
            .await?
            .replace('\n', "");

        // Convert the identity document into bytes
        let identity_bytes = identity_document.clone().into_bytes();

        // Decode the verification document (which is the signature) from base64
        let decoded_signature = STANDARD.decode(verification_document)?;

        // Deserialize identity
        let document: IdentityDocument = serde_json::from_str(&identity_document)?;

        // Parse the public key
        let raw_pem = REGION_PEMS
            .get(document.region.as_str())
            .ok_or(Error::NoPemForRegion(document.region.clone()))?;
        let pem = ::pem::parse(raw_pem)?;
        let (_rem, x509) = X509Certificate::from_der(pem.contents())?;

        let public_key = UnparsedPublicKey::new(
            &signature::RSA_PKCS1_1024_8192_SHA256_FOR_LEGACY_USE_ONLY,
            &x509.subject_pki.subject_public_key.data,
        );

        match public_key.verify(&identity_bytes, &decoded_signature) {
            Ok(()) => Ok(document),
            Err(e) => Err(Error::SignatureVerification(e.to_string())),
        }
    }

    async fn get_from_endpoint(&self, path: &str) -> Result<String> {
        let response = self
            .client
            .get(format!("{IMDS_BASE_URL}{path}"))
            .header("X-aws-ec2-metadata-token", &self.token)
            .send()
            .await?;
        let body = response.text().await?;

        Ok(body)
    }
}
