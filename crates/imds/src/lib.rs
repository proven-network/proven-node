mod error;
mod pem;

pub use error::{Error, Result};
use pem::REGION_PEMS;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use httpclient::Client;
use httpclient::InMemoryResponseExt;
use ring::signature;
use ring::signature::UnparsedPublicKey;
use serde::Deserialize;
use x509_parser::prelude::*;

static IMDS_BASE_URL: &str = "http://169.254.169.254";
static IMDS_IDENTITY_PATH: &str = "/latest/dynamic/instance-identity/document";
static IMDS_TOKEN_PATH: &str = "/latest/api/token";
static IMDS_VERIFICATION_PATH: &str = "/latest/dynamic/instance-identity/signature";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityDocument {
    pub account_id: String,
    pub architecture: String,
    pub availability_zone: String,
    pub image_id: String,
    pub instance_id: String,
    pub instance_type: String,
    pub kernel_id: Option<String>,
    pub pending_time: String,
    pub private_ip: String,
    pub ramdisk_id: Option<String>,
    pub region: String,
    pub version: String,
}

pub struct Imds {
    client: Client,
    token: String,
}

impl Imds {
    pub async fn new() -> Result<Self> {
        let client = Client::new().base_url(IMDS_BASE_URL);

        let token_response = client
            .put(IMDS_TOKEN_PATH)
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .await?;

        let token = token_response.text().unwrap();

        Ok(Self { client, token })
    }

    async fn get_from_endpoint(&self, path: &str) -> Result<String> {
        let response = self
            .client
            .get(path)
            .header("X-aws-ec2-metadata-token", &self.token)
            .await?;
        let body = response.text()?;

        Ok(body)
    }

    pub async fn get_verified_identity_document(&self) -> Result<IdentityDocument> {
        let identity_document = self.get_from_endpoint(IMDS_IDENTITY_PATH).await?;
        let verification_document = self
            .get_from_endpoint(IMDS_VERIFICATION_PATH)
            .await?
            .replace('\n', "");

        // Convert the identity document into bytes
        let identity_bytes = identity_document.clone().into_bytes();

        // Decode the verification document (which is the signature) from base64
        let decoded_signature = STANDARD.decode(verification_document.clone())?;

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
            Ok(_) => Ok(document),
            Err(e) => Err(Error::SignatureVerification(e.to_string())),
        }
    }
}
