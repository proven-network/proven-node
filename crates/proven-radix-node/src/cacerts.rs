use crate::error::{Error, Result};

use std::fs;
use std::path::Path;

use tokio::process::Command;

const KEYSTORE_PATH: &str = "/usr/lib/jvm/java-17-openjdk-arm64/lib/security/cacerts";
const KEYSTORE_PASS: &str = "changeit";
const CERTS_DIR: &str = "/etc/ssl/certs";
const TARGET_DIR: &str = "/etc/ssl/certs/java";

async fn generate_dummy_keystore() -> Result<()> {
    let status = Command::new("keytool")
        .args([
            "-genkey",
            "-noprompt",
            "-alias",
            "dummy",
            "-dname",
            "CN=Dummy",
            "-keystore",
            KEYSTORE_PATH,
            "-storepass",
            KEYSTORE_PASS,
            "-keypass",
            KEYSTORE_PASS,
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-validity",
            "3650",
        ])
        .status()
        .await
        .map_err(Error::Spawn)?;

    if !status.success() {
        return Err(Error::NonZeroExitCode(status));
    }

    Ok(())
}

async fn delete_dummy_key() -> Result<()> {
    let status = Command::new("keytool")
        .args([
            "-delete",
            "-noprompt",
            "-alias",
            "dummy",
            "-keystore",
            KEYSTORE_PATH,
            "-storepass",
            KEYSTORE_PASS,
        ])
        .status()
        .await
        .map_err(Error::Spawn)?;

    if !status.success() {
        return Err(Error::NonZeroExitCode(status));
    }

    Ok(())
}

async fn import_certificates() -> Result<()> {
    let entries = fs::read_dir(CERTS_DIR).map_err(Error::CaCertsWrite)?;
    for entry in entries {
        let entry = entry.map_err(Error::CaCertsWrite)?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("pem") {
            let cert = path.to_str().ok_or(Error::CaCertsBadPath)?;
            let ca_alias = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or(Error::CaCertsBadPath)?;
            let status = Command::new("keytool")
                .args([
                    "-cacerts",
                    "-trustcacerts",
                    "-storepass",
                    KEYSTORE_PASS,
                    "-importcert",
                    "-alias",
                    ca_alias,
                    "-file",
                    cert,
                    "-noprompt",
                ])
                .status()
                .await
                .map_err(Error::Spawn)?;

            if !status.success() {
                eprintln!("Failed to import certificate {}", ca_alias);
            }
        }
    }

    Ok(())
}

async fn copy_keystore() -> Result<()> {
    fs::create_dir_all(TARGET_DIR).map_err(Error::CaCertsWrite)?;
    let target_path = Path::new(TARGET_DIR).join("cacerts");
    fs::copy(KEYSTORE_PATH, target_path).map_err(Error::CaCertsWrite)?;

    Ok(())
}

pub async fn setup_keystore() -> Result<()> {
    generate_dummy_keystore().await?;
    delete_dummy_key().await?;
    import_certificates().await?;
    copy_keystore().await?;

    Ok(())
}
