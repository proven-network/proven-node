use std::path::PathBuf;

use crate::{Error, Result};

use serde::Deserialize;
use tokio::process::Command;

/// The measurements of the enclave boot image.
#[derive(Clone, Debug, Deserialize)]
pub struct Measurements {
    /// The hash algorithm used to compute the PCR values.
    #[serde(rename = "HashAlgorithm")]
    pub hash_algorithm: String,

    /// Value of PCR0.
    #[serde(rename = "PCR0")]
    pub pcr0: String,

    /// Value of PCR1.
    #[serde(rename = "PCR1")]
    pub pcr1: String,

    /// Value of PCR2.
    #[serde(rename = "PCR2")]
    pub pcr2: String,
}

/// The state of the enclave.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EnclaveState {
    /// The enclave is empty.
    Empty,

    /// The enclave is running.
    Running,

    /// The enclave is shutting down.
    Terminating,
}

/// Information about an enclave.
#[derive(Clone, Debug, Deserialize)]
pub struct EnclaveInfo {
    /// The CPU IDs that the enclave is running on.
    #[serde(rename = "CPUIDs")]
    pub cpu_ids: Vec<u32>,

    /// The flags that the enclave was started with.
    #[serde(rename = "Flags")]
    pub flags: String,

    /// The VSOCK CID of the enclave.
    #[serde(rename = "EnclaveCID")]
    pub enclave_cid: u32,

    /// The ID of the enclave.
    #[serde(rename = "EnclaveID")]
    pub enclave_id: String,

    /// The name of the enclave.
    #[serde(rename = "EnclaveName")]
    pub enclave_name: String,

    /// Measurements of the enclave boot image.
    #[serde(rename = "Measurements")]
    pub measurements: Measurements,

    /// The memory size of the enclave in MiB.
    #[serde(rename = "MemoryMiB")]
    pub memory_mib: u32,

    /// The number of CPUs that the enclave was started with.
    #[serde(rename = "NumberOfCPUs")]
    pub number_of_cpus: u32,

    /// The process ID of the enclave.
    #[serde(rename = "ProcessID")]
    pub process_id: u32,

    /// The state of the enclave.
    #[serde(rename = "State")]
    pub state: EnclaveState,
}

/// A client for interacting with the `nitro-cli` tool.
pub struct NitroCli;

impl NitroCli {
    /// Retrieves information about the running enclave, if any.
    ///
    /// # Errors
    ///
    /// This function will return an error if the `nitro-cli describe-enclaves` command fails to execute,
    /// if the command returns a non-zero exit status, or if the output cannot be parsed as valid JSON.
    pub async fn describe_enclaves() -> Result<Vec<EnclaveInfo>> {
        let output = Command::new("nitro-cli")
            .args(["describe-enclaves"])
            .output()
            .await
            .map_err(|e| Error::Io("failed to run nitro-cli describe-enclaves", e))?;

        if !output.status.success() {
            return Err(Error::NonZeroExit(
                "nitro-cli describe-enclaves",
                output.status,
            ));
        }

        let stdout = String::from_utf8(output.stdout)?;

        let response = Self::parse_describe_enclaves_output(&stdout)?;

        Ok(response)
    }

    /// Checks if the enclave is currently running.
    ///
    /// # Errors
    ///
    /// This function will return an error if the `nitro-cli describe-enclaves` command fails to execute,
    /// if the command returns a non-zero exit status, or if the output cannot be parsed as valid JSON.
    pub async fn is_enclave_running() -> Result<bool> {
        if let Some(enclave_info) = Self::describe_enclaves().await?.into_iter().next() {
            match enclave_info.state {
                EnclaveState::Running => Ok(true),
                _ => Ok(false),
            }
        } else {
            Ok(false)
        }
    }

    /// Starts a new enclave with the specified configuration.
    ///
    /// # Errors
    ///
    /// This function will return an error if the `nitro-cli run-enclave` command fails to execute.
    pub async fn run_enclave(
        cpu_count: u8,
        memory: u32,
        enclave_cid: u32,
        eif_path: PathBuf,
    ) -> Result<()> {
        let handle = Command::new("nitro-cli")
            .arg("run-enclave")
            .arg("--cpu-count")
            .arg(cpu_count.to_string())
            .arg("--memory")
            .arg(memory.to_string())
            .arg("--enclave-cid")
            .arg(enclave_cid.to_string())
            .arg("--eif-path")
            .arg(eif_path)
            .output()
            .await
            .map_err(|e| Error::Io("failed to start enclave", e))?;

        if !handle.status.success() {
            return Err(Error::NonZeroExit("nitro-cli run-enclave", handle.status));
        }

        Ok(())
    }

    /// Terminates all running enclaves.
    ///
    /// # Errors
    ///
    /// This function will return an error if the `nitro-cli terminate-enclave` command fails to execute
    /// or returns a non-zero exit status.
    pub async fn terminate_all_enclaves() -> Result<()> {
        let output = Command::new("nitro-cli")
            .args(["terminate-enclave", "--all"])
            .output()
            .await
            .map_err(|e| Error::Io("failed to stop existing enclaves", e))?;

        if !output.status.success() {
            return Err(Error::NonZeroExit(
                "nitro-cli terminate-enclave --all",
                output.status,
            ));
        }

        Ok(())
    }

    fn parse_describe_enclaves_output(output: &str) -> Result<Vec<EnclaveInfo>> {
        serde_json::from_str(output).map_err(Error::Json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_describe_enclaves_output() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let fixture_path = PathBuf::from(manifest_dir)
            .join("test_data")
            .join("describe-enclaves.json");

        let fixture = std::fs::read_to_string(fixture_path).unwrap();

        // Test deserialization
        let response = NitroCli::parse_describe_enclaves_output(&fixture).unwrap();
        let enclave = &response[0];

        assert_eq!(enclave.enclave_name, "enclave");
        assert_eq!(enclave.enclave_id, "i-03483a36345ae8a84-enc19379d3757be809");
        assert_eq!(enclave.process_id, 3970);
        assert_eq!(enclave.enclave_cid, 4);
        assert_eq!(enclave.number_of_cpus, 10);
        assert_eq!(enclave.memory_mib, 25000);
        assert!(matches!(enclave.state, EnclaveState::Running));
        assert_eq!(enclave.flags, "NONE");
        assert_eq!(enclave.cpu_ids, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Test PCR measurements
        assert_eq!(enclave.measurements.hash_algorithm, "Sha384 { ... }");
        assert_eq!(
            enclave.measurements.pcr0,
            "1515c1e20129b2cbc6da0153cc9b2b55d66db36773d825491cee6c61803bc176d13698ce03fec00f33265201da33c162"
        );
        assert_eq!(
            enclave.measurements.pcr1,
            "437d0fa6b3a633f61717acdcce51ff4d6e543b4f60bfc1082465597628a3f5d067d7748084be07d7edc29e7f9cefb444"
        );
        assert_eq!(
            enclave.measurements.pcr2,
            "770afa1c127e014cb646e92bf16ecbe699c853c669ca83651508c3c1c0934dc6d8d8303790e5a0286034e0a1917286a5"
        );
    }
}
