use std::process::Stdio;

use serde::Deserialize;
use tokio::process::Command;

use crate::{Error, Result};

/// Server information from the speedtest.
#[derive(Clone, Debug, Deserialize)]
pub struct Server {
    /// The name of the server.
    pub name: String,

    /// The URL of the server.
    pub url: String,
}

/// Client information from the speedtest.
#[derive(Clone, Debug, Deserialize)]
pub struct Client {
    /// The city of the client.
    pub city: String,

    /// The country of the client.
    pub country: String,

    /// The hostname of the client.
    pub hostname: String,

    /// The IP address of the client.
    pub ip: String,

    /// The location of the client.
    pub loc: String,

    /// The organization of the client.
    pub org: String,

    /// The postal code of the client.
    pub postal: String,

    /// The region of the client.
    pub region: String,

    /// The timezone of the client.
    pub timezone: String,
}

/// Results from running a speedtest.
#[derive(Clone, Debug, Deserialize)]
pub struct SpeedTestResult {
    /// Number of bytes received during the test.
    pub bytes_received: u64,

    /// Number of bytes sent during the test.
    pub bytes_sent: u64,

    /// Information about the client.
    pub client: Client,

    /// Download speed in Mbps.
    pub download: f64,

    /// Jitter in milliseconds.
    pub jitter: f64,

    /// Ping time in milliseconds.
    pub ping: f64,

    /// Information about the server used.
    pub server: Server,

    /// URL to share results.
    pub share: String,

    /// The timestamp of the test.
    pub timestamp: String,

    /// Upload speed in Mbps.
    pub upload: f64,
}

/// A client for running network speed tests.
pub struct SpeedTest;

impl SpeedTest {
    /// Creates a new SpeedTest instance.
    pub fn new() -> Self {
        Self
    }

    /// Runs a network speed test and returns the results.
    ///
    /// # Errors
    ///
    /// This function will return an error if the speedtest command fails to execute
    /// or if the output cannot be parsed.
    pub async fn run(&self) -> Result<Vec<SpeedTestResult>> {
        let output = Command::new("librespeed-cli")
            .arg("--json")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .output()
            .await
            .map_err(|e| Error::Io("failed to run speedtest", e))?;

        if !output.status.success() {
            return Err(Error::NonZeroExit("librespeed-cli", output.status));
        }

        let stdout = String::from_utf8(output.stdout)?;
        let results: Vec<SpeedTestResult> = serde_json::from_str(&stdout).map_err(Error::Json)?;

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_speedtest_results() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let fixture_path = PathBuf::from(manifest_dir)
            .join("test_data")
            .join("speedtest.json");

        let fixture = std::fs::read_to_string(fixture_path).unwrap();

        // Test deserialization
        let results: Vec<SpeedTestResult> = serde_json::from_str(&fixture).unwrap();
        let result = &results[0];

        assert_eq!(result.timestamp, "2024-12-03T16:30:53.635342996Z");
        assert_eq!(result.bytes_sent, 534380544);
        assert_eq!(result.bytes_received, 784465920);
        assert_eq!(result.ping, 10.0);
        assert_eq!(result.jitter, 0.0);
        assert_eq!(result.upload, 274.0);
        assert_eq!(result.download, 402.26);
        assert_eq!(result.share, "");

        // Test server info
        assert_eq!(result.server.name, "Chicago, USA (Sharktech)");
        assert_eq!(result.server.url, "https://chispeed.sharktech.net");

        // Test client info is empty (as per fixture)
        assert_eq!(result.client.ip, "");
        assert_eq!(result.client.hostname, "");
        assert_eq!(result.client.city, "");
        assert_eq!(result.client.region, "");
        assert_eq!(result.client.country, "");
        assert_eq!(result.client.loc, "");
        assert_eq!(result.client.org, "");
        assert_eq!(result.client.postal, "");
        assert_eq!(result.client.timezone, "");
    }
}
