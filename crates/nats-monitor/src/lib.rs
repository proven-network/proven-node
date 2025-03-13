//! Helper crate for querying NATS HTTP monitoring endpoints.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod types;

pub use error::{Error, Result};
pub use types::*;

use reqwest::Client;
use tracing::info;

/// Helper for querying NATS HTTP monitoring endpoints.
pub struct NatsMonitor {
    client: Client,
    monitoring_port: u32,
}

impl NatsMonitor {
    /// Creates a new `NatsMonitor` instance with the specified monitoring port.
    ///
    /// # Arguments
    ///
    /// * `monitoring_port` - The port on which the NATS server is running.
    ///
    /// # Returns
    ///
    /// A new `NatsMonitor` instance.
    #[must_use]
    pub fn new(monitoring_port: u32) -> Self {
        Self {
            client: Client::new(),
            monitoring_port,
        }
    }

    /// Fetches the connection information (Connz) from the NATS server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the HTTP request fails or if the response
    /// cannot be parsed into a `Connz` object.
    pub async fn get_connz(&self) -> Result<Connz> {
        let url = format!("http://localhost:{}/connz?subs=1", self.monitoring_port);
        let response = self.client.get(url).send().await?;
        let json = response.text().await?;
        info!("raw connz: {}", json); // TODO: remove this later
        let connz: Connz = serde_json::from_str(&json)?;
        Ok(connz)
    }

    /// Fetches the server information (Varz) from the NATS server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the HTTP request fails or if the response
    /// cannot be parsed into a `Varz` object.
    pub async fn get_varz(&self) -> Result<Varz> {
        let url = format!("http://localhost:{}/varz", self.monitoring_port);
        let response = self.client.get(url).send().await?;
        let json = response.text().await?;
        info!("raw varz: {}", json); // TODO: remove this later
        let varz: Varz = serde_json::from_str(&json)?;
        Ok(varz)
    }

    // TODO: Implement similar async functions for Connz, Routez, Subz, etc.
}
