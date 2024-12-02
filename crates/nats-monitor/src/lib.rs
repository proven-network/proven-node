//! Helper crate for querying NATS HTTP monitoring endpoints.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod types;

pub use error::{Error, Result};
pub use types::*;

use httpclient::{Client, InMemoryResponseExt};
use tracing::info;

/// Helper for querying NATS HTTP monitoring endpoints.
pub struct NatsMonitor {
    client: Client,
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
            client: Client::new().base_url(format!("http://localhost:{monitoring_port}").as_str()),
        }
    }

    /// Fetches the connection information (Connz) from the NATS server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the HTTP request fails or if the response
    /// cannot be parsed into a `Connz` object.
    pub async fn get_connz(&self) -> Result<Connz> {
        let response = self.client.get("/connz?subs=1").await?;
        let json = response.text()?;
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
        let response = self.client.get("/varz").await?;
        let json = response.text()?;
        info!("raw varz: {}", json); // TODO: remove this later
        let varz: Varz = serde_json::from_str(&json)?;
        Ok(varz)
    }

    // TODO: Implement similar async functions for Connz, Routez, Subz, etc.
}
