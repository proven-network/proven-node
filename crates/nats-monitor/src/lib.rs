//! Helper crate for querying NATS HTTP monitoring endpoints.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod serde_duration;
mod types;

use std::net::SocketAddr;

pub use error::{Error, Result};
pub use types::*;

use reqwest::Client;
use tracing::info;

/// Helper for querying NATS HTTP monitoring endpoints.
pub struct NatsMonitor {
    client: Client,
    socket_addr: SocketAddr,
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
    pub fn new(socket_addr: SocketAddr) -> Self {
        Self {
            client: Client::new(),
            socket_addr,
        }
    }

    /// Fetches the connection information (Connz) from the NATS server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the HTTP request fails or if the response
    /// cannot be parsed into a `Connz` object.
    pub async fn get_connz(&self) -> Result<Connz> {
        let url = format!("http://{}/connz?subs=1", self.socket_addr);
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
        let url = format!("http://{}/varz", self.socket_addr);
        let response = self.client.get(url).send().await?;
        let json = response.text().await?;
        info!("raw varz: {}", json); // TODO: remove this later
        let varz: Varz = serde_json::from_str(&json)?;
        Ok(varz)
    }

    /// Fetches the route information (Routez) from the NATS server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the HTTP request fails or if the response
    /// cannot be parsed into a `Routez` object.
    pub async fn get_routez(&self) -> Result<Routez> {
        let url = format!("http://{}/routez", self.socket_addr);
        let response = self.client.get(url).send().await?;
        let json = response.text().await?;
        let routez: Routez = serde_json::from_str(&json)?;
        Ok(routez)
    }
}
