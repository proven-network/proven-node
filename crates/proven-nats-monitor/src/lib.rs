mod error;
mod types;

pub use error::{Error, Result};
pub use types::{Connz, Varz};

use httpclient::{Client, InMemoryResponseExt};
use tracing::info;

pub struct NatsMonitor {
    client: Client,
}

impl NatsMonitor {
    pub fn new(monitoring_port: u32) -> Self {
        Self {
            client: Client::new()
                .base_url(format!("http://localhost:{}", monitoring_port).as_str()),
        }
    }

    pub async fn get_connz(&self) -> Result<Connz> {
        let response = self.client.get("/connz?subs=1").await?;
        let json = response.text()?;
        info!("raw connz: {}", json); // TODO: remove this later
        let connz: Connz = serde_json::from_str(&json)?;
        Ok(connz)
    }

    pub async fn get_varz(&self) -> Result<Varz> {
        let response = self.client.get("/varz").await?;
        let json = response.text()?;
        info!("raw varz: {}", json); // TODO: remove this later
        let varz: Varz = serde_json::from_str(&json)?;
        Ok(varz)
    }

    // Implement similar async functions for Connz, Routez, Subz, etc.
}