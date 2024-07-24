mod error;
mod types;

pub use error::{Error, Result};
pub use types::Varz;

use httpclient::{Client, InMemoryResponseExt};

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

    pub async fn get_varz(&self) -> Result<Varz> {
        let response = self.client.get("/varz").await?;
        let varz: Varz = serde_json::from_str(&response.text()?)?;
        Ok(varz)
    }

    // Implement similar async functions for Connz, Routez, Subz, etc.
}
