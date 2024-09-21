mod error;
pub mod generated;
mod networks;

pub use error::{Error, Result};
pub use networks::*;

pub struct Client {
    inner_client: generated::LowLevelClient,
}

impl Client {
    pub fn new(
        network: Network,
        application_name: Option<String>,
        dapp_definition: Option<String>,
    ) -> Result<Self> {
        match RADIX_NETWORK_CONFIG.get(&network) {
            Some(network_config) => {
                let mut default_headers = Vec::<(String, String)>::new();

                if let Some(app_name) = application_name {
                    default_headers.push(("Rdx-App-Name".to_string(), app_name));
                }

                if let Some(def) = dapp_definition {
                    default_headers.push(("Rdx-App-Dapp-Definition".to_string(), def));
                }

                let httpclient = httpclient::Client::new()
                    .base_url(network_config.gateway_url)
                    .default_headers(default_headers.into_iter());

                Ok(Self {
                    inner_client: generated::LowLevelClient::new_with(httpclient),
                })
            }
            None => Err(Error::NetworkInvalid),
        }
    }

    pub fn get_inner_client(&self) -> &generated::LowLevelClient {
        &self.inner_client
    }
}
