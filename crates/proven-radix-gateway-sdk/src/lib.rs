#![allow(clippy::result_large_err)]

mod error;
pub mod generated;

pub use error::{Error, Result};

pub struct Client {
    inner_client: generated::LowLevelClient,
}

impl Client {
    pub fn new(
        gateway_url: &str,
        application_name: Option<String>,
        dapp_definition: Option<String>,
    ) -> Result<Self> {
        let mut default_headers = Vec::<(String, String)>::new();

        if let Some(app_name) = application_name {
            default_headers.push(("Rdx-App-Name".to_string(), app_name));
        }

        if let Some(def) = dapp_definition {
            default_headers.push(("Rdx-App-Dapp-Definition".to_string(), def));
        }

        let httpclient = httpclient::Client::new()
            .base_url(gateway_url)
            .default_headers(default_headers.into_iter());

        Ok(Self {
            inner_client: generated::LowLevelClient::new_with(httpclient),
        })
    }

    pub fn get_inner_client(&self) -> &generated::LowLevelClient {
        &self.inner_client
    }
}
