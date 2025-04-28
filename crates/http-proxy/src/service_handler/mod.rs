mod error;

use error::Error;

use crate::request::Request;
use crate::response::Response;
use crate::{DeserializeError, SerializeError};

use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use reqwest::Client;
use tracing::debug;

/// A service handler that proxies HTTP requests to a target service.
#[derive(Clone, Debug)]
pub struct HttpServiceHandler {
    base_url: String,
    client: Arc<Client>,
}

impl HttpServiceHandler {
    pub fn new(target_port: u16) -> Self {
        Self {
            base_url: format!("http://localhost:{}", target_port),
            client: Arc::new(Client::new()),
        }
    }
}

#[async_trait]
impl ServiceHandler<Request, DeserializeError, SerializeError> for HttpServiceHandler {
    type Error = Error;
    type ResponseType = Response;
    type ResponseDeserializationError = DeserializeError;
    type ResponseSerializationError = SerializeError;

    /// Handle a request by proxying it to the target address.
    async fn handle<R>(
        &self,
        request: Request,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                Request,
                DeserializeError,
                SerializeError,
                Self::ResponseType,
                DeserializeError,
                SerializeError,
            >,
    {
        debug!(
            "proxying request: {} {}{}",
            request.method, self.base_url, request.path
        );

        let mut http_request = self
            .client
            .request(
                request.method.clone(),
                format!("{}{}", self.base_url, request.path),
            )
            .headers(request.headers);

        if let Some(body_bytes) = request.body {
            http_request = http_request.body(body_bytes);
        }

        let proxy_response = http_request.send().await?;

        Ok(responder
            .reply_and_delete_request(Response {
                headers: proxy_response.headers().clone(),
                status_code: proxy_response.status(),
                body: proxy_response.bytes().await?,
            })
            .await)
    }
}
