//! HTTP proxy service implementation using engine request-reply.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use proven_bootable::Bootable;
use proven_engine::{Client, Message};
use reqwest::Client as HttpClient;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::{Error, Request, Response};

/// HTTP proxy service that processes requests using request-reply.
pub struct ProxyService {
    client: Arc<Client>,
    service_subject: String,
    target_addr: SocketAddr,
    http_client: Arc<HttpClient>,
    handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl ProxyService {
    /// Create a new proxy service.
    #[must_use]
    pub fn new(client: Arc<Client>, stream_prefix: &str, target_addr: SocketAddr) -> Self {
        let service_subject = format!("{stream_prefix}.service");

        Self {
            client,
            service_subject,
            target_addr,
            http_client: Arc::new(HttpClient::new()),
            handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Process incoming requests.
    #[allow(clippy::cognitive_complexity)]
    async fn run_service(self: Arc<Self>) {
        info!(
            "Starting HTTP proxy service for subject '{}'",
            self.service_subject
        );

        // Subscribe to the service subject
        let mut subscription = match self.client.subscribe(&self.service_subject, None).await {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to service subject: {}", e);
                return;
            }
        };

        info!("HTTP proxy service subscribed and ready");

        // Process incoming requests
        while let Some(message) = subscription.next().await {
            if let Some(reply_to) = message.get_header("reply_to") {
                let correlation_id = message.get_header("correlation_id");
                let self_clone = self.clone();
                let reply_to = reply_to.to_string();
                let correlation_id = correlation_id.map(std::string::ToString::to_string);

                // Spawn a task to handle the request asynchronously
                tokio::spawn(async move {
                    if let Err(e) = self_clone
                        .process_request(message, reply_to, correlation_id)
                        .await
                    {
                        error!("Failed to process request: {}", e);
                    }
                });
            } else {
                debug!("Received message without reply_to header, ignoring");
            }
        }

        info!("HTTP proxy service stopped");
    }

    /// Process a single request.
    async fn process_request(
        &self,
        message: Message,
        reply_to: String,
        correlation_id: Option<String>,
    ) -> Result<(), Error> {
        debug!("Processing request");

        // Deserialize request
        let request: Request = ciborium::de::from_reader(message.payload.as_ref())?;

        // Process the request
        let response = self.handle_request(request).await?;

        // Serialize response
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&response, &mut payload)?;

        // Create response message with correlation ID
        let mut response_msg = Message::new(payload);
        if let Some(correlation_id) = correlation_id {
            response_msg = response_msg.with_header("correlation_id", correlation_id);
        }

        // Send response to reply_to subject
        self.client
            .publish(&reply_to, vec![response_msg])
            .await
            .map_err(|e| Error::Service(format!("Failed to send response: {e}")))?;

        Ok(())
    }

    /// Handle an HTTP request by proxying it to the target.
    async fn handle_request(&self, request: Request) -> Result<Response, Error> {
        let base_url = format!("http://{}", self.target_addr);
        debug!(
            "Proxying request: {} {}{}",
            request.method, base_url, request.path
        );

        let mut http_request = self
            .http_client
            .request(
                request.method.clone(),
                format!("{}{}", base_url, request.path),
            )
            .headers(request.headers.clone());

        if let Some(body_bytes) = request.body {
            http_request = http_request.body(body_bytes);
        }

        let proxy_response = http_request.send().await?;

        Ok(Response {
            request_id: request.id,
            headers: proxy_response.headers().clone(),
            status_code: proxy_response.status(),
            body: proxy_response.bytes().await?,
        })
    }
}

#[async_trait]
impl Bootable for ProxyService {
    fn bootable_name(&self) -> &'static str {
        "http-proxy-engine (service)"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting HTTP proxy service...");

        // Start service task
        let service = Arc::new(self.clone());
        let handle = tokio::spawn(async move {
            service.run_service().await;
        });

        *self.handle.write().await = Some(handle);

        info!("HTTP proxy service started successfully");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down HTTP proxy service...");

        let handle = self.handle.write().await.take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }

        info!("HTTP proxy service shut down");
        Ok(())
    }

    async fn wait(&self) {
        loop {
            // Check if handle exists and if task is finished
            let is_finished = {
                let handle_guard = self.handle.read().await;
                handle_guard
                    .as_ref()
                    .is_none_or(tokio::task::JoinHandle::is_finished)
            };

            if is_finished {
                return;
            }

            // Sleep briefly before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Clone for ProxyService {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            service_subject: self.service_subject.clone(),
            target_addr: self.target_addr,
            http_client: self.http_client.clone(),
            handle: Arc::new(RwLock::new(None)),
        }
    }
}
