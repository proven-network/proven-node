//! HTTP proxy service implementation using engine streams.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_engine::{Client, Message, StreamConfig};
use proven_storage::LogIndex;
use reqwest::Client as HttpClient;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{Error, Request, Response};

/// Metadata keys for request/response correlation
const REQUEST_ID_KEY: &str = "request_id";

/// HTTP proxy service that processes requests from a stream.
pub struct ProxyService {
    client: Arc<Client>,
    stream_name: String,
    target_addr: SocketAddr,
    http_client: Arc<HttpClient>,
    last_sequence: Arc<RwLock<Option<LogIndex>>>,
    handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl ProxyService {
    /// Create a new proxy service.
    #[must_use]
    pub fn new(client: Arc<Client>, stream_prefix: &str, target_addr: SocketAddr) -> Self {
        let stream_name = format!("{stream_prefix}.requests");

        Self {
            client,
            stream_name,
            target_addr,
            http_client: Arc::new(HttpClient::new()),
            last_sequence: Arc::new(RwLock::new(None)),
            handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Process incoming requests.
    #[allow(clippy::cognitive_complexity)]
    async fn run_service(self: Arc<Self>) {
        use tokio::pin;
        use tokio_stream::StreamExt;

        info!(
            "Starting HTTP proxy service for stream '{}'",
            self.stream_name
        );

        let start_sequence = LogIndex::new(1).unwrap();

        loop {
            match self
                .client
                .stream_messages(self.stream_name.clone(), start_sequence, None)
                .await
            {
                Ok(stream) => {
                    pin!(stream);

                    while let Some((message, _timestamp, sequence)) = stream.next().await {
                        if let Err(e) = self.process_message(message, sequence).await {
                            error!("Failed to process message: {}", e);
                            // Continue processing other messages
                        }
                    }

                    // Stream ended, will retry
                    info!("Request stream ended, will retry");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    error!("Error starting request stream: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Process a single message.
    async fn process_message(&self, message: Message, sequence: u64) -> Result<(), Error> {
        debug!("Processing message at sequence {}", sequence);

        // Update last processed sequence
        {
            let mut last = self.last_sequence.write().await;
            *last = Some(LogIndex::new(sequence).unwrap());
        }

        // Extract request ID from headers
        let _request_id = message
            .headers
            .iter()
            .find(|(k, _)| k == REQUEST_ID_KEY)
            .and_then(|(_, v)| Uuid::parse_str(v).ok())
            .ok_or_else(|| Error::Deserialization("Missing or invalid request ID".to_string()))?;

        // Deserialize request
        let request: Request = ciborium::de::from_reader(message.payload.as_ref())?;

        // Process the request
        let response = self.handle_request(request).await?;

        // Publish response to the response stream
        self.publish_response(response).await?;

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

    /// Publish a response to the response stream.
    async fn publish_response(&self, response: Response) -> Result<(), Error> {
        let response_stream = format!(
            "{}.responses",
            self.stream_name.trim_end_matches(".requests")
        );

        // Serialize response
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&response, &mut payload)?;

        // Create message with headers
        let message =
            Message::new(payload).with_header(REQUEST_ID_KEY, response.request_id.to_string());

        // Publish to response stream
        self.client
            .publish_to_stream(response_stream, vec![message])
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Bootable for ProxyService {
    fn bootable_name(&self) -> &'static str {
        "http-proxy-engine (service)"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting HTTP proxy service...");

        // Ensure streams exist
        let response_stream = format!(
            "{}.responses",
            self.stream_name.trim_end_matches(".requests")
        );

        let stream_config = StreamConfig::default();

        let request_stream_exists = match self.client.get_stream_info(&self.stream_name).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !request_stream_exists {
            self.client
                .create_stream(self.stream_name.clone(), stream_config.clone())
                .await?;
        }

        let response_stream_exists = match self.client.get_stream_info(&response_stream).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !response_stream_exists {
            self.client
                .create_stream(response_stream.clone(), stream_config)
                .await?;
        }

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
        if let Some(_handle) = &*self.handle.read().await {
            // Don't await the handle, just check if it's still running
            // The handle represents the running service task
        }
    }
}

impl Clone for ProxyService {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            stream_name: self.stream_name.clone(),
            target_addr: self.target_addr,
            http_client: self.http_client.clone(),
            last_sequence: Arc::new(RwLock::new(None)),
            handle: Arc::new(RwLock::new(None)),
        }
    }
}
