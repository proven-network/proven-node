use std::net::SocketAddr;

use crate::error::Error;
use crate::request::Request;
use crate::service_handler::HttpServiceHandler;
use crate::{DeserializeError, SERVICE_NAME, SerializeError};

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_logger::{error, info};
use proven_messaging::service::Service;
use proven_messaging::stream::InitializedStream;

type MessagingService<S> =
    <S as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
        HttpServiceHandler,
    >;

/// Options for the HTTP proxy service.
pub struct HttpProxyServiceOptions<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// The options for the service.
    pub service_options: <MessagingService<S> as Service<
        HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// The stream to listen for requests on.
    pub stream: S,

    /// The address to forward requests to.
    pub target_addr: SocketAddr,
}

/// The HTTP proxy service.
#[derive(Clone)]
pub struct HttpProxyService<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// The messaging service.
    service: MessagingService<S>,
}

impl<S> HttpProxyService<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// Create a new HTTP proxy service.
    ///
    /// # Errors
    ///
    /// Returns an error if the service setup fails.
    pub async fn new(
        HttpProxyServiceOptions {
            service_options,
            stream,
            target_addr,
        }: HttpProxyServiceOptions<S>,
    ) -> Result<Self, Error> {
        let service = stream
            .service::<_, HttpServiceHandler>(
                SERVICE_NAME.to_string(),
                service_options.clone(),
                HttpServiceHandler::new(target_addr),
            )
            .await
            .map_err(|e| {
                error!("HTTP proxy service setup failed: {e}");
                Error::Service(e.to_string())
            })?;

        Ok(Self { service })
    }
}

#[async_trait]
impl<S> Bootable for HttpProxyService<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    fn bootable_name(&self) -> &'static str {
        "http-proxy (service)"
    }

    /// Start the HTTP proxy.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("http proxy service starting...");

        self.service.start().await.map_err(|e| {
            error!("HTTP proxy service setup failed: {e}");
            Box::new(Error::Service(e.to_string()))
        })?;

        Ok(())
    }

    /// Shutdown the HTTP proxy service.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("http proxy service shutting down...");

        self.service.shutdown().await.map_err(|e| {
            error!("HTTP proxy service shutdown failed: {e}");
            Box::new(Error::Service(e.to_string()))
        })?;

        Ok(())
    }

    /// Wait for the HTTP proxy to exit.
    async fn wait(&self) {
        self.service.wait().await;
    }
}
