use crate::error::Error;
use crate::request::Request;
use crate::service_handler::HttpServiceHandler;
use crate::{DeserializeError, SerializeError};

use proven_messaging::service::Service;
use proven_messaging::stream::InitializedStream;
use tracing::{error, info};

type MessagingService<S> =
    <S as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
        HttpServiceHandler,
    >;

/// Options for the HTTP proxy service.
pub struct HttpProxyServiceOptions<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// The name of the service.
    pub service_name: String,

    /// The options for the service.
    pub service_options: <MessagingService<S> as Service<
        HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// The stream to listen for requests on.
    pub stream: S,

    /// The port to forward requests to.
    pub target_port: u16,
}

/// The HTTP proxy service.
pub struct HttpProxyService<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// The name of the service.
    service_name: String,

    /// The options for the service.
    service_options: <MessagingService<S> as Service<
        HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// The stream to listen for requests on.
    stream: S,

    /// The port to forward requests to.
    target_port: u16,
}

impl<S> HttpProxyService<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// Create a new HTTP proxy service.
    pub fn new(
        HttpProxyServiceOptions {
            service_name,
            service_options,
            stream,
            target_port,
        }: HttpProxyServiceOptions<S>,
    ) -> Self {
        Self {
            service_name,
            service_options,
            stream,
            target_port,
        }
    }

    /// Start the HTTP proxy.
    pub async fn start(&self) -> Result<(), Error> {
        info!("Initializing HTTP proxy service...");

        let _messaging_service = self
            .stream
            .start_service::<_, HttpServiceHandler>(
                self.service_name.clone(),
                self.service_options.clone(),
                HttpServiceHandler::new(self.target_port),
            )
            .await
            .map_err(|e| {
                error!("HTTP proxy service setup failed: {}", e);
                Error::Service(e.to_string())
            })?;

        Ok(())
    }

    /// Shutdown the HTTP proxy.
    pub async fn shutdown(&self) -> Result<(), Error> {
        todo!()
    }
}
