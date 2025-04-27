use crate::error::Error;
use crate::request::Request;
use crate::service_handler::HttpServiceHandler;
use crate::{DeserializeError, SerializeError};

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_messaging::service::Service;
use proven_messaging::stream::InitializedStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
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
#[derive(Clone)]
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

    /// The shutdown token.
    shutdown_token: CancellationToken,

    /// The stream to listen for requests on.
    stream: S,

    /// The port to forward requests to.
    target_port: u16,

    /// The task tracker.
    task_tracker: TaskTracker,
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
            shutdown_token: CancellationToken::new(),
            stream,
            target_port,
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl<S> Bootable for HttpProxyService<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    type Error = Error;

    /// Start the HTTP proxy.
    async fn start(&self) -> Result<(), Error> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        info!("Initializing HTTP proxy service...");

        // TODO: Serivces and consumers should probably become bootables
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

        self.task_tracker.close();

        Ok(())
    }

    /// Shutdown the HTTP proxy service.
    async fn shutdown(&self) -> Result<(), Error> {
        info!("http proxy service shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http proxy service shutdown");

        Ok(())
    }

    /// Wait for the HTTP proxy to exit.
    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
