use crate::error::Error;

/// Options for the NATS proxy service.
pub struct NatsProxyServiceOptions {
    /// The name of the service listen for requests on.
    service_name: String,

    /// The port to forward requests to.
    target_port: u16,
}

/// The NATS proxy service.
#[allow(dead_code)]
pub struct NatsProxyService {
    /// The name of the service listen for requests on.
    service_name: String,

    /// The port to forward requests to.
    target_port: u16,
}

impl NatsProxyService {
    /// Create a new NATS proxy service.
    pub fn new(
        NatsProxyServiceOptions {
            service_name,
            target_port,
        }: NatsProxyServiceOptions,
    ) -> Self {
        Self {
            service_name,
            target_port,
        }
    }

    /// Start the NATS proxy.
    pub async fn start(&self) -> Result<(), Error> {
        todo!()
    }

    /// Shutdown the NATS proxy.
    pub async fn shutdown(&self) -> Result<(), Error> {
        todo!()
    }
}
