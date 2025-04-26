use crate::error::Error;

use std::net::IpAddr;

/// Options for the NATS proxy service.
pub struct NatsProxyServiceOptions {
    /// The name of the service listen for requests on.
    service_name: String,

    /// The address to forward requests to.
    target_addr: IpAddr,
}

/// The NATS proxy service.
#[allow(dead_code)]
pub struct NatsProxyService {
    /// The name of the service listen for requests on.
    service_name: String,

    /// The address to forward requests to.
    target_addr: IpAddr,
}

impl NatsProxyService {
    /// Create a new NATS proxy service.
    pub fn new(
        NatsProxyServiceOptions {
            target_addr,
            service_name,
        }: NatsProxyServiceOptions,
    ) -> Self {
        Self {
            target_addr,
            service_name,
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
