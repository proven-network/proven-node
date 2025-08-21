//! Server for providing time to enclave via vsock.

use crate::constants::AWS_TIME_SYNC_MAX_UNCERTAINTY_US;
#[cfg(target_os = "linux")]
use crate::error::Error;
use crate::error::Result;
use crate::messages::{TimeRequest, TimeResponse};
use chrono::Utc;
use proven_vsock_rpc::{HandlerResponse, MessagePattern, RpcHandler, RpcServer, ServerConfig};
use tracing::{debug, error, warn};

#[cfg(target_os = "linux")]
use chrony_candm::reply::ReplyBody;
#[cfg(target_os = "linux")]
use chrony_candm::request::RequestBody;
#[cfg(target_os = "linux")]
use chrony_candm::{ClientOptions, UnixDatagramClient};

/// Time server that provides high-precision time.
pub struct TimeServer {
    #[cfg(target_os = "linux")]
    addr: tokio_vsock::VsockAddr,
    #[cfg(not(target_os = "linux"))]
    addr: std::net::SocketAddr,
    handler: TimeHandler,
}

impl TimeServer {
    /// Create a new time server.
    #[must_use]
    pub const fn new(
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> Self {
        Self {
            addr,
            handler: TimeHandler::new(),
        }
    }

    /// Start serving time requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to start or serve.
    pub async fn serve(self) -> Result<()> {
        let server = RpcServer::new(self.addr, self.handler, ServerConfig::default());
        server.serve().await.map_err(Into::into)
    }
}

/// Handler for time requests.
struct TimeHandler;

impl TimeHandler {
    const fn new() -> Self {
        Self
    }

    #[cfg(target_os = "linux")]
    async fn get_chrony_time(&self) -> Result<(chrono::DateTime<Utc>, f64, u8)> {
        let mut client = UnixDatagramClient::new()
            .await
            .map_err(|e| Error::Chrony(format!("Failed to create client: {e}")))?;

        let reply = client
            .query(RequestBody::Tracking, ClientOptions::default())
            .await
            .map_err(|e| Error::Chrony(format!("Query failed: {e}")))?;

        let ReplyBody::Tracking(tracking) = reply.body else {
            return Err(Error::Chrony("Unexpected reply from chrony".to_string()));
        };

        // Calculate current time with chrony's offset
        let system_time = Utc::now();
        let offset_us = (f64::from(tracking.current_correction) * 1_000_000.0) as i64;
        let corrected_time = system_time + chrono::Duration::microseconds(offset_us);

        // Calculate uncertainty using chrony's formula
        let uncertainty_us = (f64::from(tracking.root_delay) / 2.0
            + f64::from(tracking.root_dispersion))
            * 1_000_000.0;

        debug!(
            "Chrony: offset={:.3}ms, uncertainty={:.0}μs, stratum={}",
            f64::from(tracking.current_correction) * 1000.0,
            uncertainty_us,
            tracking.stratum
        );

        Ok((corrected_time, uncertainty_us, tracking.stratum as u8))
    }

    #[cfg(not(target_os = "linux"))]
    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn get_chrony_time(&self) -> Result<(chrono::DateTime<Utc>, f64, u8)> {
        warn!("Chrony not available on non-Linux platforms; using system time");
        Ok((Utc::now(), 1000.0, 10)) // 1ms uncertainty, stratum 10 (unreliable)
    }
}

#[async_trait::async_trait]
impl RpcHandler for TimeHandler {
    async fn handle_message(
        &self,
        message_id: &str,
        payload: bytes::Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        // Check message ID
        if message_id != crate::messages::TIME_RPC_MESSAGE_ID {
            // Return empty response for unknown messages
            return Ok(HandlerResponse::None);
        }

        // Deserialize request
        let request: TimeRequest = match bincode::deserialize(&payload) {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to deserialize time request: {}", e);
                let error_response = TimeResponse {
                    stratum: 16,
                    timestamp: Utc::now(),
                    uncertainty_us: 1_000_000.0,
                };
                return Ok(HandlerResponse::Single(
                    bincode::serialize(&error_response)
                        .unwrap_or_default()
                        .into(),
                ));
            }
        };

        debug!("Received time request: {:?}", request);

        // Get time from chrony
        #[cfg(target_os = "linux")]
        let (timestamp, uncertainty_us, stratum) = match self.get_chrony_time().await {
            Ok(time) => time,
            Err(e) => {
                error!("Failed to get chrony time: {}", e);
                // Fall back to system time
                (Utc::now(), AWS_TIME_SYNC_MAX_UNCERTAINTY_US, 16)
            }
        };

        #[cfg(not(target_os = "linux"))]
        let (timestamp, uncertainty_us, stratum) = match self.get_chrony_time() {
            Ok(time) => time,
            Err(e) => {
                error!("Failed to get chrony time: {}", e);
                // Fall back to system time
                (Utc::now(), AWS_TIME_SYNC_MAX_UNCERTAINTY_US, 16)
            }
        };

        let response = TimeResponse {
            stratum,
            timestamp,
            uncertainty_us,
        };

        // Serialize response
        match bincode::serialize(&response) {
            Ok(data) => Ok(HandlerResponse::Single(data.into())),
            Err(e) => {
                error!("Failed to serialize time response: {}", e);
                let error_response = TimeResponse {
                    stratum: 16,
                    timestamp: Utc::now(),
                    uncertainty_us: 1_000_000.0,
                };
                Ok(HandlerResponse::Single(
                    bincode::serialize(&error_response)
                        .unwrap_or_default()
                        .into(),
                ))
            }
        }
    }
}
