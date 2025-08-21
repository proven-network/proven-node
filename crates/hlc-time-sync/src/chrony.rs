//! Chrony source management for multi-source time validation using UDP.

use chrono::{DateTime, Utc};
use chrony_candm::common::ChronyAddr;
use chrony_candm::reply::ReplyBody;
use chrony_candm::request::{RequestBody, SourceData, SourceStats};
use chrony_candm::{ClientOptions, blocking_query};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tracing::{debug, warn};

use crate::error::{Error, Result};

/// Default Chrony command port (IANA registered for NTP).
pub const DEFAULT_CHRONY_PORT: u16 = 323;

/// AWS Time Sync server address (link-local, only works in EC2/Nitro).
/// For non-EC2 environments, time.aws.com will be used as fallback.
pub const AWS_TIME_SYNC_ADDR: &str = "169.254.169.123";

/// Statistics for a single NTP source.
#[derive(Debug, Clone)]
pub struct SourceStatistics {
    /// Source IP address
    pub address: String,
    /// Current offset from source in milliseconds  
    pub offset_ms: f64,
    /// Standard deviation in milliseconds
    #[allow(dead_code)]
    pub std_dev_ms: f64,
    /// Number of samples
    #[allow(dead_code)]
    pub samples: u32,
    /// Reachability (0-377 octal, 377 = fully reachable)
    #[allow(dead_code)]
    pub reachability: u16,
    /// Stratum level
    #[allow(dead_code)]
    pub stratum: u16,
}

/// Manages multiple Chrony sources for time validation.
pub struct ChronySourceManager {
    /// Maximum allowed divergence between sources
    max_divergence_ms: f64,
    /// Server address for Chrony
    server_addr: SocketAddr,
    /// Client options
    options: ClientOptions,
}

impl ChronySourceManager {
    /// Create a new source manager with default settings.
    ///
    /// The Chrony server address can be configured via the `CHRONY_CMD_PORT`
    /// environment variable (defaults to 323).
    pub fn new(max_divergence_ms: f64) -> Self {
        // Validate max_divergence_ms is reasonable
        assert!(
            max_divergence_ms > 0.0 && max_divergence_ms <= 1000.0,
            "max_divergence_ms must be between 0 and 1000ms"
        );
        // Check for custom port from environment
        let port = std::env::var("CHRONY_CMD_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(DEFAULT_CHRONY_PORT);

        // Use IPv6 localhost by default (works on both Linux and macOS)
        let server_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port);
        Self::with_server(max_divergence_ms, server_addr)
    }

    /// Create a new source manager with a custom server address.
    pub fn with_server(max_divergence_ms: f64, server_addr: SocketAddr) -> Self {
        debug!("Connecting to chronyd at {}", server_addr);
        Self {
            max_divergence_ms,
            server_addr,
            options: ClientOptions::default(),
        }
    }

    /// Get statistics for all configured sources using UDP.
    pub fn get_all_sources(&self) -> Result<Vec<SourceStatistics>> {
        // First get the number of sources
        let reply = blocking_query(RequestBody::NSources, self.options, &self.server_addr)
            .map_err(|e| Error::Chrony(format!("Failed to query sources: {e}. Make sure chronyd is running and configured to accept connections on {}", self.server_addr)))?;

        let ReplyBody::NSources(n_sources) = reply.body else {
            return Err(Error::Chrony("Unexpected reply type".to_string()));
        };

        let mut sources = Vec::new();

        // Query each source
        for index in 0..n_sources.n_sources {
            let source_data_req = RequestBody::SourceData(SourceData {
                index: index.try_into().unwrap(),
            });

            let reply = blocking_query(source_data_req, self.options, &self.server_addr)
                .map_err(|e| Error::Chrony(format!("Failed to query source {index}: {e}")))?;

            if let ReplyBody::SourceData(data) = reply.body {
                // Get source stats
                let stats_req = RequestBody::SourceStats(SourceStats { index });

                let stats_reply = blocking_query(stats_req, self.options, &self.server_addr)
                    .map_err(|e| {
                        Error::Chrony(format!("Failed to query source stats {index}: {e}"))
                    })?;

                if let ReplyBody::SourceStats(stats) = stats_reply.body {
                    let address = match data.ip_addr {
                        ChronyAddr::V4(bytes) => bytes.to_string(),
                        ChronyAddr::V6(bytes) => bytes.to_string(),
                        ChronyAddr::Unspec | ChronyAddr::Id(_) => "unknown".to_string(),
                    };

                    sources.push(SourceStatistics {
                        address,
                        offset_ms: f64::from(stats.est_offset) * 1000.0,
                        std_dev_ms: f64::from(stats.sd) * 1000.0,
                        samples: stats.n_samples,
                        reachability: data.reachability,
                        stratum: data.stratum,
                    });
                }
            }
        }

        Ok(sources)
    }

    /// Check if a source is AWS Time Sync.
    fn is_aws_time_sync(address: &str) -> bool {
        // Link-local address (used in EC2/Nitro)
        address == AWS_TIME_SYNC_ADDR
        // time.aws.com resolves to various EC2 IPs
        || address.contains("ec2-")
        || address.contains("compute-")
        || address.contains("amazonaws.com")
        // Common AWS IP ranges
        // Note: These are broad prefixes; in production, consider using
        // the AWS IP ranges JSON for precise matching
        || address.starts_with("3.")      // US-East-1
        || address.starts_with("18.")     // US-East-1
        || address.starts_with("52.")     // Multiple regions
        || address.starts_with("54.")     // Multiple regions
        || address.starts_with("35.")     // Multiple regions
        || address.starts_with("13.") // Multiple regions
    }

    /// Check if a source is Cloudflare.
    fn is_cloudflare(address: &str) -> bool {
        address.contains("time.cloudflare.com")
        // Cloudflare IPv4 ranges
        || address.starts_with("162.159.")
        // Cloudflare IPv6
        || address.contains("2606:4700")
    }

    /// Validate that AWS Time Sync and Cloudflare NTS agree.
    pub fn validate_sources(&self) -> Result<(bool, f64)> {
        let sources = self.get_all_sources()?;

        // Find AWS Time Sync sources (could be multiple)
        let aws_sources: Vec<_> = sources
            .iter()
            .filter(|s| Self::is_aws_time_sync(&s.address))
            .collect();

        // Find Cloudflare sources
        let cloudflare_sources: Vec<_> = sources
            .iter()
            .filter(|s| Self::is_cloudflare(&s.address))
            .collect();

        // Use the first reachable AWS source
        let aws_source = aws_sources
            .iter()
            .find(|s| s.samples > 0) // Has samples = reachable
            .or_else(|| aws_sources.first());

        let cloudflare_source = cloudflare_sources.first();

        match (aws_source, cloudflare_source) {
            (Some(aws), Some(cf)) => {
                let divergence = (aws.offset_ms - cf.offset_ms).abs();

                debug!(
                    "Source offsets: AWS({})={:.3}ms, Cloudflare({})={:.3}ms, divergence={:.3}ms",
                    aws.address, aws.offset_ms, cf.address, cf.offset_ms, divergence
                );

                if divergence > self.max_divergence_ms {
                    warn!(
                        "Time sources diverging! AWS offset: {:.3}ms, Cloudflare offset: {:.3}ms, divergence: {:.3}ms",
                        aws.offset_ms, cf.offset_ms, divergence
                    );
                    Ok((false, divergence))
                } else {
                    Ok((true, divergence))
                }
            }
            (None, _) => {
                warn!("No reachable AWS Time Sync source found");
                Err(Error::SourceNotFound("AWS Time Sync"))
            }
            (_, None) => {
                warn!("Cloudflare NTS source not found");
                Err(Error::SourceNotFound("Cloudflare NTS"))
            }
        }
    }

    /// Get current time with validation.
    pub fn get_validated_time(&self) -> Result<(DateTime<Utc>, f64)> {
        // Validate sources agree
        let (valid, divergence) = self.validate_sources()?;

        if !valid {
            return Err(Error::TimeSourceTampering(divergence));
        }

        // Get current tracking info from Chrony
        let tracking = self.get_tracking()?;

        Ok(tracking)
    }

    /// Get just the uncertainty from tracking without validation.
    pub fn get_tracking_uncertainty(&self) -> Result<f64> {
        let reply = blocking_query(RequestBody::Tracking, self.options, &self.server_addr)
            .map_err(|e| Error::Chrony(format!("Query failed: {e}")))?;

        let ReplyBody::Tracking(tracking) = reply.body else {
            return Err(Error::Chrony("Unexpected reply from chrony".to_string()));
        };

        // Calculate uncertainty
        let uncertainty_us = (f64::from(tracking.root_delay) / 2.0
            + f64::from(tracking.root_dispersion))
            * 1_000_000.0;

        // Cap at 1 second for safety (something is very wrong if it's higher)
        Ok(uncertainty_us.min(1_000_000.0))
    }

    /// Get current tracking information using UDP.
    fn get_tracking(&self) -> Result<(DateTime<Utc>, f64)> {
        let reply = blocking_query(RequestBody::Tracking, self.options, &self.server_addr)
            .map_err(|e| Error::Chrony(format!("Query failed: {e}")))?;

        let ReplyBody::Tracking(tracking) = reply.body else {
            return Err(Error::Chrony("Unexpected reply from chrony".to_string()));
        };

        // Calculate current time with chrony's offset
        let system_time = Utc::now();
        #[allow(clippy::cast_possible_truncation)]
        let offset_us = (f64::from(tracking.current_correction) * 1_000_000.0) as i64;
        let corrected_time = system_time + chrono::Duration::microseconds(offset_us);

        // Calculate uncertainty
        let uncertainty_us = (f64::from(tracking.root_delay) / 2.0
            + f64::from(tracking.root_dispersion))
            * 1_000_000.0;

        // Cap at 1 second for safety
        Ok((corrected_time, uncertainty_us.min(1_000_000.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_validation() {
        let manager = ChronySourceManager::new(100.0);

        // Try to get sources, skip test if chronyd not available
        match manager.get_all_sources() {
            Ok(sources) => {
                println!("Found {} sources", sources.len());
                for source in &sources {
                    println!("  {} - offset: {:.3}ms", source.address, source.offset_ms);
                }

                // Try validation
                match manager.validate_sources() {
                    Ok((valid, divergence)) => {
                        println!("Validation result: valid={valid}, divergence={divergence:.3}ms");
                    }
                    Err(e) => {
                        println!("Validation error (expected if sources not configured): {e}");
                    }
                }
            }
            Err(e) => {
                println!("Skipping test - chronyd not available: {e}");
            }
        }
    }
}
