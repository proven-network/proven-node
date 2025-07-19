//! Monitoring service for consensus system observability
//!
//! This service provides monitoring views, metrics collection,
//! and health checking functionality for the consensus system.
//!
//! ## Overview
//!
//! The monitoring service is responsible for:
//! - Collecting and aggregating metrics from consensus components
//! - Providing health status for the system
//! - Offering read-only views for decision-making
//! - Managing Prometheus metrics if enabled
//!
//! ## Architecture
//!
//! The service follows a pull-based model where:
//! - Components expose their metrics and state
//! - The monitoring service periodically collects this data
//! - Aggregated views are made available for querying
//!
//! ## Usage
//!
//! ```rust,ignore
//! let monitoring_service = MonitoringService::new(config);
//! monitoring_service.start().await?;
//!
//! // Get system health
//! let health = monitoring_service.get_health().await?;
//!
//! // Get specific views
//! let stream_health = monitoring_service.get_stream_view("my-stream").await?;
//! ```

mod health;
mod messages;
mod metrics;
mod service;
mod types;
mod views;

pub use messages::{MonitoringServiceMessage, MonitoringServiceResponse};
pub use metrics::{MetricsCollector, MetricsRegistry};
pub use service::{MonitoringConfig, MonitoringService};
pub use types::{
    ComponentHealth, GroupHealth, HealthReport, HealthStatus, MonitoringError, MonitoringResult,
    NodeHealth, StreamHealth, SystemHealth,
};
pub use views::SystemView;
