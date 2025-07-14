//! Monitoring and observability for the consensus system
//!
//! This module provides monitoring views, metrics collection,
//! and health checking functionality for the consensus system.

pub mod coordinator;
pub mod health;
pub mod metrics;
pub mod views;

pub use views::{GroupHealth, NodeHealth, StreamHealth, SystemView};

pub use metrics::{MetricsCollector, MetricsRegistry};

pub use health::HealthReport;

pub use coordinator::MonitoringCoordinator;
