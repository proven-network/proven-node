//! Metrics collection for the event system

use dashmap::DashMap;
use prometheus::{
    CounterVec, HistogramVec, IntGaugeVec, register_counter_vec, register_histogram_vec,
    register_int_gauge_vec,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::types::TypeStats;

lazy_static::lazy_static! {
    static ref EVENT_COUNTER: CounterVec = register_counter_vec!(
        "events_v2_total",
        "Total number of events by type and status",
        &["event_type", "status"]
    ).unwrap();

    static ref EVENT_DURATION: HistogramVec = register_histogram_vec!(
        "events_v2_duration_seconds",
        "Event processing duration in seconds",
        &["event_type"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    ).unwrap();

    static ref QUEUE_DEPTH: IntGaugeVec = register_int_gauge_vec!(
        "events_v2_queue_depth",
        "Current depth of event queues",
        &["event_type", "priority"]
    ).unwrap();

    static ref IN_FLIGHT: IntGaugeVec = register_int_gauge_vec!(
        "events_v2_in_flight",
        "Number of events currently being processed",
        &["event_type"]
    ).unwrap();
}

/// Tracks metrics for the event system
pub struct MetricsCollector {
    /// Per-type statistics
    type_stats: Arc<DashMap<&'static str, Arc<TypeStats>>>,
    /// Whether Prometheus metrics are enabled
    prometheus_enabled: bool,
}

impl MetricsCollector {
    pub fn new(prometheus_enabled: bool) -> Self {
        Self {
            type_stats: Arc::new(DashMap::new()),
            prometheus_enabled,
        }
    }

    /// Get or create stats for a type
    pub fn get_type_stats(&self, type_name: &'static str) -> Arc<TypeStats> {
        self.type_stats
            .entry(type_name)
            .or_insert_with(|| Arc::new(TypeStats::default()))
            .clone()
    }

    /// Record that an event was sent
    pub fn record_sent(&self, type_name: &'static str) {
        let stats = self.get_type_stats(type_name);
        stats.record_sent();

        if self.prometheus_enabled {
            EVENT_COUNTER.with_label_values(&[type_name, "sent"]).inc();
            IN_FLIGHT.with_label_values(&[type_name]).inc();
        }
    }

    /// Record successful processing
    pub fn record_success(&self, type_name: &'static str, start_time: Instant) {
        let duration = start_time.elapsed();
        let duration_us = duration.as_micros() as u64;

        let stats = self.get_type_stats(type_name);
        stats.record_success(duration_us);

        if self.prometheus_enabled {
            EVENT_COUNTER
                .with_label_values(&[type_name, "success"])
                .inc();
            EVENT_DURATION
                .with_label_values(&[type_name])
                .observe(duration.as_secs_f64());
            IN_FLIGHT.with_label_values(&[type_name]).dec();
        }
    }

    /// Record processing error
    pub fn record_error(&self, type_name: &'static str) {
        let stats = self.get_type_stats(type_name);
        stats.record_error();

        if self.prometheus_enabled {
            EVENT_COUNTER.with_label_values(&[type_name, "error"]).inc();
            IN_FLIGHT.with_label_values(&[type_name]).dec();
        }
    }

    /// Record timeout
    pub fn record_timeout(&self, type_name: &'static str) {
        let stats = self.get_type_stats(type_name);
        stats.record_timeout();

        if self.prometheus_enabled {
            EVENT_COUNTER
                .with_label_values(&[type_name, "timeout"])
                .inc();
            IN_FLIGHT.with_label_values(&[type_name]).dec();
        }
    }

    /// Update queue depth metric
    pub fn update_queue_depth(&self, type_name: &'static str, priority: &str, depth: usize) {
        if self.prometheus_enabled {
            QUEUE_DEPTH
                .with_label_values(&[type_name, priority])
                .set(depth as i64);
        }
    }

    /// Get a summary of all statistics
    pub fn get_summary(&self) -> HashMap<&'static str, TypeStatsSummary> {
        self.type_stats
            .iter()
            .map(|entry| {
                let stats = entry.value();
                let summary = TypeStatsSummary {
                    sent_count: stats.sent_count.load(std::sync::atomic::Ordering::Relaxed),
                    success_count: stats
                        .success_count
                        .load(std::sync::atomic::Ordering::Relaxed),
                    error_count: stats.error_count.load(std::sync::atomic::Ordering::Relaxed),
                    timeout_count: stats
                        .timeout_count
                        .load(std::sync::atomic::Ordering::Relaxed),
                    average_duration_us: stats.average_duration_us(),
                    in_flight: stats.in_flight.load(std::sync::atomic::Ordering::Relaxed),
                };
                (*entry.key(), summary)
            })
            .collect()
    }
}

/// Summary of statistics for a type
#[derive(Debug, Clone)]
pub struct TypeStatsSummary {
    pub sent_count: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub timeout_count: u64,
    pub average_duration_us: f64,
    pub in_flight: u64,
}

/// RAII guard for tracking event processing
pub struct MetricsGuard<'a> {
    collector: &'a MetricsCollector,
    type_name: &'static str,
    start_time: Instant,
    completed: bool,
}

impl<'a> MetricsGuard<'a> {
    pub fn new(collector: &'a MetricsCollector, type_name: &'static str) -> Self {
        collector.record_sent(type_name);
        Self {
            collector,
            type_name,
            start_time: Instant::now(),
            completed: false,
        }
    }

    /// Mark as successfully completed
    pub fn success(mut self) {
        self.completed = true;
        self.collector
            .record_success(self.type_name, self.start_time);
    }

    /// Mark as failed
    pub fn error(mut self) {
        self.completed = true;
        self.collector.record_error(self.type_name);
    }

    /// Mark as timed out
    pub fn timeout(mut self) {
        self.completed = true;
        self.collector.record_timeout(self.type_name);
    }
}

impl<'a> Drop for MetricsGuard<'a> {
    fn drop(&mut self) {
        if !self.completed {
            // If guard is dropped without explicit completion, assume error
            self.collector.record_error(self.type_name);
        }
    }
}
