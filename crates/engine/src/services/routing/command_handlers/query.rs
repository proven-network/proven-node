//! Query command handlers for routing service

use async_trait::async_trait;
use std::sync::Arc;
use std::time::SystemTime;

use crate::foundation::events::{EventMetadata, RequestHandler};
use crate::services::routing::{
    GroupLocation, RoutingInfo, RoutingMetrics, RoutingStrategy, RoutingTable, StreamRoute,
    commands::{GetRoutingInfo, GetStreamRoutingInfo, IsGroupLocal},
};

/// Handler for GetRoutingInfo command
#[derive(Clone)]
pub struct GetRoutingInfoHandler {
    routing_table: Arc<RoutingTable>,
    default_strategy: RoutingStrategy,
}

impl GetRoutingInfoHandler {
    pub fn new(routing_table: Arc<RoutingTable>, default_strategy: RoutingStrategy) -> Self {
        Self {
            routing_table,
            default_strategy,
        }
    }
}

#[async_trait]
impl RequestHandler<GetRoutingInfo> for GetRoutingInfoHandler {
    async fn handle(
        &self,
        _request: GetRoutingInfo,
        _metadata: EventMetadata,
    ) -> Result<RoutingInfo, crate::foundation::events::Error> {
        let (stream_routes, group_routes) = self
            .routing_table
            .get_all_routes()
            .await
            .map_err(|e| crate::foundation::events::Error::Internal(e.to_string()))?;

        Ok(RoutingInfo {
            stream_routes,
            group_routes,
            default_strategy: self.default_strategy,
            metrics: RoutingMetrics::default(),
            last_refresh: SystemTime::now(),
        })
    }
}

/// Handler for GetStreamRoutingInfo command
#[derive(Clone)]
pub struct GetStreamRoutingInfoHandler {
    routing_table: Arc<RoutingTable>,
}

impl GetStreamRoutingInfoHandler {
    pub fn new(routing_table: Arc<RoutingTable>) -> Self {
        Self { routing_table }
    }
}

#[async_trait]
impl RequestHandler<GetStreamRoutingInfo> for GetStreamRoutingInfoHandler {
    async fn handle(
        &self,
        request: GetStreamRoutingInfo,
        _metadata: EventMetadata,
    ) -> Result<Option<StreamRoute>, crate::foundation::events::Error> {
        self.routing_table
            .get_stream_route(&request.stream_name)
            .await
            .map_err(|e| crate::foundation::events::Error::Internal(e.to_string()))
    }
}

/// Handler for IsGroupLocal command
#[derive(Clone)]
pub struct IsGroupLocalHandler {
    routing_table: Arc<RoutingTable>,
}

impl IsGroupLocalHandler {
    pub fn new(routing_table: Arc<RoutingTable>) -> Self {
        Self { routing_table }
    }
}

#[async_trait]
impl RequestHandler<IsGroupLocal> for IsGroupLocalHandler {
    async fn handle(
        &self,
        request: IsGroupLocal,
        _metadata: EventMetadata,
    ) -> Result<bool, crate::foundation::events::Error> {
        let group_routes = self
            .routing_table
            .get_all_group_routes()
            .await
            .map_err(|e| crate::foundation::events::Error::Internal(e.to_string()))?;

        if let Some(route) = group_routes.get(&request.group_id) {
            Ok(route.location == GroupLocation::Local
                || route.location == GroupLocation::Distributed)
        } else {
            Ok(false)
        }
    }
}
