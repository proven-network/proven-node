use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use axum::Router;
use tokio::task::JoinHandle;

#[async_trait]
pub trait HttpServer: Send + Sync + 'static {
    type Error: Debug + Error + Send + Sync;

    async fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::Error>;
    async fn shutdown(&self);
}
