use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use axum::Router;
use tokio::task::JoinHandle;

#[async_trait]
pub trait HttpServer: Send + Sync {
    type HE: Debug + Error + Send + Sync;
    fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::HE>;
    async fn shutdown(&self);
}
