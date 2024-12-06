mod error;

pub use error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use proven_stream::{HandlerResponse, StreamHandler};

/// A stream handler that executes SQL queries and migrations.
#[derive(Clone, Debug)]
pub struct Handler {}

impl Handler {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl StreamHandler for Handler {
    type Error = Error;

    async fn handle(&self, _data: Bytes) -> Result<HandlerResponse, Self::Error> {
        Ok(HandlerResponse {
            data: Bytes::new(),
            ..Default::default()
        })
    }
}
