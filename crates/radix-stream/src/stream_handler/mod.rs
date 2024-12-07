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
    pub(crate) const fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl StreamHandler for Handler {
    type Error = Error;
    type Request = Bytes;
    type Response = Bytes;

    async fn handle(&self, _data: Bytes) -> Result<HandlerResponse<Self::Response>, Self::Error> {
        Ok(HandlerResponse {
            data: Bytes::new(),
            ..Default::default()
        })
    }
}
