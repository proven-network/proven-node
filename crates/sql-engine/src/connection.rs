//! SQL connection implementation using engine streams.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::Stream;
use proven_engine::Client;
use proven_sql::{SqlConnection, SqlParam};
use proven_storage::LogIndex;
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    error::Error,
    request::Request,
    response::Response,
    service::{StreamRequest, StreamResponse},
    transaction::Transaction,
};

/// Metadata keys for request/response correlation
const REQUEST_ID_KEY: &str = "request_id";
const RESPONSE_TO_KEY: &str = "response_to";
const REQUEST_TYPE_KEY: &str = "request_type";

/// Default timeout for SQL operations
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Connection to a SQL database via engine streams.
#[derive(Clone)]
pub struct Connection {
    /// Engine client
    client: Arc<Client>,
    /// Command stream name
    command_stream: String,
    /// Node ID for identification
    node_id: String,
    /// Operation timeout
    timeout_duration: Duration,
}

impl Connection {
    /// Create a new connection.
    #[must_use]
    pub fn new(client: Arc<Client>, command_stream: String) -> Self {
        let node_id = client.node_id().to_string();
        Self {
            client,
            command_stream,
            node_id,
            timeout_duration: DEFAULT_TIMEOUT,
        }
    }

    /// Set the operation timeout.
    #[must_use]
    pub const fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout_duration = duration;
        self
    }

    /// Send a request and wait for response.
    async fn execute_request(&self, request: Request) -> Result<Response, Error> {
        let request_id = Uuid::new_v4();

        // Create stream request
        let stream_request = StreamRequest {
            id: request_id,
            request,
            from_node: self.node_id.clone(),
        };

        // Serialize request
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&stream_request, &mut payload)?;

        // Create request metadata
        let mut metadata = HashMap::new();
        metadata.insert(REQUEST_ID_KEY.to_string(), request_id.to_string());
        metadata.insert(REQUEST_TYPE_KEY.to_string(), "sql_request".to_string());

        // Create message with headers
        let mut message = proven_engine::Message::new(payload);
        for (k, v) in metadata {
            message = message.with_header(k, v);
        }

        // Publish request
        self.client
            .publish_to_stream(self.command_stream.clone(), vec![message])
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        debug!("Published SQL request {}", request_id);

        // Wait for response using streaming
        self.wait_for_response(request_id).await
    }

    /// Wait for a response using streaming API.
    #[allow(clippy::cognitive_complexity)]
    async fn wait_for_response(&self, request_id: Uuid) -> Result<Response, Error> {
        use tokio::pin;
        use tokio_stream::StreamExt;

        // Start from the beginning to find our response
        let start_seq = LogIndex::new(1).unwrap();

        // Create timeout future
        let timeout_fut = tokio::time::sleep(self.timeout_duration);
        tokio::pin!(timeout_fut);

        // Start streaming messages
        let stream = self
            .client
            .stream_messages(self.command_stream.clone(), Some(start_seq))
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        pin!(stream);

        loop {
            tokio::select! {
                () = &mut timeout_fut => {
                    error!("Request {} timed out", request_id);
                    return Err(Error::RequestTimeout);
                }
                result = stream.next() => {
                    if let Some((message, _timestamp, _sequence)) = result {
                        // Check headers
                        let headers: HashMap<String, String> = message
                            .headers
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();

                        // Is this a response to our request?
                        if headers.get(REQUEST_TYPE_KEY).map(String::as_str) == Some("sql_response")
                            && headers.get(RESPONSE_TO_KEY).map(String::as_str)
                                == Some(&request_id.to_string())
                        {
                            // Found our response!
                            let stream_response: StreamResponse =
                                ciborium::de::from_reader(&message.payload[..])
                                    .map_err(|e| Error::Deserialization(e.to_string()))?;

                            debug!("Found response for request {}", request_id);
                            return Ok(stream_response.response);
                        }
                    } else {
                        error!("Stream ended while waiting for response");
                        return Err(Error::Stream("Stream ended unexpectedly".to_string()));
                    }
                }
            }
        }
    }

    /// Execute a query and stream the results.
    async fn query_stream(
        &self,
        query: String,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn Stream<Item = Vec<SqlParam>> + Send + Unpin>, Error> {
        use tokio::pin;
        use tokio_stream::StreamExt;

        // For queries, we need to handle streaming results differently
        // For now, we'll collect all results and return them as a stream

        let request_id = Uuid::new_v4();
        let request = Request::Query(query, params);

        // Create stream request
        let stream_request = StreamRequest {
            id: request_id,
            request,
            from_node: self.node_id.clone(),
        };

        // Serialize and send request (similar to execute_request)
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&stream_request, &mut payload)?;

        let mut metadata = HashMap::new();
        metadata.insert(REQUEST_ID_KEY.to_string(), request_id.to_string());
        metadata.insert(REQUEST_TYPE_KEY.to_string(), "sql_request".to_string());

        let mut message = proven_engine::Message::new(payload);
        for (k, v) in metadata {
            message = message.with_header(k, v);
        }

        self.client
            .publish_to_stream(self.command_stream.clone(), vec![message])
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        // Wait for response using streaming API

        let mut rows = Vec::new();
        let start_seq = LogIndex::new(1).unwrap();

        // Create timeout future
        let timeout_fut = tokio::time::sleep(self.timeout_duration);
        tokio::pin!(timeout_fut);

        // Start streaming messages
        let stream = self
            .client
            .stream_messages(self.command_stream.clone(), Some(start_seq))
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        pin!(stream);

        loop {
            tokio::select! {
                () = &mut timeout_fut => {
                    return Err(Error::RequestTimeout);
                }
                result = stream.next() => {
                    match result {
                        Some((message, _timestamp, _sequence)) => {
                            let headers: HashMap<String, String> = message
                                .headers
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();

                            if headers.get(REQUEST_TYPE_KEY).map(String::as_str) == Some("sql_response")
                                && headers.get(RESPONSE_TO_KEY).map(String::as_str)
                                    == Some(&request_id.to_string())
                            {
                                let stream_response: StreamResponse =
                                    ciborium::de::from_reader(&message.payload[..])
                                        .map_err(|e| Error::Deserialization(e.to_string()))?;

                                match stream_response.response {
                                    Response::Row(row) => {
                                        if !row.is_empty() {
                                            rows.push(row);
                                        }
                                        break;
                                    }
                                    Response::Rows(all_rows) => {
                                        rows = all_rows;
                                        break;
                                    }
                                    Response::Failed(e) => return Err(Error::Libsql(e)),
                                    _ => {
                                        break;
                                    }
                                }
                            }
                        }
                        None => {
                            return Err(Error::Stream("Stream ended unexpectedly".to_string()));
                        }
                    }
                }
            }
        }

        // Return as a stream
        Ok(Box::new(futures::stream::iter(rows)))
    }
}

#[async_trait]
impl SqlConnection for Connection {
    type Error = Error;
    type Transaction = Transaction;

    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let response = self
            .execute_request(Request::Execute(query.into(), params))
            .await?;

        match response {
            Response::Execute(count) => Ok(count),
            Response::Failed(e) => Err(Error::Libsql(e)),
            _ => Err(Error::Client("Unexpected response type".to_string())),
        }
    }

    async fn execute_batch<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Self::Error> {
        let response = self
            .execute_request(Request::ExecuteBatch(query.into(), params))
            .await?;

        match response {
            Response::ExecuteBatch(count) => Ok(count),
            Response::Failed(e) => Err(Error::Libsql(e)),
            _ => Err(Error::Client("Unexpected response type".to_string())),
        }
    }

    async fn migrate<Q: Clone + Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        let response = self.execute_request(Request::Migrate(query.into())).await?;

        match response {
            Response::Migrate(needed) => Ok(needed),
            Response::Failed(e) => Err(Error::Libsql(e)),
            _ => Err(Error::Client("Unexpected response type".to_string())),
        }
    }

    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn Stream<Item = Vec<SqlParam>> + Send + Unpin>, Self::Error> {
        self.query_stream(query.into(), params).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let response = self.execute_request(Request::BeginTransaction).await?;

        match response {
            Response::TransactionBegun(id) => Ok(Transaction::new(
                id,
                Arc::clone(&self.client),
                self.command_stream.clone(),
            )),
            Response::Failed(e) => Err(Error::Libsql(e)),
            _ => Err(Error::Client("Unexpected response type".to_string())),
        }
    }
}
