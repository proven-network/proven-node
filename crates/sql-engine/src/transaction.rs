//! Transaction implementation for SQL engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::Stream;
use proven_engine::Client;
use proven_sql::{SqlParam, SqlTransaction};
use proven_storage::LogIndex;
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    error::Error,
    request::Request,
    response::Response,
    service::{StreamRequest, StreamResponse},
};

/// Metadata keys for request/response correlation
const REQUEST_ID_KEY: &str = "request_id";
const RESPONSE_TO_KEY: &str = "response_to";
const REQUEST_TYPE_KEY: &str = "request_type";

/// Default timeout for SQL operations
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// A transaction handle for SQL engine.
pub struct Transaction {
    /// Transaction ID
    id: Uuid,
    /// Engine client
    client: Arc<Client>,
    /// Command stream name
    command_stream: String,
    /// Node ID for identification
    node_id: String,
    /// Operation timeout
    timeout_duration: Duration,
}

impl Transaction {
    /// Create a new transaction handle.
    pub(crate) fn new(id: Uuid, client: Arc<Client>, command_stream: String) -> Self {
        let node_id = client.node_id().to_string();
        Self {
            id,
            client,
            command_stream,
            node_id,
            timeout_duration: DEFAULT_TIMEOUT,
        }
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
        let sequence = self
            .client
            .publish_to_stream(self.command_stream.clone(), vec![message])
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        debug!(
            "Published SQL transaction request {} at sequence {}",
            request_id, sequence
        );

        // Poll for response
        self.poll_for_response(request_id, sequence).await
    }

    /// Poll the command stream for a response matching the request ID.
    async fn poll_for_response(
        &self,
        request_id: Uuid,
        start_sequence: LogIndex,
    ) -> Result<Response, Error> {
        let deadline = tokio::time::Instant::now() + self.timeout_duration;
        let mut last_sequence = start_sequence.get();

        loop {
            // Check if we've timed out
            if tokio::time::Instant::now() >= deadline {
                error!("Transaction request {} timed out", request_id);
                return Err(Error::RequestTimeout);
            }

            // Read messages from the stream
            let messages = self
                .client
                .read_from_stream(
                    self.command_stream.clone(),
                    LogIndex::new(last_sequence + 1).unwrap(),
                    20, // Read up to 20 messages at a time
                )
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;

            if messages.is_empty() {
                // No new messages, wait a bit
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            // Check each message for our response
            for message in messages {
                last_sequence = message.sequence.get();

                // Check headers
                let headers: HashMap<String, String> = message
                    .data
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
                        ciborium::de::from_reader(&message.data.payload[..])
                            .map_err(|e| Error::Deserialization(e.to_string()))?;

                    debug!("Found response for transaction request {}", request_id);
                    return Ok(stream_response.response);
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
        // For queries, we need to handle streaming results differently
        // For now, we'll collect all results and return them as a stream

        let request_id = Uuid::new_v4();
        let request = Request::TransactionQuery(self.id, query, params);

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

        let sequence = self
            .client
            .publish_to_stream(self.command_stream.clone(), vec![message])
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        // For now, collect all row responses
        let mut rows = Vec::new();
        let deadline = tokio::time::Instant::now() + self.timeout_duration;
        let mut last_sequence = sequence.get();
        let mut found_response = false;

        while !found_response && tokio::time::Instant::now() < deadline {
            let messages = self
                .client
                .read_from_stream(
                    self.command_stream.clone(),
                    LogIndex::new(last_sequence + 1).unwrap(),
                    20,
                )
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            for message in messages {
                last_sequence = message.sequence.get();

                let headers: HashMap<String, String> = message
                    .data
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if headers.get(REQUEST_TYPE_KEY).map(String::as_str) == Some("sql_response")
                    && headers.get(RESPONSE_TO_KEY).map(String::as_str)
                        == Some(&request_id.to_string())
                {
                    let stream_response: StreamResponse =
                        ciborium::de::from_reader(&message.data.payload[..])
                            .map_err(|e| Error::Deserialization(e.to_string()))?;

                    match stream_response.response {
                        Response::TransactionRow(row) => {
                            if !row.is_empty() {
                                rows.push(row);
                            }
                            found_response = true;
                        }
                        Response::Failed(e) => return Err(Error::Libsql(e)),
                        _ => {
                            found_response = true;
                        }
                    }
                }
            }
        }

        if !found_response {
            return Err(Error::RequestTimeout);
        }

        // Return as a stream
        Ok(Box::new(futures::stream::iter(rows)))
    }
}

#[async_trait]
impl SqlTransaction for Transaction {
    type Error = Error;

    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let response = self
            .execute_request(Request::TransactionExecute(self.id, query.into(), params))
            .await?;

        match response {
            Response::TransactionExecute(count) => Ok(count),
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

    async fn commit(self) -> Result<(), Self::Error> {
        let response = self
            .execute_request(Request::TransactionCommit(self.id))
            .await?;

        match response {
            Response::TransactionCommitted => Ok(()),
            Response::Failed(e) => Err(Error::Libsql(e)),
            _ => Err(Error::Client("Unexpected response type".to_string())),
        }
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        let response = self
            .execute_request(Request::TransactionRollback(self.id))
            .await?;

        match response {
            Response::TransactionRolledBack => Ok(()),
            Response::Failed(e) => Err(Error::Libsql(e)),
            _ => Err(Error::Client("Unexpected response type".to_string())),
        }
    }
}
