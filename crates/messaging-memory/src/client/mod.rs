mod error;

use crate::stream::InitializedMemoryStream;
use crate::{ClientRequest, GLOBAL_STATE, GlobalState, ServiceResponse};
pub use error::Error;
use proven_messaging::stream::InitializedStream;

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::channel::mpsc::{self, Sender};
use proven_messaging::client::{Client, ClientOptions, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;
use tokio::sync::{Mutex, broadcast, oneshot};
use uuid::Uuid;

type ResponseMap<R> = HashMap<String, oneshot::Sender<ClientResponseType<R>>>;
type StreamMap<R> = HashMap<String, Sender<R>>;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryClientOptions;
impl ClientOptions for MemoryClientOptions {}

/// A client for an in-memory service.
#[derive(Debug)]
pub struct MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    client_id: String,
    request_id_counter: Arc<AtomicUsize>,
    response_map: Arc<Mutex<ResponseMap<X::ResponseType>>>,
    stream_map: Arc<Mutex<StreamMap<X::ResponseType>>>,
    service_name: String,
    stream: InitializedMemoryStream<T, D, S>,
}

impl<X, T, D, S> Clone for MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client_id: self.client_id.clone(),
            request_id_counter: self.request_id_counter.clone(),
            response_map: self.response_map.clone(),
            stream_map: self.stream_map.clone(),
            service_name: self.service_name.clone(),
            stream: self.stream.clone(),
        }
    }
}

impl<X, T, D, S> MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn spawn_response_handler(
        mut receiver: broadcast::Receiver<ServiceResponse<X::ResponseType>>,
        response_map: Arc<Mutex<ResponseMap<X::ResponseType>>>,
        stream_map: Arc<Mutex<StreamMap<X::ResponseType>>>,
    ) {
        tokio::spawn(async move {
            while let Ok(response) = receiver.recv().await {
                match response {
                    // Single response
                    ServiceResponse::Single(request_id, payload) => {
                        let mut map = response_map.lock().await;
                        if let Some(sender) = map.remove(&request_id) {
                            let _ = sender.send(ClientResponseType::Response(payload));
                        }
                    }
                    // Empty stream response
                    ServiceResponse::StreamEmpty(request_id) => {
                        let mut map = response_map.lock().await;
                        if let Some(sender) = map.remove(&request_id) {
                            let _ = sender.send(ClientResponseType::Stream(Box::new(
                                futures::stream::empty(),
                            )));
                        }
                    }
                    // Stream response with end marker
                    ServiceResponse::StreamEnd(request_id, payload) => {
                        let mut stream_map = stream_map.lock().await;
                        if let Some(sender) = stream_map.get_mut(&request_id) {
                            let _ = sender.try_send(payload);
                            sender.close_channel();
                            stream_map.remove(&request_id);
                        } else {
                            let mut response_map = response_map.lock().await;
                            if let Some(sender) = response_map.remove(&request_id) {
                                let _ = sender.send(ClientResponseType::Stream(Box::new(
                                    futures::stream::iter(vec![payload]),
                                )));
                            }
                            drop(response_map);
                        }

                        drop(stream_map);
                    }
                    // Stream response without end marker
                    ServiceResponse::StreamItem(request_id, payload) => {
                        let mut stream_map = stream_map.lock().await;
                        if let Some(sender) = stream_map.get_mut(&request_id) {
                            let _ = sender.try_send(payload);
                        } else {
                            let (mut tx, rx) = mpsc::channel(32);

                            let _ = tx.try_send(payload);
                            stream_map.insert(request_id.clone(), tx);

                            let mut response_map = response_map.lock().await;
                            if let Some(sender) = response_map.remove(&request_id) {
                                let stream = Box::new(rx);
                                let _ = sender.send(ClientResponseType::Stream(stream));
                            }
                        }

                        drop(stream_map);
                    }
                }
            }
        });
    }
}

#[async_trait]
impl<X, T, D, S> Client<X, T, D, S> for MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = Error;
    type Options = MemoryClientOptions;
    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
    ) -> Result<Self, Self::Error> {
        let client_id = Uuid::new_v4().to_string();
        let (sender, receiver) = broadcast::channel(100);
        let response_map = Arc::new(Mutex::new(HashMap::new()));
        let stream_map = Arc::new(Mutex::new(HashMap::new()));

        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<GlobalState<X::ResponseType>>() {
            state.put(GlobalState::<X::ResponseType>::default());
        }
        let global_state = state.borrow::<GlobalState<X::ResponseType>>();
        let mut service_responses = global_state.service_responses.lock().await;

        // Format key as "{service_name}:{client_id}"
        let response_key = format!("{name}:{client_id}");
        service_responses.insert(response_key, sender);

        drop(service_responses);
        drop(state);

        Self::spawn_response_handler(receiver, response_map.clone(), stream_map.clone());

        Ok(Self {
            client_id,
            request_id_counter: Arc::new(AtomicUsize::new(0)),
            response_map,
            stream_map,
            service_name: name,
            stream,
        })
    }

    async fn request(
        &self,
        request: T,
    ) -> Result<ClientResponseType<X::ResponseType>, Self::Error> {
        let sequence_number = self
            .stream
            .publish(request.clone())
            .await
            .map_err(|_| Error::StreamPublish)?;

        let request_id = self
            .request_id_counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string();
        let (sender, receiver) = oneshot::channel();

        {
            let mut map = self.response_map.lock().await;
            map.insert(request_id.clone(), sender);
        }

        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<GlobalState<T>>() {
            state.put(GlobalState::<T>::default());
        }
        let subject_state = state.borrow::<GlobalState<T>>();
        let client_requests = subject_state.client_requests.lock().await;

        if let Some(sender) = client_requests.get(&self.service_name) {
            let client_request = ClientRequest {
                client_id: self.client_id.clone(),
                request_id: request_id.clone(),
                payload: request,
                sequence_number,
            };
            sender.send(client_request).map_err(|_| Error::Send)?;
        } else {
            let mut map = self.response_map.lock().await;
            map.remove(&request_id);
            drop(map);
            return Err(Error::NoService);
        }
        drop(client_requests);
        drop(state);

        match tokio::time::timeout(std::time::Duration::from_secs(5), receiver).await {
            Ok(Ok(response)) => Ok(response),
            _ => {
                let mut map = self.response_map.lock().await;
                map.remove(&request_id);
                drop(map);
                Err(Error::NoResponse)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::MemoryServiceOptions;
    use crate::stream::{MemoryStream, MemoryStreamOptions};

    use std::convert::Infallible;
    use std::time::Duration;

    use proven_bootable::Bootable;
    use proven_messaging::service_responder::ServiceResponder;
    use proven_messaging::stream::Stream;
    use tokio::time::timeout;

    #[derive(Clone, Debug)]
    struct TestHandler;

    #[async_trait]
    impl ServiceHandler<Bytes, Infallible, Infallible> for TestHandler {
        type Error = Infallible;
        type ResponseType = Bytes;
        type ResponseDeserializationError = Infallible;
        type ResponseSerializationError = Infallible;

        async fn handle<R>(&self, msg: Bytes, responder: R) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                    Bytes,
                    Infallible,
                    Infallible,
                    Self::ResponseType,
                    Self::ResponseDeserializationError,
                    Self::ResponseSerializationError,
                >,
        {
            Ok(responder
                .reply(Bytes::from(format!(
                    "response: {}",
                    String::from_utf8(msg.to_vec()).unwrap()
                )))
                .await)
        }
    }

    #[tokio::test]
    async fn test_client_request_response() {
        // Create stream
        let stream = MemoryStream::<Bytes, Infallible, Infallible>::new(
            "test_client_stream",
            MemoryStreamOptions {},
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Start service
        let service = initialized_stream
            .clone()
            .service("test_service", MemoryServiceOptions {}, TestHandler)
            .await
            .expect("Failed to start service");

        service.start().await.expect("Failed to start service");

        // Create client
        let client = initialized_stream
            .client::<_, TestHandler>("test_service", MemoryClientOptions {})
            .await
            .expect("Failed to create client");

        // Send request and get response with timeout
        let request = Bytes::from("hello");

        match timeout(Duration::from_secs(5), client.request(request))
            .await
            .expect("Request timed out")
            .expect("Failed to send request")
        {
            ClientResponseType::Response(response) => {
                assert_eq!(response, Bytes::from("response: hello"));
            }
            _ => {
                panic!("Expected single response");
            }
        }
    }

    #[tokio::test]
    async fn test_client_request_no_service() {
        // Create stream without service
        let stream = MemoryStream::<Bytes, Infallible, Infallible>::new(
            "test_client_timeout",
            MemoryStreamOptions {},
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Create client
        let client = initialized_stream
            .client::<_, TestHandler>("test_client_timeout", MemoryClientOptions {})
            .await
            .expect("Failed to create client");

        // Send request without service running
        let request = Bytes::from("hello");

        let result = client.request(request).await;
        assert!(result.is_err(), "Expected no service error");
    }
}
