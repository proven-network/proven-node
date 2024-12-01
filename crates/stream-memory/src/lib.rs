mod error;

pub use error::Error;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_stream::{Stream, Stream1, Stream2, Stream3, StreamHandler};
use tokio::sync::{mpsc, Mutex};

type ReceiverType = mpsc::Receiver<(Bytes, mpsc::Sender<Bytes>)>;

#[derive(Clone)]
struct ChannelPair {
    tx: mpsc::Sender<(Bytes, mpsc::Sender<Bytes>)>,
    rx: Arc<Mutex<ReceiverType>>,
}

type ChannelMap = Arc<Mutex<HashMap<String, ChannelPair>>>;

#[derive(Clone, Default)]
pub struct MemoryStream<H>
where
    H: StreamHandler,
{
    channels: ChannelMap,
    prefix: String,
    _handler: std::marker::PhantomData<H>,
}

impl<H> MemoryStream<H>
where
    H: StreamHandler,
{
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            prefix: String::new(),
            _handler: std::marker::PhantomData,
        }
    }

    async fn get_or_create_channel(&self) -> ChannelPair {
        println!("get_or_create_channel: {}", self.prefix);

        let mut channels = self.channels.lock().await;
        if let Some(pair) = channels.get(&self.prefix) {
            pair.clone()
        } else {
            let (tx, rx) = mpsc::channel(32);
            let pair = ChannelPair {
                tx,
                rx: Arc::new(Mutex::new(rx)),
            };
            channels.insert(self.prefix.clone(), pair.clone());
            pair
        }
    }

    fn with_scope(&self, scope: String) -> Self {
        let new_prefix = if self.prefix.is_empty() {
            scope
        } else {
            format!("{}_{}", self.prefix, scope)
        };

        Self {
            channels: self.channels.clone(),
            prefix: new_prefix,
            _handler: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<H> Stream<H> for MemoryStream<H>
where
    H: StreamHandler,
{
    type Error = Error<H::HandlerError>;

    async fn handle(&self, handler: H) -> Result<(), Self::Error> {
        handler.on_caught_up().await?;

        let pair = self.get_or_create_channel().await;

        let rx_clone = pair.rx.clone();
        tokio::spawn(async move {
            let mut rx = rx_clone.lock().await;

            while let Some((data, response_tx)) = rx.recv().await {
                match handler.handle(data).await {
                    Ok(response) => {
                        let _ = response_tx.send(response.data).await;
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(())
    }

    fn name(&self) -> String {
        self.prefix.clone()
    }

    async fn publish(&self, data: Bytes) -> Result<(), Self::Error> {
        let (response_tx, _) = mpsc::channel(1);
        let pair = self.get_or_create_channel().await;

        pair.tx
            .send((data, response_tx))
            .await
            .map_err(|_| Error::Send)
    }

    async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error> {
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let pair = self.get_or_create_channel().await;

        pair.tx
            .send((data, response_tx))
            .await
            .map_err(|_| Error::Send)?;

        response_rx.recv().await.ok_or(Error::Receive)
    }
}

macro_rules! impl_scoped_stream {
    ($name:ident, $parent:ident) => {
        #[async_trait]
        impl<H> $name<H> for MemoryStream<H>
        where
            H: StreamHandler,
        {
            type Error = Error<H::HandlerError>;
            type Scoped = MemoryStream<H>;

            fn scope(&self, scope: String) -> Self::Scoped {
                self.with_scope(scope)
            }
        }
    };
}

impl_scoped_stream!(Stream1, Stream);
impl_scoped_stream!(Stream2, Stream1);
impl_scoped_stream!(Stream3, Stream2);

#[cfg(test)]
mod tests {
    use super::*;

    use proven_stream::{HandlerResponse, StreamHandlerError};

    #[derive(Clone, Debug)]
    struct TestHandlerError;

    impl std::fmt::Display for TestHandlerError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TestHandlerError")
        }
    }

    impl std::error::Error for TestHandlerError {}
    impl StreamHandlerError for TestHandlerError {}

    #[derive(Clone)]
    struct TestHandler;

    #[async_trait]
    impl StreamHandler for TestHandler {
        type HandlerError = TestHandlerError;

        async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::HandlerError> {
            Ok(HandlerResponse {
                data,
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn test_scoping() {
        let stream = MemoryStream::<TestHandler>::new();
        let scoped = stream.with_scope("test".to_string());

        assert_eq!(stream.name(), "");
        assert_eq!(scoped.name(), "test");

        let scoped = scoped.with_scope("sub".to_string());
        assert_eq!(scoped.name(), "test_sub");
    }
}
