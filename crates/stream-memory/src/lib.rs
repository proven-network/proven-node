//! In-memory (single node) implementation of streams for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

use async_trait::async_trait;
use bytes::Bytes;
use proven_stream::{Stream, Stream1, Stream2, Stream3, StreamHandler};
use tokio::sync::{mpsc, Mutex};

type ReceiverType = mpsc::Receiver<(Bytes, mpsc::Sender<Bytes>)>;

#[derive(Clone, Debug)]
struct ChannelPair {
    tx: mpsc::Sender<(Bytes, mpsc::Sender<Bytes>)>,
    rx: Arc<Mutex<ReceiverType>>,
}

type ChannelMap = Arc<Mutex<HashMap<String, ChannelPair>>>;

/// In-memory stream implementation.
#[derive(Clone, Debug, Default)]
pub struct MemoryStream<H>
where
    H: StreamHandler,
{
    channels: ChannelMap,
    last_message: Arc<Mutex<Option<Bytes>>>,
    prefix: String,
    _handler: std::marker::PhantomData<H>,
}

impl<H> MemoryStream<H>
where
    H: StreamHandler,
{
    /// Creates a new `MemoryStream`.
    #[must_use]
    // TODO: Should take a name
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            last_message: Arc::new(Mutex::new(None)),
            prefix: String::new(),
            _handler: std::marker::PhantomData,
        }
    }

    async fn get_or_create_channel(&self) -> ChannelPair {
        let mut channels = self.channels.lock().await;

        if let Some(pair) = channels.get(&self.prefix) {
            return pair.clone();
        }

        let (tx, rx) = mpsc::channel(32);
        let pair = ChannelPair {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        };
        channels.insert(self.prefix.clone(), pair.clone());

        pair
    }
}

#[async_trait]
impl<H> Stream<H> for MemoryStream<H>
where
    H: StreamHandler,
{
    type Error = Error<H::Error>;

    async fn handle(&self, handler: H) -> Result<(), Self::Error> {
        handler.on_caught_up().await?;

        let pair = self.get_or_create_channel().await;

        let rx_clone = pair.rx;
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

    async fn last_message(&self) -> Result<Option<Bytes>, Self::Error> {
        Ok(self.last_message.lock().await.clone())
    }

    fn name(&self) -> String {
        self.prefix.clone()
    }

    async fn publish(&self, data: Bytes) -> Result<(), Self::Error> {
        let (response_tx, _) = mpsc::channel(1);
        let pair = self.get_or_create_channel().await;

        *self.last_message.lock().await = Some(data.clone());

        pair.tx
            .send((data, response_tx))
            .await
            .map_err(|_| Error::Send)
    }

    async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error> {
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let pair = self.get_or_create_channel().await;

        *self.last_message.lock().await = Some(data.clone());

        pair.tx
            .send((data, response_tx))
            .await
            .map_err(|_| Error::Send)?;

        response_rx.recv().await.ok_or(Error::Receive)
    }
}

macro_rules! impl_scoped_stream {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! MemoryStream $index]]
            [!set! #trait_name = [!ident! Stream $index]]

            #[doc = $doc]
            #[derive(Clone, Debug, Default)]
            pub struct #name<H>
            where
                H: StreamHandler,
            {
                channels: ChannelMap,
                last_message: Arc<Mutex<Option<Bytes>>>,
                prefix: String,
                _handler: std::marker::PhantomData<H>,
            }

            impl<H> #name<H>
            where H: StreamHandler {
                /// Creates a new `#name`.
                #[must_use]
                pub fn new () -> Self {
                    Self {
                        channels: Arc::new(Mutex::new(HashMap::new())),
                        last_message: Arc::new(Mutex::new(None)),
                        prefix: String::new(),
                        _handler: std::marker::PhantomData,
                    }
                }

                #[allow(dead_code)]
                fn with_scope(&self, scope: String) -> $parent<H> {
                    let new_prefix = if self.prefix.is_empty() {
                        scope
                    } else {
                        format!("{}_{}", self.prefix, scope)
                    };

                    $parent {
                        channels: self.channels.clone(),
                        last_message: self.last_message.clone(),
                        prefix: new_prefix,
                        _handler: std::marker::PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<H> #trait_name<H> for #name<H> where H: StreamHandler {
                type Error = Error<H::Error>;
                type Scoped = $parent<H>;

                fn [!ident! scope_ $index]<S: Into<String> + Send>(&self, scope: S) -> $parent<H> {
                    self.with_scope(scope.into())
                }
            }
        }
    };
}

impl_scoped_stream!(1, MemoryStream, Stream, "A single-scoped in-memory stream.");
impl_scoped_stream!(
    2,
    MemoryStream1,
    Stream1,
    "A double-scoped in-memory stream."
);
impl_scoped_stream!(
    3,
    MemoryStream2,
    Stream2,
    "A triple-scoped in-memory stream."
);

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

    #[derive(Clone, Debug)]
    struct TestHandler;

    #[async_trait]
    impl StreamHandler for TestHandler {
        type Error = TestHandlerError;

        async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::Error> {
            Ok(HandlerResponse {
                data,
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn test_scoping() {
        let stream = MemoryStream1::<TestHandler>::new();
        let scoped = stream.with_scope("test".to_string());

        assert_eq!(scoped.name(), "test_sub");
    }
}
