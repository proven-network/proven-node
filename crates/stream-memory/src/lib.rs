mod error;

pub use error::Error;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_stream::{Stream, Stream1, Stream2, Stream3, StreamHandlerError};
use tokio::sync::{mpsc, Mutex};

type ReceiverType = mpsc::Receiver<(Bytes, mpsc::Sender<Bytes>)>;

#[derive(Clone)]
struct ChannelPair {
    tx: mpsc::Sender<(Bytes, mpsc::Sender<Bytes>)>,
    rx: Arc<Mutex<ReceiverType>>,
}

type ChannelMap = Arc<Mutex<HashMap<String, ChannelPair>>>;

#[derive(Clone, Default)]
pub struct MemoryStream<HE: StreamHandlerError> {
    channels: ChannelMap,
    prefix: String,
    _handler_error: std::marker::PhantomData<HE>,
}

impl<HE> MemoryStream<HE>
where
    HE: StreamHandlerError,
{
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            prefix: String::new(),
            _handler_error: std::marker::PhantomData,
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
            _handler_error: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<HE> Stream<HE> for MemoryStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;

    async fn handle(
        &self,
        handler: impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, HE>> + Send>>
            + Send
            + Sync
            + 'static,
    ) -> Result<(), Self::Error> {
        let pair = self.get_or_create_channel().await;

        let rx_clone = pair.rx.clone();
        tokio::spawn(async move {
            let mut rx = rx_clone.lock().await;

            while let Some((data, response_tx)) = rx.recv().await {
                match handler(data).await {
                    Ok(response) => {
                        let _ = response_tx.send(response).await;
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

#[async_trait]
impl<HE> Stream1<HE> for MemoryStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;
    type Scoped = MemoryStream<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl<HE> Stream2<HE> for MemoryStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;
    type Scoped = MemoryStream<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl<HE> Stream3<HE> for MemoryStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;
    type Scoped = MemoryStream<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct TestHandlerError;

    impl std::fmt::Display for TestHandlerError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TestHandlerError")
        }
    }

    impl std::error::Error for TestHandlerError {}
    impl StreamHandlerError for TestHandlerError {}

    #[tokio::test]
    async fn test_scoping() {
        let stream = MemoryStream::<TestHandlerError>::new();
        let scoped = stream.with_scope("test".to_string());

        assert_eq!(stream.name(), "");
        assert_eq!(scoped.name(), "test");

        let scoped = scoped.with_scope("sub".to_string());
        assert_eq!(scoped.name(), "test_sub");
    }
}