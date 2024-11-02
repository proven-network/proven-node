mod error;

use crate::ScopeMethod;
pub use error::SubscriberError;

use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use async_trait::async_trait;
use futures::StreamExt;
use proven_stream::{StreamSubscriber, StreamSubscriber1, StreamSubscriber2, StreamSubscriber3};

pub struct NatsStreamSubscriberOptions {
    pub client: Client,
    pub scope_method: ScopeMethod,
    pub stream_name: String,
}

#[derive(Clone)]
pub struct NatsStreamSubscriber<HE: Clone + Debug + Error + Send + Sync> {
    client: Client,
    jetstream_context: JetStreamContext,
    scope_method: ScopeMethod,
    stream_name: String,
    subject_prefix: Option<String>,
    _handler_error: std::marker::PhantomData<HE>,
}

impl<HE> NatsStreamSubscriber<HE>
where
    HE: Clone + Debug + Error + Send + Sync,
{
    pub fn new(
        NatsStreamSubscriberOptions {
            client,
            scope_method,
            stream_name,
        }: NatsStreamSubscriberOptions,
    ) -> Self {
        let jetstream_context = jetstream::new(client.clone());

        Self {
            client,
            jetstream_context,
            scope_method,
            stream_name,
            subject_prefix: None,
            _handler_error: std::marker::PhantomData,
        }
    }

    fn with_scope(&self, scope: String) -> Self {
        match self.scope_method {
            ScopeMethod::StreamPostfix => Self {
                client: self.client.clone(),
                jetstream_context: self.jetstream_context.clone(),
                scope_method: self.scope_method.clone(),
                stream_name: format!("{}_{}", self.stream_name, scope),
                subject_prefix: None,
                _handler_error: std::marker::PhantomData,
            },
            ScopeMethod::SubjectPrefix => Self {
                client: self.client.clone(),
                jetstream_context: self.jetstream_context.clone(),
                scope_method: self.scope_method.clone(),
                stream_name: self.stream_name.clone(),
                subject_prefix: match &self.subject_prefix {
                    Some(prefix) => Some(format!("{}.{}", prefix, scope)),
                    None => Some(scope),
                },
                _handler_error: std::marker::PhantomData,
            },
        }
    }

    fn get_subject_binding(&self, subject: String) -> String {
        match &self.subject_prefix {
            Some(prefix) => format!("{}.{}", prefix, subject),
            None => subject,
        }
    }
}

#[async_trait]
impl<HE> StreamSubscriber<HE> for NatsStreamSubscriber<HE>
where
    HE: Clone + Debug + Error + Send + Sync + 'static,
{
    type SubscriberError = SubscriberError<HE>;

    async fn subscribe(
        &self,
        subject: String,
        handler: impl Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, HE>> + Send>>
            + Send
            + Sync,
    ) -> Result<(), Self::SubscriberError> {
        println!(
            "Subscribing to {}",
            self.get_subject_binding(subject.clone())
        );

        // Setup stream and consumer
        let mut messages = self
            .jetstream_context
            .create_stream(StreamConfig {
                name: self.stream_name.clone(),
                subjects: vec![self.get_subject_binding(subject.clone())],
                ..Default::default()
            })
            .await
            .map_err(|e| SubscriberError::StreamCreate(e.kind()))?
            .create_consumer(ConsumerConfig {
                ..Default::default()
            })
            .await
            .map_err(|e| SubscriberError::ConsumerCreate(e.kind()))?
            .messages()
            .await
            .map_err(|e| SubscriberError::ConsumerStream(e.kind()))?;

        println!(
            "Subscribed to {}",
            self.get_subject_binding(subject.clone())
        );

        // Process messages
        while let Some(message) = messages.next().await {
            let message = message.map_err(|e| SubscriberError::ConsumerMessages(e.kind()))?;

            let response = handler(message.payload.to_vec())
                .await
                .map_err(|e| SubscriberError::Handler(e))?;

            if let Some(reply) = message.reply.clone() {
                self.client
                    .publish(reply, response.into())
                    .await
                    .map_err(|e| SubscriberError::ReplyPublish(e.kind()))?;
            }

            message
                .double_ack()
                .await
                .map_err(|_| SubscriberError::ConsumerAck)?;
        }

        Ok(())
    }
}

#[async_trait]
impl<HE> StreamSubscriber1<HE> for NatsStreamSubscriber<HE>
where
    HE: Clone + Debug + Error + Send + Sync + 'static,
{
    type SubscriberError = SubscriberError<HE>;
    type Scoped = NatsStreamSubscriber<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl<HE> StreamSubscriber2<HE> for NatsStreamSubscriber<HE>
where
    HE: Clone + Debug + Error + Send + Sync + 'static,
{
    type SubscriberError = SubscriberError<HE>;
    type Scoped = NatsStreamSubscriber<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl<HE> StreamSubscriber3<HE> for NatsStreamSubscriber<HE>
where
    HE: Clone + Debug + Error + Send + Sync + 'static,
{
    type SubscriberError = SubscriberError<HE>;
    type Scoped = NatsStreamSubscriber<HE>;

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

    #[tokio::test]
    async fn test_stream_name_scoping() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();

        let subscriber =
            NatsStreamSubscriber::<TestHandlerError>::new(NatsStreamSubscriberOptions {
                client,
                scope_method: ScopeMethod::StreamPostfix,
                stream_name: "SQL".to_string(),
            });

        let subscriber = subscriber.with_scope("app1".to_string());
        assert_eq!(subscriber.stream_name, "SQL_app1");

        let subscriber = subscriber.with_scope("db1".to_string());
        assert_eq!(subscriber.stream_name, "SQL_app1_db1");

        // Test subject not prefixed
        assert_eq!(
            subscriber.get_subject_binding("create".to_string()),
            "create"
        );
    }

    #[tokio::test]
    async fn test_subject_scoping() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();

        let subscriber =
            NatsStreamSubscriber::<TestHandlerError>::new(NatsStreamSubscriberOptions {
                client,
                scope_method: ScopeMethod::SubjectPrefix,
                stream_name: "EVENT".to_string(),
            });

        let subscriber = subscriber.with_scope("app1".to_string());
        assert_eq!(
            subscriber.get_subject_binding("action".to_string()),
            "app1.action"
        );

        let subscriber = subscriber.with_scope("user1".to_string());
        assert_eq!(
            subscriber.get_subject_binding("action".to_string()),
            "app1.user1.action"
        );

        // Test table name still base
        assert_eq!(subscriber.stream_name, "EVENT");
    }
}
