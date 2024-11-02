mod error;

use crate::ScopeMethod;
pub use error::PublisherError;

use async_nats::Client;
use async_trait::async_trait;
use proven_stream::{StreamPublisher, StreamPublisher1, StreamPublisher2, StreamPublisher3};

pub struct NatsStreamPublisherOptions {
    pub client: Client,
    pub scope_method: ScopeMethod,
    pub stream_name: String,
}

#[derive(Clone)]
pub struct NatsStreamPublisher {
    client: Client,
    scope_method: ScopeMethod,
    stream_name: String,
    subject_prefix: Option<String>,
}

impl NatsStreamPublisher {
    pub fn new(
        NatsStreamPublisherOptions {
            client,
            scope_method,
            stream_name,
        }: NatsStreamPublisherOptions,
    ) -> Self {
        Self {
            client,
            scope_method,
            stream_name,
            subject_prefix: None,
        }
    }

    fn with_scope(&self, scope: String) -> Self {
        match self.scope_method {
            ScopeMethod::StreamPostfix => Self {
                client: self.client.clone(),
                scope_method: self.scope_method.clone(),
                stream_name: format!("{}_{}", self.stream_name, scope),
                subject_prefix: None,
            },
            ScopeMethod::SubjectPrefix => Self {
                client: self.client.clone(),
                scope_method: self.scope_method.clone(),
                stream_name: self.stream_name.clone(),
                subject_prefix: match &self.subject_prefix {
                    Some(prefix) => Some(format!("{}.{}", prefix, scope)),
                    None => Some(scope),
                },
            },
        }
    }

    fn get_full_subject(&self, subject: String) -> String {
        match &self.subject_prefix {
            Some(prefix) => format!("{}.{}", prefix, subject),
            None => subject,
        }
    }
}

#[async_trait]
impl StreamPublisher for NatsStreamPublisher {
    type PublisherError = PublisherError;

    async fn publish(&self, subject: String, data: Vec<u8>) -> Result<(), Self::PublisherError> {
        println!(
            "publishing on subject: {}",
            self.get_full_subject(subject.clone())
        );

        self.client
            .publish(self.get_full_subject(subject), data.into())
            .await
            .map_err(|e| PublisherError::Publish(e.kind()))
    }

    async fn request(
        &self,
        subject: String,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Self::PublisherError> {
        println!(
            "requesting on subject: {}",
            self.get_full_subject(subject.clone())
        );

        self.client
            .request(self.get_full_subject(subject), data.into())
            .await
            .map(|r| r.payload.to_vec())
            .map_err(|e| PublisherError::Request(e.kind()))
    }
}

#[async_trait]
impl StreamPublisher1 for NatsStreamPublisher {
    type PublisherError = PublisherError;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl StreamPublisher2 for NatsStreamPublisher {
    type PublisherError = PublisherError;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl StreamPublisher3 for NatsStreamPublisher {
    type PublisherError = PublisherError;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}
