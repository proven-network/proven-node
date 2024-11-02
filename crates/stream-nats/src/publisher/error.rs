use derive_more::From;

#[derive(Clone, Debug, From)]
pub enum PublisherError {
    #[from]
    Publish(async_nats::client::PublishErrorKind),

    #[from]
    Request(async_nats::client::RequestErrorKind),
}

impl std::fmt::Display for PublisherError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Store error")
    }
}

impl std::error::Error for PublisherError {}
