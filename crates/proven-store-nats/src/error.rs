use derive_more::From;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Create(async_nats::jetstream::context::CreateKeyValueErrorKind),

    #[from]
    Delete(async_nats::jetstream::kv::DeleteErrorKind),

    #[from]
    Get(async_nats::jetstream::kv::EntryErrorKind),

    #[from]
    Put(async_nats::jetstream::kv::PutErrorKind),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Store error")
    }
}

impl std::error::Error for Error {}
