use async_nats::jetstream::kv::{EntryError, PutError, Store};
use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use derive_more::From;
use ring::digest::{Context, SHA256};
use tokio_rustls_acme::{AccountCache, CertCache};

#[derive(Debug, From)]
pub enum NatsCertCacheError {
    #[from]
    RetrieveError(EntryError),
    #[from]
    StoreError(PutError),
}

pub struct NatsCertCache {
    inner: Store,
}

impl NatsCertCache {
    pub fn new(kv_store: Store) -> Self {
        Self { inner: kv_store }
    }

    async fn read_if_exist(
        &self,
        key: impl AsRef<str>,
    ) -> Result<Option<Vec<u8>>, NatsCertCacheError> {
        match self.inner.get(key.as_ref()).await {
            Ok(Some(content)) => Ok(Some(content.into())),
            Ok(None) => Ok(None),
            Err(e) => Err(NatsCertCacheError::RetrieveError(e)),
        }
    }

    async fn write(
        &self,
        file: impl AsRef<str>,
        contents: Vec<u8>,
    ) -> Result<(), NatsCertCacheError> {
        let key = file.as_ref();
        let value = contents;
        match self.inner.put(key, value.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(NatsCertCacheError::StoreError(e)),
        }
    }

    fn cached_account_key(contact: &[String], directory_url: impl AsRef<str>) -> String {
        let mut ctx = Context::new(&SHA256);
        for el in contact {
            ctx.update(el.as_ref());
            ctx.update(&[0])
        }
        ctx.update(directory_url.as_ref().as_bytes());
        let hash = URL_SAFE_NO_PAD.encode(ctx.finish());
        format!("cached_account_{}", hash)
    }

    fn cached_cert_key(domains: &[String], directory_url: impl AsRef<str>) -> String {
        let mut ctx = Context::new(&SHA256);
        for domain in domains {
            ctx.update(domain.as_ref());
            ctx.update(&[0])
        }
        ctx.update(directory_url.as_ref().as_bytes());
        let hash = URL_SAFE_NO_PAD.encode(ctx.finish());
        format!("cached_cert_{}", hash)
    }
}

#[async_trait]
impl CertCache for NatsCertCache {
    type EC = NatsCertCacheError;

    async fn load_cert(
        &self,
        domains: &[String],
        directory_url: &str,
    ) -> Result<Option<Vec<u8>>, Self::EC> {
        let key = Self::cached_cert_key(domains, directory_url);
        self.read_if_exist(key).await
    }

    async fn store_cert(
        &self,
        domains: &[String],
        directory_url: &str,
        cert: &[u8],
    ) -> Result<(), Self::EC> {
        let key = Self::cached_cert_key(domains, directory_url);

        self.write(key, cert.into()).await
    }
}

#[async_trait]
impl AccountCache for NatsCertCache {
    type EA = NatsCertCacheError;

    async fn load_account(
        &self,
        contact: &[String],
        directory_url: &str,
    ) -> Result<Option<Vec<u8>>, Self::EA> {
        let key = Self::cached_account_key(contact, directory_url);
        self.read_if_exist(key).await.map_err(Into::into)
    }

    async fn store_account(
        &self,
        contact: &[String],
        directory_url: &str,
        account: &[u8],
    ) -> Result<(), Self::EA> {
        let key = Self::cached_account_key(contact, directory_url);
        self.write(key, account.into()).await.map_err(Into::into)
    }
}
