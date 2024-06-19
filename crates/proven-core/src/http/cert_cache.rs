use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use derive_more::From;
use proven_store::Store;
use ring::digest::{Context, SHA256};

#[derive(Debug, From)]
pub enum NatsCertCacheError {
    RetrieveError,
    StoreError,
}

pub struct CertCache<S: Store> {
    store: S,
}

impl<S: Store> CertCache<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    async fn read_if_exist(
        &self,
        key: impl AsRef<str>,
    ) -> Result<Option<Vec<u8>>, NatsCertCacheError> {
        self.store
            .get(key.as_ref().to_string())
            .await
            .map_err(|_| NatsCertCacheError::RetrieveError)
    }

    async fn write(
        &self,
        key: impl AsRef<str>,
        contents: Vec<u8>,
    ) -> Result<(), NatsCertCacheError> {
        self.store
            .put(key.as_ref().to_string(), contents)
            .await
            .map_err(|_| NatsCertCacheError::StoreError)
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
impl<S: Store> tokio_rustls_acme::CertCache for CertCache<S> {
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
impl<S: Store> tokio_rustls_acme::AccountCache for CertCache<S> {
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
