mod acceptor;
mod cert_cache;
mod error;
mod multi_resolver;

use acceptor::AxumAcceptor;
use cert_cache::CertCache;
use error::Error;
use multi_resolver::MultiResolver;

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::Method;
use axum::Router;
use proven_http::HttpServer;
use proven_store::Store;
use tokio::task::JoinHandle;
use tokio_rustls_acme::AcmeConfig;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

pub struct LetsEncryptHttpServer<S: Store> {
    acceptor: AxumAcceptor,
    cert_store: S,
    emails: Vec<String>,
    listen_addr: SocketAddr,
    resolver: Arc<MultiResolver>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<S: Store> LetsEncryptHttpServer<S> {
    pub fn new(
        listen_addr: SocketAddr,
        domains: Vec<String>,
        emails: Vec<String>,
        cert_store: S,
    ) -> Self {
        let mut node_endpoints_state = AcmeConfig::new(domains.clone())
            .contact(emails.iter().map(|e| format!("mailto:{}", e)))
            .cache(CertCache::new(cert_store.clone()))
            .directory_lets_encrypt(true)
            .state();

        let resolver = Arc::new(MultiResolver::new(node_endpoints_state.resolver()));
        let acceptor = AxumAcceptor::new(Arc::clone(&resolver));

        tokio::spawn(async move {
            loop {
                match node_endpoints_state.next().await.unwrap() {
                    Ok(ok) => info!("event: {:?}", ok),
                    Err(err) => info!("error: {:?}", err),
                }
            }
        });

        Self {
            acceptor,
            cert_store,
            emails,
            listen_addr,
            resolver,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub fn add_domain(&self, domain: String) {
        let new_domains = vec![domain];
        let mut new_state = AcmeConfig::new(new_domains.clone())
            .contact(self.emails.iter().map(|e| format!("mailto:{}", e)))
            .cache(CertCache::new(self.cert_store.clone()))
            .directory_lets_encrypt(true)
            .state();

        self.resolver
            .add_resolver(new_domains, new_state.resolver());

        // TODO: We need to integrate DNS resolution to validate domain set up correctly before trying Let's Encrypt
        tokio::spawn(async move {
            loop {
                match new_state.next().await.unwrap() {
                    Ok(ok) => info!("event: {:?}", ok),
                    Err(err) => info!("error: {:?}", err),
                }
            }
        });
    }
}

#[async_trait]
impl<S: Store> HttpServer for LetsEncryptHttpServer<S> {
    type Error = Error<S::Error>;

    async fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::Error> {
        let acceptor = self.acceptor.clone();
        let listen_addr = self.listen_addr;
        let shutdown_token = self.shutdown_token.clone();

        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_origin(Any);

        let router = router.layer(cors);

        let handle = self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum_server::bind(listen_addr).acceptor(acceptor).serve(router.into_make_service()).into_future() => {
                    info!("https server exited {:?}", e);
                }
                _ = shutdown_token.cancelled() => {}
            };
        });

        self.task_tracker.close();

        Ok(handle)
    }

    async fn shutdown(&self) {
        info!("http server shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http server shutdown");
    }
}
