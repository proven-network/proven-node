mod cert_cache;
mod error;
mod multi_acceptor;

use cert_cache::CertCache;
use error::Error;
use multi_acceptor::MultiAxumAcceptor;

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::Method;
use axum::Router;
use proven_http::HttpServer;
use proven_store::Store;
use rustls::ServerConfig;
use tokio::task::JoinHandle;
use tokio_rustls_acme::AcmeConfig;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

pub struct LetsEncryptHttpServer {
    acceptor: MultiAxumAcceptor,
    listen_addr: SocketAddr,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl LetsEncryptHttpServer {
    pub fn new<S: Store + 'static>(
        listen_addr: SocketAddr,
        domains: Vec<String>,
        email: Vec<String>,
        cert_store: S,
    ) -> Self {
        let mut node_endpoints_state = AcmeConfig::new(domains.clone())
            .contact(email.iter().map(|e| format!("mailto:{}", e)))
            .cache(CertCache::new(cert_store.clone()))
            .directory_lets_encrypt(true)
            .state();

        let mut node_rustls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(node_endpoints_state.resolver());
        node_rustls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        // Create a second AcmeState to simulate also issuing certificates for individual client endpoints on different certs
        let example_client_domains = vec!["example.is.proven.network".to_string()];
        let mut example_client_state = AcmeConfig::new(example_client_domains.clone())
            .contact(email.iter().map(|e| format!("mailto:{}", e)))
            .cache(CertCache::new(cert_store))
            .directory_lets_encrypt(true)
            .state();

        let mut example_client_rustls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(example_client_state.resolver());
        example_client_rustls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let mut acceptor = MultiAxumAcceptor::new(Arc::new(node_rustls_config));
        acceptor.add_config(
            example_client_domains,
            Arc::new(example_client_rustls_config),
        );

        tokio::spawn(async move {
            loop {
                match node_endpoints_state.next().await.unwrap() {
                    Ok(ok) => info!("event: {:?}", ok),
                    Err(err) => info!("error: {:?}", err),
                }
            }
        });

        tokio::spawn(async move {
            loop {
                match example_client_state.next().await.unwrap() {
                    Ok(ok) => info!("event: {:?}", ok),
                    Err(err) => info!("error: {:?}", err),
                }
            }
        });

        Self {
            acceptor,
            listen_addr,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl HttpServer for LetsEncryptHttpServer {
    type HE = Error;

    async fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::HE> {
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
