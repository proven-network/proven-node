mod cert_cache;

use cert_cache::CertCache;

use axum::http::Method;
use axum::Router;
use proven_store::Store;
use rustls::ServerConfig;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_rustls_acme::AcmeConfig;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

#[derive(Debug)]
pub enum HttpsServerError {
    AlreadyStarted,
}

pub struct HttpsServer {
    acceptor: tokio_rustls_acme::axum::AxumAcceptor,
    listen_addr: SocketAddr,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl HttpsServer {
    pub fn new<S: Store + 'static>(
        listen_addr: SocketAddr,
        fqdn: String,
        email: Vec<String>,
        use_self_signed_cert: bool,
        cert_store: S,
    ) -> Self {
        let mut state = AcmeConfig::new([fqdn.clone()])
            .contact(email.iter().map(|e| format!("mailto:{}", e)))
            .cache(CertCache::new(cert_store))
            .directory_lets_encrypt(!use_self_signed_cert)
            .state();

        let mut rustls_config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_cert_resolver(state.resolver());

        rustls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let acceptor = state.axum_acceptor(Arc::new(rustls_config));

        tokio::spawn(async move {
            loop {
                match state.next().await.unwrap() {
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

    pub fn start(&self, router: Router) -> Result<JoinHandle<()>, HttpsServerError> {
        let acceptor = self.acceptor.clone();
        let listen_addr = self.listen_addr;
        let shutdown_token = self.shutdown_token.clone();

        if self.task_tracker.is_closed() {
            return Err(HttpsServerError::AlreadyStarted);
        }

        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_origin(Any);

        let router = router.layer(cors);

        let handle = self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum_server::bind(listen_addr).acceptor(acceptor).serve(router.into_make_service_with_connect_info::<SocketAddr>()).into_future() => {
                    info!("https server exited {:?}", e);
                }
                _ = shutdown_token.cancelled() => {}
            };
        });

        self.task_tracker.close();

        Ok(handle)
    }

    pub async fn shutdown(&self) {
        info!("http server shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http server shutdown");
    }
}
