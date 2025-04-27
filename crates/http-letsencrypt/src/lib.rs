//! Implementation of secure HTTPS server using Let's Encrypt and enclave-only
//! storage of certificates provisioned using draft-ietf-acme-tls-alpn-01.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod acceptor;
mod cert_cache;
mod error;
mod multi_resolver;

use acceptor::AxumAcceptor;
use cert_cache::CertCache;
pub use error::Error;
use multi_resolver::MultiResolver;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use hickory_proto::rr::{RData, RecordType};
use hickory_resolver::Resolver;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use parking_lot::RwLock;
use proven_bootable::Bootable;
use proven_http::HttpServer;
use proven_store::Store;
use tokio::time::{Duration, sleep};
use tokio_rustls_acme::AcmeConfig;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::Service;
use tracing::{error, info};

/// Secure HTTPS server using Let's Encrypt certificates.
#[derive(Clone)]
pub struct LetsEncryptHttpServer<S>
where
    S: Store<Bytes, Infallible, Infallible>,
{
    acceptor: AxumAcceptor,
    cert_store: S,
    cname_domain: String,
    emails: Vec<String>,
    fallback_router: Router,
    hostname_routers: Arc<RwLock<HashMap<String, Router>>>,
    listen_addr: SocketAddr,
    registered_domains: Arc<RwLock<HashSet<String>>>,
    resolver: Arc<MultiResolver>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Options for creating a new `LetsEncryptHttpServer`.
pub struct LetsEncryptHttpServerOptions<S>
where
    S: Store<Bytes, Infallible, Infallible>,
{
    /// The store for certificates.
    pub cert_store: S,

    /// The CNAME domain used to point to the network.
    pub cname_domain: String,

    /// The list of domains to serve.
    pub domains: Vec<String>,

    /// The list of email addresses to use for Let's Encrypt.
    pub emails: Vec<String>,

    /// The fallback router.
    pub fallback_router: Router,

    /// The address to listen on.
    pub listen_addr: SocketAddr,
}

impl<S> LetsEncryptHttpServer<S>
where
    S: Store<Bytes, Infallible, Infallible>,
{
    /// Creates a new `LetsEncryptHttpServer`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `node_endpoints_state.next().await` call returns an error.
    pub fn new(
        LetsEncryptHttpServerOptions {
            cert_store,
            cname_domain,
            domains,
            emails,
            fallback_router,
            listen_addr,
        }: LetsEncryptHttpServerOptions<S>,
    ) -> Self {
        let mut node_endpoints_state = AcmeConfig::new(domains.clone())
            .contact(emails.iter().map(|e| format!("mailto:{e}")))
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

        let registered_domains = Arc::new(RwLock::new(HashSet::from_iter(domains)));

        Self {
            acceptor,
            cert_store,
            cname_domain,
            emails,
            fallback_router,
            hostname_routers: Arc::new(RwLock::new(HashMap::new())),
            listen_addr,
            registered_domains,
            resolver,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Adds a new domain to the Let's Encrypt server.
    ///
    /// # Panics
    ///
    /// This function will panic if the `new_state.next().await.unwrap()` call returns an error.
    pub fn add_domain(&self, domain: String) {
        let mut new_state = AcmeConfig::new(vec![domain.clone()])
            .contact(self.emails.iter().map(|e| format!("mailto:{e}")))
            .cache(CertCache::new(self.cert_store.clone()))
            .directory_lets_encrypt(true)
            .state();

        self.resolver
            .add_resolver(vec![domain.clone()], new_state.resolver());

        let expected_target = self.cname_domain.clone();
        tokio::spawn(async move {
            // Wait for DNS verification to pass before proceeding with the ACME challenge
            for attempt in 0..5 {
                if Self::verify_domain_dns(&domain, &expected_target).await {
                    break;
                }

                if attempt == 4 {
                    error!(
                        "DNS verification failed after 5 attempts for domain: {}",
                        domain
                    );
                    return;
                }

                sleep(Duration::from_secs(2_u64.pow(attempt))).await;
            }

            loop {
                match new_state.next().await.unwrap() {
                    Ok(ok) => info!("event: {:?}", ok),
                    Err(err) => info!("error: {:?}", err),
                }
            }
        });
    }

    async fn verify_domain_dns(domain: &str, expected_cname: &str) -> bool {
        let dns_resolver = Resolver::tokio(ResolverConfig::default(), ResolverOpts::default());

        if let Ok(response) = dns_resolver.lookup(domain, RecordType::CNAME).await {
            if let Some(RData::CNAME(cname)) = response.iter().next() {
                if cname.to_ascii().trim_end_matches('.') == expected_cname {
                    return true;
                }
            }
        }

        false
    }
}

#[async_trait]
impl<S> Bootable for LetsEncryptHttpServer<S>
where
    S: Store<Bytes, Infallible, Infallible>,
{
    type Error = Error;

    async fn start(&self) -> Result<(), Error> {
        let acceptor = self.acceptor.clone();
        let listen_addr = self.listen_addr;
        let shutdown_token = self.shutdown_token.clone();

        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let fallback_router = self.fallback_router.clone();
        let routers = self.hostname_routers.clone();
        let router = Router::new().fallback_service(tower::service_fn(
            move |req: axum::http::Request<_>| {
                let mut fallback = fallback_router.clone();
                let routers = routers.clone();

                async move {
                    let host = req
                        .headers()
                        .get(axum::http::header::HOST)
                        .and_then(|hv| {
                            hv.to_str()
                                // Ignore port component of host header
                                .map(|h| h.split(':').next().unwrap_or_default())
                                .ok()
                        })
                        .unwrap_or_default();

                    let router = if host.is_empty() {
                        None
                    } else {
                        routers.read().get(host).cloned()
                    };

                    match router {
                        Some(mut r) => r.call(req).await,
                        None => fallback.call(req).await,
                    }
                }
            },
        ));

        self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum_server::bind(listen_addr).acceptor(acceptor).serve(router.into_make_service()).into_future() => {
                    info!("https server exited {:?}", e);
                }
                () = shutdown_token.cancelled() => {}
            };
        });

        self.task_tracker.close();

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Error> {
        info!("http server shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http server shutdown");

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}

#[async_trait]
impl<S> HttpServer for LetsEncryptHttpServer<S>
where
    S: Store<Bytes, Infallible, Infallible>,
{
    type Error = Error;

    async fn set_router_for_hostname(&self, hostname: String, router: Router) -> Result<(), Error> {
        if self.registered_domains.write().insert(hostname.clone()) {
            self.add_domain(hostname.clone());
        }

        self.hostname_routers.write().insert(hostname, router);

        Ok(())
    }

    async fn remove_hostname(&self, hostname: String) -> Result<(), Error> {
        self.hostname_routers.write().remove(&hostname);
        Ok(())
    }
}
