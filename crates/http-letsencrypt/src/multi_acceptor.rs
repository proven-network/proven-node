use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use multi_key_map::MultiKeyMap;
use rustls::server::Acceptor;
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{Accept, LazyConfigAcceptor};
use tokio_rustls_acme::acme::ACME_TLS_ALPN_NAME;

#[derive(Clone)]
pub struct MultiAxumAcceptor {
    configs: MultiKeyMap<String, Arc<ServerConfig>>,
    default_config: Arc<ServerConfig>,
}

impl MultiAxumAcceptor {
    pub fn new(default_config: Arc<ServerConfig>) -> Self {
        Self {
            configs: MultiKeyMap::new(),
            default_config,
        }
    }

    pub fn add_config(&mut self, domains: Vec<String>, config: Arc<ServerConfig>) {
        self.configs.insert_many(domains, config);
    }
}

// Implementation of axum's Accept trait
impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static>
    axum_server::accept::Accept<I, S> for MultiAxumAcceptor
{
    type Stream = tokio_rustls::server::TlsStream<I>;
    type Service = S;
    type Future = MultiAxumAccept<I, S>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        MultiAxumAccept {
            acceptor: LazyConfigAcceptor::new(Acceptor::default(), stream),
            configs: self.configs.clone(),
            default_config: self.default_config.clone(),
            tls_accept: None,
            service: Some(service),
            validation_accept: None,
        }
    }
}

// Custom Accept future
pub struct MultiAxumAccept<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> {
    acceptor: LazyConfigAcceptor<I>,
    configs: MultiKeyMap<String, Arc<ServerConfig>>,
    default_config: Arc<ServerConfig>,
    tls_accept: Option<Accept<I>>,
    service: Option<S>,
    validation_accept: Option<Accept<I>>,
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> Unpin
    for MultiAxumAccept<I, S>
{
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> Future
    for MultiAxumAccept<I, S>
{
    type Output = io::Result<(tokio_rustls::server::TlsStream<I>, S)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            // First check if we're in validation
            if let Some(validation_accept) = &mut self.validation_accept {
                return match Pin::new(validation_accept).poll(cx) {
                    Poll::Ready(Ok(_)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "TLS-ALPN-01 validation request",
                    ))),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                };
            }

            // Then check regular TLS accept
            if let Some(tls_accept) = &mut self.tls_accept {
                return match Pin::new(tls_accept).poll(cx) {
                    Poll::Ready(Ok(tls)) => Poll::Ready(Ok((tls, self.service.take().unwrap()))),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                };
            }

            // Handle initial handshake
            return match Pin::new(&mut self.acceptor).poll(cx) {
                Poll::Ready(Ok(handshake)) => {
                    // Check if this is an ACME validation request
                    let is_validation = handshake
                        .client_hello()
                        .alpn()
                        .into_iter()
                        .flatten()
                        .eq([ACME_TLS_ALPN_NAME]);

                    // Get the appropriate config based on SNI
                    let config = if let Some(server_name) = handshake.client_hello().server_name() {
                        let domain = server_name.to_string();
                        self.configs
                            .get(&domain)
                            .unwrap_or(&self.default_config)
                            .clone()
                    } else {
                        self.default_config.clone()
                    };

                    if is_validation {
                        self.validation_accept = Some(handshake.into_stream(config));
                        continue;
                    }

                    self.tls_accept = Some(handshake.into_stream(config));
                    Poll::Pending
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}
