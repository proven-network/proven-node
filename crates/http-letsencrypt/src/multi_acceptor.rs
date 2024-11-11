use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use multi_key_map::MultiKeyMap;
use rustls::server::Acceptor;
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{Accept, LazyConfigAcceptor, StartHandshake};
use tokio_rustls_acme::acme::ACME_TLS_ALPN_NAME;

#[derive(Clone)]
pub struct MultiAxumAcceptor {
    acme_acceptor: MultiAcmeAcceptor,
    configs: MultiKeyMap<String, Arc<ServerConfig>>,
    default_config: Arc<ServerConfig>,
}

impl MultiAxumAcceptor {
    pub fn new(default_config: Arc<ServerConfig>) -> Self {
        Self {
            acme_acceptor: MultiAcmeAcceptor::new(default_config.clone()),
            configs: MultiKeyMap::new(),
            default_config,
        }
    }

    pub fn add_config(&mut self, domains: Vec<String>, config: Arc<ServerConfig>) {
        self.configs.insert_many(domains.clone(), config.clone());
        self.acme_acceptor.add_config(domains, config);
    }
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static>
    axum_server::accept::Accept<I, S> for MultiAxumAcceptor
{
    type Stream = tokio_rustls::server::TlsStream<I>;
    type Service = S;
    type Future = MultiAxumAccept<I, S>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let acme_accept = self.acme_acceptor.accept(stream);
        MultiAxumAccept {
            configs: self.configs.clone(),
            default_config: self.default_config.clone(),
            acme_accept,
            tls_accept: None,
            service: Some(service),
        }
    }
}

pub struct MultiAxumAccept<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> {
    configs: MultiKeyMap<String, Arc<ServerConfig>>,
    default_config: Arc<ServerConfig>,
    acme_accept: MultiAcmeAccept<I>,
    tls_accept: Option<Accept<I>>,
    service: Option<S>,
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
            if let Some(tls_accept) = &mut self.tls_accept {
                return match Pin::new(tls_accept).poll(cx) {
                    Poll::Ready(Ok(tls)) => Poll::Ready(Ok((tls, self.service.take().unwrap()))),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                };
            }

            return match Pin::new(&mut self.acme_accept).poll(cx) {
                Poll::Ready(Ok(Some(start_handshake))) => {
                    let config =
                        if let Some(server_name) = start_handshake.client_hello().server_name() {
                            let domain = server_name.to_string();
                            self.configs
                                .get(&domain)
                                .unwrap_or(&self.default_config)
                                .clone()
                        } else {
                            self.default_config.clone()
                        };
                    self.tls_accept = Some(start_handshake.into_stream(config));
                    continue;
                }
                Poll::Ready(Ok(None)) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "TLS-ALPN-01 validation request",
                ))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

pub struct MultiAcmeAccept<IO: AsyncRead + AsyncWrite + Unpin> {
    acceptor: LazyConfigAcceptor<IO>,
    configs: MultiKeyMap<String, Arc<ServerConfig>>,
    default_config: Arc<ServerConfig>,
    validation_accept: Option<Accept<IO>>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> MultiAcmeAccept<IO> {
    pub(crate) fn new(
        io: IO,
        configs: MultiKeyMap<String, Arc<ServerConfig>>,
        default_config: Arc<ServerConfig>,
    ) -> Self {
        Self {
            acceptor: LazyConfigAcceptor::new(Acceptor::default(), io),
            configs,
            default_config,
            validation_accept: None,
        }
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Future for MultiAcmeAccept<IO> {
    type Output = io::Result<Option<StartHandshake<IO>>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(validation_accept) = &mut self.validation_accept {
                return match Pin::new(validation_accept).poll(cx) {
                    Poll::Ready(Ok(_)) => Poll::Ready(Ok(None)),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                };
            }

            return match Pin::new(&mut self.acceptor).poll(cx) {
                Poll::Ready(Ok(handshake)) => {
                    let is_validation = handshake
                        .client_hello()
                        .alpn()
                        .into_iter()
                        .flatten()
                        .eq([ACME_TLS_ALPN_NAME]);

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
                    Poll::Ready(Ok(Some(handshake)))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

#[derive(Clone)]
pub struct MultiAcmeAcceptor {
    configs: MultiKeyMap<String, Arc<ServerConfig>>,
    default_config: Arc<ServerConfig>,
}

impl MultiAcmeAcceptor {
    pub fn new(default_config: Arc<ServerConfig>) -> Self {
        Self {
            configs: MultiKeyMap::new(),
            default_config,
        }
    }

    pub fn add_config(&mut self, domains: Vec<String>, config: Arc<ServerConfig>) {
        self.configs.insert_many(domains, config);
    }

    pub fn accept<IO: AsyncRead + AsyncWrite + Unpin>(&self, io: IO) -> MultiAcmeAccept<IO> {
        MultiAcmeAccept::new(io, self.configs.clone(), self.default_config.clone())
    }
}
