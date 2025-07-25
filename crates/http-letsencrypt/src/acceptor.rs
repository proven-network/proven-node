use crate::MultiResolver;

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::ServerConfig;
use rustls::server::Acceptor;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{Accept, LazyConfigAcceptor, StartHandshake};
use tokio_rustls_acme::acme::ACME_TLS_ALPN_NAME;

#[derive(Clone)]
pub struct AxumAcceptor {
    acme_acceptor: AcmeAcceptor,
    config: Arc<ServerConfig>,
}

impl AxumAcceptor {
    pub fn new(resolver: Arc<MultiResolver>) -> Self {
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(resolver.clone());
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        Self {
            acme_acceptor: AcmeAcceptor::new(resolver),
            config: Arc::new(config),
        }
    }
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static>
    axum_server::accept::Accept<I, S> for AxumAcceptor
{
    type Stream = tokio_rustls::server::TlsStream<I>;
    type Service = S;
    type Future = AxumAccept<I, S>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let acme_accept = self.acme_acceptor.accept(stream);
        AxumAccept {
            acme_accept,
            config: self.config.clone(),
            tls_accept: None,
            service: Some(service),
        }
    }
}

pub struct AxumAccept<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> {
    acme_accept: AcmeAccept<I>,
    config: Arc<ServerConfig>,
    tls_accept: Option<Accept<I>>,
    service: Option<S>,
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> Unpin
    for AxumAccept<I, S>
{
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> Future
    for AxumAccept<I, S>
{
    type Output = io::Result<(tokio_rustls::server::TlsStream<I>, S)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(tls_accept) = &mut self.tls_accept {
                return match Pin::new(&mut *tls_accept).poll(cx) {
                    Poll::Ready(Ok(tls)) => Poll::Ready(Ok((tls, self.service.take().unwrap()))),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                };
            }
            return match Pin::new(&mut self.acme_accept).poll(cx) {
                Poll::Ready(Ok(Some(start_handshake))) => {
                    let config = self.config.clone();
                    self.tls_accept = Some(start_handshake.into_stream(config));
                    continue;
                }
                Poll::Ready(Ok(None)) => {
                    Poll::Ready(Err(io::Error::other("TLS-ALPN-01 validation request")))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

#[derive(Clone)]
pub struct AcmeAcceptor {
    config: Arc<ServerConfig>,
}

impl AcmeAcceptor {
    pub fn new(resolver: Arc<MultiResolver>) -> Self {
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(resolver);
        config.alpn_protocols.push(ACME_TLS_ALPN_NAME.to_vec());

        Self {
            config: Arc::new(config),
        }
    }

    pub fn accept<IO: AsyncRead + AsyncWrite + Unpin>(&self, io: IO) -> AcmeAccept<IO> {
        AcmeAccept::new(io, self.config.clone())
    }
}

pub struct AcmeAccept<IO: AsyncRead + AsyncWrite + Unpin> {
    acceptor: LazyConfigAcceptor<IO>,
    config: Arc<ServerConfig>,
    validation_accept: Option<Accept<IO>>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AcmeAccept<IO> {
    pub(crate) fn new(io: IO, config: Arc<ServerConfig>) -> Self {
        Self {
            acceptor: LazyConfigAcceptor::new(Acceptor::default(), io),
            config,
            validation_accept: None,
        }
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Future for AcmeAccept<IO> {
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
                    if is_validation {
                        self.validation_accept = Some(handshake.into_stream(self.config.clone()));
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
