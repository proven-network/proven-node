use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
use tracing::info;

pub struct HttpServer {
    listen_addr: SocketAddr,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl HttpServer {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<()>, HttpServerError> {
        if self.task_tracker.is_closed() {
            return Err(HttpServerError::AlreadyStarted);
        }

        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(HttpServerError::BindError)?;

        let shutdown_token = self.shutdown_token.clone();
        let handle = self.task_tracker.spawn(async move {

            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                if let Err(err) = http1::Builder::new()
                                    .timer(TokioTimer::new())
                                    .serve_connection(TokioIo::new(stream), service_fn(redirect_to_https))
                                    .await
                                {
                                    error!("error serving connection: {:?}", err);
                                }
                            }
                            Err(err) => {
                                error!("error accepting connection: {:?}", err);
                            }
                        }
                    }
                }
            }
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

async fn redirect_to_https(
    req: Request<hyper::body::Incoming>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let host = req
        .headers()
        .get(header::HOST)
        .map(|h| h.to_str().unwrap())
        .unwrap_or("weareborderline.com");
    let path = req.uri().path();
    let query = req.uri().query();
    let location = format!(
        "https://{}{}{}",
        host,
        path,
        query.map(|q| format!("?{}", q)).unwrap_or_default()
    );

    let resp = Response::builder()
        .status(301)
        .header("Location", location)
        .body(Full::new(Bytes::from("Redirecting to HTTPS...")))
        .unwrap();

    Ok(resp)
}

#[derive(Debug)]
pub enum HttpServerError {
    AlreadyStarted,
    BindError(std::io::Error),
}

impl std::fmt::Display for HttpServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HttpServerError::AlreadyStarted => write!(f, "http server already started"),
            HttpServerError::BindError(err) => write!(f, "error binding to address: {}", err),
        }
    }
}

impl std::error::Error for HttpServerError {}
