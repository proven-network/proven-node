//! Routes local HTTP requests from loopback to target services over messaging primitives.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod client;
mod error;
mod request;
mod response;
mod service;
mod service_handler;

pub use client::*;
pub use error::*;
pub use request::*;
pub use response::*;
pub use service::*;

/// The error type for deserializing HTTP proxy requests.
pub type DeserializeError = ciborium::de::Error<std::io::Error>;

/// The error type for serializing HTTP proxy requests.
pub type SerializeError = ciborium::ser::Error<std::io::Error>;

/// The name of the HTTP proxy service.
pub const SERVICE_NAME: &str = "HTTP_PROXY";

#[cfg(test)]
mod tests {
    use crate::client::{HttpProxyClient, HttpProxyClientOptions};
    use crate::service::{HttpProxyService, HttpProxyServiceOptions};

    use std::net::{Ipv4Addr, SocketAddr};

    use axum::{Router, response::IntoResponse};
    use bytes::Bytes;
    use http::{HeaderMap, Method, StatusCode, Uri};
    use proven_bootable::Bootable;
    use proven_logger_macros::logged_test;
    use proven_messaging::stream::InitializedStream;
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        service::MemoryServiceOptions,
        stream::{InitializedMemoryStream, MemoryStreamOptions},
    };
    use serde::{Deserialize, Serialize};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct EchoResponse {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Option<String>,
    }

    async fn echo_handler(
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let path = uri
            .path_and_query()
            .map_or_else(|| uri.path().to_string(), std::string::ToString::to_string);

        let echo_response = EchoResponse {
            method: method.to_string(),
            path: path.clone(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect(),
            body: if body.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(&body).to_string())
            },
        };

        let response_body = serde_json::to_string(&echo_response).unwrap();
        let mut response_headers = HeaderMap::new();
        response_headers.insert("X-Echo-Method", method.to_string().parse().unwrap());
        response_headers.insert("X-Echo-Path", path.parse().unwrap());

        (StatusCode::OK, response_headers, response_body).into_response()
    }

    async fn run_echo_server() -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let app = Router::new().fallback(echo_handler);

        let handle = tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });

        (addr, handle)
    }

    #[logged_test]
    #[tokio::test]
    async fn test_http_proxy_integration() {
        let test_timeout = std::time::Duration::from_secs(5);

        let result = tokio::time::timeout(test_timeout, async {
            // 1. Start Echo Server
            let (echo_addr, echo_server_handle) = run_echo_server().await;

            // 2. Setup Stream
            let stream =
                InitializedMemoryStream::new("test_http_proxy_integration", MemoryStreamOptions)
                    .await
                    .expect("Failed to create memory stream");

            // 3. Setup Proxy Client
            let client_listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .unwrap();
            let client_addr = client_listener.local_addr().unwrap();
            let client_port = client_addr.port();
            drop(client_listener);

            let client_options = HttpProxyClientOptions {
                client_options: MemoryClientOptions,
                http_port: client_port,
                stream: stream.clone(),
            };
            let proxy_client = HttpProxyClient::new(client_options);
            proxy_client
                .start()
                .await
                .expect("Failed to start proxy client");

            // 4. Setup Proxy Service
            let service_options = HttpProxyServiceOptions {
                service_options: MemoryServiceOptions,
                stream: stream.clone(),
                target_addr: echo_addr,
            };
            let proxy_service = HttpProxyService::new(service_options)
                .await
                .expect("Failed to create proxy service");
            proxy_service
                .start()
                .await
                .expect("Failed to start proxy service");

            // 5. Make Request through Proxy Client
            let http_client = reqwest::Client::new();
            let test_path = "/test/path?query=1";
            let test_body = "Hello, Proxy!";
            let test_url = format!("http://{client_addr}{test_path}");

            let response = http_client
                .post(&test_url)
                .header("X-Custom-Header", "TestValue")
                .body(test_body)
                .send()
                .await
                .expect("Failed to send request to proxy client");

            // 6. Verify Response
            assert_eq!(response.status(), StatusCode::OK);

            let headers = response.headers();
            assert_eq!(headers.get("X-Echo-Method").unwrap(), "POST");
            assert_eq!(headers.get("X-Echo-Path").unwrap(), test_path);

            let body_bytes = response
                .bytes()
                .await
                .expect("Failed to read response body");
            let echo_response: EchoResponse =
                serde_json::from_slice(&body_bytes).expect("Failed to parse echo response body");

            assert_eq!(echo_response.method, "POST");
            assert_eq!(echo_response.path, test_path);
            assert_eq!(echo_response.body.unwrap(), test_body);

            let custom_header = echo_response
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("x-custom-header"));
            assert!(custom_header.is_some());
            assert_eq!(custom_header.unwrap().1, "TestValue");

            // 7. Cleanup
            proxy_client
                .shutdown()
                .await
                .expect("Client shutdown failed");
            proxy_service
                .shutdown()
                .await
                .expect("Service shutdown failed");
            echo_server_handle.abort();
        })
        .await;

        assert!(result.is_ok(), "Test timed out after {test_timeout:?}");
    }
}
