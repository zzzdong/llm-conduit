//! Listening, TLS termination, serving connections and graceful shutdown.

use std::convert::Infallible;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::error::StartupError;
use crate::gateway::Gateway;
use crate::proxy::RespBody;

/// Run the accept loop until a shutdown signal arrives.
pub async fn run(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    gateway: Arc<Gateway>,
) -> Result<(), StartupError> {
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());

    loop {
        tokio::select! {
            _ = shutdown.as_mut() => {
                info!("shutdown signal received, no longer accepting connections");
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(value) => value,
                    Err(e) => {
                        warn!("accept failed: {e}");
                        continue;
                    }
                };

                let gateway = Arc::clone(&gateway);
                // Watcher is the movable handle of GracefulShutdown (which intentionally has no Clone).
                let watcher = graceful.watcher();

                match &tls {
                    Some(acceptor) => {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    serve(http1::Builder::new(), TokioIo::new(tls_stream), gateway, watcher).await;
                                }
                                Err(e) => warn!(%peer, "TLS handshake failed: {e}"),
                            }
                        });
                    }
                    None => {
                        tokio::spawn(async move {
                            serve(http1::Builder::new(), TokioIo::new(stream), gateway, watcher).await;
                        });
                    }
                }
            }
        }
    }

    graceful.shutdown().await;
    info!("graceful shutdown complete");
    Ok(())
}

/// Serve a single connection and register it with the graceful shutdown tracker.
async fn serve<I>(builder: http1::Builder, io: TokioIo<I>, gateway: Arc<Gateway>, watcher: Watcher)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let gateway = Arc::clone(&gateway);
        async move { Ok::<Response<RespBody>, Infallible>(gateway.handle(request).await) }
    });

    let connection = builder.serve_connection(io, service);
    if let Err(e) = watcher.watch(connection).await {
        debug!("connection closed: {e}");
    }
}

/// Wait for ctrl-c or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::body::{Body, Frame};
    use hyper::header::CONTENT_TYPE;
    use hyper::{Method, StatusCode, Version};

    use super::*;
    use crate::config::Config;
    use crate::tls::build_client;

    /// Chunked response body: proves the response is streamed out rather than buffered first.
    struct Chunks(VecDeque<Bytes>);

    impl Body for Chunks {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(
                self.get_mut()
                    .0
                    .pop_front()
                    .map(|data| Ok(Frame::data(data))),
            )
        }
    }

    /// Start a fake (OpenAI-compatible) upstream: echo the request back and reply with SSE chunks.
    async fn spawn_mock_upstream() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let service = service_fn(|request: Request<Incoming>| async move {
                        let path = request
                            .uri()
                            .path_and_query()
                            .map(|p| p.to_string())
                            .unwrap_or_default();
                        let header = |name: &str| {
                            request
                                .headers()
                                .get(name)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string)
                        };
                        let authorization = header("authorization");
                        let x_model = header("x-model");
                        let host = header("host");
                        let body = request.into_body().collect().await.unwrap().to_bytes();

                        let echo = serde_json::json!({
                            "path": path,
                            "authorization": authorization,
                            "x_model": x_model,
                            "host": host,
                            "body": String::from_utf8_lossy(&body),
                        });

                        let mut chunks = VecDeque::new();
                        chunks.push_back(Bytes::from(format!("data: {echo}\n\n")));
                        chunks.push_back(Bytes::from_static(b"data: [DONE]\n\n"));

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "text/event-stream")
                                .body(Chunks(chunks))
                                .unwrap(),
                        )
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        addr
    }

    /// Start the gateway on a random port and return the listen address.
    async fn spawn_gateway(config_text: &str) -> SocketAddr {
        let config = Config::parse(config_text).unwrap();
        let gateway = Arc::new(Gateway::new(config).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = run(listener, None, gateway).await;
        });
        addr
    }

    fn config_text(upstream: SocketAddr, extra: &str) -> String {
        format!(
            r#"
[server]
listen = "127.0.0.1:0"
{extra}

[auth]
enabled = true
keys = {{ "sk-gateway-0001" = "test-caller" }}

[upstreams.mock]
base_url = "http://{upstream}"
api_key = "upstream-secret"
upstream_model = "served-name"
"#
        )
    }

    async fn send(
        addr: SocketAddr,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (StatusCode, String, String) {
        let client = build_client(false).unwrap();
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(format!("http://{addr}{path}"))
            .version(Version::HTTP_11);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();

        let response = client.request(request).await.unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            content_type,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    fn echo_of(body: &str) -> serde_json::Value {
        let first_line = body
            .lines()
            .next()
            .expect("the response body should contain an SSE data line");
        let payload = first_line.trim_start_matches("data: ");
        serde_json::from_str(payload)
            .expect("the first line should be the JSON echoed by the upstream")
    }

    #[tokio::test]
    async fn routes_rewrites_key_and_streams_response() {
        let upstream = spawn_mock_upstream().await;
        let gateway = spawn_gateway(&config_text(upstream, "default_upstream = \"mock\"")).await;

        let (status, content_type, body) = send(
            gateway,
            "/v1/chat/completions?trace=1",
            &[
                ("authorization", "Bearer sk-gateway-0001"),
                ("content-type", "application/json"),
            ],
            r#"{"model":"mock","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, "text/event-stream");
        assert!(
            body.ends_with("data: [DONE]\n\n"),
            "SSE chunks must be passed through intact: {body}"
        );

        let echo = echo_of(&body);
        assert_eq!(echo["path"], "/v1/chat/completions?trace=1");
        assert_eq!(
            echo["authorization"], "Bearer upstream-secret",
            "should be replaced with the upstream key"
        );
        assert_eq!(
            echo["x_model"],
            serde_json::Value::Null,
            "the internal routing header must not be forwarded"
        );
        assert_eq!(
            echo["host"],
            upstream.to_string(),
            "host should point at the upstream"
        );

        let forwarded: serde_json::Value =
            serde_json::from_str(echo["body"].as_str().unwrap()).unwrap();
        assert_eq!(
            forwarded["model"], "served-name",
            "model should be rewritten"
        );
        assert_eq!(forwarded["messages"][0]["content"], "hi");
        assert_eq!(forwarded["stream"], true);
    }

    #[tokio::test]
    async fn x_model_header_routes_without_passing_through() {
        let upstream = spawn_mock_upstream().await;
        let gateway = spawn_gateway(&config_text(upstream, "")).await;

        let (status, _, body) = send(
            gateway,
            "/v1/completions",
            &[
                ("authorization", "Bearer sk-gateway-0001"),
                ("x-model", "mock"),
            ],
            r#"{"prompt":"hello"}"#,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let echo = echo_of(&body);
        assert_eq!(echo["path"], "/v1/completions");
        assert_eq!(echo["x_model"], serde_json::Value::Null);
        // no model field in the body -> no rewrite, passed through as-is
        let forwarded: serde_json::Value =
            serde_json::from_str(echo["body"].as_str().unwrap()).unwrap();
        assert_eq!(forwarded["prompt"], "hello");
        assert!(forwarded.get("model").is_none());
    }

    #[tokio::test]
    async fn rejects_oversized_request_body() {
        let upstream = spawn_mock_upstream().await;
        let gateway = spawn_gateway(&format!(
            r#"
[server]
listen = "127.0.0.1:0"
max_body_bytes = 64

[auth]
enabled = true
keys = {{ "sk-gateway-0001" = "test-caller" }}

[upstreams.mock]
base_url = "http://{upstream}"
"#
        ))
        .await;

        let oversized = format!(r#"{{"model":"mock","padding":"{}"}}"#, "x".repeat(4096));
        let (status, _, _) = send(
            gateway,
            "/v1/chat/completions",
            &[("authorization", "Bearer sk-gateway-0001")],
            &oversized,
        )
        .await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn rejects_bad_requests_with_openai_style_errors() {
        let upstream = spawn_mock_upstream().await;
        // This config has no default_upstream, so it exercises the "no model specified" branch
        let gateway = spawn_gateway(&config_text(upstream, "")).await;

        // 401: missing unified key
        let (status, _, body) =
            send(gateway, "/v1/chat/completions", &[], r#"{"model":"mock"}"#).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let error: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(error["error"]["type"], "gateway_error");

        // 400: neither X-Model nor body.model, and no default_upstream
        let (status, _, _) = send(
            gateway,
            "/v1/chat/completions",
            &[("authorization", "Bearer sk-gateway-0001")],
            r#"{"messages":[]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // 404: unknown model
        let (status, _, _) = send(
            gateway,
            "/v1/chat/completions",
            &[("authorization", "Bearer sk-gateway-0001")],
            r#"{"model":"ghost"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
