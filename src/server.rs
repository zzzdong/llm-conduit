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
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::body::{Body, Frame};
    use hyper::header::CONTENT_TYPE;
    use hyper::{HeaderMap, Method, StatusCode, Version};

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
        spawn_mock_upstream_with(&[]).await
    }

    /// Same, but the given chunks are appended before `[DONE]`, so a test can let the upstream
    /// report a `usage` object.
    async fn spawn_mock_upstream_with(extra: &'static [&'static str]) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| async move {
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
                        let x_request_id = header("x-request-id");
                        let host = header("host");
                        let body = request.into_body().collect().await.unwrap().to_bytes();

                        let echo = serde_json::json!({
                            "path": path,
                            "authorization": authorization,
                            "x_model": x_model,
                            "x_request_id": x_request_id,
                            "host": host,
                            "body": String::from_utf8_lossy(&body),
                        });

                        let mut chunks = VecDeque::new();
                        chunks.push_back(Bytes::from(format!("data: {echo}\n\n")));
                        for chunk in extra {
                            chunks.push_back(Bytes::from_static(chunk.as_bytes()));
                        }
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
        gateway.spawn_health_prober();
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

    /// Send any request and return the status, the response headers and the body.
    async fn send_full(
        addr: SocketAddr,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (StatusCode, HeaderMap, String) {
        let client = build_client(false).unwrap();
        let mut builder = Request::builder()
            .method(method)
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
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    async fn send(
        addr: SocketAddr,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (StatusCode, String, String) {
        let (status, response_headers, body) =
            send_full(addr, Method::POST, path, headers, body).await;
        let content_type = response_headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        (status, content_type, body)
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

    /// Poll `/readyz` until it reports `expected`, so the assertions do not depend on
    /// how quickly the first probe pass finishes.
    async fn wait_for_readyz(addr: SocketAddr, expected: StatusCode) -> serde_json::Value {
        for _ in 0..40 {
            let (status, _, body) = send_full(addr, Method::GET, "/readyz", &[], "").await;
            if status == expected {
                return serde_json::from_str(&body).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("/readyz never returned {expected}");
    }

    #[tokio::test]
    async fn health_endpoints_are_not_proxied_and_report_upstream_state() {
        let upstream = spawn_mock_upstream().await;
        let gateway = spawn_gateway(&config_text(upstream, "")).await;

        // /healthz is unauthenticated, always 200 and never touches an upstream.
        let (status, headers, body) = send_full(gateway, Method::GET, "/healthz", &[], "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["status"],
            "ok"
        );

        // The mock upstream answers the /v1/models probe, so everything is up.
        let body = wait_for_readyz(gateway, StatusCode::OK).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["probing"], "enabled");
        assert_eq!(body["upstreams"]["mock"]["status"], "up");
        assert!(body["upstreams"]["mock"]["latency_ms"].is_number());
        assert_eq!(body["upstreams"].as_object().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn readyz_is_unavailable_when_an_upstream_is_unreachable() {
        // Nothing listens on this port: the address comes from a dropped listener.
        let closed = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            addr
        };
        let gateway = spawn_gateway(&config_text(closed, "")).await;

        let body = wait_for_readyz(gateway, StatusCode::SERVICE_UNAVAILABLE).await;
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["upstreams"]["mock"]["status"], "down");
        assert!(
            body["upstreams"]["mock"]["error"]
                .as_str()
                .unwrap()
                .contains("GET /v1/models"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn models_endpoint_aggregates_the_configured_upstreams() {
        let upstream = spawn_mock_upstream().await;
        let gateway = spawn_gateway(&config_text(upstream, "")).await;

        // It is a regular /v1 endpoint, so auth applies.
        let (status, _, _) = send_full(gateway, Method::GET, "/v1/models", &[], "").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let (status, headers, body) = send_full(
            gateway,
            Method::GET,
            "/v1/models",
            &[("authorization", "Bearer sk-gateway-0001")],
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/json");

        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["object"], "list");
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"][0]["id"], "mock");
        assert_eq!(body["data"][0]["object"], "model");
        assert_eq!(body["data"][0]["owned_by"], "llm-conduit");
    }

    /// Run `body` with the process log redirected to a string, and return what was written.
    ///
    /// The request log is emitted from the connection task, which runs on another worker thread,
    /// so a thread-local subscriber would not see it. The global subscriber is therefore
    /// installed once and writes into `CAPTURE`, and this helper takes the lock for the whole
    /// call so the tests that use it cannot interleave.
    fn captured_log_lines<F, Fut>(body: F) -> String
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        static CAPTURE: Mutex<Vec<u8>> = Mutex::new(Vec::new());

        struct Shared;
        impl std::io::Write for Shared {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                CAPTURE.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(|| Shared)
                .with_ansi(false)
                .without_time()
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        });

        // A dedicated guard: the capture buffer itself is locked per write, so it cannot also
        // serialize the tests. Held across the request so they do not interleave.
        static SERIALIZE: Mutex<()> = Mutex::new(());
        let _serialize = SERIALIZE.lock().unwrap_or_else(|error| error.into_inner());

        CAPTURE.lock().unwrap().clear();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(body());
        String::from_utf8(CAPTURE.lock().unwrap().clone()).unwrap()
    }

    /// Lines of the captured log that describe the proxied request, i.e. not the health prober.
    fn request_log_line(lines: &str) -> &str {
        lines
            .lines()
            .find(|line| line.contains("path=/v1/chat/completions"))
            .unwrap_or_else(|| panic!("no request log line in: {lines}"))
    }

    #[test]
    fn a_completed_request_logs_effort_and_token_usage() {
        let lines = captured_log_lines(|| async {
            let upstream = spawn_mock_upstream_with(&[
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":34,\"total_tokens\":46,\"completion_tokens_details\":{\"reasoning_tokens\":20}}}\n\n",
            ])
            .await;
            let gateway =
                spawn_gateway(&config_text(upstream, "default_upstream = \"mock\"")).await;

            let (status, _, _) = send(
                gateway,
                "/v1/chat/completions",
                &[("authorization", "Bearer sk-gateway-0001")],
                r#"{"model":"mock","messages":[],"reasoning_effort":"high","max_tokens":512,"n":3}"#,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        });

        let line = request_log_line(&lines);
        for expected in [
            "effort=high",
            "max_tokens=512",
            "requested_choices=3",
            "prompt_tokens=12",
            "completion_tokens=34",
            "total_tokens=46",
            "reasoning_tokens=20",
            // Counters the upstream did not report stay in the line as `-`.
            "cached_tokens=-",
        ] {
            assert!(line.contains(expected), "missing {expected} in: {line}");
        }
    }

    #[test]
    fn a_streamed_usage_split_over_two_writes_is_logged() {
        let lines = captured_log_lines(|| async {
            // The upstream flushes the second half of the usage object as its own chunk, which
            // is what the scanner's carry-over buffer exists for.
            let upstream = spawn_mock_upstream_with(&[
                "data: {\"choices\":[],\"usage\":{\"prompt",
                "_tokens\":7,\"completion_tokens\":9}}\n\n",
            ])
            .await;
            let gateway =
                spawn_gateway(&config_text(upstream, "default_upstream = \"mock\"")).await;

            let (status, _, _) = send(
                gateway,
                "/v1/chat/completions",
                &[("authorization", "Bearer sk-gateway-0001")],
                r#"{"model":"mock","messages":[]}"#,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        });

        let line = request_log_line(&lines);
        for expected in [
            "prompt_tokens=7",
            "completion_tokens=9",
            "total_tokens=16",
            // No effort was requested, so the field is present but empty.
            "effort=-",
        ] {
            assert!(line.contains(expected), "missing {expected} in: {line}");
        }
    }

    #[tokio::test]
    async fn request_ids_are_reused_forwarded_and_echoed_back() {
        let upstream = spawn_mock_upstream().await;
        let gateway = spawn_gateway(&config_text(upstream, "default_upstream = \"mock\"")).await;

        // A caller supplied ID is reused in the response and in the upstream request.
        let (status, headers, body) = send_full(
            gateway,
            Method::POST,
            "/v1/chat/completions",
            &[
                ("authorization", "Bearer sk-gateway-0001"),
                ("x-request-id", "caller-supplied-id"),
            ],
            r#"{"model":"mock","messages":[]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get("x-request-id").unwrap(), "caller-supplied-id");
        assert_eq!(echo_of(&body)["x_request_id"], "caller-supplied-id");

        // Without one, the gateway generates it and forwards that same value.
        let (status, headers, body) = send_full(
            gateway,
            Method::POST,
            "/v1/chat/completions",
            &[("authorization", "Bearer sk-gateway-0001")],
            r#"{"model":"mock","messages":[]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let generated = headers
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(generated.len(), 36, "generated IDs are UUID shaped");
        assert_eq!(echo_of(&body)["x_request_id"], generated.as_str());
    }
}
