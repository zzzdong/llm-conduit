//! Upstream health probing and the `/healthz` and `/readyz` endpoints.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode, Uri};
use serde_json::json;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::config::{HealthConfig, HealthMode, Upstream};
use crate::proxy::{self, RespBody};
use crate::tls::HttpClient;

/// Path probed in [`HealthMode::Models`]; any `base_url` path prefix is kept.
const MODELS_PATH: &str = "/v1/models";

/// Result of the most recent probe of a single upstream.
#[derive(Debug, Clone)]
pub struct UpstreamHealth {
    pub healthy: bool,
    pub latency: Duration,
    pub checked_at: Instant,
    pub error: Option<String>,
}

/// Health of every upstream, shared between the prober task and `/readyz`.
#[derive(Debug)]
pub struct HealthState {
    names: Vec<String>,
    upstreams: RwLock<BTreeMap<String, UpstreamHealth>>,
}

impl HealthState {
    /// `names` are the configured upstream names, reported by `/readyz` even before
    /// the first probe finished.
    pub fn new(names: Vec<String>) -> Self {
        Self {
            names,
            upstreams: RwLock::new(BTreeMap::new()),
        }
    }

    fn record(&self, name: &str, health: UpstreamHealth) {
        // Nothing panics while the lock is held, so a poisoned lock cannot happen;
        // recovering keeps a panicking probe from taking the endpoint down with it.
        self.upstreams
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(name.to_string(), health);
    }

    /// Last known health of every upstream that has been probed at least once.
    fn snapshot(&self) -> BTreeMap<String, UpstreamHealth> {
        self.upstreams
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// `GET /healthz`: the process is alive and serving. Never depends on upstreams.
pub fn healthz() -> Response<RespBody> {
    json_response(StatusCode::OK, json!({"status": "ok"}))
}

/// `GET /readyz`: 200 only when every upstream passed its last probe, 503 otherwise.
///
/// Upstreams that have not been probed yet count as ready, so enabling probing does
/// not make the gateway unready for the first probe interval.
pub fn readyz(state: &HealthState, probing: bool) -> Response<RespBody> {
    let snapshot = state.snapshot();
    let mut upstreams = serde_json::Map::new();
    let mut ready = true;

    for name in &state.names {
        let entry = match snapshot.get(name) {
            Some(health) if health.healthy => json!({
                "status": "up",
                "latency_ms": health.latency.as_millis() as u64,
                "checked_secs_ago": health.checked_at.elapsed().as_secs(),
            }),
            Some(health) => {
                ready = false;
                json!({
                    "status": "down",
                    "error": health.error.clone().unwrap_or_else(|| "unknown error".to_string()),
                    "checked_secs_ago": health.checked_at.elapsed().as_secs(),
                })
            }
            None => json!({"status": "unchecked"}),
        };
        upstreams.insert(name.clone(), entry);
    }

    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json_response(
        status,
        json!({
            "status": if ready { "ok" } else { "degraded" },
            "probing": if probing { "enabled" } else { "disabled" },
            "upstreams": upstreams,
        }),
    )
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response<RespBody> {
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let mut response = Response::new(proxy::full_body(Bytes::from(bytes)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// Probe every upstream until the task is cancelled. The first pass runs immediately.
pub async fn probe_loop(
    state: Arc<HealthState>,
    upstreams: Arc<BTreeMap<String, Upstream>>,
    secure: HttpClient,
    insecure: Option<HttpClient>,
    config: HealthConfig,
) {
    let interval = Duration::from_secs(config.interval_secs);

    loop {
        let mut probes = JoinSet::new();
        for (name, upstream) in upstreams.iter() {
            let client = if upstream.insecure_skip_verify {
                insecure.clone().unwrap_or_else(|| secure.clone())
            } else {
                secure.clone()
            };
            let upstream = upstream.clone();
            let config = config.clone();
            let name = name.clone();
            probes.spawn(async move { (name, probe(&upstream, &client, &config).await) });
        }

        while let Some(joined) = probes.join_next().await {
            let Ok((name, outcome)) = joined else {
                continue;
            };
            let health = match outcome {
                Ok(latency) => UpstreamHealth {
                    healthy: true,
                    latency,
                    checked_at: Instant::now(),
                    error: None,
                },
                Err(error) => UpstreamHealth {
                    healthy: false,
                    latency: Duration::ZERO,
                    checked_at: Instant::now(),
                    error: Some(error),
                },
            };
            let was_healthy = state.snapshot().get(&name).map(|known| known.healthy);
            // Log state changes, keep the steady state at debug so a long outage does
            // not flood the log with one line per interval.
            match (health.healthy, was_healthy) {
                (true, Some(true)) => debug!(upstream = %name, "upstream is healthy"),
                (true, _) => info!(
                    upstream = %name,
                    latency_ms = health.latency.as_millis() as u64,
                    "upstream is healthy"
                ),
                (false, Some(false)) => {
                    debug!(upstream = %name, error = health.error.as_deref().unwrap_or("-"), "upstream is still unhealthy")
                }
                (false, _) => warn!(
                    upstream = %name,
                    error = health.error.as_deref().unwrap_or("-"),
                    "upstream is unhealthy"
                ),
            }
            state.record(&name, health);
        }

        tokio::time::sleep(interval).await;
    }
}

/// Probe one upstream, returning the round trip latency on success.
async fn probe(
    upstream: &Upstream,
    client: &HttpClient,
    config: &HealthConfig,
) -> Result<Duration, String> {
    let started = Instant::now();
    let timeout = Duration::from_secs(config.timeout_secs);
    match config.mode {
        HealthMode::Tcp => tcp_probe(upstream, timeout).await?,
        HealthMode::Models => models_probe(upstream, client, timeout).await?,
    }
    Ok(started.elapsed())
}

/// Reachability check: can a TCP connection be established?
async fn tcp_probe(upstream: &Upstream, timeout: Duration) -> Result<(), String> {
    let authority = upstream
        .base
        .authority()
        .ok_or_else(|| "upstream base_url has no host".to_string())?;
    let port = authority
        .port_u16()
        .unwrap_or(if upstream.uses_tls() { 443 } else { 80 });
    let address = format!("{}:{port}", authority.host());

    tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&address))
        .await
        .map_err(|_| format!("tcp connect to {address} timed out after {timeout:?}"))?
        .map_err(|error| format!("tcp connect to {address} failed: {error}"))?;
    Ok(())
}

/// Functional check: does the upstream answer `GET /v1/models` with a success status?
async fn models_probe(
    upstream: &Upstream,
    client: &HttpClient,
    timeout: Duration,
) -> Result<(), String> {
    let target = upstream
        .target_uri(&Uri::from_static(MODELS_PATH))
        .map_err(|error| error.message)?;

    let mut builder = Request::builder().method(Method::GET).uri(target);
    if let Some(api_key) = &upstream.api_key {
        builder = builder.header(AUTHORIZATION, format!("Bearer {api_key}"));
    }
    let request = builder
        .body(Full::new(Bytes::new()))
        .map_err(|error| format!("could not build the probe request: {error}"))?;

    let response = tokio::time::timeout(timeout, client.request(request))
        .await
        .map_err(|_| format!("GET {MODELS_PATH} timed out after {timeout:?}"))?
        .map_err(|error| format!("GET {MODELS_PATH} failed: {error}"))?;

    let status = response.status();
    // Drain the body so the pooled connection can be reused.
    let _ = response.into_body().collect().await;

    if status.is_success() {
        Ok(())
    } else {
        Err(format!("GET {MODELS_PATH} returned {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn upstream(base_url: &str) -> Upstream {
        cfg(base_url)
            .resolve_upstreams()
            .unwrap()
            .remove("probe")
            .unwrap()
    }

    fn cfg(base_url: &str) -> Config {
        Config::parse(&format!(
            r#"
[server]
listen = "0.0.0.0:4000"
[upstreams.probe]
base_url = "{base_url}"
"#
        ))
        .unwrap()
    }

    fn config(enabled: bool, mode: HealthMode) -> HealthConfig {
        HealthConfig {
            enabled,
            interval_secs: 10,
            timeout_secs: 1,
            mode,
        }
    }

    fn state(names: &[&str]) -> HealthState {
        HealthState::new(names.iter().map(|name| name.to_string()).collect())
    }

    fn healthy(latency_ms: u64) -> UpstreamHealth {
        UpstreamHealth {
            healthy: true,
            latency: Duration::from_millis(latency_ms),
            checked_at: Instant::now(),
            error: None,
        }
    }

    fn unhealthy(error: &str) -> UpstreamHealth {
        UpstreamHealth {
            healthy: false,
            latency: Duration::ZERO,
            checked_at: Instant::now(),
            error: Some(error.to_string()),
        }
    }

    #[tokio::test]
    async fn healthz_is_always_ok() {
        let response = healthz();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
    }

    #[tokio::test]
    async fn readyz_is_ok_until_an_upstream_is_known_to_be_down() {
        let state = state(&["a", "b"]);
        // Nothing probed yet: optimistic, so the gateway is not unready at boot.
        assert_eq!(readyz(&state, true).status(), StatusCode::OK);

        state.record("a", healthy(3));
        assert_eq!(readyz(&state, true).status(), StatusCode::OK);

        state.record("b", unhealthy("tcp connect failed"));
        let response = readyz(&state, true);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["probing"], "enabled");
        assert_eq!(body["upstreams"]["a"]["status"], "up");
        assert_eq!(body["upstreams"]["a"]["latency_ms"], 3);
        assert_eq!(body["upstreams"]["b"]["status"], "down");
        assert_eq!(body["upstreams"]["b"]["error"], "tcp connect failed");
    }

    #[tokio::test]
    async fn readyz_reports_when_probing_is_disabled() {
        let state = state(&["a"]);
        let response = readyz(&state, false);
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["probing"], "disabled");
        assert_eq!(body["upstreams"]["a"]["status"], "unchecked");
    }

    #[tokio::test]
    async fn tcp_probe_detects_open_and_closed_ports() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open = listener.local_addr().unwrap();
        assert!(
            tcp_probe(&upstream(&format!("http://{open}")), Duration::from_secs(1))
                .await
                .is_ok()
        );

        // Dropping the listener frees the port, so connecting must fail.
        drop(listener);
        let error = tcp_probe(&upstream(&format!("http://{open}")), Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.contains("tcp connect"), "{error}");
    }

    #[tokio::test]
    async fn tcp_probe_uses_scheme_default_ports() {
        let upstream = upstream("http://127.0.0.1");
        let error = tcp_probe(&upstream, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(error.contains("127.0.0.1:80"), "{error}");
    }

    #[tokio::test]
    async fn models_probe_fails_on_an_unreachable_upstream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed = listener.local_addr().unwrap();
        drop(listener);

        let client = crate::tls::build_client(false).unwrap();
        let error = models_probe(
            &upstream(&format!("http://{closed}")),
            &client,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(error.contains("GET /v1/models"), "{error}");
    }

    #[test]
    fn health_mode_names_are_stable() {
        assert_eq!(HealthMode::Models.as_str(), "models");
        assert_eq!(HealthMode::Tcp.as_str(), "tcp");
        assert!(config(true, HealthMode::Tcp).enabled);
    }
}
