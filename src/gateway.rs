//! Request orchestration: request ID -> auth -> read body -> resolve model -> match upstream
//! -> forward -> log.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use hyper::http::request::Parts;
use hyper::{Method, Request, Response, StatusCode};
use serde_json::json;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::auth;
use crate::config::{Config, Upstream};
use crate::error::{GatewayError, StartupError};
use crate::health::{self, HealthState};
use crate::json_model;
use crate::observe::{self, MeteredBody, RequestLog};
use crate::proxy::{self, RespBody};
use crate::tls;

/// Routing header, which takes precedence over the `model` field of the body.
const X_MODEL: &str = "x-model";

/// Endpoints answered by the gateway itself instead of being proxied.
const HEALTHZ_PATH: &str = "/healthz";
const READYZ_PATH: &str = "/readyz";
const MODELS_PATH: &str = "/v1/models";

pub struct Gateway {
    config: Arc<Config>,
    upstreams: Arc<BTreeMap<String, Upstream>>,
    client: tls::HttpClient,
    insecure_client: Option<tls::HttpClient>,
    health: Arc<HealthState>,
    /// Pre-built `GET /v1/models` body; the routing names are fixed after startup.
    models: Bytes,
}

impl Gateway {
    pub fn new(config: Config) -> Result<Self, StartupError> {
        config.validate()?;

        let upstreams = Arc::new(config.resolve_upstreams()?);
        let needs_insecure = upstreams.values().any(|u| u.insecure_skip_verify);

        let client = tls::build_client(false)?;
        let insecure_client = if needs_insecure {
            Some(tls::build_client(true)?)
        } else {
            None
        };

        let models = models_body(upstreams.keys());
        let health = Arc::new(HealthState::new(upstreams.keys().cloned().collect()));

        Ok(Self {
            config: Arc::new(config),
            upstreams,
            client,
            insecure_client,
            health,
            models,
        })
    }

    /// Spawn the background health prober. Returns `None` when probing is disabled.
    pub fn spawn_health_prober(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        let config = self.config.server.health.clone();
        if !config.enabled {
            return None;
        }

        Some(tokio::spawn(health::probe_loop(
            Arc::clone(&self.health),
            Arc::clone(&self.upstreams),
            self.client.clone(),
            self.insecure_client.clone(),
            config,
        )))
    }

    /// Handle one request. All errors are turned into a response here and never reach
    /// the connection layer.
    pub async fn handle(&self, request: Request<Incoming>) -> Response<RespBody> {
        // Health endpoints answer without auth, without touching an upstream and without
        // an info level log line, because orchestrators poll them every few seconds.
        if let Some(response) = self.health_response(&request) {
            return response;
        }

        let started = Instant::now();
        let mut request = request;

        // Reuse the caller's request ID when it supplied a usable one, otherwise generate
        // one; either way it is forwarded upstream and echoed back to the caller.
        let request_id = observe::request_id(request.headers());
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            request
                .headers_mut()
                .insert(HeaderName::from_static(observe::REQUEST_ID_HEADER), value);
        }

        let mut log = RequestLog::new(
            request_id,
            request.method().clone(),
            request.uri().path().to_string(),
            started,
        );

        let (parts, body) = request.into_parts();
        let mut response = match self.dispatch(&parts, body, &mut log).await {
            Ok(response) => response,
            Err(error) => {
                log.error = Some(error.message.clone());
                proxy::boxed_response(error.into_response())
            }
        };

        if let Ok(value) = HeaderValue::from_str(&log.request_id) {
            response
                .headers_mut()
                .insert(HeaderName::from_static(observe::REQUEST_ID_HEADER), value);
        }
        log.status = response.status();
        log.streaming = is_streaming(response.headers());

        // The log line is emitted when the body is done, so that `response_bytes`,
        // `elapsed_ms` and `aborted` describe what actually happened.
        let (parts, body) = response.into_parts();
        let body = MeteredBody::new(
            body,
            Box::new(move |bytes, aborted| log.finish(bytes, aborted)),
        );
        Response::from_parts(parts, body.boxed())
    }

    /// `/healthz` and `/readyz`, or `None` when this is a normal gateway request.
    fn health_response(&self, request: &Request<Incoming>) -> Option<Response<RespBody>> {
        if !matches!(*request.method(), Method::GET | Method::HEAD) {
            return None;
        }

        let response = match request.uri().path() {
            HEALTHZ_PATH => health::healthz(),
            READYZ_PATH => health::readyz(&self.health, self.config.server.health.enabled),
            _ => return None,
        };
        debug!(
            path = %request.uri().path(),
            status = response.status().as_u16(),
            "served health endpoint"
        );
        Some(response)
    }

    /// `GET /v1/models`, answered from the configuration.
    fn models_response(&self) -> Response<RespBody> {
        let mut response = Response::new(proxy::full_body(self.models.clone()));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
    }

    async fn dispatch(
        &self,
        parts: &Parts,
        body: Incoming,
        log: &mut RequestLog,
    ) -> Result<Response<RespBody>, GatewayError> {
        // 1. Authenticate
        log.caller = auth::authenticate(&self.config.auth, &parts.headers)?.map(str::to_string);

        // 2. Aggregate `GET /v1/models` from the configuration, so that an SDK calling
        //    `client.models.list()` only sees names this gateway can actually route.
        if parts.method == Method::GET && parts.uri.path() == MODELS_PATH {
            return Ok(self.models_response());
        }

        // 3. Read the request body (over the limit -> 413)
        let body = proxy::read_body(body, self.config.server.max_body_bytes).await?;
        log.request_bytes = body.len() as u64;

        // 4. Resolve the model: X-Model -> body.model -> default_upstream
        let header_model = header_model(&parts.headers);
        let body_model = match &header_model {
            // Routing is already decided by the header, so the body needs no parsing at all
            Some(_) => None,
            None => json_model::extract_model(&body),
        };
        let model = header_model
            .clone()
            .or_else(|| body_model.clone())
            .or_else(|| self.config.server.default_upstream.clone())
            .ok_or_else(|| {
                GatewayError::bad_request(
                    "no model specified: provide an 'X-Model' header, a top-level 'model' field in the body, or configure server.default_upstream",
                )
            })?;

        // 5. Match the upstream
        let upstream = self.upstreams.get(&model).ok_or_else(|| {
            GatewayError::not_found(format!(
                "unknown model '{model}': no upstream with that name is configured"
            ))
        })?;
        log.model = Some(model);
        log.upstream = Some(upstream.name.clone());

        // 6. Forward (including the optional model rewrite and key mapping)
        let client = if upstream.insecure_skip_verify {
            self.insecure_client.as_ref().unwrap_or(&self.client)
        } else {
            &self.client
        };

        let response = proxy::forward(client, upstream, parts, body, body_model.as_deref()).await?;
        if response.status().is_server_error() {
            tracing::warn!(
                upstream = %upstream.name,
                status = response.status().as_u16(),
                "upstream returned 5xx"
            );
        }

        Ok(response)
    }
}

/// Build the OpenAI style `GET /v1/models` payload: one entry per configured upstream,
/// which is exactly the set of values accepted as a `model`.
fn models_body<'a>(names: impl Iterator<Item = &'a String>) -> Bytes {
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let data: Vec<serde_json::Value> = names
        .map(|name| {
            json!({
                "id": name,
                "object": "model",
                "created": created,
                "owned_by": env!("CARGO_PKG_NAME"),
            })
        })
        .collect();

    Bytes::from(serde_json::to_vec(&json!({ "object": "list", "data": data })).unwrap_or_default())
}

/// A response streams when the upstream answers with server-sent events.
fn is_streaming(headers: &hyper::HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
}

/// Read the `X-Model` header.
fn header_model(headers: &hyper::HeaderMap) -> Option<String> {
    let value = headers.get(X_MODEL)?.to_str().ok()?.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    #[test]
    fn header_model_is_trimmed_and_validated() {
        let mut headers = hyper::HeaderMap::new();
        assert!(header_model(&headers).is_none());

        headers.insert("x-model", HeaderValue::from_static("  "));
        assert!(header_model(&headers).is_none());

        headers.insert(
            "x-model",
            HeaderValue::from_static(" llama-3.1-8b-instruct "),
        );
        assert_eq!(
            header_model(&headers).as_deref(),
            Some("llama-3.1-8b-instruct")
        );
    }

    #[test]
    fn models_body_lists_every_configured_upstream() {
        // Production passes the keys of a BTreeMap, so the list is alphabetical.
        let names = ["a-model".to_string(), "b-model".to_string()];
        let body: serde_json::Value = serde_json::from_slice(&models_body(names.iter())).unwrap();

        assert_eq!(body["object"], "list");
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
        assert_eq!(body["data"][0]["id"], "a-model");
        assert_eq!(body["data"][1]["id"], "b-model");
        assert_eq!(body["data"][0]["object"], "model");
        assert_eq!(body["data"][0]["owned_by"], env!("CARGO_PKG_NAME"));
        assert!(body["data"][0]["created"].is_number());
    }

    #[test]
    fn streaming_is_detected_from_the_content_type() {
        let mut headers = hyper::HeaderMap::new();
        assert!(!is_streaming(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!is_streaming(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        assert!(is_streaming(&headers));
    }
}
