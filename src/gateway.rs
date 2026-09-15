//! Request orchestration: auth -> read body -> resolve model -> match upstream -> forward -> log.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use hyper::body::Incoming;
use hyper::http::request::Parts;
use hyper::{Request, Response};
use tracing::{info, warn};

use crate::auth;
use crate::config::{Config, Upstream};
use crate::error::{GatewayError, StartupError};
use crate::json_model;
use crate::proxy::{self, RespBody};
use crate::tls;

/// Routing header, which takes precedence over the `model` field of the body.
const X_MODEL: &str = "x-model";

pub struct Gateway {
    config: Arc<Config>,
    upstreams: BTreeMap<String, Upstream>,
    client: tls::HttpClient,
    insecure_client: Option<tls::HttpClient>,
}

/// Per-request context, kept so that one structured log line can be emitted at the end.
#[derive(Default)]
struct Trace {
    caller: Option<String>,
    model: Option<String>,
    upstream: Option<String>,
    error: Option<String>,
}

impl Gateway {
    pub fn new(config: Config) -> Result<Self, StartupError> {
        config.validate()?;

        let upstreams = config.resolve_upstreams()?;
        let needs_insecure = upstreams.values().any(|u| u.insecure_skip_verify);

        let client = tls::build_client(false)?;
        let insecure_client = if needs_insecure {
            Some(tls::build_client(true)?)
        } else {
            None
        };

        Ok(Self {
            config: Arc::new(config),
            upstreams,
            client,
            insecure_client,
        })
    }

    /// Handle one request. All errors are turned into a response here and never reach the connection layer.
    pub async fn handle(&self, request: Request<Incoming>) -> Response<RespBody> {
        let started = Instant::now();
        let method = request.method().clone();
        let path = request.uri().path().to_string();

        let (parts, body) = request.into_parts();
        let mut trace = Trace::default();

        let response = match self.dispatch(&parts, body, &mut trace).await {
            Ok(response) => response,
            Err(err) => {
                trace.error = Some(err.message.clone());
                proxy::boxed_response(err.into_response())
            }
        };

        self.log(&trace, &method, &path, response.status(), started.elapsed());

        response
    }

    async fn dispatch(
        &self,
        parts: &Parts,
        body: Incoming,
        trace: &mut Trace,
    ) -> Result<Response<RespBody>, GatewayError> {
        // 1. Authenticate
        trace.caller = auth::authenticate(&self.config.auth, &parts.headers)?.map(str::to_string);

        // 2. Read the request body (over the limit -> 413)
        let body = proxy::read_body(body, self.config.server.max_body_bytes).await?;

        // 3. Resolve the model: X-Model -> body.model -> default_upstream
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
        trace.model = Some(model.clone());

        // 4. Match the upstream
        let upstream = self.upstreams.get(&model).ok_or_else(|| {
            GatewayError::not_found(format!(
                "unknown model '{model}': no upstream with that name is configured"
            ))
        })?;
        trace.upstream = Some(upstream.name.clone());

        // 5. Forward (including the optional model rewrite and key mapping)
        let client = if upstream.insecure_skip_verify {
            self.insecure_client.as_ref().unwrap_or(&self.client)
        } else {
            &self.client
        };

        let response = proxy::forward(client, upstream, parts, body, body_model.as_deref()).await?;
        if response.status().is_server_error() {
            warn!(
                upstream = %upstream.name,
                status = response.status().as_u16(),
                "upstream returned 5xx"
            );
        }

        Ok(response)
    }

    fn log(
        &self,
        trace: &Trace,
        method: &hyper::Method,
        path: &str,
        status: hyper::StatusCode,
        elapsed: std::time::Duration,
    ) {
        let caller = trace.caller.as_deref().unwrap_or("-");
        let model = trace.model.as_deref().unwrap_or("-");
        let upstream = trace.upstream.as_deref().unwrap_or("-");
        let elapsed_ms = elapsed.as_millis() as u64;

        if let Some(message) = &trace.error {
            warn!(
                caller,
                model,
                upstream,
                status = status.as_u16(),
                elapsed_ms,
                method = %method,
                path,
                error = %message,
                "request failed"
            );
        } else {
            info!(
                caller,
                model,
                upstream,
                status = status.as_u16(),
                elapsed_ms,
                method = %method,
                path,
                "request completed"
            );
        }
    }
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
}
