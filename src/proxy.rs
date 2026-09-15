//! Forwarding: body reading, header filtering, upstream request building, streamed response passthrough.

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Incoming};
use hyper::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, HeaderMap, HeaderName, HeaderValue,
};
use hyper::http::request::Parts;
use hyper::{Request, Response, Uri, Version};

use crate::config::Upstream;
use crate::error::{BoxError, GatewayError};
use crate::json_model;
use crate::tls::HttpClient;

/// Response body type: forwarded as it arrives instead of being buffered as a whole.
pub type RespBody = BoxBody<Bytes, BoxError>;

/// Hop-by-hop headers, which a proxy has to filter out per RFC 7230.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Extra headers stripped from the request:
/// - `host`: must be regenerated from the upstream URI;
/// - `authorization` / `x-api-key`: re-injected by the key mapping;
/// - `x-model`: gateway-internal routing header, never leaked upstream;
/// - `expect`: keeps 100-continue semantics from being misread by an intermediary.
const REQUEST_STRIP: &[&str] = &[
    "host",
    "content-length",
    "authorization",
    "x-api-key",
    "x-model",
    "expect",
];

/// Nothing extra is stripped from responses; only the hop-by-hop rules apply.
const RESPONSE_STRIP: &[&str] = &[];

/// Wrap a fixed-length body into the unified response body type.
pub fn full_body(bytes: impl Into<Bytes>) -> RespBody {
    Full::new(bytes.into())
        .map_err(|e: Infallible| -> BoxError { match e {} })
        .boxed()
}

/// Convert an error response into the unified response body type.
pub fn boxed_response(response: Response<Full<Bytes>>) -> Response<RespBody> {
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, full_body(body.into_inner().unwrap_or_default()))
}

/// Read the client request body; returns 413 when the limit is exceeded.
pub async fn read_body(body: Incoming, max_body_bytes: u64) -> Result<Bytes, GatewayError> {
    // With a known Content-Length, reject early and pre-allocate, so large bodies do not re-grow.
    let declared_len = body.size_hint().exact();
    if let Some(len) = declared_len
        && len > max_body_bytes
    {
        return Err(payload_too_large(max_body_bytes));
    }

    let capacity = declared_len.unwrap_or(0).min(max_body_bytes) as usize;
    let mut body = body;
    let mut buffer = Vec::with_capacity(capacity);
    while let Some(frame) = body.frame().await {
        let frame = frame
            .map_err(|e| GatewayError::bad_request(format!("failed to read request body: {e}")))?;
        if let Ok(chunk) = frame.into_data() {
            if buffer.len() as u64 + chunk.len() as u64 > max_body_bytes {
                return Err(payload_too_large(max_body_bytes));
            }
            buffer.extend_from_slice(&chunk);
        }
    }
    Ok(Bytes::from(buffer))
}

fn payload_too_large(max_body_bytes: u64) -> GatewayError {
    GatewayError::payload_too_large(format!(
        "request body exceeds the {max_body_bytes}-byte limit"
    ))
}

/// Forward the request upstream and stream the upstream response back to the client.
///
/// `body_model` is the model already extracted from the body; a rewrite only happens when it
/// differs from `upstream.upstream_model`.
pub async fn forward(
    client: &HttpClient,
    upstream: &Upstream,
    parts: &Parts,
    body: Bytes,
    body_model: Option<&str>,
) -> Result<Response<RespBody>, GatewayError> {
    let body = match upstream.upstream_model.as_deref() {
        Some(target) if body_model == Some(target) => body,
        Some(target) => json_model::try_rewrite_model(&body, target).unwrap_or(body),
        None => body,
    };

    let target_uri = upstream.target_uri(&parts.uri)?;

    let request = build_request(upstream, parts, target_uri, body)?;

    let upstream_response = client.request(request).await.map_err(|e| {
        GatewayError::bad_gateway(format!("upstream '{}' request failed: {e}", upstream.name))
    })?;

    let (upstream_parts, upstream_body) = upstream_response.into_parts();
    let mut headers = HeaderMap::new();
    copy_headers(&upstream_parts.headers, &mut headers, RESPONSE_STRIP);

    let body = upstream_body
        .map_err(|e| -> BoxError { Box::new(e) })
        .boxed();

    let mut response = Response::new(body);
    *response.status_mut() = upstream_parts.status;
    *response.version_mut() = Version::HTTP_11;
    *response.headers_mut() = headers;
    Ok(response)
}

/// Build the request that is sent upstream.
fn build_request(
    upstream: &Upstream,
    parts: &Parts,
    target_uri: Uri,
    body: Bytes,
) -> Result<Request<Full<Bytes>>, GatewayError> {
    let mut headers = HeaderMap::new();
    copy_headers(&parts.headers, &mut headers, REQUEST_STRIP);

    // The original content-length was stripped, so declare the real body length here.
    headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len() as u64));

    if let Some(api_key) = &upstream.api_key {
        let value = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|_| GatewayError::internal("upstream api_key contains invalid characters"))?;
        headers.insert(AUTHORIZATION, value);
    }

    let mut request = Request::builder()
        .method(parts.method.clone())
        .uri(target_uri)
        .version(Version::HTTP_11)
        .body(Full::new(body))
        .map_err(|e| GatewayError::internal(format!("failed to build upstream request: {e}")))?;
    *request.headers_mut() = headers;
    Ok(request)
}

/// Copy end-to-end headers, filtering hop-by-hop headers, headers named in `Connection`, and `extra_strip`.
///
/// The request side additionally drops host / content-length / credentials / routing headers;
/// the response side only drops hop-by-hop headers and keeps `content-length` (the upstream length
/// is accurate, and keeping it avoids pointless chunked encoding).
fn copy_headers(source: &HeaderMap, target: &mut HeaderMap, extra_strip: &[&str]) {
    let tokens = connection_tokens(source);
    for (name, value) in source.iter() {
        if HOP_BY_HOP.contains(&name.as_str())
            || extra_strip.contains(&name.as_str())
            || tokens.iter().any(|token| token == name)
        {
            continue;
        }
        target.append(name, value.clone());
    }
}

/// Parse the custom hop-by-hop headers named in the `Connection` header.
fn connection_tokens(headers: &HeaderMap) -> Vec<HeaderName> {
    let mut tokens = Vec::new();
    for value in headers.get_all(CONNECTION).iter() {
        let Ok(text) = value.to_str() else { continue };
        for token in text.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            if let Ok(name) = HeaderName::from_bytes(token.as_bytes()) {
                tokens.push(name);
            }
        }
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Method;
    use hyper::header::CONTENT_TYPE;

    fn upstream(base_url: &str) -> Upstream {
        let cfg = crate::config::Config::parse(&format!(
            r#"
[server]
listen = "0.0.0.0:4000"
[upstreams.a]
base_url = "{base_url}"
api_key = "upstream-secret"
"#
        ))
        .unwrap();
        cfg.resolve_upstreams().unwrap().remove("a").unwrap()
    }

    fn parts(headers: &[(&str, &str)]) -> Parts {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/v1/chat/completions");
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        builder.body(()).unwrap().into_parts().0
    }

    #[test]
    fn strips_hop_by_hop_and_rewrites_key() {
        let up = upstream("http://127.0.0.1:8000");
        let p = parts(&[
            ("authorization", "Bearer sk-gateway-0001"),
            ("x-api-key", "sk-gateway-0001"),
            ("x-model", "llama-3.1-8b-instruct"),
            ("host", "gateway:4000"),
            ("content-length", "999"),
            ("connection", "keep-alive, x-custom"),
            ("x-custom", "drop-me"),
            ("content-type", "application/json"),
            ("accept", "text/event-stream"),
        ]);
        let target: Uri = "http://127.0.0.1:8000/v1/chat/completions".parse().unwrap();
        let request = build_request(&up, &p, target, Bytes::from_static(b"{}")).unwrap();

        let headers = request.headers();
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap(),
            "Bearer upstream-secret"
        );
        assert!(headers.get("x-api-key").is_none());
        assert!(headers.get("x-model").is_none());
        assert!(
            headers.get("host").is_none(),
            "host must be regenerated from the upstream URI"
        );
        assert!(
            headers.get("x-custom").is_none(),
            "headers named in Connection must be stripped"
        );
        assert!(headers.get("keep-alive").is_none());
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), "2");
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(headers.get("accept").unwrap(), "text/event-stream");
    }

    #[test]
    fn omits_authorization_when_upstream_has_no_key() {
        let cfg = crate::config::Config::parse(
            r#"
[server]
listen = "0.0.0.0:4000"
[upstreams.a]
base_url = "http://127.0.0.1:8000"
"#,
        )
        .unwrap();
        let up = cfg.resolve_upstreams().unwrap().remove("a").unwrap();
        let p = parts(&[("authorization", "Bearer sk-gateway-0001")]);
        let target: Uri = "http://127.0.0.1:8000/v1/models".parse().unwrap();
        let request = build_request(&up, &p, target, Bytes::new()).unwrap();
        assert!(request.headers().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn keeps_content_headers_of_upstream_response() {
        let mut source = HeaderMap::new();
        source.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        source.insert(CONTENT_LENGTH, HeaderValue::from_static("128"));
        source.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        source.insert("x-request-id", HeaderValue::from_static("abc"));
        let mut target = HeaderMap::new();
        copy_headers(&source, &mut target, RESPONSE_STRIP);
        assert_eq!(target.get(CONTENT_TYPE).unwrap(), "text/event-stream");
        assert_eq!(target.get("x-request-id").unwrap(), "abc");
        assert!(target.get("transfer-encoding").is_none());
        assert_eq!(
            target.get(CONTENT_LENGTH).unwrap(),
            "128",
            "the response side must keep the upstream content-length instead of chunking"
        );
    }

    #[test]
    fn oversized_body_maps_to_413() {
        assert_eq!(payload_too_large(10).status.as_u16(), 413);
    }
}
