//! Error types.
//!
//! Two kinds of errors:
//! - [`StartupError`]: fatal startup failure (config, certificates, port in use); the process exits.
//! - [`GatewayError`]: request-time failure; rendered as an OpenAI-style JSON body.

use std::fmt;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header;
use hyper::{Response, StatusCode};

/// Unified boxed error, used for boxed bodies.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Fatal error raised during startup.
#[derive(Debug, Clone)]
pub struct StartupError(pub String);

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StartupError {}

impl From<String> for StartupError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StartupError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Request-time error carrying the HTTP status code returned to the client.
#[derive(Debug, Clone)]
pub struct GatewayError {
    pub status: StatusCode,
    pub message: String,
}

impl GatewayError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    /// 401: the caller key is missing or invalid.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }

    /// 400: malformed request (no model specified, unparsable body, ...).
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    /// 404: unknown model.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    /// 413: request body exceeds the configured limit.
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, message)
    }

    /// 502: upstream unreachable or forwarding failed.
    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, message)
    }

    /// 500: internal gateway error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    /// Render as an OpenAI-style error response:
    /// `{"error":{"message":"...","type":"gateway_error"}}`
    pub fn into_response(self) -> Response<Full<Bytes>> {
        let payload = serde_json::json!({
            "error": {
                "message": self.message,
                "type": "gateway_error",
            }
        });
        let body = serde_json::to_vec(&payload).unwrap_or_else(|_| {
            br#"{"error":{"message":"internal error","type":"gateway_error"}}"#.to_vec()
        });
        Response::builder()
            .status(self.status)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Full::new(Bytes::from(body)))
            .expect("statically built response is always valid")
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status.as_u16(), self.message)
    }
}

impl std::error::Error for GatewayError {}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn error_response_has_openai_shape() {
        let resp = GatewayError::not_found("unknown model 'x'").into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"]["message"], "unknown model 'x'");
        assert_eq!(value["error"]["type"], "gateway_error");
    }

    #[test]
    fn error_constructors_map_to_expected_status() {
        assert_eq!(GatewayError::unauthorized("").status.as_u16(), 401);
        assert_eq!(GatewayError::bad_request("").status.as_u16(), 400);
        assert_eq!(GatewayError::not_found("").status.as_u16(), 404);
        assert_eq!(GatewayError::payload_too_large("").status.as_u16(), 413);
        assert_eq!(GatewayError::bad_gateway("").status.as_u16(), 502);
        assert_eq!(GatewayError::internal("").status.as_u16(), 500);
    }
}
