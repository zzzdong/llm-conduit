//! Caller authentication: validates the unified key, returns 401 on failure.

use hyper::header::{AUTHORIZATION, HeaderMap};

use crate::config::AuthConfig;
use crate::error::GatewayError;

/// Alternative auth header, accepted for compatibility with some clients.
pub const API_KEY_HEADER: &str = "x-api-key";

/// Validate the unified key carried by the request.
///
/// - With `auth.enabled = false` every request gets through and `Ok(None)` is returned.
/// - On success, the description mapped to the key is returned (used for audit logs).
pub fn authenticate<'a>(
    auth: &'a AuthConfig,
    headers: &HeaderMap,
) -> Result<Option<&'a str>, GatewayError> {
    if !auth.enabled {
        return Ok(None);
    }

    let key = extract_key(headers).ok_or_else(|| {
        GatewayError::unauthorized(
            "missing credentials: provide 'Authorization: Bearer <key>' or 'X-API-Key'",
        )
    })?;

    auth.keys
        .get(key)
        .map(|desc| Some(desc.as_str()))
        .ok_or_else(|| GatewayError::unauthorized("invalid API key"))
}

/// Tries `Authorization` first, then `X-API-Key`; returns the key without the `Bearer ` prefix.
fn extract_key(headers: &HeaderMap) -> Option<&str> {
    if let Some(value) = headers.get(AUTHORIZATION)
        && let Ok(raw) = value.to_str()
    {
        let key = strip_bearer(raw);
        if !key.is_empty() {
            return Some(key);
        }
    }

    if let Some(value) = headers.get(API_KEY_HEADER)
        && let Ok(raw) = value.to_str()
    {
        let key = raw.trim();
        if !key.is_empty() {
            return Some(key);
        }
    }

    None
}

/// Strips the case-insensitive `Bearer ` prefix.
fn strip_bearer(raw: &str) -> &str {
    let trimmed = raw.trim();
    // Use get(..7) to avoid panicking on a non-ASCII char boundary.
    match trimmed.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => trimmed[7..].trim(),
        _ => trimmed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    fn auth_config(enabled: bool) -> AuthConfig {
        let mut keys = std::collections::BTreeMap::new();
        keys.insert("sk-gateway-0001".to_string(), "frontend".to_string());
        AuthConfig { enabled, keys }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    #[test]
    fn disabled_auth_skips_validation() {
        let cfg = auth_config(false);
        assert!(authenticate(&cfg, &HeaderMap::new()).unwrap().is_none());
    }

    #[test]
    fn bearer_token_is_accepted_case_insensitively() {
        let cfg = auth_config(true);
        let h = headers(&[("authorization", "bearer sk-gateway-0001")]);
        assert_eq!(authenticate(&cfg, &h).unwrap(), Some("frontend"));
    }

    #[test]
    fn x_api_key_is_accepted() {
        let cfg = auth_config(true);
        let h = headers(&[("x-api-key", "sk-gateway-0001")]);
        assert_eq!(authenticate(&cfg, &h).unwrap(), Some("frontend"));
    }

    #[test]
    fn missing_and_invalid_key_are_rejected() {
        let cfg = auth_config(true);
        assert_eq!(
            authenticate(&cfg, &HeaderMap::new())
                .unwrap_err()
                .status
                .as_u16(),
            401
        );
        let h = headers(&[("authorization", "Bearer sk-wrong")]);
        assert_eq!(authenticate(&cfg, &h).unwrap_err().status.as_u16(), 401);
    }

    #[test]
    fn strip_bearer_handles_edge_cases() {
        assert_eq!(strip_bearer("  Bearer  abc "), "abc");
        assert_eq!(strip_bearer("abc"), "abc");
        assert_eq!(strip_bearer(""), "");
        assert_eq!(strip_bearer("ключ-тест"), "ключ-тест");
    }
}
