//! Configuration loading and validation.
//!
//! The config file is TOML; see section 5 of `docs/DESIGN.md`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use hyper::Uri;
use serde::Deserialize;

use crate::error::{GatewayError, StartupError};

/// Default request body limit: 200 MB.
pub const DEFAULT_MAX_BODY_BYTES: u64 = 200_000_000;

/// Top-level configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    pub upstreams: BTreeMap<String, UpstreamConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Listen address, e.g. `0.0.0.0:4000`.
    pub listen: String,
    /// Fallback upstream used when no model is specified.
    #[serde(default)]
    pub default_upstream: Option<String>,
    /// Request body limit; anything larger gets a 413.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: u64,
    /// Configuring this section enables server-side HTTPS.
    #[serde(default)]
    pub tls: Option<TlsServerConfig>,
    /// Upstream probing behind `GET /readyz`.
    #[serde(default)]
    pub health: HealthConfig,
}

/// Upstream health probing, surfaced through `GET /readyz`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    /// Probe upstreams in the background; when disabled `/readyz` is always ready.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Seconds between two probe passes.
    #[serde(default = "default_health_interval_secs")]
    pub interval_secs: u64,
    /// Timeout of a single probe, in seconds.
    #[serde(default = "default_health_timeout_secs")]
    pub timeout_secs: u64,
    /// Probe kind: `models` requires a successful `GET /v1/models`, `tcp` only opens a connection.
    #[serde(default)]
    pub mode: HealthMode,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_health_interval_secs(),
            timeout_secs: default_health_timeout_secs(),
            mode: HealthMode::default(),
        }
    }
}

/// How an upstream is probed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthMode {
    /// `GET /v1/models` has to answer with a success status.
    #[default]
    Models,
    /// A plain TCP connection is enough; only reachability is checked.
    Tcp,
}

impl HealthMode {
    /// Stable name, used in logs and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Models => "models",
            Self::Tcp => "tcp",
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_health_interval_secs() -> u64 {
    10
}

fn default_health_timeout_secs() -> u64 {
    2
}

fn default_max_body_bytes() -> u64 {
    DEFAULT_MAX_BODY_BYTES
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsServerConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Unified key -> caller description.
    #[serde(default)]
    pub keys: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    pub base_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub upstream_model: Option<String>,
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

/// Validated upstream definition (`base_url` already parsed into a [`Uri`]).
#[derive(Debug, Clone)]
pub struct Upstream {
    pub name: String,
    /// Upstream root address: scheme + authority only.
    pub base: Uri,
    /// Path prefix taken from `base_url` (no trailing slash); empty when there is none.
    pub path_prefix: String,
    pub api_key: Option<String>,
    pub upstream_model: Option<String>,
    pub insecure_skip_verify: bool,
}

impl Upstream {
    fn from_config(name: &str, cfg: &UpstreamConfig) -> Result<Self, StartupError> {
        let (base, path_prefix) = parse_base_url(name, &cfg.base_url)?;
        Ok(Self {
            name: name.to_string(),
            base,
            path_prefix,
            api_key: cfg.api_key.clone(),
            upstream_model: cfg.upstream_model.clone(),
            insecure_skip_verify: cfg.insecure_skip_verify,
        })
    }

    /// Build the absolute upstream URI from the client request URI: keep path + query, prepend `base_url` prefix.
    pub fn target_uri(&self, req: &Uri) -> Result<Uri, GatewayError> {
        let scheme = self.base.scheme_str().unwrap_or("http");
        let authority = self
            .base
            .authority()
            .map(|a| a.as_str())
            .ok_or_else(|| GatewayError::internal("upstream base_url has no host"))?;

        let mut path_and_query =
            String::with_capacity(self.path_prefix.len() + req.path().len() + 16);
        path_and_query.push_str(&self.path_prefix);
        path_and_query.push_str(req.path());
        if let Some(query) = req.query() {
            path_and_query.push('?');
            path_and_query.push_str(query);
        }

        Uri::builder()
            .scheme(scheme)
            .authority(authority)
            .path_and_query(path_and_query)
            .build()
            .map_err(|e| GatewayError::internal(format!("failed to build upstream URL: {e}")))
    }

    /// Whether this upstream is reached over TLS.
    pub fn uses_tls(&self) -> bool {
        self.base.scheme_str() == Some("https")
    }
}

impl Config {
    /// Load from a file and validate.
    pub fn load(path: &Path) -> Result<Self, StartupError> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            StartupError(format!(
                "failed to read config file {}: {e}",
                path.display()
            ))
        })?;
        let config: Self = toml::from_str(&text).map_err(|e| {
            StartupError(format!(
                "failed to parse config file {}: {e}",
                path.display()
            ))
        })?;
        config
            .validate()
            .map_err(|e| StartupError(format!("invalid config file {}: {e}", path.display())))?;
        Ok(config)
    }

    /// Parse TOML text and validate (test helper).
    #[cfg(test)]
    pub fn parse(text: &str) -> Result<Self, StartupError> {
        let config: Self = toml::from_str(text).map_err(|e| StartupError(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Full static validation. The goal is to catch config errors at startup rather than per request.
    pub fn validate(&self) -> Result<(), StartupError> {
        if self.server.listen.trim().is_empty() {
            return Err(StartupError("server.listen must not be empty".into()));
        }
        self.socket_addr()?;

        if self.server.max_body_bytes == 0 {
            return Err(StartupError(
                "server.max_body_bytes must be greater than 0".into(),
            ));
        }

        if self.server.health.enabled && self.server.health.interval_secs == 0 {
            return Err(StartupError(
                "server.health.interval_secs must be greater than 0".into(),
            ));
        }

        if self.server.health.enabled && self.server.health.timeout_secs == 0 {
            return Err(StartupError(
                "server.health.timeout_secs must be greater than 0".into(),
            ));
        }

        if self.upstreams.is_empty() {
            return Err(StartupError(
                "at least one [upstreams.<name>] upstream must be configured".into(),
            ));
        }

        if self.auth.enabled && self.auth.keys.is_empty() {
            return Err(StartupError(
                "auth.enabled = true but auth.keys is empty: every request would be rejected"
                    .into(),
            ));
        }

        for (name, upstream) in &self.upstreams {
            if name.trim().is_empty() {
                return Err(StartupError("upstream name must not be empty".into()));
            }
            parse_base_url(name, &upstream.base_url)?;
            if upstream
                .api_key
                .as_deref()
                .is_some_and(|k| k.trim().is_empty())
            {
                return Err(StartupError(format!(
                    "upstream '{name}': api_key must not be an empty string"
                )));
            }
            if upstream
                .upstream_model
                .as_deref()
                .is_some_and(|m| m.trim().is_empty())
            {
                return Err(StartupError(format!(
                    "upstream '{name}': upstream_model must not be an empty string"
                )));
            }
        }

        if let Some(default) = &self.server.default_upstream
            && !self.upstreams.contains_key(default)
        {
            return Err(StartupError(format!(
                "server.default_upstream = \"{default}\" is not defined in [upstreams]"
            )));
        }

        Ok(())
    }

    /// Parse the listen address.
    pub fn socket_addr(&self) -> Result<SocketAddr, StartupError> {
        self.server.listen.parse().map_err(|_| {
            StartupError(format!(
                "server.listen = \"{}\" is not a valid <IP>:<port>",
                self.server.listen
            ))
        })
    }

    /// Resolve every configured upstream.
    pub fn resolve_upstreams(&self) -> Result<BTreeMap<String, Upstream>, StartupError> {
        let mut resolved = BTreeMap::new();
        for (name, cfg) in &self.upstreams {
            resolved.insert(name.clone(), Upstream::from_config(name, cfg)?);
        }
        Ok(resolved)
    }
}

/// Parse and validate `base_url`; returns (root URI, path prefix).
fn parse_base_url(name: &str, base_url: &str) -> Result<(Uri, String), StartupError> {
    let uri: Uri = base_url.parse().map_err(|e| {
        StartupError(format!(
            "upstream '{name}': base_url \"{base_url}\" is not a valid URI: {e}"
        ))
    })?;

    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        other => {
            return Err(StartupError(format!(
                "upstream '{name}': base_url must use http:// or https:// (current scheme: {})",
                other.unwrap_or("none")
            )));
        }
    }

    if uri.authority().is_none() {
        return Err(StartupError(format!(
            "upstream '{name}': base_url \"{base_url}\" has no host"
        )));
    }

    if uri.query().is_some() {
        return Err(StartupError(format!(
            "upstream '{name}': base_url must not contain a query (put the path prefix in the path)"
        )));
    }

    let prefix = match uri.path() {
        "/" | "" => String::new(),
        path => path.trim_end_matches('/').to_string(),
    };

    Ok((uri, prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[server]
listen = "0.0.0.0:4000"
default_upstream = "llama-3.1-8b-instruct"
max_body_bytes = 1000

[auth]
enabled = true
keys = { "sk-gateway-0001" = "frontend", "sk-gateway-0002" = "backend" }

[upstreams."llama-3.1-8b-instruct"]
base_url = "http://127.0.0.1:8000"
api_key = "upstream-key-0001"
upstream_model = "meta-llama/Llama-3.1-8B-Instruct"

[upstreams.mistral-7b-instruct]
base_url = "https://127.0.0.1:8001/"
insecure_skip_verify = true
"#;

    #[test]
    fn parses_and_resolves_upstreams() {
        let cfg = Config::parse(SAMPLE).unwrap();
        assert_eq!(cfg.server.max_body_bytes, 1000);
        assert_eq!(cfg.socket_addr().unwrap().port(), 4000);
        assert_eq!(cfg.auth.keys.get("sk-gateway-0001").unwrap(), "frontend");

        let upstreams = cfg.resolve_upstreams().unwrap();
        let primary = &upstreams["llama-3.1-8b-instruct"];
        assert_eq!(primary.path_prefix, "");
        assert_eq!(primary.api_key.as_deref(), Some("upstream-key-0001"));
        assert_eq!(
            primary.upstream_model.as_deref(),
            Some("meta-llama/Llama-3.1-8B-Instruct")
        );

        let secondary = &upstreams["mistral-7b-instruct"];
        assert_eq!(secondary.path_prefix, "");
        assert!(secondary.insecure_skip_verify);
    }

    #[test]
    fn max_body_bytes_defaults_to_200mb() {
        let text = r#"
[server]
listen = "0.0.0.0:4000"
[upstreams.a]
base_url = "http://127.0.0.1:8000"
"#;
        let cfg = Config::parse(text).unwrap();
        assert_eq!(cfg.server.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert!(!cfg.auth.enabled);
    }

    #[test]
    fn rejects_unknown_fields() {
        let text = r#"
[server]
listen = "0.0.0.0:4000"
typo_field = 1
[upstreams.a]
base_url = "http://127.0.0.1:8000"
"#;
        assert!(Config::parse(text).is_err());
    }

    #[test]
    fn rejects_bad_configs() {
        let cases = [
            // no upstreams at all
            r#"[server]
listen = "0.0.0.0:4000"
[upstreams]"#,
            // invalid listen address
            r#"[server]
listen = "not-an-address"
[upstreams.a]
base_url = "http://127.0.0.1:8000""#,
            // invalid scheme
            r#"[server]
listen = "0.0.0.0:4000"
[upstreams.a]
base_url = "ftp://127.0.0.1:8000""#,
            // default_upstream does not exist
            r#"[server]
listen = "0.0.0.0:4000"
default_upstream = "ghost"
[upstreams.a]
base_url = "http://127.0.0.1:8000""#,
            // auth enabled but keys empty
            r#"[server]
listen = "0.0.0.0:4000"
[auth]
enabled = true
[upstreams.a]
base_url = "http://127.0.0.1:8000""#,
        ];
        for text in cases {
            assert!(
                Config::parse(text).is_err(),
                "this config should be rejected:\n{text}"
            );
        }
    }

    #[test]
    fn target_uri_keeps_path_query_and_prefix() {
        let cfg = Config::parse(
            r#"
[server]
listen = "0.0.0.0:4000"
[upstreams.a]
base_url = "http://127.0.0.1:8000/vllm/"
"#,
        )
        .unwrap();
        let upstreams = cfg.resolve_upstreams().unwrap();
        let upstream = &upstreams["a"];
        assert_eq!(upstream.path_prefix, "/vllm");

        let req: Uri = "/v1/chat/completions?x=1".parse().unwrap();
        let target = upstream.target_uri(&req).unwrap();
        assert_eq!(
            target.to_string(),
            "http://127.0.0.1:8000/vllm/v1/chat/completions?x=1"
        );
    }

    #[test]
    fn health_defaults_are_enabled_models_with_ten_second_interval() {
        let text = r#"
[server]
listen = "0.0.0.0:4000"
[upstreams.a]
base_url = "http://127.0.0.1:8000"
"#;
        let cfg = Config::parse(text).unwrap();
        assert!(cfg.server.health.enabled);
        assert_eq!(cfg.server.health.interval_secs, 10);
        assert_eq!(cfg.server.health.timeout_secs, 2);
        assert_eq!(cfg.server.health.mode, HealthMode::Models);
    }

    #[test]
    fn health_section_is_configurable() {
        let text = r#"
[server]
listen = "0.0.0.0:4000"
[server.health]
enabled = false
interval_secs = 30
timeout_secs = 5
mode = "tcp"
[upstreams.a]
base_url = "https://127.0.0.1:8000"
"#;
        let cfg = Config::parse(text).unwrap();
        assert!(!cfg.server.health.enabled);
        assert_eq!(cfg.server.health.interval_secs, 30);
        assert_eq!(cfg.server.health.timeout_secs, 5);
        assert_eq!(cfg.server.health.mode, HealthMode::Tcp);
        assert!(cfg.resolve_upstreams().unwrap()["a"].uses_tls());
    }

    #[test]
    fn rejects_bad_health_settings() {
        for override_line in ["interval_secs = 0", "timeout_secs = 0", "mode = \"ping\""] {
            let text = format!(
                r#"
[server]
listen = "0.0.0.0:4000"
[server.health]
{override_line}
[upstreams.a]
base_url = "http://127.0.0.1:8000"
"#
            );
            assert!(
                Config::parse(&text).is_err(),
                "{override_line} should be rejected"
            );
        }
    }
}
