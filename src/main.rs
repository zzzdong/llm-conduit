//! llm-conduit: a lightweight, OpenAI-compatible AI model gateway.
//!
//! Single binary, optional TLS, routes by `model`, maps caller keys to upstream keys, streams bodies.

mod auth;
mod config;
mod error;
mod gateway;
mod health;
mod json_model;
mod observe;
mod proxy;
mod server;
mod tls;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::error::StartupError;
use crate::gateway::Gateway;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Command line arguments.
///
/// `--help` / `--version` / usage errors are all handled by clap: help and version go to stdout
/// with exit code 0, usage errors go to stderr with exit code 2.
#[derive(Debug, Parser)]
#[command(
    name = "llm-conduit",
    version = VERSION,
    about = "Lightweight OpenAI-compatible AI model gateway",
    long_about = "Lightweight, single-binary, optionally TLS-enabled AI model gateway.\n\n\
                  Exposes one OpenAI-compatible API endpoint, routes each request to an upstream \
                  LLM service (e.g. vLLM, TGI or any OpenAI-compatible server) based on the model name, and replaces the \
                  caller's unified API key with the matching upstream key.\n\n\
                  Request and response bodies are streamed end to end, so memory usage stays \
                  proportional to a single request body rather than to the number of concurrent \
                  requests.",
    after_help = "Environment:\n  \
                  RUST_LOG    Log level, defaults to `info` (e.g. RUST_LOG=llm_conduit=debug).\n\n\
                  Run with no arguments to use ./config.toml."
)]
struct Cli {
    /// Path to the TOML config file
    #[arg(
        value_name = "CONFIG",
        env = "LLM_CONDUIT_CONFIG",
        default_value = "config.toml"
    )]
    config: PathBuf,

    /// Path to the TOML config file (overrides CONFIG)
    #[arg(short = 'c', long = "config", value_name = "PATH")]
    config_flag: Option<PathBuf>,
}

impl Cli {
    /// Config file actually used: `-c/--config` wins over the positional argument (env var + default).
    fn config_path(&self) -> &Path {
        self.config_flag.as_deref().unwrap_or(&self.config)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli.config_path()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("llm-conduit: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(config_path: &Path) -> Result<(), StartupError> {
    init_logging();
    tls::install_crypto_provider();

    let config = Config::load(config_path)?;
    let listen = config.socket_addr()?;

    let tls_acceptor = match &config.server.tls {
        Some(tls_config) => {
            let server_config =
                tls::load_server_config(&tls_config.cert_path, &tls_config.key_path)?;
            info!(cert = %tls_config.cert_path.display(), "server TLS enabled");
            Some(TlsAcceptor::from(Arc::new(server_config)))
        }
        None => None,
    };

    let upstream_names: Vec<String> = config.upstreams.keys().cloned().collect();
    let auth_enabled = config.auth.enabled;
    let default_upstream = config.server.default_upstream.clone();
    let health = config.server.health.clone();
    let gateway = Arc::new(Gateway::new(config)?);

    // Start probing before the listener comes up, so /readyz reports real results early.
    if gateway.spawn_health_prober().is_some() {
        info!(
            interval_secs = health.interval_secs,
            timeout_secs = health.timeout_secs,
            mode = health.mode.as_str(),
            "upstream health probing enabled"
        );
    } else {
        info!("upstream health probing disabled; /readyz always reports ready");
    }

    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| StartupError(format!("failed to bind {listen}: {e}")))?;

    let scheme = if tls_acceptor.is_some() {
        "https"
    } else {
        "http"
    };
    info!(
        listen = %listen,
        scheme,
        auth_enabled,
        default_upstream = default_upstream.as_deref().unwrap_or("-"),
        upstreams = ?upstream_names,
        "llm-conduit {VERSION} started (config file {})",
        config_path.display(),
    );

    let result = server::run(listener, tls_acceptor, gateway).await;
    if let Err(e) = &result {
        error!("server exited with an error: {e}");
    }
    result
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use clap::error::ErrorKind;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn config_path_follows_documented_precedence() {
        // No arguments: the default of the positional argument (when LLM_CONDUIT_CONFIG is unset)
        if std::env::var_os("LLM_CONDUIT_CONFIG").is_none() {
            let cli = Cli::try_parse_from(["llm-conduit"]).unwrap();
            assert_eq!(cli.config_path(), Path::new("config.toml"));
        }

        // Positional argument (the form used in the design doc)
        let cli = Cli::try_parse_from(["llm-conduit", "/etc/llm-conduit/config.toml"]).unwrap();
        assert_eq!(cli.config_path(), Path::new("/etc/llm-conduit/config.toml"));

        // -c / --config overrides the positional argument
        let cli = Cli::try_parse_from(["llm-conduit", "/tmp/a.toml", "-c", "/tmp/b.toml"]).unwrap();
        assert_eq!(cli.config_path(), Path::new("/tmp/b.toml"));

        let cli = Cli::try_parse_from(["llm-conduit", "--config", "/tmp/b.toml"]).unwrap();
        assert_eq!(cli.config_path(), Path::new("/tmp/b.toml"));
    }

    #[test]
    fn help_and_version_are_owned_by_clap() {
        let version = Cli::try_parse_from(["llm-conduit", "--version"]).unwrap_err();
        assert_eq!(version.kind(), ErrorKind::DisplayVersion);

        let short_version = Cli::try_parse_from(["llm-conduit", "-V"]).unwrap_err();
        assert_eq!(short_version.kind(), ErrorKind::DisplayVersion);

        let help = Cli::try_parse_from(["llm-conduit", "--help"]).unwrap_err();
        assert_eq!(help.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn usage_errors_are_rejected() {
        let unknown = Cli::try_parse_from(["llm-conduit", "--nope"]).unwrap_err();
        assert_eq!(unknown.kind(), ErrorKind::UnknownArgument);

        let missing_value = Cli::try_parse_from(["llm-conduit", "-c"]).unwrap_err();
        assert_eq!(missing_value.kind(), ErrorKind::InvalidValue);

        let extra_positional =
            Cli::try_parse_from(["llm-conduit", "a.toml", "b.toml"]).unwrap_err();
        assert_eq!(extra_positional.kind(), ErrorKind::UnknownArgument);
    }
}
