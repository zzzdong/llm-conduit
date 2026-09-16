//! TLS: server certificate loading and upstream HTTPS client construction.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
use rustls_pemfile::{certs, private_key};
use tracing::warn;

use crate::error::StartupError;

/// Upstream HTTP client: pooled connections, fixed-length `Bytes` body.
pub type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// Install the process-wide rustls crypto provider (aws-lc-rs).
///
/// Installing it explicitly avoids a panic from `ClientConfig::builder()` when several providers are compiled in.
pub fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

/// Load the server TLS configuration (PEM certificate chain + private key).
pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<ServerConfig, StartupError> {
    let certificates = load_certificates(cert_path)?;
    if certificates.is_empty() {
        return Err(StartupError(format!(
            "no certificate found in {}",
            cert_path.display()
        )));
    }
    let key = load_private_key(key_path)?;

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|e| {
            StartupError(format!(
                "certificate and private key do not match or are unusable: {e}"
            ))
        })
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, StartupError> {
    let file = File::open(path).map_err(|e| {
        StartupError(format!(
            "failed to open certificate {}: {e}",
            path.display()
        ))
    })?;
    certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            StartupError(format!(
                "failed to parse certificate {}: {e}",
                path.display()
            ))
        })
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, StartupError> {
    let file = File::open(path).map_err(|e| {
        StartupError(format!(
            "failed to open private key {}: {e}",
            path.display()
        ))
    })?;
    private_key(&mut BufReader::new(file))
        .map_err(|e| {
            StartupError(format!(
                "failed to parse private key {}: {e}",
                path.display()
            ))
        })?
        .ok_or_else(|| {
            StartupError(format!(
                "no usable private key in {} (PKCS#8 / RSA / EC are supported)",
                path.display()
            ))
        })
}

/// Build the upstream HTTP(S) client.
///
/// - `insecure_skip_verify = false`: use the system roots, fall back to the bundled webpki roots.
/// - `insecure_skip_verify = true`: skip certificate verification (test environments only).
pub fn build_client(insecure_skip_verify: bool) -> Result<HttpClient, StartupError> {
    let connector = if insecure_skip_verify {
        let config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier::new()))
            .with_no_client_auth();
        HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_or_http()
            .enable_http1()
            .build()
    } else {
        let builder = match HttpsConnectorBuilder::new().with_native_roots() {
            Ok(builder) => builder,
            Err(e) => {
                warn!(
                    "failed to load native root certificates ({e}); falling back to bundled webpki roots"
                );
                HttpsConnectorBuilder::new().with_webpki_roots()
            }
        };
        builder.https_or_http().enable_http1().build()
    };

    Ok(Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(64)
        .build(connector))
}

/// Certificate verification is skipped; signature verification is still delegated to the crypto provider.
#[derive(Debug)]
struct NoVerifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl NoVerifier {
    fn new() -> Self {
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
        Self { provider }
    }
}

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypto_provider_is_installed_and_clients_build() {
        // Guards the crypto provider wiring: with no provider installed, building a
        // ClientConfig panics and no TLS client can be constructed at all.
        install_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());

        assert!(build_client(false).is_ok(), "client using the system roots");
        assert!(
            build_client(true).is_ok(),
            "client that skips certificate verification"
        );
        assert!(
            !NoVerifier::new()
                .provider
                .signature_verification_algorithms
                .supported_schemes()
                .is_empty()
        );
    }
}
