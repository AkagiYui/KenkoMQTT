//! 构造 rumqttc 使用的 rustls 客户端配置：跳过校验 / 指定 CA / 系统根证书。
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

/// 不校验服务端证书（不安全，仅用于自签名调试）。
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// 解析客户端证书链与私钥（双向 TLS）。任一为空返回 None。
fn client_auth(cert_pem: &str, key_pem: &str) -> Option<(Vec<CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>)> {
    if cert_pem.trim().is_empty() || key_pem.trim().is_empty() {
        return None;
    }
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_bytes()).flatten().collect();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes()).ok().flatten()?;
    if certs.is_empty() {
        return None;
    }
    Some((certs, key))
}

fn base_builder() -> rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert> {
    // 显式指定 ring provider，避免依赖“进程级默认 CryptoProvider 已安装”。
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("rustls 默认协议版本")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
}

pub fn client_config(skip_verify: bool, ca_pem: &str, client_cert: &str, client_key: &str, alpn: &[String]) -> Arc<ClientConfig> {
    let auth = client_auth(client_cert, client_key);

    // 分别构造「跳过校验」与「校验」两条路径的 WantsClientCert 构建器。
    let mut config: ClientConfig = if skip_verify {
        let b = base_builder();
        match auth {
            Some((certs, key)) => b.with_client_auth_cert(certs, key).unwrap_or_else(|_| base_builder().with_no_client_auth()),
            None => b.with_no_client_auth(),
        }
    } else {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mk_roots = || {
            let mut roots = RootCertStore::empty();
            if !ca_pem.trim().is_empty() {
                for cert in rustls_pemfile::certs(&mut ca_pem.as_bytes()).flatten() {
                    let _ = roots.add(cert);
                }
            } else {
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            roots
        };
        let b = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("rustls")
            .with_root_certificates(mk_roots());
        match auth {
            Some((certs, key)) => b.with_client_auth_cert(certs, key).unwrap_or_else(|_| {
                ClientConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()
                    .expect("rustls")
                    .with_root_certificates(mk_roots())
                    .with_no_client_auth()
            }),
            None => b.with_no_client_auth(),
        }
    };

    if !alpn.is_empty() {
        config.alpn_protocols = alpn.iter().map(|s| s.as_bytes().to_vec()).collect();
    }
    Arc::new(config)
}
