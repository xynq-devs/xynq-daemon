use std::{
    sync::Arc,
    time::Duration,
};

use quinn::{
    ClientConfig,
    TransportConfig,
    crypto::rustls::QuicClientConfig,
};
use rustls::pki_types::CertificateDer;

use crate::config::RelayConfig;

pub fn create_client_config(config: &Arc<RelayConfig>) -> anyhow::Result<ClientConfig> {
    // Load the server cert
    let cert_der = CertificateDer::from(config.cert_der.clone());

    // Create a root cert-store and add the server cert
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(cert_der)
        .map_err(|_| anyhow::anyhow!("invalid server cert"))?;

    // Create a transport config with appropriate params
    let mut tc = TransportConfig::default();
    tc.keep_alive_interval(Some(Duration::from_secs(5)));
    tc.max_idle_timeout(Some(Duration::from_secs(15).try_into()?));

    let transport_config = Arc::new(tc);

    // Create TLS config for client using the device cert and root store for verification
    let mut tls_client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    tls_client_crypto.alpn_protocols = vec![b"h3".to_vec()];

    let quic_client_crypto = QuicClientConfig::try_from(tls_client_crypto)
        .map_err(|_| anyhow::anyhow!("invalid TLS client crypto config"))?;

    let mut client_cfg = ClientConfig::new(Arc::new(quic_client_crypto));
    client_cfg.transport_config(transport_config);

    Ok(client_cfg)
}
