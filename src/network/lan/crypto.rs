use std::{
    sync::Arc,
    time::Duration,
};

use quinn::{
    ClientConfig,
    ServerConfig,
    TransportConfig,
    crypto::rustls::{
        QuicClientConfig,
        QuicServerConfig,
    },
};
use rustls::{
    pki_types::{
        CertificateDer,
        PrivateKeyDer,
        PrivatePkcs8KeyDer,
    },
    server::WebPkiClientVerifier,
};

use crate::config::LanConfig;

pub fn create_server_client_configs(
    config: Arc<LanConfig>,
) -> anyhow::Result<(ServerConfig, ClientConfig)> {
    // Load the Cluster CA
    let cluster_ca = CertificateDer::from(config.ca_der.clone());

    // Load the Device cert
    let peer_cert = CertificateDer::from(config.cert_der.clone());
    let peer_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(config.key_der.clone()));

    // Create a transport config with appropriate params
    let mut tc = TransportConfig::default();
    tc.keep_alive_interval(Some(Duration::from_secs(5)));
    tc.max_idle_timeout(Some(
        Duration::from_secs(15)
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid idle timeout duration"))?,
    ));

    let transport_config = Arc::new(tc);

    // Create a root cert-store and add the cluster CA
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(cluster_ca)
        .map_err(|_| anyhow::anyhow!("invalid cluster CA"))?;

    // Create a client cert verifier using the root store for server
    let client_cert_verifier = WebPkiClientVerifier::builder(Arc::new(root_store.clone()))
        .build()
        .map_err(|_| anyhow::anyhow!("invalid root store"))?;

    // Create TLS server crypto using the device cert and client cert verifier
    let mut tls_server_crypto = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_cert_verifier)
        .with_single_cert(vec![peer_cert.clone()], peer_key.clone_key())
        .map_err(|_| anyhow::anyhow!("invalid peer cert or key"))?;

    tls_server_crypto.alpn_protocols = vec![b"h3".to_vec()];

    // Configure QUIC server crypto from the TLS server crypto
    let quic_server_crypto = QuicServerConfig::try_from(tls_server_crypto)
        .map_err(|_| anyhow::anyhow!("invalid TLS server crypto config"))?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_crypto));

    // Add transport configuration to server config
    server_config.transport_config(transport_config.clone());

    // Create TLS config for client using the device cert and root store for verification
    let mut tls_client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![peer_cert], peer_key)
        .map_err(|_| anyhow::anyhow!("invalid peer cert or key"))?;

    tls_client_crypto.alpn_protocols = vec![b"h3".to_vec()];

    // Configure quic client crypto from the TLS client crypto
    let quic_client_crypto = QuicClientConfig::try_from(tls_client_crypto)
        .map_err(|_| anyhow::anyhow!("invalid TLS client crypto config"))?;

    // Configure QUIC client with the TLS config
    let mut client_config = ClientConfig::new(Arc::new(quic_client_crypto));

    // Add transport configuration to client config as well
    client_config.transport_config(transport_config);

    Ok((server_config, client_config))
}
