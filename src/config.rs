use std::sync::Arc;

use rustls::pki_types::PrivateKeyDer;
use x509_parser::prelude::{
    FromDer,
    X509Certificate,
};

use crate::id::{
    ClusterId,
    PeerId,
    PeerName,
};

#[derive(Clone)]
pub struct Config {
    pub peer_config: Arc<PeerConfig>,
    pub lan_config: Arc<LanConfig>,
    pub relay_config: Option<Arc<RelayConfig>>,
}

#[derive(Clone)]
pub struct PeerConfig {
    pub cluster_id: ClusterId,
    pub peer_id: PeerId,
    pub peer_name: PeerName,
    pub secret: [u8; 32],
}

impl TryFrom<xynq_proto::PeerConfig> for PeerConfig {
    type Error = anyhow::Error;

    fn try_from(value: xynq_proto::PeerConfig) -> Result<Self, Self::Error> {
        let cluster_id = value
            .cluster_id
            .ok_or_else(|| anyhow::anyhow!("missing cluster id"))?
            .try_into()?;

        let peer_id = value
            .peer_id
            .ok_or_else(|| anyhow::anyhow!("missing peer id"))?
            .try_into()?;

        let peer_name = value.peer_name.into();

        let secret = value
            .secret
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid secret length, expected 32 bytes"))?;

        Ok(Self {
            cluster_id,
            peer_id,
            peer_name,
            secret,
        })
    }
}

pub struct LanConfig {
    pub ca_der: Vec<u8>,
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
}

impl TryFrom<xynq_proto::LanConfig> for LanConfig {
    type Error = anyhow::Error;

    fn try_from(value: xynq_proto::LanConfig) -> Result<Self, Self::Error> {
        let ca_der = value.ca_der;
        if X509Certificate::from_der(&ca_der).is_err() {
            return Err(anyhow::anyhow!("invalid CA certificate DER"));
        }

        let cert_der = value.cert_der;
        if X509Certificate::from_der(&cert_der).is_err() {
            return Err(anyhow::anyhow!("invalid certificate DER"));
        }

        let key_der = value.key_der;
        if PrivateKeyDer::try_from(key_der.as_slice()).is_err() {
            return Err(anyhow::anyhow!(
                "invalid private key DER; expected a supported DER-encoded private key"
            ));
        }

        Ok(Self {
            ca_der,
            cert_der,
            key_der,
        })
    }
}

pub struct RelayConfig {
    pub url: String,
    pub port: u16,
    pub cert_der: Vec<u8>,
}

impl TryFrom<xynq_proto::RelayConfig> for RelayConfig {
    type Error = anyhow::Error;

    fn try_from(value: xynq_proto::RelayConfig) -> Result<Self, Self::Error> {
        let url = value.url;
        let port = value
            .port
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid relay client port"))?;

        let cert_der = value.cert_der;

        if X509Certificate::from_der(&cert_der).is_err() {
            return Err(anyhow::anyhow!("invalid relay certificate DER"));
        }

        Ok(Self {
            url,
            port,
            cert_der,
        })
    }
}
