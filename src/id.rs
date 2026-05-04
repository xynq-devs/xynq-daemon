use std::sync::Arc;

use serde::{
    Deserialize,
    Serialize,
};
use uuid::{
    ContextV7,
    Timestamp,
    Uuid,
    Version,
};

/// UUIDv4 Id for cluster identity
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClusterId(Uuid);

const CLUSTER_ID_UUID_VERSION: Version = Version::Random;

impl Default for ClusterId {
    fn default() -> Self {
        ClusterId(Uuid::new_v4())
    }
}

impl std::ops::Deref for ClusterId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for ClusterId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(s)?;
        if uuid
            .get_version()
            .is_some_and(|v| v != CLUSTER_ID_UUID_VERSION)
        {
            return Err(anyhow::anyhow!("invalid version, expected UUIDv4"));
        }

        Ok(ClusterId(uuid))
    }
}

impl TryFrom<xynq_proto::ClusterId> for ClusterId {
    type Error = anyhow::Error;

    fn try_from(value: xynq_proto::ClusterId) -> Result<Self, Self::Error> {
        let bytes: [u8; 16] = value
            .bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid cluster id length, expected 16 bytes"))?;

        let uuid = Uuid::from_bytes(bytes);

        if uuid
            .get_version()
            .is_some_and(|v| v != CLUSTER_ID_UUID_VERSION)
        {
            return Err(anyhow::anyhow!("invalid version, expected UUIDv4"));
        }

        Ok(ClusterId(uuid))
    }
}

impl From<ClusterId> for xynq_proto::ClusterId {
    fn from(value: ClusterId) -> Self {
        Self {
            bytes: value.0.as_bytes().to_vec(),
        }
    }
}

/// UUIDv7 Id for device identity
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerId(pub Uuid);

const PEER_ID_UUID_VERSION: Version = Version::SortRand;

impl Default for PeerId {
    fn default() -> Self {
        let ts = Timestamp::now(ContextV7::new());
        PeerId(Uuid::new_v7(ts))
    }
}

impl std::ops::Deref for PeerId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for PeerId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::from_str(s)?;

        if uuid
            .get_version()
            .is_some_and(|v| v != PEER_ID_UUID_VERSION)
        {
            return Err(anyhow::anyhow!("invalid version, expected UUIDv7"));
        }

        Ok(PeerId(uuid))
    }
}

impl TryFrom<xynq_proto::PeerId> for PeerId {
    type Error = anyhow::Error;

    fn try_from(value: xynq_proto::PeerId) -> Result<Self, Self::Error> {
        let bytes: [u8; 16] = value
            .bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid device id length, expected 16 bytes"))?;

        let uuid = Uuid::from_bytes(bytes);

        if uuid
            .get_version()
            .is_some_and(|v| v != PEER_ID_UUID_VERSION)
        {
            return Err(anyhow::anyhow!("invalid version, expected UUIDv7"));
        }

        Ok(PeerId(uuid))
    }
}

impl From<PeerId> for xynq_proto::PeerId {
    fn from(value: PeerId) -> Self {
        Self {
            bytes: value.0.as_bytes().to_vec(),
        }
    }
}

pub type PeerName = Arc<str>;
