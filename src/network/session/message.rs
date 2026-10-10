use serde::{
    Deserialize,
    Serialize,
};
use xynq_proto::{
    DeviceCommand,
    DeviceEvent,
    DeviceState,
};

use crate::id::{
    PeerId,
    PeerName,
};

#[derive(Serialize, Deserialize)]
pub struct HandshakeMessage {
    // Peer Id
    pub peer_id: PeerId,

    // Name of the peer
    pub peer_name: PeerName,

    // Timestamp of the creation of the handshake message
    pub timestamp: i64,

    // Random nonce (used to prevent replay attacks)
    pub nonce: [u8; 32],

    // Signed using the cluster secret key over identity, timestamp, and nonce
    pub signature: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
pub enum DeviceMessage {
    State(DeviceState),
    Event(DeviceEvent),
    Command(DeviceCommand),
}
