use xynq_proto::{
    DeviceCommand,
    DeviceCommandReceived,
    DeviceEvent,
    DeviceEventReceived,
    DeviceState,
    DeviceStateReceived,
    IpcEvent,
    PeerConnected,
    PeerDisconnected,
    ipc_event::Kind,
};

use crate::id::{
    PeerId,
    PeerName,
};

pub enum Command {
    EmitPeerConnected {
        peer_id: PeerId,
        peer_name: PeerName,
    },
    EmitPeerDisconnected {
        peer_id: PeerId,
    },
    ForwardDeviceState {
        peer_id: PeerId,
        state: DeviceState,
    },
    ForwardDeviceEvent {
        peer_id: PeerId,
        event: DeviceEvent,
    },
    ForwardDeviceAction {
        peer_id: PeerId,
        command: DeviceCommand,
    },
}

impl From<Command> for xynq_proto::IpcEvent {
    fn from(value: Command) -> Self {
        match value {
            Command::EmitPeerConnected { peer_id, peer_name } => {
                let peer_id = peer_id.into();
                let peer_name = peer_name.to_string();

                IpcEvent {
                    kind: Some(Kind::PeerConnected(PeerConnected {
                        peer_id: Some(peer_id),
                        peer_name,
                    })),
                }
            }
            Command::EmitPeerDisconnected { peer_id: device_id } => {
                let peer_id = device_id.into();

                IpcEvent {
                    kind: Some(Kind::PeerDisconnected(PeerDisconnected {
                        peer_id: Some(peer_id),
                    })),
                }
            }
            Command::ForwardDeviceState { peer_id, state } => {
                let peer_id = peer_id.into();

                IpcEvent {
                    kind: Some(Kind::DeviceStateReceived(DeviceStateReceived {
                        peer_id: Some(peer_id),
                        state: Some(state),
                    })),
                }
            }
            Command::ForwardDeviceEvent { peer_id, event } => {
                let peer_id = peer_id.into();

                IpcEvent {
                    kind: Some(Kind::DeviceEventReceived(DeviceEventReceived {
                        peer_id: Some(peer_id),
                        event: Some(event),
                    })),
                }
            }
            Command::ForwardDeviceAction { peer_id, command } => {
                let peer_id = peer_id.into();

                IpcEvent {
                    kind: Some(Kind::DeviceCommandReceived(DeviceCommandReceived {
                        peer_id: Some(peer_id),
                        command: Some(command),
                    })),
                }
            }
        }
    }
}
