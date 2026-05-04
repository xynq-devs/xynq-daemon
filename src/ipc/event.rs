use std::sync::Arc;

use crate::{
    config::{
        LanConfig,
        PeerConfig,
        RelayConfig,
    },
    id::PeerId,
};

pub enum Event {
    StartNetworkService {
        peer_config: Arc<PeerConfig>,
        lan_config: Arc<LanConfig>,
        relay_config: Option<Arc<RelayConfig>>,
    },
    RetryRequested,
    SendDeviceStateCommanded {
        peer_id: PeerId,
        state: xynq_proto::DeviceState,
    },
    BroadcastDeviceEventCommanded {
        event: xynq_proto::DeviceEvent,
    },
    SendDeviceCommandCommanded {
        peer_id: PeerId,
        command: xynq_proto::DeviceCommand,
    },
}

impl TryFrom<xynq_proto::IpcCommand> for Event {
    type Error = anyhow::Error;

    fn try_from(value: xynq_proto::IpcCommand) -> Result<Self, Self::Error> {
        let kind = value
            .kind
            .ok_or_else(|| anyhow::anyhow!("missing command kind"))?;

        let event = match kind {
            xynq_proto::ipc_command::Kind::RunNetworkService(run_network_service) => {
                let peer_config = run_network_service
                    .peer_config
                    .ok_or_else(|| anyhow::anyhow!("missing peer config"))?
                    .try_into()
                    .map(Arc::new)?;

                let lan_config = run_network_service
                    .lan_config
                    .ok_or_else(|| anyhow::anyhow!("missing lan config"))?
                    .try_into()
                    .map(Arc::new)?;

                let relay_config = run_network_service
                    .relay_config
                    .map(|config| config.try_into().map(Arc::new))
                    .transpose()?;

                Event::StartNetworkService {
                    peer_config,
                    lan_config,
                    relay_config,
                }
            }

            xynq_proto::ipc_command::Kind::TriggerRetry(_) => Event::RetryRequested,
            xynq_proto::ipc_command::Kind::SendDeviceState(send_device_state) => {
                let peer_id = send_device_state
                    .peer_id
                    .ok_or_else(|| anyhow::anyhow!("missing peer id"))?
                    .try_into()?;

                let state = send_device_state
                    .state
                    .ok_or_else(|| anyhow::anyhow!("missing device state"))?;

                Event::SendDeviceStateCommanded { peer_id, state }
            }
            xynq_proto::ipc_command::Kind::BroadcastDeviceEvent(broadcast_device_event) => {
                let event = broadcast_device_event
                    .event
                    .ok_or_else(|| anyhow::anyhow!("missing device event"))?;

                Event::BroadcastDeviceEventCommanded { event }
            }
            xynq_proto::ipc_command::Kind::SendDeviceCommand(send_device_command) => {
                let peer_id = send_device_command
                    .peer_id
                    .ok_or_else(|| anyhow::anyhow!("missing peer id"))?
                    .try_into()?;

                let command = send_device_command
                    .command
                    .ok_or_else(|| anyhow::anyhow!("missing device command"))?;

                Event::SendDeviceCommandCommanded { peer_id, command }
            }
        };

        Ok(event)
    }
}
