// Services
mod ipc;
mod network;

// Traits
mod service;
mod transport;

// Types
mod config;
mod id;

mod logging;

use std::env;

use tokio::sync::{
    mpsc,
    watch,
};
use tokio_util::sync::CancellationToken;
use tracing::{
    debug,
    info,
};

use crate::{
    config::Config,
    ipc::IpcService,
    logging::init_logging,
    network::NetworkService,
    service::Service,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    info!("starting xync daemon");

    let global_token = CancellationToken::new();

    let socket = env::var("XYNC_IPC_SOCKET").ok();
    let ipc_token = global_token.child_token();
    let (ipc_event_tx, mut ipc_event_rx) = mpsc::channel(100);
    let (ipc_command_tx, ipc_command_rx) = mpsc::channel(100);
    IpcService::start(
        ipc_token.clone(),
        Some(ipc_command_rx),
        Some(ipc_event_tx),
        socket.clone(),
    )?;

    // Create placeholder channels for network service
    let mut network_token = global_token.child_token();
    let (mut network_command_tx, _) = mpsc::channel(100);
    let (_, mut network_event_rx) = mpsc::channel(100);

    // Watch channel to save the latest config for restarting the network service on failure or on config update
    let (config_watch_tx, mut config_watch_rx) = watch::channel::<Option<Config>>(None);

    loop {
        tokio::select! {
            Some(event) = ipc_event_rx.recv() => {
                if handle_ipc_event(event, &network_command_tx, &config_watch_tx).await.is_err() {
                    network_token.cancel();
                }
            }

            _ = ipc_token.cancelled() => {
                anyhow::bail!("ipc service shut down, exiting");
            }

            Some(event) = network_event_rx.recv() => {
                if handle_network_event(event, &ipc_command_tx).await.is_err() {
                    ipc_token.cancel();
                }
            }

            _ = network_token.cancelled() => {
                if let Some(config) = config_watch_rx.borrow().as_ref() {
                    let token = global_token.child_token();
                    let (command_tx, network_command_rx) = mpsc::channel(100);
                    let (network_event_tx, event_rx) = mpsc::channel(100);

                    NetworkService::start(
                        token.clone(),
                        Some(network_command_rx),
                        Some(network_event_tx),
                        config.clone(),
                    )?;

                    network_token = token;
                    network_command_tx = command_tx;
                    network_event_rx = event_rx;
                }
            }


            _ = config_watch_rx.changed() => {
                // Cancel the network token so that the service can be restarted with the new config
                debug!("config updated, stopping network service");
                network_token.cancel();
            }
        }
    }
}

async fn handle_ipc_event(
    ipc_event: ipc::Event,
    network_command_tx: &mpsc::Sender<network::Command>,
    config_watch_tx: &watch::Sender<Option<Config>>,
) -> anyhow::Result<()> {
    match ipc_event {
        ipc::Event::StartNetworkService {
            peer_config,
            lan_config,
            relay_config,
        } => {
            let config = Config {
                peer_config,
                lan_config,
                relay_config,
            };

            let _ = config_watch_tx.send(Some(config));
        }
        ipc::Event::RetryRequested => {
            network_command_tx
                .send(network::Command::RetryRelayConnection)
                .await?;
        }
        ipc::Event::SendDeviceStateCommanded { peer_id, state } => {
            network_command_tx
                .send(network::Command::SendDeviceStateToPeer { peer_id, state })
                .await?;
        }
        ipc::Event::BroadcastDeviceEventCommanded { event } => {
            network_command_tx
                .send(network::Command::BroadcastDeviceEventToPeers { event })
                .await?;
        }
        ipc::Event::SendDeviceCommandCommanded { peer_id, command } => {
            network_command_tx
                .send(network::Command::SendDeviceCommandToPeer { peer_id, command })
                .await?;
        }
    }

    Ok(())
}

async fn handle_network_event(
    network_event: network::Event,
    ipc_command_tx: &mpsc::Sender<ipc::Command>,
) -> anyhow::Result<()> {
    match network_event {
        network::Event::PeerConnected { peer_id, peer_name } => {
            ipc_command_tx
                .send(ipc::Command::EmitPeerConnected { peer_id, peer_name })
                .await?;
        }
        network::Event::PeerDisconnected { peer_id } => {
            ipc_command_tx
                .send(ipc::Command::EmitPeerDisconnected { peer_id })
                .await?;
        }
        network::Event::DeviceStateReceivedFromPeer { peer_id, state } => {
            ipc_command_tx
                .send(ipc::Command::ForwardDeviceState { peer_id, state })
                .await?;
        }
        network::Event::DeviceEventReceivedFromPeer { peer_id, event } => {
            ipc_command_tx
                .send(ipc::Command::ForwardDeviceEvent { peer_id, event })
                .await?;
        }
        network::Event::DeviceCommandReceivedFromPeer { peer_id, command } => {
            ipc_command_tx
                .send(ipc::Command::ForwardDeviceAction { peer_id, command })
                .await?;
        }
    }

    Ok(())
}
