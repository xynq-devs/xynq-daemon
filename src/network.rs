// services
mod lan;
mod relay;
mod session;

use std::{
    collections::HashMap,
    sync::Arc,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{
    Instrument,
    debug,
    error,
    info,
    warn,
};
use xynq_proto::{
    DeviceCommand,
    DeviceEvent,
    DeviceState,
};

use crate::{
    config::{
        Config,
        PeerConfig,
    },
    id::{
        PeerId,
        PeerName,
    },
    network::{
        lan::LanService,
        session::SessionService,
    },
    service::Service,
};

pub struct NetworkService {
    token: CancellationToken,

    // channels
    event_tx: mpsc::Sender<Event>,
    command_rx: mpsc::Receiver<Command>,

    // config
    config: Config,
}

impl Service<Command, Event> for NetworkService {
    type Params = Config;

    fn start(
        token: CancellationToken,
        command_rx: Option<mpsc::Receiver<Command>>,
        event_tx: Option<mpsc::Sender<Event>>,
        config: Self::Params,
    ) -> anyhow::Result<()> {
        let command_rx = command_rx.ok_or_else(|| anyhow::anyhow!("command receiver missing"))?;
        let event_tx = event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let service = Self {
            token: token.clone(),

            event_tx,
            command_rx,

            config,
        };

        tokio::spawn(
            async move {
                info!("service started");
                // Let run() observe cancellation and drain its active-peer registry
                // before returning; selecting cancellation here would drop that cleanup.
                service.run().await;
                token.cancel();
                info!("service stopped");
            }
            .in_current_span(),
        );

        Ok(())
    }

    async fn run(mut self) {
        loop {
            if self.token.is_cancelled() {
                return;
            }

            // Start a fresh service generation.
            let shared_service_token = self.token.child_token();

            // Create a stream ready event channel for LAN and Relay services to get notified when a new stream is ready to start a session.
            let (stream_ready_event_tx, mut stream_ready_event_rx) = mpsc::channel(8);

            // Start relay service if configured and get the command sender
            let retry_conn_command_tx = if let Some(relay_config) = self.config.relay_config.clone()
            {
                // Create command channel solely for relay service because this service can't be restarted immediately
                // and hence, user retry signals are passed down to the service
                let (retry_conn_command_tx, retry_conn_command_rx) = mpsc::channel(8);

                if let Err(err) = relay::RelayService::start(
                    shared_service_token.clone(),
                    Some(retry_conn_command_rx),
                    Some(stream_ready_event_tx.clone()),
                    (
                        relay_config,
                        self.config.peer_config.cluster_id,
                        self.config.peer_config.peer_id,
                    ),
                ) {
                    error!(reason = %err, "failed to start Relay service");
                    // If relay service failed to start, then we have some critical error
                    // therefore, we stop the entire network service
                    return;
                }

                Some(retry_conn_command_tx)
            } else {
                None
            };

            // Start LAN service
            if let Err(err) = LanService::start(
                shared_service_token.clone(),
                None,
                Some(stream_ready_event_tx),
                (
                    self.config.peer_config.clone(),
                    self.config.lan_config.clone(),
                ),
            ) {
                error!(reason = %err, "failed to start LAN service");
                // If LAN service failed to start, then we have some critical error
                // therefore, we stop the entire network service
                return;
            }

            // Active sessions map to handle the command sender for peer session
            let mut active_sessions = Sessions::new();

            // Create a session event channel for sessions to send events back to the this service.
            // This tx will be cloned and shared to all the sessions
            let (session_event_tx, mut session_event_rx) = mpsc::channel(256);

            loop {
                tokio::select! {
                    Some(command) = self.command_rx.recv() => {
                        let _ =  handle_command(command, &retry_conn_command_tx, &mut active_sessions).await;
                    },
                    Some(StreamReadyEvent { session_kind, send, recv }) = stream_ready_event_rx.recv() => {
                        create_session(send, recv, session_kind, &session_event_tx, &shared_service_token, &self.config.peer_config);
                    }
                    _ = shared_service_token.cancelled() => {
                        debug!("restarting services");
                        break;
                    }
                    Some(session_event) = session_event_rx.recv() => {
                        if handle_session_event(session_event, &mut active_sessions, &self.event_tx).await.is_err() {
                            break;
                        }
                    }

                }
            }

            // The service generation is ending; report remaining peers as disconnected
            // before the active-session registry is discarded.
            let disconnected_peers: Vec<_> = active_sessions.keys().copied().collect();
            for peer_id in disconnected_peers {
                if self.event_tx.send(Event::PeerDisconnected { peer_id }).await.is_err() {
                    return;
                }
            }
        }
    }
}

pub enum Command {
    RetryRelayConnection,
    SendDeviceStateToPeer {
        peer_id: PeerId,
        state: DeviceState,
    },
    BroadcastDeviceEventToPeers {
        event: DeviceEvent,
    },
    SendDeviceCommandToPeer {
        peer_id: PeerId,
        command: DeviceCommand,
    },
}

pub enum Event {
    PeerConnected {
        peer_id: PeerId,
        peer_name: PeerName,
    },
    PeerDisconnected {
        peer_id: PeerId,
    },
    DeviceStateReceivedFromPeer {
        peer_id: PeerId,
        state: DeviceState,
    },
    DeviceEventReceivedFromPeer {
        peer_id: PeerId,
        event: DeviceEvent,
    },
    DeviceCommandReceivedFromPeer {
        peer_id: PeerId,
        command: DeviceCommand,
    },
}

// Command for the Relay Service
struct RetryConnectionCommand;

// Event for Relay and LAN service to emit when a new stream is ready with a connected peer
struct StreamReadyEvent {
    session_kind: SessionKind,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

// Sessions Map to store the channels to Session handlers
type Sessions = HashMap<PeerId, SessionCommandChannel>;

struct SessionHandle {
    session_id: u64,
    sender: mpsc::Sender<session::SendToPeer>,
}

// Session IDs prevent late close events from removing replacement sessions.
#[derive(Default)]
struct SessionCommandChannel {
    local: Option<SessionHandle>,
    relay: Option<SessionHandle>,
}

impl SessionCommandChannel {
    fn is_connected(&self) -> bool {
        self.local.is_some() || self.relay.is_some()
    }
}

// Session kind to differentiate between LAN and Relay sessions
// This is required because for a same peer, we can have sessions via both, LAN and Relay
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionKind {
    Lan,
    Relay,
}

async fn handle_command(
    command: Command,
    retry_conn_command_tx: &Option<mpsc::Sender<RetryConnectionCommand>>,
    active_sessions: &mut Sessions,
) -> anyhow::Result<()> {
    match command {
        Command::RetryRelayConnection => {
            if let Some(tx) = retry_conn_command_tx {
                if tx.send(RetryConnectionCommand).await.is_err() {
                    warn!(
                        reason = "channel closed",
                        "failed to send RetryConnectionCommand"
                    );
                }
            } else {
                debug!("relay service not configured");
            }
        }
        Command::SendDeviceStateToPeer { peer_id, state } => {
            let Some(session_command_channel) = active_sessions.get(&peer_id) else {
                return Ok(());
            };

            let Some(session_command_tx) = session_command_channel
                .local
                .as_ref()
                .or(session_command_channel.relay.as_ref())
            else {
                return Ok(());
            };

            if session_command_tx.sender
                .send(session::SendToPeer::State(state))
                .await
                .is_err()
            {
                warn!(
                    reason = "channel closed",
                    "failed to send SendToPeer command"
                );
            }
        }
        Command::BroadcastDeviceEventToPeers { event } => {
            for session_command_channel in active_sessions.values() {
                let Some(session_command_tx) = session_command_channel
                    .local
                    .as_ref()
                    .or(session_command_channel.relay.as_ref())
                else {
                    continue;
                };

                if session_command_tx.sender
                    .send(session::SendToPeer::Event(event.clone()))
                    .await
                    .is_err()
                {
                    warn!(
                        reason = "channel closed",
                        "failed to send BroadcastDeviceEvent command"
                    );
                }
            }
        }
        Command::SendDeviceCommandToPeer { peer_id, command } => {
            let Some(session_command_channel) = active_sessions.get(&peer_id) else {
                return Ok(());
            };

            let Some(session_command_tx) = session_command_channel
                .local
                .as_ref()
                .or(session_command_channel.relay.as_ref())
            else {
                return Ok(());
            };

            if session_command_tx.sender
                .send(session::SendToPeer::Command(command))
                .await
                .is_err()
            {
                warn!(
                    reason = "channel closed",
                    "failed to send SendDeviceCommand command"
                );
            }
        }
    }

    Ok(())
}

fn create_session(
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    session_kind: SessionKind,
    event_tx: &mpsc::Sender<session::SessionEvent>,
    token: &CancellationToken,
    peer_config: &Arc<PeerConfig>,
) {
    if let Err(err) = SessionService::start(
        token.child_token(),
        None,
        Some(event_tx.clone()),
        (send, recv, session_kind, peer_config.clone()),
    ) {
        warn!(reason = %err, "failed to start session service");
        token.cancel();
    }
}

async fn handle_session_event(
    session_event: session::SessionEvent,
    active_sessions: &mut Sessions,
    event_tx: &mpsc::Sender<Event>,
) -> anyhow::Result<()> {
    let event = match session_event {
        session::SessionEvent::SessionReady {
            peer_id, peer_name, session_kind, session_id, session_command_tx,
        } => {
            let channels = active_sessions.entry(peer_id).or_default();
            let was_connected = channels.is_connected();
            let handle = SessionHandle { session_id, sender: session_command_tx };
            match session_kind {
                SessionKind::Lan => channels.local = Some(handle),
                SessionKind::Relay => channels.relay = Some(handle),
            }
            if was_connected { None } else { Some(Event::PeerConnected { peer_id, peer_name }) }
        }
        session::SessionEvent::SessionClosed { peer_id, session_kind, session_id } => {
            let Some(channels) = active_sessions.get_mut(&peer_id) else { return Ok(()); };
            let slot = match session_kind {
                SessionKind::Lan => &mut channels.local,
                SessionKind::Relay => &mut channels.relay,
            };
            if slot.as_ref().is_none_or(|session| session.session_id != session_id) {
                return Ok(());
            }
            *slot = None;
            if channels.is_connected() {
                None
            } else {
                active_sessions.remove(&peer_id);
                Some(Event::PeerDisconnected { peer_id })
            }
        }
        session::SessionEvent::ReceivedDeviceState { peer_id, state } => Some(Event::DeviceStateReceivedFromPeer { peer_id, state }),
        session::SessionEvent::ReceivedDeviceEvent { peer_id, event } => Some(Event::DeviceEventReceivedFromPeer { peer_id, event }),
        session::SessionEvent::ReceivedDeviceCommand { peer_id, command } => Some(Event::DeviceCommandReceivedFromPeer { peer_id, command }),
    };
    if let Some(event) = event {
        if event_tx.send(event).await.is_err() {
            warn!(reason = "channel closed", "failed to send Event");
            anyhow::bail!("event channel closed");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::{Event, SessionKind, Sessions, handle_session_event};
    use crate::id::PeerId;
    use crate::network::session::{SendToPeer, SessionEvent};

    #[tokio::test]
    async fn stale_close_does_not_remove_replacement_or_disconnect_peer_early() {
        let peer_id = PeerId::default();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (command_tx_1, _command_rx_1) = mpsc::channel::<SendToPeer>(1);
        let (command_tx_2, _command_rx_2) = mpsc::channel::<SendToPeer>(1);
        let (command_tx_3, _command_rx_3) = mpsc::channel::<SendToPeer>(1);
        let mut sessions = Sessions::new();

        handle_session_event(SessionEvent::SessionReady {
            peer_id,
            peer_name: Arc::from("device"),
            session_kind: SessionKind::Lan,
            session_id: 1,
            session_command_tx: command_tx_1,
        }, &mut sessions, &event_tx).await.unwrap();
        assert!(matches!(event_rx.recv().await, Some(Event::PeerConnected { .. })));

        // Replacing the LAN session must not emit a second peer-connected event.
        handle_session_event(SessionEvent::SessionReady {
            peer_id,
            peer_name: Arc::from("device"),
            session_kind: SessionKind::Lan,
            session_id: 2,
            session_command_tx: command_tx_2,
        }, &mut sessions, &event_tx).await.unwrap();
        assert!(event_rx.try_recv().is_err());

        // A delayed close from the replaced session must be ignored.
        handle_session_event(SessionEvent::SessionClosed {
            peer_id,
            session_kind: SessionKind::Lan,
            session_id: 1,
        }, &mut sessions, &event_tx).await.unwrap();
        assert!(event_rx.try_recv().is_err());

        // LAN and relay are independent routes to the same peer.
        handle_session_event(SessionEvent::SessionReady {
            peer_id,
            peer_name: Arc::from("device"),
            session_kind: SessionKind::Relay,
            session_id: 3,
            session_command_tx: command_tx_3,
        }, &mut sessions, &event_tx).await.unwrap();
        assert!(event_rx.try_recv().is_err());

        handle_session_event(SessionEvent::SessionClosed {
            peer_id,
            session_kind: SessionKind::Lan,
            session_id: 2,
        }, &mut sessions, &event_tx).await.unwrap();
        assert!(event_rx.try_recv().is_err());

        handle_session_event(SessionEvent::SessionClosed {
            peer_id,
            session_kind: SessionKind::Relay,
            session_id: 3,
        }, &mut sessions, &event_tx).await.unwrap();
        assert!(matches!(event_rx.recv().await, Some(Event::PeerDisconnected { .. })));
        assert!(sessions.is_empty());
    }
}
