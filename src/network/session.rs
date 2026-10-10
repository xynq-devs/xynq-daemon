mod message;
mod sign;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use quinn::{RecvStream, SendStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};
use xynq_proto::{DeviceCommand, DeviceEvent, DeviceState};

use crate::{
    config::PeerConfig,
    id::PeerId,
    network::{
        SessionKind,
        session::{
            message::{DeviceMessage, HandshakeMessage},
            sign::{sign_handshake, verify_handshake},
        },
    },
    service::Service,
    transport::{recv_bytes, send_bytes},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

pub struct SessionService {
    token: CancellationToken,
    event_tx: mpsc::Sender<SessionEvent>,
    peer_config: Arc<PeerConfig>,
    session_kind: SessionKind,
    send: SendStream,
    recv: RecvStream,
}

impl Service<SendToPeer, SessionEvent> for SessionService {
    type Params = (SendStream, RecvStream, SessionKind, Arc<PeerConfig>);

    fn start(
        token: CancellationToken,
        _command_rx: Option<mpsc::Receiver<SendToPeer>>,
        event_tx: Option<mpsc::Sender<SessionEvent>>,
        (send, recv, session_kind, peer_config): Self::Params,
    ) -> anyhow::Result<()> {
        let event_tx = event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let service = Self {
            token: token.clone(),
            event_tx,
            peer_config,
            session_kind,
            send,
            recv,
        };

        // run() owns cancellation and cleanup so SessionClosed is emitted once,
        // regardless of which stream ends first or whether shutdown is requested.
        tokio::spawn(
            async move {
                info!("session started");
                service.run().await;
                token.cancel();
                info!("session stopped");
            }
            .in_current_span(),
        );

        Ok(())
    }

    async fn run(mut self) {
        let token = self.token.clone();
        let (peer_id, peer_name) = tokio::select! {
            _ = token.cancelled() => {
                debug!("session cancelled during handshake");
                return;
            }
            result = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.authenticate()) => {
                match result {
                    Ok(Ok(peer)) => peer,
                    Ok(Err(err)) => {
                        warn!(reason = %err, "peer handshake failed");
                        return;
                    }
                    Err(_) => {
                        warn!(timeout_secs = HANDSHAKE_TIMEOUT.as_secs(), "peer handshake timed out");
                        return;
                    }
                }
            }
        };

        if peer_id == self.peer_config.peer_id {
            warn!("rejecting self-connection");
            return;
        }

        let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        let (command_tx, mut command_rx) = mpsc::channel(64);

        tokio::select! {
            _ = token.cancelled() => {
                debug!(session_id, "session cancelled before activation");
                return;
            }
            result = self.event_tx.send(SessionEvent::SessionReady {
                peer_id,
                peer_name,
                session_kind: self.session_kind,
                session_id,
                session_command_tx: command_tx,
            }) => {
                if result.is_err() {
                    warn!("event channel closed before session became active");
                    return;
                }
            }
        }

        // One supervisor owns both stream futures. The first failure/cancellation
        // drops the other future and emits exactly one close event.
        let token = self.token.clone();
        tokio::select! {
            _ = token.cancelled() => {
                debug!(session_id, "session cancellation requested");
            }
            result = handle_recv_stream(&mut self.recv, &self.event_tx, peer_id) => {
                if let Err(err) = result {
                    warn!(session_id, reason = %err, "receive stream ended");
                }
            }
            result = handle_send_stream(&mut self.send, &mut command_rx) => {
                if let Err(err) = result {
                    warn!(session_id, reason = %err, "send stream ended");
                }
            }
        }

        if self.event_tx.send(SessionEvent::SessionClosed {
            peer_id,
            session_kind: self.session_kind,
            session_id,
        }).await.is_err() {
            debug!(peer_id = %peer_id, session_id, "event channel closed while reporting session close");
        }
    }
}

impl SessionService {
    async fn authenticate(&mut self) -> anyhow::Result<(PeerId, Arc<str>)> {
        let timestamp = Utc::now().timestamp();
        let nonce = rand::random::<[u8; 32]>();
        let signature = sign_handshake(
            &self.peer_config.secret,
            self.peer_config.peer_id,
            &self.peer_config.peer_name,
            timestamp,
            &nonce,
        );

        let handshake = HandshakeMessage {
            peer_id: self.peer_config.peer_id,
            peer_name: self.peer_config.peer_name.clone(),
            timestamp,
            nonce,
            signature,
        };

        let bytes = postcard::to_allocvec(&handshake)?;
        // Both handshake and application messages use the same length-prefixed framing.
        send_bytes(&mut self.send, bytes).await?;

        let bytes = recv_bytes(&mut self.recv).await?;
        let remote: HandshakeMessage = postcard::from_bytes(&bytes)
            .map_err(|err| anyhow::anyhow!("invalid handshake payload: {err}"))?;

        if remote.timestamp.abs_diff(Utc::now().timestamp()) > 10 {
            anyhow::bail!("handshake timestamp outside allowed window");
        }

        if !verify_handshake(
            &self.peer_config.secret,
            remote.peer_id,
            &remote.peer_name,
            remote.timestamp,
            &remote.nonce,
            &remote.signature,
        ) {
            anyhow::bail!("invalid handshake signature");
        }

        Ok((remote.peer_id, remote.peer_name))
    }
}

pub enum SendToPeer {
    State(DeviceState),
    Event(DeviceEvent),
    Command(DeviceCommand),
}

pub enum SessionEvent {
    SessionReady {
        peer_id: PeerId,
        peer_name: Arc<str>,
        session_kind: SessionKind,
        session_id: u64,
        session_command_tx: mpsc::Sender<SendToPeer>,
    },
    SessionClosed {
        peer_id: PeerId,
        session_kind: SessionKind,
        session_id: u64,
    },
    ReceivedDeviceState {
        peer_id: PeerId,
        state: DeviceState,
    },
    ReceivedDeviceEvent {
        peer_id: PeerId,
        event: DeviceEvent,
    },
    ReceivedDeviceCommand {
        peer_id: PeerId,
        command: DeviceCommand,
    },
}

async fn handle_recv_stream(
    recv: &mut RecvStream,
    event_tx: &mpsc::Sender<SessionEvent>,
    peer_id: PeerId,
) -> anyhow::Result<()> {
    loop {
        let bytes = recv_bytes(recv).await?;
        let msg: DeviceMessage = postcard::from_bytes(&bytes)
            .map_err(|err| anyhow::anyhow!("invalid DeviceMessage: {err}"))?;

        let event = match msg {
            DeviceMessage::State(state) => SessionEvent::ReceivedDeviceState { peer_id, state },
            DeviceMessage::Event(event) => SessionEvent::ReceivedDeviceEvent { peer_id, event },
            DeviceMessage::Command(command) => SessionEvent::ReceivedDeviceCommand { peer_id, command },
        };

        event_tx.send(event).await
            .map_err(|_| anyhow::anyhow!("session event channel closed"))?;
    }
}

async fn handle_send_stream(
    send: &mut SendStream,
    command_rx: &mut mpsc::Receiver<SendToPeer>,
) -> anyhow::Result<()> {
    while let Some(command) = command_rx.recv().await {
        let msg = match command {
            SendToPeer::State(state) => DeviceMessage::State(state),
            SendToPeer::Event(event) => DeviceMessage::Event(event),
            SendToPeer::Command(command) => DeviceMessage::Command(command),
        };

        let buf = postcard::to_allocvec(&msg)
            .map_err(|err| anyhow::anyhow!("failed to serialize DeviceMessage: {err}"))?;
        send_bytes(send, buf).await?;
    }

    Ok(())
}
