mod message;
mod sign;

use std::sync::Arc;

use chrono::Utc;
use quinn::{
    RecvStream,
    SendStream,
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
    config::PeerConfig,
    id::PeerId,
    network::{
        SessionKind,
        session::{
            message::{
                DeviceMessage,
                HandshakeMessage,
            },
            sign::{
                sign_handshake,
                verify_handshake,
            },
        },
    },
    service::Service,
    transport::{
        recv_bytes,
        send_bytes,
    },
};

pub struct SessionService {
    token: CancellationToken,

    // channels
    event_tx: mpsc::Sender<SessionEvent>,

    // config
    peer_config: Arc<PeerConfig>,

    // stream
    session_kind: SessionKind,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl Service<SendToPeer, SessionEvent> for SessionService {
    type Params = (
        quinn::SendStream,
        quinn::RecvStream,
        SessionKind,
        Arc<PeerConfig>,
    );

    fn start(
        token: CancellationToken,
        _command_rx: Option<mpsc::Receiver<SendToPeer>>,
        event_tx: Option<mpsc::Sender<SessionEvent>>,
        (send, recv, stream_mode, peer_config): Self::Params,
    ) -> anyhow::Result<()> {
        let event_tx = event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let service = Self {
            token: token.clone(),

            event_tx,

            peer_config,

            session_kind: stream_mode,
            send,
            recv,
        };

        tokio::spawn(
            async move {
                info!("service started");
                tokio::select! {
                    _ = token.cancelled() => {
                        debug!("token cancelled");
                    }
                    _ = service.run() => {
                        // Service run completed, likely due to an error or stream closure.
                        // Cancel the token to inform the parent and stop any child tasks.
                        token.cancel();
                    }
                }
                info!("service stopped");
            }
            .in_current_span(),
        );

        Ok(())
    }

    async fn run(mut self) {
        // create handshake message with timestamp, random nonce, and signature
        let timestamp = Utc::now().timestamp();
        let nonce = rand::random::<[u8; 32]>();
        let signature = sign_handshake(&self.peer_config.secret, timestamp, &nonce);

        let msg = HandshakeMessage {
            peer_id: self.peer_config.peer_id,
            peer_name: self.peer_config.peer_name.clone(),
            timestamp,
            nonce,
            signature,
        };

        // Serialize the handshake message to bytes
        let Ok(bytes) = postcard::to_allocvec(&msg) else {
            error!(
                reason = "invalid type",
                "failed to serialize HandshakeMessage"
            );
            return;
        };

        // Send the handshake message bytes to the peer
        if let Err(err) = self.send.write_all(&bytes).await {
            error!(reason = %err, "failed to send HandshakeMessage");
            return;
        }

        let bytes = match recv_bytes(&mut self.recv).await {
            Ok(b) => b,
            Err(err) => {
                error!(reason = %err, "failed to receive HandshakeMessage");
                return;
            }
        };

        let Ok(msg): Result<HandshakeMessage, _> = postcard::from_bytes(&bytes) else {
            error!(
                reason = "invalid bytes",
                "failed to deserialize HandshakeMessage"
            );
            return;
        };

        if msg.timestamp.abs_diff(Utc::now().timestamp()) > 10 {
            error!(
                reason = "timestamp out of allowed window",
                "handshake rejected"
            );
            return;
        }

        if !verify_handshake(
            &self.peer_config.secret,
            msg.timestamp,
            msg.nonce,
            &msg.signature,
        ) {
            error!(reason = "invalid signature", "handshake rejected");
            return;
        }

        let (command_tx, mut command_rx) = mpsc::channel(64);

        if self
            .event_tx
            .send(SessionEvent::SessionReady {
                peer_id: msg.peer_id,
                peer_name: msg.peer_name,
                session_kind: self.session_kind,
                session_command_tx: command_tx,
            })
            .await
            .is_err()
        {
            warn!(
                reason = "channel closed",
                "failed to send SessionReady event"
            );
            return;
        }

        // Start a task to handle the recv stream, and cancel the token if the task ends (either due to stream closure or an error)
        tokio::spawn(
            async move {
                tokio::select! {
                    _ = self.token.cancelled() => {
                        // Service cancelled
                    }
                    _ = handle_recv_stream(self.recv, &self.event_tx, msg.peer_id) => {
                        // Task completed, likely due to stream closure or an error.
                        // Cancel the token to stop the service and inform the parent.
                        self.token.cancel();
                    }
                }

                if self
                    .event_tx
                    .send(SessionEvent::SessionClosed {
                        peer_id: msg.peer_id,
                        session_kind: self.session_kind,
                    })
                    .await
                    .is_err()
                {
                    warn!(
                        reason = "channel closed",
                        "failed to send SessionClosed event"
                    );
                }
            }
            .in_current_span(),
        );

        handle_send_stream(self.send, &mut command_rx).await;
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
        session_command_tx: mpsc::Sender<SendToPeer>,
    },
    SessionClosed {
        peer_id: PeerId,
        session_kind: SessionKind,
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
    mut recv: RecvStream,
    event_tx: &mpsc::Sender<SessionEvent>,
    peer_id: PeerId,
) {
    loop {
        let bytes = match recv_bytes(&mut recv).await {
            Ok(b) => b,
            Err(err) => {
                warn!(reason = %err, "failed to receive DeviceMessage");
                return;
            }
        };

        let Ok(msg) = postcard::from_bytes(&bytes) else {
            debug!(
                reason = "invalid bytes",
                "failed to deserialize DeviceMessage"
            );
            continue;
        };

        let event = match msg {
            DeviceMessage::State(state) => SessionEvent::ReceivedDeviceState { peer_id, state },
            DeviceMessage::Event(event) => SessionEvent::ReceivedDeviceEvent { peer_id, event },
            DeviceMessage::Command(command) => {
                SessionEvent::ReceivedDeviceCommand { peer_id, command }
            }
        };

        if event_tx.send(event).await.is_err() {
            error!(reason = "channel closed", "failed to send Event");
            return;
        }
    }
}

async fn handle_send_stream(mut send: SendStream, command_rx: &mut mpsc::Receiver<SendToPeer>) {
    while let Some(command) = command_rx.recv().await {
        let msg = match command {
            SendToPeer::State(state) => DeviceMessage::State(state),
            SendToPeer::Event(event) => DeviceMessage::Event(event),
            SendToPeer::Command(command) => DeviceMessage::Command(command),
        };

        let Ok(buf) = postcard::to_allocvec(&msg) else {
            warn!(reason = "invalid type", "failed to serialize DeviceMessage");
            continue;
        };

        if let Err(err) = send_bytes(&mut send, buf).await {
            warn!(reason = %err, "failed to send DeviceMessage");
            return;
        }
    }
    error!(reason = "channel closed", "failed to receive Command");
}
