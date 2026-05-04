mod command;
mod event;

use interprocess::local_socket::{
    GenericNamespaced,
    ListenerOptions,
    ToNsName,
    tokio::{
        Listener,
        RecvHalf,
        SendHalf,
    },
    traits::tokio::{
        Listener as _,
        Stream as _,
    },
};
use prost::Message;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{
    Instrument,
    error,
    info,
    warn,
};

pub use crate::ipc::{
    command::Command,
    event::Event,
};
use crate::{
    service::Service,
    transport::{
        recv_bytes,
        send_bytes,
    },
};

pub struct IpcService {
    token: CancellationToken,

    // channels
    command_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<Event>,

    // ipc
    listener: Listener,
}

impl Service<Command, Event> for IpcService {
    type Params = Option<String>;

    #[tracing::instrument(name = "IPC", skip_all)]
    fn start(
        token: CancellationToken,
        command_rx: Option<mpsc::Receiver<Command>>,
        event_tx: Option<mpsc::Sender<Event>>,
        socket: Self::Params,
    ) -> anyhow::Result<()> {
        let command_rx = command_rx.ok_or_else(|| anyhow::anyhow!("command receiver missing"))?;
        let event_tx = event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let namespace = socket
            .unwrap_or_else(|| "xync".to_owned())
            .to_ns_name::<GenericNamespaced>()
            .inspect_err(|err| {
                error!(reason = %err, "failed to create namespace");
            })?;

        // Create listener with the namespace
        let listener = ListenerOptions::new()
            .name(namespace)
            .try_overwrite(true)
            .create_tokio()
            .inspect_err(|err| {
                error!(reason = %err, "failed to create listener");
            })?;

        let service = Self {
            token: token.clone(),
            command_rx,
            event_tx,
            listener,
        };

        tokio::spawn(
            async move {
                info!("service started");
                tokio::select! {
                    _ = token.cancelled() => {
                        info!("token cancelled");
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
        // Await a connection
        info!("waiting for app to connect");
        let stream = match self.listener.accept().await {
            Ok(stream) => stream,
            Err(err) => {
                error!(reason = %err, "failed to accept app connection");
                return;
            }
        };
        info!("app connected");

        // Split the stream into a recv half and a send half
        let (recv, send) = stream.split();

        // Spawn a task to receive IpcCommand from the client and send them as Event to the core
        tokio::spawn(
            async move {
                tokio::select! {
                    _ = self.token.cancelled() => {
                        // Service cancelled
                    },
                    _ = handle_recv_stream(recv, &self.event_tx) => {
                        // Task completed, likely due to stream closure or an error.
                        // Cancel the token to stop the service and inform the parent.
                        self.token.cancel();
                    }
                }
            }
            .in_current_span(),
        );

        // Start another task to receive Command from the core and send them as IpcEvent to the client
        handle_send_stream(send, &mut self.command_rx).await;
    }
}

async fn handle_recv_stream(mut recv: RecvHalf, event_tx: &mpsc::Sender<Event>) {
    loop {
        let bytes = match recv_bytes(&mut recv).await {
            Ok(bytes) => bytes,
            Err(err) => {
                error!(reason = %err, "failed to receive IpcCommand");
                break;
            }
        };

        let ipc_action = match xynq_proto::IpcCommand::decode(bytes.as_slice()) {
            Ok(action) => action,
            Err(err) => {
                warn!(reason = %err, "failed to decode IpcCommand");
                continue;
            }
        };

        let event = match Event::try_from(ipc_action) {
            Ok(event) => event,
            Err(err) => {
                warn!(reason = %err, "failed to convert IpcAction into Event");
                continue;
            }
        };

        if event_tx.send(event).await.is_err() {
            error!(reason = "channel closed", "failed to send Event");
            break;
        }
    }
}

async fn handle_send_stream(mut send: SendHalf, command_rx: &mut mpsc::Receiver<Command>) {
    while let Some(command) = command_rx.recv().await {
        let ipc_event = xynq_proto::IpcEvent::from(command);

        let mut buf = Vec::with_capacity(ipc_event.encoded_len());

        if let Err(err) = ipc_event.encode(&mut buf) {
            warn!(reason = %err, "failed to encode IpcEvent");
            continue;
        }

        if let Err(err) = send_bytes(&mut send, buf).await {
            error!(reason = %err, "failed to send IpcEvent");
            break;
        }
    }
    error!(reason = "channel closed", "failed to receive Command");
}
