// services
mod discovery;

mod crypto;

use std::{
    net::{
        Ipv4Addr,
        SocketAddr,
    },
    sync::Arc,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{
    Instrument,
    debug,
    info,
    warn,
};

use crate::{
    config::{
        LanConfig,
        PeerConfig,
    },
    id::{
        ClusterId,
        PeerId,
    },
    network::{
        SessionKind,
        StreamReadyEvent,
        lan::{
            crypto::create_server_client_configs,
            discovery::DiscoveryService,
        },
    },
    service::Service,
};

pub struct LanService {
    token: CancellationToken,

    // channels
    stream_ready_event_tx: mpsc::Sender<StreamReadyEvent>,

    // config
    cluster_id: ClusterId,
    peer_id: PeerId,

    // state
    endpoint: quinn::Endpoint,
    port: u16,

    // discovery
    discovery_token: CancellationToken,
    discovery_event_tx: mpsc::Sender<discovery::Event>,
    discovery_event_rx: mpsc::Receiver<discovery::Event>,
}

impl Service<(), StreamReadyEvent> for LanService {
    type Params = (Arc<PeerConfig>, Arc<LanConfig>);

    fn start(
        token: CancellationToken,
        _command_rx: Option<mpsc::Receiver<()>>,
        event_tx: Option<mpsc::Sender<StreamReadyEvent>>,
        (peer_config, lan_config): Self::Params,
    ) -> anyhow::Result<()> {
        let stream_ready_event_tx =
            event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let (server_cfg, client_cfg) = create_server_client_configs(lan_config.clone())?;

        // Use unspecified address and let the OS assign a port
        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);

        // Create the QUIC endpoint using the server config and bind it to the address
        let mut endpoint = quinn::Endpoint::server(server_cfg, addr)
            .map_err(|_| anyhow::anyhow!("socket bind failed"))?;

        // Use the client config for outgoing connections
        endpoint.set_default_client_config(client_cfg);

        let port = endpoint
            .local_addr()
            .map_err(|_| anyhow::anyhow!("no local address binded"))?
            .port();

        let discovery_token = token.child_token();
        let (discovery_event_tx, discovery_event_rx) = mpsc::channel(8);

        let service = Self {
            token: token.clone(),

            stream_ready_event_tx,

            cluster_id: peer_config.cluster_id,
            peer_id: peer_config.peer_id,

            endpoint,
            port,

            discovery_token,
            discovery_event_tx,
            discovery_event_rx,
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
        loop {
            if let Err(err) = DiscoveryService::start(
                self.discovery_token.clone(),
                None,
                Some(self.discovery_event_tx.clone()),
                (self.cluster_id, self.peer_id, self.port),
            ) {
                warn!(reason = %err, "unable to start discovery service");
            }

            loop {
                tokio::select! {
                    Some(incoming) = self.endpoint.accept() => {
                        tokio::spawn(accept_connection(incoming, self.stream_ready_event_tx.clone()));
                    }

                    Some(discovery::Event::PeerDiscovered { addr }) = self.discovery_event_rx.recv() => {
                        tokio::spawn(initiate_connection(addr, self.endpoint.clone(), self.stream_ready_event_tx.clone()));
                    }

                    _ = self.discovery_token.cancelled() => {
                        info!("restarting discovery service");

                        // Generate a new token for the discovery service
                        self.discovery_token = self.token.child_token();


                        break;
                    }
                }
            }
        }
    }
}

async fn accept_connection(
    incoming: quinn::Incoming,
    event_tx: mpsc::Sender<StreamReadyEvent>,
) -> anyhow::Result<()> {
    let addr = incoming.remote_address();

    debug!(address = %addr, "accepting connection");

    let connection = incoming
        .accept()
        .inspect_err(|err| {
            debug!(reason = %err, address = %addr, "failed to accept incoming connection");
        })?
        .await
        .inspect_err(|err| {
            debug!(reason = %err, address = %addr, "failed to establish incoming connection");
        })?;

    let (send, recv) = connection.accept_bi().await.inspect_err(|err| {
        debug!(reason = %err, address = %addr, "failed to accept bidirectional stream");
    })?;

    event_tx
        .send(StreamReadyEvent {
            session_kind: SessionKind::Lan,
            send,
            recv,
        })
        .await?;

    Ok(())
}

async fn initiate_connection(
    addr: SocketAddr,
    endpoint: quinn::Endpoint,
    event_tx: mpsc::Sender<StreamReadyEvent>,
) -> anyhow::Result<()> {
    debug!(address = %addr, "initiating connection");

    let connection = endpoint
        .connect(addr, "xync")
        .inspect_err(|err| {
            debug!(reason = %err, address = %addr, "failed to initiate connection");
        })?
        .await
        .inspect_err(|err| {
            debug!(reason = %err, address = %addr, "failed to establish outgoing connection");
        })?;

    let (send, recv) = connection.open_bi().await.inspect_err(|err| {
        debug!(reason = %err, address = %addr, "failed to open bidirectional stream");
    })?;

    event_tx
        .send(StreamReadyEvent {
            session_kind: SessionKind::Lan,
            send,
            recv,
        })
        .await?;

    Ok(())
}
