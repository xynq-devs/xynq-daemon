// services
mod discovery;

mod crypto;

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tokio::{sync::mpsc, task::JoinSet};
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
        // Own connection tasks and deduplicate repeated mDNS announcements by address.
        // JoinSet aborts remaining tasks when this service is dropped.
        let mut connection_tasks: JoinSet<(Option<SocketAddr>, anyhow::Result<()>)> = JoinSet::new();
        let mut pending_outgoing = HashSet::new();

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
                    Some(incoming) = self.endpoint.accept(), if connection_tasks.len() < 128 => {
                        let event_tx = self.stream_ready_event_tx.clone();
                        let token = self.token.child_token();
                        connection_tasks.spawn(async move {
                            (None, accept_connection(incoming, event_tx, token).await)
                        });
                    }

                    Some(discovery::Event::PeerDiscovered { addr }) = self.discovery_event_rx.recv(),
                        if connection_tasks.len() < 128 && !pending_outgoing.contains(&addr) => {
                        pending_outgoing.insert(addr);
                        let endpoint = self.endpoint.clone();
                        let event_tx = self.stream_ready_event_tx.clone();
                        let token = self.token.child_token();
                        connection_tasks.spawn(async move {
                            (Some(addr), initiate_connection(addr, endpoint, event_tx, token).await)
                        });
                    }

                    Some(result) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        match result {
                            Ok((addr, result)) => {
                                if let Some(addr) = addr {
                                    pending_outgoing.remove(&addr);
                                }
                                if let Err(err) = result {
                                    debug!(reason = %err, "LAN connection attempt ended");
                                }
                            }
                            Err(err) => warn!(reason = %err, "LAN connection task failed"),
                        }
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
    token: CancellationToken,
) -> anyhow::Result<()> {
    tokio::select! {
        _ = token.cancelled() => Ok(()),
        result = async move {
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
        } => result
    }
}

async fn initiate_connection(
    addr: SocketAddr,
    endpoint: quinn::Endpoint,
    event_tx: mpsc::Sender<StreamReadyEvent>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    tokio::select! {
        _ = token.cancelled() => Ok(()),
        result = async move {
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
        } => result
    }
}
