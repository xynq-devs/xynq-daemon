mod crypto;

use std::{
    net::{
        Ipv4Addr,
        SocketAddr,
    },
    sync::Arc,
    time::Duration,
};

use hickory_resolver::{
    Resolver,
    net::runtime::TokioRuntimeProvider,
};
use prost::Message;
use quinn::{
    Connection,
    Endpoint,
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
    JoinClusterRequest,
    JoinClusterResponse,
};

use crate::{
    RETRY_DELAY,
    config::RelayConfig,
    id::{
        ClusterId,
        PeerId,
    },
    network::{
        RetryConnectionCommand,
        SessionKind,
        StreamReadyEvent,
        relay::crypto::create_client_config,
    },
    service::Service,
    transport::{
        recv_bytes,
        send_bytes,
    },
};

pub struct RelayService {
    // channels
    retry_conn_command_rx: mpsc::Receiver<RetryConnectionCommand>,
    stream_ready_event_tx: mpsc::Sender<StreamReadyEvent>,

    // config
    cluster_id: ClusterId,
    peer_id: PeerId,
    relay_url: String,
    relay_port: u16,

    // network
    endpoint: Endpoint,
    resolver: Resolver<TokioRuntimeProvider>,
}

impl Service<RetryConnectionCommand, StreamReadyEvent> for RelayService {
    type Params = (Arc<RelayConfig>, ClusterId, PeerId);

    #[tracing::instrument(name = "Relay", skip_all)]
    fn start(
        token: CancellationToken,
        command_rx: Option<mpsc::Receiver<RetryConnectionCommand>>,
        event_tx: Option<mpsc::Sender<StreamReadyEvent>>,
        (relay_config, cluster_id, peer_id): Self::Params,
    ) -> anyhow::Result<()> {
        let retry_conn_command_rx =
            command_rx.ok_or_else(|| anyhow::anyhow!("command receiver missing"))?;

        let stream_ready_event_tx =
            event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let client_config = create_client_config(&relay_config)?;

        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);

        // Create a client endpoint and bind to the specified address
        let mut endpoint =
            Endpoint::client(addr).map_err(|_| anyhow::anyhow!("socket bind failed"))?;

        endpoint.set_default_client_config(client_config);

        let resolver = hickory_resolver::Resolver::builder_tokio()
            .map_err(|_| anyhow::anyhow!("resolver failure"))?
            .build()
            .map_err(|_| anyhow::anyhow!("resolver failure"))?;

        let service = Self {
            retry_conn_command_rx,
            stream_ready_event_tx,

            relay_url: relay_config.url.clone(),
            relay_port: relay_config.port,
            cluster_id,
            peer_id,

            endpoint,
            resolver,
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
        let mut delay = Duration::ZERO;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                Some(RetryConnectionCommand) = self.retry_conn_command_rx.recv() => {}
                else => {
                    error!(reason = "channel closed", "failed to receive Command");
                    return;
                }
            };
            delay = RETRY_DELAY;

            let addrs =
                match resolve_relay_address(&self.resolver, &self.relay_url, self.relay_port).await
                {
                    Ok(addrs) => addrs,
                    Err(err) => {
                        warn!(reason = %err, "failed to resolve relay address");
                        continue;
                    }
                };

            let conn = match try_connect(&self.endpoint, addrs).await {
                Some(conn) => conn,
                None => {
                    warn!("failed to connect to relay at any resolved address");
                    continue;
                }
            };

            let (mut send, mut recv) = match conn.open_bi().await {
                Ok(s) => s,
                Err(err) => {
                    warn!(reason = %err, "failed to open stream to relay");
                    debug!("closing connection");
                    continue;
                }
            };

            if let Err(err) = send_join_cluster_req(&mut send, self.cluster_id, self.peer_id).await
            {
                warn!(reason = %err, "failed to send JoinClusterRequest to relay");
                debug!("closing connection");
                continue;
            }

            let peer_count = match recv_join_cluster_response(&mut recv).await {
                Ok(JoinClusterResponse { peer_count }) => peer_count,
                Err(err) => {
                    warn!(reason = %err, "failed to receive JoinClusterResponse from relay");
                    debug!("closing connection");
                    continue;
                }
            };

            for _ in 0..peer_count {
                tokio::spawn(
                    open_stream_for_peer(conn.clone(), self.stream_ready_event_tx.clone())
                        .in_current_span(),
                );
            }

            loop {
                let (send, recv) = match conn.accept_bi().await {
                    Ok(s) => s,
                    Err(err) => {
                        warn!(reason = %err, "failed to accept stream from relay");
                        break;
                    }
                };

                if self
                    .stream_ready_event_tx
                    .send(StreamReadyEvent {
                        session_kind: SessionKind::Relay,
                        send,
                        recv,
                    })
                    .await
                    .is_err()
                {
                    warn!(
                        reason = "channel closed",
                        "failed to send StreamReady event"
                    );

                    // Stop the service because the event channel is closed
                    return;
                }
            }
        }
    }
}

// Resolve the relay URL to a list of socket addresses using the provided resolver
async fn resolve_relay_address(
    resolver: &Resolver<TokioRuntimeProvider>,
    url: &str,
    port: u16,
) -> anyhow::Result<Vec<SocketAddr>> {
    let lookup = resolver.lookup_ip(url).await?;

    Ok(lookup.iter().map(|ip| SocketAddr::new(ip, port)).collect())
}

// Attempt to connect to the relay server at each of the provided addresses until one succeeds
async fn try_connect(endpoint: &Endpoint, addrs: Vec<SocketAddr>) -> Option<quinn::Connection> {
    for addr in addrs {
        debug!(address = %addr, "attempting to connect to relay");
        let connecting = match endpoint.connect(addr, "xynq-relay") {
            Ok(c) => c,
            Err(err) => {
                debug!(reason = %err, address = %addr, "failed to initiate connection");
                continue;
            }
        };
        match connecting.await {
            Ok(conn) => return Some(conn),
            Err(err) => {
                debug!(reason = %err, address = %addr, "failed to connect");
            }
        }
    }
    None
}

// Send a ClusterJoinMsg to the relay to identify the cluster and peer
async fn send_join_cluster_req(
    send: &mut SendStream,
    cluster_id: ClusterId,
    peer_id: PeerId,
) -> anyhow::Result<()> {
    let request = JoinClusterRequest {
        cluster_id: Some(cluster_id.into()),
        peer_id: Some(peer_id.into()),
    };
    // Pre-allocate a buffer of the correct size
    let mut buf = Vec::with_capacity(request.encoded_len());
    let _ = request.encode(&mut buf); // Infallible because buffer is pre-allocated
    send_bytes(send, buf).await
}

// Receive an OpenStreamMsg from the relay to determine how many streams to open for existing peers
async fn recv_join_cluster_response(recv: &mut RecvStream) -> anyhow::Result<JoinClusterResponse> {
    let bytes = recv_bytes(recv).await?;
    let msg = JoinClusterResponse::decode(bytes.as_slice())?;
    Ok(msg)
}

// Open a stream to the relay for each existing peer in the cluster
async fn open_stream_for_peer(
    conn: Connection,
    event_tx: mpsc::Sender<StreamReadyEvent>,
) -> anyhow::Result<()> {
    let (send, recv) = conn.open_bi().await.inspect_err(|err| {
        error!(reason = %err, "failed to open stream to relay for peer");
    })?;

    event_tx
        .send(StreamReadyEvent {
            session_kind: SessionKind::Relay,
            send,
            recv,
        })
        .await?;

    Ok(())
}
