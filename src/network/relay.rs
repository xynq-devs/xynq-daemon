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
use tokio::{sync::mpsc, task::JoinSet};
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
        const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
        let mut delay = Duration::ZERO;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                command = self.retry_conn_command_rx.recv() => {
                    if command.is_none() {
                        error!("relay retry command channel closed");
                        return;
                    }
                    // Explicit retry bypasses the current backoff.
                    delay = Duration::ZERO;
                }
            }

            let addrs = match tokio::time::timeout(
                CONNECT_TIMEOUT,
                resolve_relay_address(&self.resolver, &self.relay_url, self.relay_port),
            ).await {
                Ok(Ok(addrs)) if !addrs.is_empty() => addrs,
                Ok(Ok(_)) => {
                    warn!("relay DNS lookup returned no addresses");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
                Ok(Err(err)) => {
                    warn!(reason = %err, "failed to resolve relay address");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
                Err(_) => {
                    warn!("relay DNS resolution timed out");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
            };

            let conn = match tokio::time::timeout(CONNECT_TIMEOUT, try_connect(&self.endpoint, addrs)).await {
                Ok(Some(conn)) => conn,
                Ok(None) => {
                    warn!("failed to connect to relay at any resolved address");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
                Err(_) => {
                    warn!("relay connection attempt timed out");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
            };

            let (mut send, mut recv) = match tokio::time::timeout(CONNECT_TIMEOUT, conn.open_bi()).await {
                Ok(Ok(streams)) => streams,
                Ok(Err(err)) => {
                    warn!(reason = %err, "failed to open stream to relay");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
                Err(_) => {
                    warn!("opening relay control stream timed out");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
            };

            if let Err(err) = tokio::time::timeout(
                CONNECT_TIMEOUT,
                send_join_cluster_req(&mut send, self.cluster_id, self.peer_id),
            ).await.map_err(|_| anyhow::anyhow!("sending relay join request timed out")).and_then(|result| result) {
                warn!(reason = %err, "failed to join relay cluster");
                delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                continue;
            }

            let peer_count = match tokio::time::timeout(
                CONNECT_TIMEOUT,
                recv_join_cluster_response(&mut recv),
            ).await {
                Ok(Ok(JoinClusterResponse { peer_count })) => peer_count,
                Ok(Err(err)) => {
                    warn!(reason = %err, "failed to receive relay join response");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
                Err(_) => {
                    warn!("receiving relay join response timed out");
                    delay = next_retry_delay(delay, MAX_RETRY_DELAY);
                    continue;
                }
            };

            // Reset backoff after a successful connection. If it drops, retry quickly.
            delay = Duration::ZERO;

            // Supervise locally opened streams. JoinSet aborts unfinished opens if
            // the relay connection drops or the service is cancelled.
            let mut stream_tasks = JoinSet::new();
            for _ in 0..peer_count {
                let conn = conn.clone();
                let event_tx = self.stream_ready_event_tx.clone();
                stream_tasks.spawn(open_stream_for_peer(conn, event_tx).in_current_span());
            }

            loop {
                tokio::select! {
                    accepted = conn.accept_bi() => {
                        let (send, recv) = match accepted {
                            Ok(streams) => streams,
                            Err(err) => {
                                warn!(reason = %err, "relay connection closed");
                                break;
                            }
                        };

                        if self.stream_ready_event_tx.send(StreamReadyEvent {
                            session_kind: SessionKind::Relay,
                            send,
                            recv,
                        }).await.is_err() {
                            warn!("stream-ready event channel closed");
                            return;
                        }
                    }
                    Some(result) = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => warn!(reason = %err, "failed to open peer stream through relay"),
                            Err(err) => warn!(reason = %err, "relay stream task failed"),
                        }
                    }
                }
            }

            stream_tasks.abort_all();
            while stream_tasks.join_next().await.is_some() {}
            delay = next_retry_delay(delay, MAX_RETRY_DELAY);
        }
    }
}

fn next_retry_delay(current: Duration, maximum: Duration) -> Duration {
    if current.is_zero() {
        Duration::from_secs(1)
    } else {
        current.saturating_mul(2).min(maximum)
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::next_retry_delay;

    #[test]
    fn relay_backoff_grows_and_caps() {
        let max = Duration::from_secs(60);
        assert_eq!(next_retry_delay(Duration::ZERO, max), Duration::from_secs(1));
        assert_eq!(next_retry_delay(Duration::from_secs(1), max), Duration::from_secs(2));
        assert_eq!(next_retry_delay(Duration::from_secs(32), max), max);
        assert_eq!(next_retry_delay(max, max), max);
    }
}
