use std::net::{
    IpAddr,
    SocketAddr,
};

use mdns_sd::IfKind;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{
    Instrument,
    debug,
    error,
    info,
};

use crate::{
    id::{
        ClusterId,
        PeerId,
    },
    service::Service,
};

const SERVICE_TYPE: &str = "_xync._udp.local.";

const CLUSTER_ID_KEY: &str = "cluster_id";
const PEER_ID_KEY: &str = "peer_id";

pub struct DiscoveryService {
    // channels
    event_tx: mpsc::Sender<Event>,

    // config
    cluster_id: ClusterId,
    peer_id: PeerId,

    // state
    service_event_rx: mdns_sd::Receiver<mdns_sd::ServiceEvent>,
}

impl Service<(), Event> for DiscoveryService {
    type Params = (ClusterId, PeerId, u16);

    fn start(
        token: CancellationToken,
        _command_rx: Option<mpsc::Receiver<()>>,
        event_tx: Option<mpsc::Sender<Event>>,
        (cluster_id, peer_id, port): Self::Params,
    ) -> anyhow::Result<()> {
        let event_tx = event_tx.ok_or_else(|| anyhow::anyhow!("event sender missing"))?;

        let mdns_daemon = mdns_sd::ServiceDaemon::new()
            .map_err(|_| anyhow::anyhow!("mdns daemon failed to start"))?;

        // Disable unwanted interfaces to avoid receiving our own service events and reduce noise.
        mdns_daemon
            .disable_interface(vec![IfKind::IPv6, IfKind::LoopbackV4, IfKind::LoopbackV6])
            .map_err(|_| anyhow::anyhow!("mdns interface configuration failed"))?;
        // Enable only IPv4 interface
        mdns_daemon
            .enable_interface(IfKind::IPv4)
            .map_err(|_| anyhow::anyhow!("mdns interface configuration failed"))?;

        // Disable multicast loopback to avoid receiving our own service events.
        mdns_daemon
            .set_multicast_loop_v4(false)
            .map_err(|_| anyhow::anyhow!("mdns interface configuration failed"))?;
        mdns_daemon
            .set_multicast_loop_v6(false)
            .map_err(|_| anyhow::anyhow!("mdns interface configuration failed"))?;

        let peer_id_str = peer_id.to_string();

        let properties = [
            (CLUSTER_ID_KEY, cluster_id.to_string()),
            (PEER_ID_KEY, peer_id_str.clone()),
        ];

        // Create service info with the specified type, name, hostname, port, and properties.
        let service_info = mdns_sd::ServiceInfo::new(
            SERVICE_TYPE,
            &peer_id_str,
            &format!("{}.local.", peer_id_str), // Use peer_id as hostname
            "",                                 // Empty ip addrs
            port,
            properties.as_slice(),
        )
        .map_err(|_| anyhow::anyhow!("invalid mdns service info"))?
        // Since we have not provided any ip addrs, we have to enable auto-addr
        .enable_addr_auto();

        // Register the service with the daemon to start advertising it on the network.
        mdns_daemon
            .register(service_info)
            .map_err(|_| anyhow::anyhow!("mdns service register failed"))?;

        // Browse for services and get a receiver for service events.
        let service_event_rx = mdns_daemon
            .browse(SERVICE_TYPE)
            .map_err(|_| anyhow::anyhow!("mdns service browse failed"))?;

        let service = Self {
            event_tx,
            cluster_id,
            peer_id,
            service_event_rx,
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
                // Shutdown the daemon to stop any background tasks and free resources.
                let _ = mdns_daemon.unregister(SERVICE_TYPE);
                let _ = mdns_daemon.shutdown();
                info!("service stopped");
            }
            .in_current_span(),
        );

        Ok(())
    }

    async fn run(self) {
        loop {
            let service_event = match self.service_event_rx.recv_async().await {
                Ok(service_event) => service_event,
                Err(err) => {
                    error!(reason = %err, "failed to receive mDNS service event");
                    return;
                }
            };

            let resolved_service = match service_event {
                mdns_sd::ServiceEvent::ServiceResolved(resolved_service) => resolved_service,
                mdns_sd::ServiceEvent::SearchStopped(_) => {
                    error!("mDNS search stopped unexpectedly");
                    return;
                }
                _ => {
                    continue;
                }
            };

            let Some(cid_str) = resolved_service.get_property_val_str(CLUSTER_ID_KEY) else {
                debug!(reason = "missing cluster_id", "ignoring service");
                continue;
            };

            let cluster_id = match cid_str.parse::<ClusterId>() {
                Ok(cluster_id) => cluster_id,
                Err(err) => {
                    debug!(reason = %err, "failed to parse cluster_id property");
                    continue;
                }
            };

            if cluster_id != self.cluster_id {
                debug!(reason = "different cluster", "ignoring service");
                continue;
            }

            let Some(pid_str) = resolved_service.get_property_val_str(PEER_ID_KEY) else {
                debug!(reason = "missing peer_id", "ignoring service");
                continue;
            };

            let peer_id = match pid_str.parse::<PeerId>() {
                Ok(peer_id) => peer_id,
                Err(err) => {
                    debug!(reason = %err, "failed to parse peer_id property");
                    continue;
                }
            };

            if peer_id == self.peer_id {
                debug!(reason = "peer_id matches self", "ignoring service");
                continue;
            }

            if peer_id < self.peer_id {
                debug!(reason = "peer_id younger then self", "ignoring service");
                continue;
            }

            let ipv4_addrs = resolved_service.get_addresses_v4();

            for ipv4_addr in ipv4_addrs {
                if ipv4_addr.is_private() {
                    debug!(address = %ipv4_addr, "found private IPv4 address");
                    let addr = SocketAddr::new(IpAddr::V4(ipv4_addr), resolved_service.get_port());

                    info!(address = %addr, "peer discovered");
                    if self
                        .event_tx
                        .send(Event::PeerDiscovered { addr })
                        .await
                        .is_err()
                    {
                        error!(
                            reason = "channel closed",
                            "failed to send PeerDiscoveredEvent to Local"
                        );
                        return;
                    }
                    break;
                } else {
                    debug!(address = %ipv4_addr, reason = "found non-private IPv4 address", "ignoring address");
                }
            }
        }
    }
}

pub enum Event {
    PeerDiscovered { addr: SocketAddr },
}
