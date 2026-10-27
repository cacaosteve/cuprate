//! Snapshots of handshaken peers for the private administrative RPC.

use cuprate_helper::net::ip_is_local;
use cuprate_types::{AddressType, ConnectionState};
use cuprate_wire::NetworkAddress;

use crate::{types::ConnectionInfo, ConnectionDirection, NetZoneAddress, NetworkZone};

use super::{InternalPeerID, PeerInformation};

impl<A: NetZoneAddress> PeerInformation<A> {
    pub fn connection_info<Z: NetworkZone<Addr = A>>(&self) -> ConnectionInfo<A> {
        let address = match self.id {
            InternalPeerID::KnownAddr(address) => Some(address),
            InternalPeerID::Unknown(_) => None,
        };
        let (address_type, host, socket_addr) = match address.map(Into::into) {
            Some(NetworkAddress::Clear(socket)) => (
                if socket.is_ipv4() {
                    AddressType::Ipv4
                } else {
                    AddressType::Ipv6
                },
                socket.ip().to_string(),
                Some(socket),
            ),
            Some(NetworkAddress::Tor(address)) => (AddressType::Tor, address.addr_string(), None),
            Some(NetworkAddress::I2p(address)) => (AddressType::I2p, address.addr_string(), None),
            None => (Z::ANONYMOUS_ADDRESS_TYPE, String::new(), None),
        };

        ConnectionInfo {
            address,
            address_type,
            host,
            socket_addr,
            incoming: self.direction == ConnectionDirection::Inbound,
            live_time: self.connected_at.elapsed().as_secs(),
            height: self.core_sync_data.lock().unwrap().current_height,
            localhost: socket_addr.is_some_and(|socket| socket.ip().is_loopback()),
            local_ip: socket_addr.is_some_and(|socket| ip_is_local(socket.ip())),
            peer_id: self.basic_node_data.peer_id,
            pruning_seed: self.pruning_seed,
            rpc_port: self.basic_node_data.rpc_port,
            rpc_credits_per_hash: self.basic_node_data.rpc_credits_per_hash,
            support_flags: self.basic_node_data.support_flags.bits(),
            // Handshaken peers. Cuprate does not use monerod's per-connection
            // block-sync state machine.
            state: ConnectionState::Normal,
            // Byte/rate/idle accounting and monerod UUIDs are not implemented.
            // Keep the schema's zero sentinels, not invented estimates.
            avg_download: 0,
            avg_upload: 0,
            current_download: 0,
            current_upload: 0,
            recv_count: 0,
            recv_idle_time: 0,
            send_count: 0,
            send_idle_time: 0,
            connection_id: Default::default(),
        }
    }
}
