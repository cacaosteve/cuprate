//! Functions to send [`AddressBookRequest`]s.

use anyhow::{anyhow, Error};
use tower::ServiceExt;

use cuprate_helper::map::u32_from_ipv4;
use cuprate_p2p_core::{
    services::{AddressBookRequest, AddressBookResponse, ZoneSpecificPeerListEntryBase},
    types::BanState,
    AddressBook, NetworkZone,
};
use cuprate_rpc_types::misc::ConnectionInfo;
use cuprate_types::rpc::Peer;
use cuprate_wire::NetworkAddress;

// FIXME: use `anyhow::Error` over `tower::BoxError` in address book.

/// [`AddressBookRequest::ConnectionInfo`]
pub(crate) async fn connection_info<Z: NetworkZone>(
    address_book: &mut impl AddressBook<Z>,
) -> Result<Vec<ConnectionInfo>, Error> {
    let AddressBookResponse::ConnectionInfo(vec) = address_book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::ConnectionInfo)
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!();
    };

    Ok(vec.into_iter().map(Into::into).collect())
}

/// [`AddressBookRequest::SetBan`]
pub(crate) async fn set_ban<Z: NetworkZone>(
    address_book: &mut impl AddressBook<Z>,
    set_ban: cuprate_p2p_core::types::SetBan<Z::Addr>,
) -> Result<(), Error> {
    let AddressBookResponse::Ok = address_book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::SetBan(set_ban))
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!();
    };

    Ok(())
}

/// [`AddressBookRequest::GetBan`]
pub(crate) async fn get_ban<Z: NetworkZone>(
    address_book: &mut impl AddressBook<Z>,
    peer: Z::Addr,
) -> Result<Option<std::time::Instant>, Error> {
    let AddressBookResponse::GetBan { unban_instant } = address_book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::GetBan(peer))
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!();
    };

    Ok(unban_instant)
}

/// [`AddressBookRequest::GetBans`]
pub(crate) async fn get_bans<Z: NetworkZone>(
    address_book: &mut impl AddressBook<Z>,
) -> Result<Vec<BanState<Z::Addr>>, Error> {
    let AddressBookResponse::GetBans(bans) = address_book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::GetBans)
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!();
    };

    Ok(bans)
}

/// [`AddressBookRequest::Peerlist`]
pub(crate) async fn peerlist<Z: NetworkZone>(
    address_book: &mut impl AddressBook<Z>,
    include_blocked: bool,
) -> Result<(Vec<Peer>, Vec<Peer>), Error> {
    let AddressBookResponse::Peerlist(peerlist) = address_book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::Peerlist { include_blocked })
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!();
    };

    fn map<Z: NetworkZone>(peers: Vec<ZoneSpecificPeerListEntryBase<Z::Addr>>) -> Vec<Peer> {
        peers
            .into_iter()
            .map(|peer| {
                let ZoneSpecificPeerListEntryBase {
                    adr,
                    id,
                    last_seen,
                    pruning_seed,
                    rpc_port,
                    rpc_credits_per_hash,
                } = peer;

                let (host, ip, port) = match adr.into() {
                    NetworkAddress::Clear(socket) => (
                        socket.ip().to_string(),
                        match socket.ip() {
                            std::net::IpAddr::V4(ip) => u32_from_ipv4(ip),
                            std::net::IpAddr::V6(_) => 0,
                        },
                        socket.port(),
                    ),
                    NetworkAddress::Tor(address) => (address.addr_string(), 0, address.port()),
                    NetworkAddress::I2p(address) => (address.addr_string(), 0, address.port()),
                };

                let last_seen = last_seen.try_into().unwrap_or(0);
                let pruning_seed = pruning_seed.compress();

                Peer {
                    id,
                    host,
                    ip,
                    port,
                    rpc_port,
                    rpc_credits_per_hash,
                    last_seen,
                    pruning_seed,
                }
            })
            .collect()
    }

    let white = map::<Z>(peerlist.white);
    let grey = map::<Z>(peerlist.grey);

    Ok((white, grey))
}
