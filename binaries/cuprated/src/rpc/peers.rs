//! Shared handles to the real zone address books, including zones started after sync.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Error};
use tower::{util::BoxCloneService, Service, ServiceExt};

use cuprate_helper::cast::usize_to_u64;
use cuprate_p2p::NetworkInterface;
use cuprate_p2p_core::{
    services::{AddressBookRequest, AddressBookResponse},
    ClearNet, I2p, NetworkZone, Tor,
};
use cuprate_rpc_types::misc::ConnectionInfo;
use cuprate_types::rpc::Peer;

use super::service::address_book;

type Book<Z> = BoxCloneService<AddressBookRequest<Z>, AddressBookResponse<Z>, tower::BoxError>;

#[derive(Clone, Default)]
struct Books {
    clearnet: Option<Book<ClearNet>>,
    tor: Option<Book<Tor>>,
    i2p: Option<Book<I2p>>,
}

/// Does not own the networking tasks; only clones query handles.
#[derive(Clone, Default)]
pub(crate) struct PeerServices(Arc<Mutex<Books>>);

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct PeerCounts {
    pub incoming: u64,
    pub outgoing: u64,
    pub white: u64,
    pub grey: u64,
}

impl PeerServices {
    pub(crate) fn set_clearnet(&self, interface: &NetworkInterface<ClearNet>) {
        self.0.lock().unwrap().clearnet = Some(interface.address_book());
    }

    pub(crate) fn set_tor(&self, interface: &NetworkInterface<Tor>) {
        self.0.lock().unwrap().tor = Some(interface.address_book());
    }

    pub(crate) fn set_i2p(&self, interface: &NetworkInterface<I2p>) {
        self.0.lock().unwrap().i2p = Some(interface.address_book());
    }

    /// Never query or disclose private topology on restricted RPC.
    pub(super) async fn counts(&self, restricted: bool) -> Result<PeerCounts, Error> {
        if restricted {
            return Ok(PeerCounts::default());
        }
        let books = self.0.lock().unwrap().clone();
        let counts = futures::try_join!(
            zone_counts(books.clearnet),
            zone_counts(books.tor),
            zone_counts(books.i2p),
        )?;
        let mut total = PeerCounts::default();
        for counts in [counts.0, counts.1, counts.2] {
            total.incoming += counts.incoming;
            total.outgoing += counts.outgoing;
            total.white += counts.white;
            total.grey += counts.grey;
        }
        Ok(total)
    }

    pub(super) async fn connections(&self) -> Result<Vec<ConnectionInfo>, Error> {
        let books = self.0.lock().unwrap().clone();
        let (mut clear, tor, i2p) = futures::try_join!(
            zone_connections(books.clearnet),
            zone_connections(books.tor),
            zone_connections(books.i2p),
        )?;
        clear.extend(tor);
        clear.extend(i2p);
        // Stable ordering helps operators compare snapshots without depending on
        // hash-map iteration order. Unknown inbound identities remain blank.
        clear.sort_by(|a, b| {
            (a.address_type.to_u8(), &a.address, a.incoming, &a.peer_id).cmp(&(
                b.address_type.to_u8(),
                &b.address,
                b.incoming,
                &b.peer_id,
            ))
        });
        Ok(clear)
    }

    pub(super) async fn peerlist(
        &self,
        public_only: bool,
        include_blocked: bool,
    ) -> Result<(Vec<Peer>, Vec<Peer>), Error> {
        let books = self.0.lock().unwrap().clone();
        let (mut white, mut grey) = zone_peerlist(books.clearnet, include_blocked).await?;
        // Monero compatibility: "public_only" is a zone filter, not an RPC
        // authorization setting. Even public_only=true is admin-only.
        if !public_only {
            let (tor, i2p) = futures::try_join!(
                zone_peerlist(books.tor, include_blocked),
                zone_peerlist(books.i2p, include_blocked),
            )?;
            white.extend(tor.0);
            white.extend(i2p.0);
            grey.extend(tor.1);
            grey.extend(i2p.1);
        }
        white.sort_by(|a, b| (&a.host, a.port).cmp(&(&b.host, b.port)));
        grey.sort_by(|a, b| (&a.host, a.port).cmp(&(&b.host, b.port)));
        Ok((white, grey))
    }
}

async fn zone_counts<Z: NetworkZone>(book: Option<Book<Z>>) -> Result<PeerCounts, Error> {
    let Some(mut book) = book else {
        return Ok(PeerCounts::default());
    };
    let AddressBookResponse::ConnectionCount { incoming, outgoing } = book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::ConnectionCount)
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!()
    };
    let AddressBookResponse::PeerlistSize { white, grey } = book
        .ready()
        .await
        .map_err(|e| anyhow!(e))?
        .call(AddressBookRequest::PeerlistSize)
        .await
        .map_err(|e| anyhow!(e))?
    else {
        unreachable!()
    };
    Ok(PeerCounts {
        incoming: usize_to_u64(incoming),
        outgoing: usize_to_u64(outgoing),
        white: usize_to_u64(white),
        grey: usize_to_u64(grey),
    })
}

async fn zone_connections<Z: NetworkZone>(
    book: Option<Book<Z>>,
) -> Result<Vec<ConnectionInfo>, Error> {
    match book {
        Some(mut book) => address_book::connection_info::<Z>(&mut book).await,
        None => Ok(vec![]),
    }
}

async fn zone_peerlist<Z: NetworkZone>(
    book: Option<Book<Z>>,
    include_blocked: bool,
) -> Result<(Vec<Peer>, Vec<Peer>), Error> {
    match book {
        Some(mut book) => address_book::peerlist::<Z>(&mut book, include_blocked).await,
        None => Ok((vec![], vec![])),
    }
}

#[cfg(test)]
mod tests;
