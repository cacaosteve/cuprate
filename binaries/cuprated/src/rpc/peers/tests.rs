use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cuprate_address_book::{init_address_book, AddressBookConfig};
use cuprate_p2p_core::{
    client::{InternalPeerID, PeerInformation},
    handles::{ConnectionGuard, HandleBuilder},
    services::{AddressBookRequest as Req, AddressBookResponse as Resp},
    ConnectionDirection, CoreSyncData, NetZoneAddress,
};
use cuprate_pruning::PruningSeed;
use cuprate_wire::{common::PeerSupportFlags, BasicNodeData};
use tower::{buffer::Buffer, ServiceExt};

use super::*;

async fn book<Z: NetworkZone>() -> (Book<Z>, tempfile::TempDir)
where
    Z::Addr: borsh::BorshSerialize + borsh::BorshDeserialize,
{
    let directory = tempfile::tempdir().unwrap();
    let book = init_address_book(AddressBookConfig::<Z> {
        max_white_list_length: 100,
        max_gray_list_length: 100,
        peer_store_directory: directory.path().to_path_buf(),
        peer_save_period: Duration::from_secs(60),
        our_own_address: None,
    })
    .await
    .unwrap();
    (Buffer::new(book, 8).boxed_clone(), directory)
}

fn peer<A: NetZoneAddress>(
    id: InternalPeerID<A>,
    direction: ConnectionDirection,
) -> (PeerInformation<A>, ConnectionGuard) {
    let (guard, handle) = HandleBuilder::new().build();
    (
        PeerInformation {
            connected_at: Instant::now().checked_sub(Duration::from_secs(12)).unwrap(),
            id,
            handle,
            direction,
            pruning_seed: PruningSeed::NotPruned,
            basic_node_data: BasicNodeData {
                network_id: [0; 16],
                my_port: 0,
                peer_id: 0x0102030405060708,
                support_flags: PeerSupportFlags::FLUFFY_BLOCKS,
                rpc_port: 18081,
                rpc_credits_per_hash: 9,
            },
            core_sync_data: Arc::new(Mutex::new(CoreSyncData {
                current_height: 99,
                cumulative_difficulty: 0,
                cumulative_difficulty_top64: 0,
                pruning_seed: 0,
                top_id: [0; 32],
                top_version: 0,
            })),
        },
        guard,
    )
}

async fn connect<Z: NetworkZone>(
    book: &Book<Z>,
    address: Option<Z::Addr>,
    direction: ConnectionDirection,
) -> ConnectionGuard {
    let id = address.map_or(InternalPeerID::Unknown([7; 16]), InternalPeerID::KnownAddr);
    let (peer, guard) = peer(id, direction);
    let response = book
        .clone()
        .oneshot(Req::NewConnection {
            peer,
            public_address: address,
        })
        .await
        .unwrap();
    assert!(matches!(response, Resp::Ok));
    guard
}

#[tokio::test]
async fn counts_and_connections_follow_late_zones_and_disconnects() {
    let services = PeerServices::default();
    let rpc_clone = services.clone();
    assert_eq!(
        rpc_clone.counts(false).await.unwrap(),
        PeerCounts::default()
    );
    assert!(rpc_clone.connections().await.unwrap().is_empty());

    let (clear, _clear_dir) = book::<ClearNet>().await;
    let clear_in = connect(
        &clear,
        Some("192.0.2.3:55000".parse().unwrap()),
        ConnectionDirection::Inbound,
    )
    .await;
    let clear_out = connect(
        &clear,
        Some("[2001:db8::3]:18080".parse().unwrap()),
        ConnectionDirection::Outbound,
    )
    .await;
    services.0.lock().unwrap().clearnet = Some(clear);
    assert_eq!(
        rpc_clone.counts(false).await.unwrap(),
        PeerCounts {
            incoming: 1,
            outgoing: 1,
            white: 2,
            grey: 0
        }
    );

    let (i2p, _i2p_dir) = book::<I2p>().await;
    let i2p_out = connect(
        &i2p,
        Some(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p"
                .parse()
                .unwrap(),
        ),
        ConnectionDirection::Outbound,
    )
    .await;
    let i2p_in = connect(&i2p, None, ConnectionDirection::Inbound).await;
    services.0.lock().unwrap().i2p = Some(i2p);

    let (tor, _tor_dir) = book::<Tor>().await;
    let tor_out = connect(
        &tor,
        Some(cuprate_wire::OnionAddr::new(&format!("{}.onion", "b".repeat(56)), 18083).unwrap()),
        ConnectionDirection::Outbound,
    )
    .await;
    services.0.lock().unwrap().tor = Some(tor);
    assert_eq!(
        rpc_clone.counts(false).await.unwrap(),
        PeerCounts {
            incoming: 2,
            outgoing: 3,
            white: 4,
            grey: 0
        }
    );
    let connections = rpc_clone.connections().await.unwrap();
    assert_eq!(connections.len(), 5);
    assert!(connections
        .iter()
        .all(|peer| peer.peer_id == "0102030405060708"
            && peer.height == 99
            && peer.live_time >= 12));
    assert_eq!(connections.iter().filter(|peer| peer.incoming).count(), 2);
    let unknown = connections
        .iter()
        .find(|peer| peer.address_type.to_u8() == 3 && peer.incoming)
        .unwrap();
    assert!(
        unknown.address.is_empty()
            && unknown.host.is_empty()
            && unknown.ip.is_empty()
            && unknown.port.is_empty()
    );
    let i2p_known = connections
        .iter()
        .find(|peer| peer.address_type.to_u8() == 3 && !peer.incoming)
        .unwrap();
    assert_eq!(
        i2p_known.host,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p"
    );
    assert_eq!(i2p_known.port, "1");
    assert!(i2p_known.ip.is_empty());
    let ipv6 = connections
        .iter()
        .find(|peer| peer.address_type.to_u8() == 2)
        .unwrap();
    assert_eq!(ipv6.ip, "2001:db8::3");
    assert_eq!(ipv6.port, "18080");

    drop((clear_in, clear_out, i2p_in, i2p_out, tor_out));
    assert_eq!(
        rpc_clone.counts(false).await.unwrap(),
        PeerCounts {
            incoming: 0,
            outgoing: 0,
            white: 4,
            grey: 0
        }
    );
    assert!(rpc_clone.connections().await.unwrap().is_empty());
}

#[tokio::test]
async fn peerlist_public_only_filters_zones_but_does_not_mean_public_rpc() {
    let services = PeerServices::default();
    let (clear, _clear_dir) = book::<ClearNet>().await;
    let clear_guard = connect(
        &clear,
        Some("192.0.2.3:18080".parse().unwrap()),
        ConnectionDirection::Outbound,
    )
    .await;
    let (i2p, _i2p_dir) = book::<I2p>().await;
    let i2p_guard = connect(
        &i2p,
        Some(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p"
                .parse()
                .unwrap(),
        ),
        ConnectionDirection::Outbound,
    )
    .await;
    let (tor, _tor_dir) = book::<Tor>().await;
    let tor_guard = connect(
        &tor,
        Some(cuprate_wire::OnionAddr::new(&format!("{}.onion", "b".repeat(56)), 18083).unwrap()),
        ConnectionDirection::Outbound,
    )
    .await;
    {
        let mut books = services.0.lock().unwrap();
        books.clearnet = Some(clear);
        books.i2p = Some(i2p);
        books.tor = Some(tor);
    }
    let (public, _) = services.peerlist(true, false).await.unwrap();
    assert_eq!(public.len(), 1);
    assert_eq!(public[0].host, "192.0.2.3");
    assert_eq!(public[0].port, 18080);
    let (all, _) = services.peerlist(false, false).await.unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(
        all.iter()
            .find(|peer| peer.host.ends_with(".b32.i2p"))
            .unwrap()
            .port,
        1
    );
    assert_eq!(
        all.iter()
            .find(|peer| peer.host == format!("{}.onion", "b".repeat(56)))
            .unwrap()
            .port,
        18083
    );
    drop((clear_guard, i2p_guard, tor_guard));
}

#[tokio::test]
async fn restricted_counts_never_query_private_books_and_admin_errors_are_not_fake_zeros() {
    let services = PeerServices::default();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts = Arc::clone(&calls);
    let failing = tower::service_fn(move |_: Req<ClearNet>| {
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async { Err::<Resp<ClearNet>, tower::BoxError>("address book unavailable".into()) }
    });
    services.0.lock().unwrap().clearnet = Some(failing.boxed_clone());
    assert_eq!(services.counts(true).await.unwrap(), PeerCounts::default());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(services.counts(false).await.is_err());
    assert!(services.connections().await.is_err());
    assert!(services.peerlist(true, false).await.is_err());
}
