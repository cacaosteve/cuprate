use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::StreamExt;
use tokio::time::interval;

use cuprate_p2p_core::{
    client::PeerInformation,
    handles::{ConnectionHandle, HandleBuilder},
    ConnectionDirection, CoreSyncData, NetZoneAddress, NetworkZone,
};
use cuprate_pruning::PruningSeed;
use cuprate_wire::{common::PeerSupportFlags, BasicNodeData};

use super::{AddressBook, ConnectionPeerEntry, InternalPeerID};
use crate::{peer_list::tests::make_fake_peer_list, AddressBookConfig, AddressBookError};

use cuprate_test_utils::test_netzone::{TestNetZone, TestNetZoneAddr};

type I2pAddress = <cuprate_p2p_core::I2p as NetworkZone>::Addr;

fn peer_info<A: NetZoneAddress>(
    id: InternalPeerID<A>,
    handle: ConnectionHandle,
    pruning_seed: PruningSeed,
) -> PeerInformation<A> {
    PeerInformation {
        connected_at: Instant::now().checked_sub(Duration::from_secs(20)).unwrap(),
        id,
        handle,
        pruning_seed,
        direction: ConnectionDirection::Outbound,
        basic_node_data: BasicNodeData {
            my_port: 0,
            network_id: [0; 16],
            peer_id: 0x1234,
            support_flags: PeerSupportFlags::FLUFFY_BLOCKS,
            rpc_port: 18081,
            rpc_credits_per_hash: 5,
        },
        core_sync_data: Arc::new(Mutex::new(CoreSyncData {
            current_height: 42,
            cumulative_difficulty: 0,
            cumulative_difficulty_top64: 0,
            pruning_seed: 0,
            top_id: [0; 32],
            top_version: 0,
        })),
    }
}

#[tokio::test]
async fn rpc_counts_use_direction_and_drop_closed_connections_between_ready_and_call() {
    use cuprate_p2p_core::{
        services::{AddressBookRequest as Req, AddressBookResponse as Resp},
        ClearNet,
    };
    use tower::{Service, ServiceExt};
    let mut book = AddressBook::<ClearNet>::new(test_cfg(), vec![], vec![], vec![]);
    let (in_guard, in_handle) = HandleBuilder::new().build();
    let (out_guard, out_handle) = HandleBuilder::new().build();
    let mut incoming = peer_info(
        InternalPeerID::KnownAddr("192.0.2.1:50000".parse().unwrap()),
        in_handle,
        PruningSeed::NotPruned,
    );
    incoming.direction = ConnectionDirection::Inbound;
    let outgoing = peer_info(
        InternalPeerID::KnownAddr("[2001:db8::1]:18080".parse().unwrap()),
        out_handle,
        PruningSeed::NotPruned,
    );
    for peer in [incoming, outgoing] {
        assert!(matches!(
            book.call(Req::NewConnection {
                peer,
                public_address: None
            })
            .await
            .unwrap(),
            Resp::Ok
        ));
    }
    assert!(matches!(
        book.call(Req::ConnectionCount).await.unwrap(),
        Resp::ConnectionCount {
            incoming: 1,
            outgoing: 1
        }
    ));
    let Resp::ConnectionInfo(peers) = book.call(Req::ConnectionInfo).await.unwrap() else {
        panic!()
    };
    assert_eq!(peers.len(), 2);
    assert_eq!(peers.iter().filter(|peer| peer.incoming).count(), 1);
    assert!(peers.iter().all(|peer| peer.live_time >= 20));
    let ready = book.ready().await.unwrap();
    drop(in_guard);
    assert!(matches!(
        ready.call(Req::ConnectionCount).await.unwrap(),
        Resp::ConnectionCount {
            incoming: 0,
            outgoing: 1
        }
    ));
    drop(out_guard);
    let Resp::ConnectionInfo(peers) = book.call(Req::ConnectionInfo).await.unwrap() else {
        panic!()
    };
    assert!(peers.is_empty());
}

#[tokio::test]
async fn rpc_unknown_i2p_inbound_is_counted_without_fabricated_address_or_stale_height() {
    use cuprate_p2p_core::{
        services::{AddressBookRequest as Req, AddressBookResponse as Resp},
        I2p,
    };
    use tower::Service;
    let mut book = AddressBook::<I2p>::new(test_cfg(), vec![], vec![], vec![]);
    let (_guard, handle) = HandleBuilder::new().build();
    let mut peer = peer_info(
        InternalPeerID::Unknown([3; 16]),
        handle,
        PruningSeed::NotPruned,
    );
    peer.direction = ConnectionDirection::Inbound;
    let sync = Arc::clone(&peer.core_sync_data);
    book.call(Req::NewConnection {
        peer,
        public_address: None,
    })
    .await
    .unwrap();
    sync.lock().unwrap().current_height = 100;
    assert!(matches!(
        book.call(Req::ConnectionCount).await.unwrap(),
        Resp::ConnectionCount {
            incoming: 1,
            outgoing: 0
        }
    ));
    let Resp::ConnectionInfo(peers) = book.call(Req::ConnectionInfo).await.unwrap() else {
        panic!()
    };
    assert_eq!(peers.len(), 1);
    let info = &peers[0];
    assert!(info.address.is_none());
    assert!(info.host.is_empty());
    assert!(info.socket_addr.is_none());
    assert_eq!(info.address_type.to_u8(), 3);
    assert_eq!(info.height, 100);
    assert_eq!(info.peer_id, 0x1234);
    assert_eq!(info.support_flags, PeerSupportFlags::FLUFFY_BLOCKS.bits());
    assert!(matches!(
        book.call(Req::PeerlistSize).await.unwrap(),
        Resp::PeerlistSize { white: 0, grey: 0 }
    ));
}

#[tokio::test]
async fn rpc_peerlists_return_saved_not_just_connected_peers_and_honor_block_filter() {
    use cuprate_p2p_core::{
        services::{AddressBookRequest as Req, AddressBookResponse as Resp},
        I2p,
    };
    use tower::Service;
    let (white, grey) = i2p_addresses();
    let mut book = AddressBook::<I2p>::new(
        test_cfg(),
        vec![i2p_entry(white)],
        vec![i2p_entry(grey)],
        vec![],
    );
    assert!(matches!(
        book.call(Req::PeerlistSize).await.unwrap(),
        Resp::PeerlistSize { white: 1, grey: 1 }
    ));
    assert!(matches!(
        book.call(Req::ConnectionCount).await.unwrap(),
        Resp::ConnectionCount {
            incoming: 0,
            outgoing: 0
        }
    ));
    // Simulate a retained blocked entry (normal banning purges entries).
    book.banned_peers.insert(
        white.ban_id(),
        tokio::time::Instant::now() + Duration::from_secs(60),
    );
    let Resp::Peerlist(list) = book
        .call(Req::Peerlist {
            include_blocked: false,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(list.white.is_empty());
    assert_eq!(list.grey[0].adr, grey);
    let Resp::Peerlist(list) = book
        .call(Req::Peerlist {
            include_blocked: true,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(list.white[0].adr, white);
}

fn i2p_addresses() -> (I2pAddress, I2pAddress) {
    (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:18084"
            .parse()
            .unwrap(),
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.b32.i2p:1"
            .parse()
            .unwrap(),
    )
}

fn i2p_entry(
    adr: I2pAddress,
) -> cuprate_p2p_core::services::ZoneSpecificPeerListEntryBase<I2pAddress> {
    cuprate_p2p_core::services::ZoneSpecificPeerListEntryBase {
        adr,
        id: 1,
        last_seen: 0,
        pruning_seed: PruningSeed::NotPruned,
        rpc_port: 0,
        rpc_credits_per_hash: 0,
    }
}

#[tokio::test]
async fn i2p_gossip_rejects_own_destination_even_with_different_port() {
    let (own, other) = i2p_addresses();
    let alias = I2pAddress::new(&own.addr_string(), 1).unwrap();
    let mut cfg = test_cfg::<cuprate_p2p_core::I2p>();
    cfg.our_own_address = Some(own);
    let mut book = AddressBook::new(cfg, vec![], vec![], vec![]);
    book.handle_incoming_peer_list(vec![i2p_entry(own), i2p_entry(alias), i2p_entry(other)]);
    assert_eq!(book.gray_list.len(), 1);
    assert_eq!(book.take_random_gray_peer(None).unwrap().adr, other);
    assert!(book.take_random_gray_peer(None).is_none());
    const {
        assert!(
            !cuprate_p2p_core::I2p::CHECK_NODE_ID,
            "do not break anonymous peers by comparing their shared peer ID"
        );
    }
}

#[tokio::test]
async fn i2p_saved_white_gray_and_anchor_lists_discard_self_peers() {
    let (own, other) = i2p_addresses();
    let alias = I2pAddress::new(&own.addr_string(), 1).unwrap();
    let mut cfg = test_cfg::<cuprate_p2p_core::I2p>();
    cfg.our_own_address = Some(own);
    let book = AddressBook::new(
        cfg,
        vec![i2p_entry(own), i2p_entry(alias), i2p_entry(other)],
        vec![i2p_entry(alias), i2p_entry(other)],
        vec![own, alias, other],
    );
    assert_eq!(book.white_list.len(), 1);
    assert_eq!(book.gray_list.len(), 1);
    assert_eq!(book.anchor_list.len(), 1);
    assert!(book.anchor_list.contains(&other));
    assert!(!book.white_list.contains_peer(&own));
    assert!(!book.white_list.contains_peer(&alias));
}

#[tokio::test]
async fn i2p_self_connection_cannot_enter_connected_peer_set() {
    let (own, _) = i2p_addresses();
    let alias = I2pAddress::new(&own.addr_string(), 1).unwrap();
    let mut cfg = test_cfg::<cuprate_p2p_core::I2p>();
    cfg.our_own_address = Some(own);
    let mut book = AddressBook::new(cfg, vec![], vec![], vec![]);
    for (id, address) in [
        (InternalPeerID::KnownAddr(alias), None),
        (InternalPeerID::Unknown([7; 16]), Some(alias)),
    ] {
        let (_guard, handle) = HandleBuilder::new().build();
        assert_eq!(
            book.handle_new_connection(
                id,
                ConnectionPeerEntry {
                    addr: address,
                    info: peer_info(id, handle, PruningSeed::NotPruned),
                }
            ),
            Err(AddressBookError::OwnAddress)
        );
        assert!(book.connected_peers.is_empty());
        assert!(book.connected_peers_ban_id.is_empty());
    }
}

fn test_cfg<Z: NetworkZone>() -> AddressBookConfig<Z> {
    AddressBookConfig {
        max_white_list_length: 100,
        max_gray_list_length: 500,
        peer_store_directory: PathBuf::new(),
        peer_save_period: Duration::from_secs(60),
        our_own_address: None,
    }
}

fn make_fake_address_book(numb_white: u32, numb_gray: u32) -> AddressBook<TestNetZone<true>> {
    let white_list = make_fake_peer_list(0, numb_white);
    let gray_list = make_fake_peer_list(numb_white, numb_gray);

    AddressBook {
        white_list,
        gray_list,
        anchor_list: Default::default(),
        connected_peers: Default::default(),
        connected_peers_ban_id: Default::default(),
        banned_peers: Default::default(),
        banned_peers_queue: Default::default(),
        peer_save_task_handle: None,
        peer_save_interval: interval(Duration::from_secs(60)),
        cfg: test_cfg(),
    }
}

#[tokio::test]
async fn take_random_peers() {
    let mut address_book = make_fake_address_book(50, 250);
    let peer = address_book.take_random_white_peer(None).unwrap();
    assert!(!address_book.white_list.contains_peer(&peer.adr));
    assert!(!address_book.gray_list.contains_peer(&peer.adr));

    let peer = address_book.take_random_gray_peer(None).unwrap();
    assert!(!address_book.white_list.contains_peer(&peer.adr));
    assert!(!address_book.gray_list.contains_peer(&peer.adr));
}

#[tokio::test]
async fn get_white_peers() {
    let address_book = make_fake_address_book(100, 0);
    let peers = address_book.get_white_peers(50);
    assert_eq!(peers.len(), 50);
    let peers = address_book.get_white_peers(60);
    assert_eq!(peers.len(), 60);
    for window in peers.windows(2) {
        assert_ne!(window[0], window[1]);
    }

    let address_book = make_fake_address_book(45, 0);
    let peers = address_book.get_white_peers(50);
    assert_eq!(peers.len(), 45);
    let peers = address_book.get_white_peers(60);
    assert_eq!(peers.len(), 45);
    for window in peers.windows(2) {
        assert_ne!(window[0], window[1]);
    }
}

#[tokio::test]
async fn add_new_peer_already_connected() {
    let mut address_book = make_fake_address_book(0, 0);

    let (_, handle) = HandleBuilder::default().build();

    address_book
        .handle_new_connection(
            InternalPeerID::KnownAddr(TestNetZoneAddr(1)),
            ConnectionPeerEntry {
                addr: None,
                info: peer_info(
                    InternalPeerID::KnownAddr(TestNetZoneAddr(1)),
                    handle,
                    PruningSeed::decompress(385).unwrap(),
                ),
            },
        )
        .unwrap();

    let (_, handle) = HandleBuilder::default().build();

    assert_eq!(
        address_book.handle_new_connection(
            InternalPeerID::KnownAddr(TestNetZoneAddr(1)),
            ConnectionPeerEntry {
                addr: None,
                info: peer_info(
                    InternalPeerID::KnownAddr(TestNetZoneAddr(1)),
                    handle,
                    PruningSeed::decompress(385).unwrap()
                ),
            },
        ),
        Err(AddressBookError::PeerAlreadyConnected)
    );
}

#[tokio::test]
async fn banned_peer_removed_from_peer_lists() {
    let mut address_book = make_fake_address_book(100, 0);

    assert_eq!(address_book.banned_peers.len(), 0);
    assert_eq!(address_book.white_list.len(), 100);

    address_book.ban_peer(TestNetZoneAddr(1), Duration::from_secs(1));
    assert_eq!(address_book.banned_peers.len(), 1);
    assert_eq!(address_book.white_list.len(), 99);

    address_book.ban_peer(TestNetZoneAddr(1), Duration::from_secs(1));
    assert_eq!(address_book.banned_peers.len(), 1);
    assert_eq!(address_book.white_list.len(), 99);

    address_book.ban_peer(TestNetZoneAddr(1), Duration::from_secs(1));
    assert_eq!(address_book.banned_peers.len(), 1);
    assert_eq!(address_book.white_list.len(), 99);

    address_book.ban_peer(TestNetZoneAddr(5), Duration::from_secs(100));
    assert_eq!(address_book.banned_peers.len(), 2);
    assert_eq!(address_book.white_list.len(), 98);

    assert_eq!(
        address_book
            .banned_peers_queue
            .next()
            .await
            .unwrap()
            .into_inner(),
        TestNetZoneAddr(1)
    );
}
