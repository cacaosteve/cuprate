use std::{path::PathBuf, time::Duration};

use futures::StreamExt;
use tokio::time::interval;

use cuprate_p2p_core::{handles::HandleBuilder, NetworkZone};
use cuprate_pruning::PruningSeed;

use super::{AddressBook, ConnectionPeerEntry, InternalPeerID};
use crate::{peer_list::tests::make_fake_peer_list, AddressBookConfig, AddressBookError};

use cuprate_test_utils::test_netzone::{TestNetZone, TestNetZoneAddr};

type I2pAddress = <cuprate_p2p_core::I2p as NetworkZone>::Addr;

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
                    id: 1,
                    handle,
                    pruning_seed: PruningSeed::NotPruned,
                    rpc_port: 0,
                    rpc_credits_per_hash: 0,
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
                id: 0,
                handle,
                pruning_seed: PruningSeed::decompress(385).unwrap(),
                rpc_port: 0,
                rpc_credits_per_hash: 0,
            },
        )
        .unwrap();

    let (_, handle) = HandleBuilder::default().build();

    assert_eq!(
        address_book.handle_new_connection(
            InternalPeerID::KnownAddr(TestNetZoneAddr(1)),
            ConnectionPeerEntry {
                addr: None,
                id: 0,
                handle,
                pruning_seed: PruningSeed::decompress(385).unwrap(),
                rpc_port: 0,
                rpc_credits_per_hash: 0,
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
