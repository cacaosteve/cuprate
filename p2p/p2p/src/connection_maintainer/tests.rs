use super::*;
use cuprate_p2p_core::I2p;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tower::service_fn;

#[tokio::test]
async fn self_seeds_and_direct_candidates_never_reach_connector() {
    let own = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:18084"
        .parse()
        .unwrap();
    let alias = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:1"
        .parse()
        .unwrap();
    let other = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.b32.i2p:1"
        .parse()
        .unwrap();
    let config = P2PConfig::<I2p> {
        network: cuprate_helper::network::Network::Mainnet,
        seeds: vec![own, alias, other],
        offline: false,
        outbound_connections: 1,
        extra_outbound_connections: 0,
        max_inbound_connections: 1,
        gray_peers_percent: 0.3,
        p2p_port: 18084,
        rpc_port: 0,
        address_book_config: crate::AddressBookConfig {
            max_white_list_length: 100,
            max_gray_list_length: 100,
            peer_store_directory: std::path::PathBuf::new(),
            peer_save_period: Duration::from_mins(1),
            our_own_address: Some(own),
        },
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let connector = service_fn(move |_: ConnectRequest<I2p>| {
        count.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<Result<Client<I2p>, HandshakeError>>()
    });
    let address_book = service_fn(|_: AddressBookRequest<I2p>| {
        std::future::ready(Ok::<_, tower::BoxError>(AddressBookResponse::Ok))
    });
    let (new_peers, _new_rx) = mpsc::channel(1);
    let (_requests, request_rx) = mpsc::channel(1);
    let mut keeper =
        OutboundConnectionKeeper::new(config, new_peers, request_rx, address_book, connector, None);
    assert_eq!(keeper.config.seeds, vec![other]);
    for address in [own, alias] {
        keeper
            .connect_to_outbound_peer(
                Arc::clone(&keeper.outbound_semaphore)
                    .acquire_owned()
                    .await
                    .unwrap(),
                address,
            )
            .await;
        assert_eq!(keeper.outbound_semaphore.available_permits(), 1);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // Filtering self-only seeds leaves an empty list. Wait for discovery, do not panic.
    keeper.config.seeds.clear();
    assert!(matches!(
        keeper.connect_to_random_seeds().await,
        Err(OutboundConnectorError::NoAvailablePeers)
    ));
    drop(keeper);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
