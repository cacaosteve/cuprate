//! Full handler + HTTP routing regression, using isolated databases and offline
//! networking. No peer sockets, live chain, or external services are involved.

use std::sync::{Arc, Mutex};

use cuprate_p2p::config::TransportConfig;
use cuprate_p2p_core::{
    client::{InternalPeerID, PeerInformation},
    handles::{ConnectionGuard, HandleBuilder},
    services::AddressBookRequest,
    transports::Tcp,
    ClearNet, ConnectionDirection, CoreSyncData, NetZoneAddress,
};
use cuprate_pruning::PruningSeed;
use cuprate_wire::{common::PeerSupportFlags, BasicNodeData};
use tokio::sync::{oneshot, RwLock};

use crate::{
    blockchain::{self, BlockchainInterface, BlockchainManagerHandle, BlockchainSyncerHandle},
    config::Config,
    monitor::TaskExecutor,
    p2p::start_zone_p2p,
    rpc::PeerServices,
    txpool::IncomingTxHandler,
    LaunchContext,
};

use super::*;

fn make_peer<A: NetZoneAddress>(
    address: Option<A>,
    direction: ConnectionDirection,
) -> (PeerInformation<A>, ConnectionGuard) {
    let (guard, handle) = HandleBuilder::new().build();
    (
        PeerInformation {
            connected_at: std::time::Instant::now(),
            id: address.map_or(InternalPeerID::Unknown([8; 16]), InternalPeerID::KnownAddr),
            handle,
            direction,
            pruning_seed: PruningSeed::NotPruned,
            basic_node_data: BasicNodeData {
                my_port: 0,
                network_id: [0; 16],
                peer_id: 0x1234,
                support_flags: PeerSupportFlags::FLUFFY_BLOCKS,
                rpc_port: 0,
                rpc_credits_per_hash: 0,
            },
            core_sync_data: Arc::new(Mutex::new(CoreSyncData {
                current_height: 1,
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

async fn json_request(router: &Router, uri: &str, body: serde_json::Value) -> serde_json::Value {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

#[tokio::test]
async fn real_rpc_dispatch_reports_live_peers_and_redacts_the_public_listener() {
    let directory = tempfile::tempdir().unwrap();
    let mut config: Config =
        toml::from_str("offline = true\ntarget_max_memory = 134217728").unwrap();
    config.fs.fast_data_directory = directory.path().join("fast");
    config.fs.slow_data_directory = config.fs.fast_data_directory.clone();
    config.fs.cache_directory = directory.path().join("cache");
    for path in [
        &config.fs.fast_data_directory,
        &config.fs.slow_data_directory,
        &config.fs.cache_directory,
    ] {
        std::fs::create_dir_all(path).unwrap();
    }
    config.storage.reader_threads = 1;
    let config = Arc::new(config);
    let workers = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap(),
    );
    let db = fjall::Database::builder(directory.path().join("db"))
        .open()
        .unwrap();
    let blockchain_config = config.blockchain_config();
    std::fs::create_dir_all(&blockchain_config.blob_dir).unwrap();
    std::fs::create_dir_all(&blockchain_config.index_dir).unwrap();
    let (mut read, mut write, _) = cuprate_blockchain::service::init_with_pool(
        &blockchain_config,
        db.clone(),
        Arc::clone(&workers),
    )
    .unwrap();
    blockchain::check_add_genesis(&mut read, &mut write, config.network)
        .await
        .unwrap();
    let context = blockchain::init_consensus(read.clone(), config.context_config())
        .await
        .unwrap();
    let (pool_read, pool_write) = cuprate_txpool::service::init_with_pool(db, workers).unwrap();
    let (syncer, _synced) = BlockchainSyncerHandle::new();
    let (manager, _commands) = BlockchainManagerHandle::new();
    let launch = LaunchContext {
        config: Arc::clone(&config),
        reorg_lock: Arc::new(RwLock::new(())),
        blockchain: BlockchainInterface::new(read, context, manager, syncer),
        txpool_read: pool_read,
        peers: PeerServices::default(),
        task_executor: TaskExecutor::new(),
    };
    let (clear, _subscriber) = start_zone_p2p::<ClearNet, Tcp>(
        &launch.blockchain,
        launch.txpool_read.clone(),
        config.clearnet_p2p_config(),
        TransportConfig {
            client_config: (),
            server_config: None,
        },
        None,
    )
    .await
    .unwrap();
    launch.peers.set_clearnet(&clear);
    let tx_handler = IncomingTxHandler::init(
        &launch,
        clear.clone(),
        None,
        oneshot::channel().1,
        pool_write,
    )
    .await
    .unwrap();
    let admin_handler = CupratedRpcHandler::new(false, tx_handler.clone(), &launch);
    let public_handler = CupratedRpcHandler::new(true, tx_handler, &launch);
    let admin = init_rpc_router(admin_handler.clone(), 1024 * 1024, Duration::from_secs(5));
    let public = init_rpc_router(public_handler.clone(), 1024 * 1024, Duration::from_secs(5));
    let zero = json_request(&admin, "/get_info", serde_json::json!({})).await;
    assert_eq!(zero["incoming_connections_count"], 0);
    assert_eq!(zero["outgoing_connections_count"], 0);

    let address = "192.0.2.4:18080".parse().unwrap();
    let (peer, clear_guard) = make_peer(Some(address), ConnectionDirection::Outbound);
    clear
        .address_book()
        .oneshot(AddressBookRequest::NewConnection {
            peer,
            public_address: Some(address),
        })
        .await
        .unwrap();

    // Register I2P only after both HTTP routers were created, as happens after sync.
    let (i2p, _i2p_subscriber) =
        start_zone_p2p::<cuprate_p2p_core::I2p, cuprate_p2p_transport::I2pSocks>(
            &launch.blockchain,
            launch.txpool_read.clone(),
            config.i2p_p2p_config(),
            crate::i2p::transport_config(&config),
            None,
        )
        .await
        .unwrap();
    launch.peers.set_i2p(&i2p);
    let (peer, i2p_guard) = make_peer(None, ConnectionDirection::Inbound);
    i2p.address_book()
        .oneshot(AddressBookRequest::NewConnection {
            peer,
            public_address: None,
        })
        .await
        .unwrap();

    for uri in ["/get_info", "/getinfo", "/json_rpc"] {
        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"get_info"});
        let mut info = json_request(&admin, uri, body.clone()).await;
        let mut redacted = json_request(&public, uri, body).await;
        if uri == "/json_rpc" {
            info = info["result"].take();
            redacted = redacted["result"].take();
        }
        assert_eq!(info["incoming_connections_count"], 1);
        assert_eq!(info["outgoing_connections_count"], 1);
        assert_eq!(info["white_peerlist_size"], 1);
        for field in [
            "incoming_connections_count",
            "outgoing_connections_count",
            "white_peerlist_size",
            "grey_peerlist_size",
        ] {
            assert_eq!(redacted[field], 0);
        }
    }
    let body = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"get_connections"});
    let connections = json_request(&admin, "/json_rpc", body.clone()).await;
    let connections = connections["result"]["connections"].as_array().unwrap();
    assert_eq!(connections.len(), 2);
    assert!(connections
        .iter()
        .any(|peer| peer["address"] == "192.0.2.4:18080" && peer["incoming"] == false));
    assert!(connections.iter().any(|peer| peer["address_type"] == 3
        && peer["address"] == ""
        && peer["incoming"] == true));
    let forbidden = json_request(&public, "/json_rpc", body).await;
    assert_eq!(forbidden["error"]["code"], -32601);
    assert!(forbidden.get("result").is_none());
    let peers = json_request(
        &admin,
        "/get_peer_list",
        serde_json::json!({"public_only":false}),
    )
    .await;
    assert_eq!(peers["white_list"].as_array().unwrap().len(), 1);
    assert_eq!(peers["white_list"][0]["host"], "192.0.2.4");

    // Defense in depth: bypassing the HTTP router still cannot read peer details.
    assert!(super::super::super::handlers::json_rpc::map_request(
        public_handler.clone(),
        cuprate_rpc_types::json::JsonRpcRequest::GetConnections(Default::default())
    )
    .await
    .is_err());
    assert!(super::super::super::handlers::other_json::map_request(
        public_handler,
        cuprate_rpc_types::other::OtherRequest::GetPeerList(Default::default())
    )
    .await
    .is_err());

    drop((clear_guard, i2p_guard));
    let info = json_request(&admin, "/get_info", serde_json::json!({})).await;
    assert_eq!(info["incoming_connections_count"], 0);
    assert_eq!(info["outgoing_connections_count"], 0);
    assert_eq!(info["white_peerlist_size"], 1);
    launch.task_executor.trigger_shutdown();
    drop(
        timeout(
            Duration::from_secs(5),
            launch.task_executor.wait_for_shutdown(),
        )
        .await
        .unwrap(),
    );
}
