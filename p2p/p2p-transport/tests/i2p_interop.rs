//! Opt-in, real-i2pd mainnet P2P interoperability probe. No wallet, funds,
//! transaction submission, block download, or local chain database is involved.

#![expect(
    unused_crate_dependencies,
    reason = "this integration target exercises the public transport and wire APIs only"
)]

use std::{env, time::Duration};

use cuprate_p2p_core::Transport;
use cuprate_p2p_transport::{I2pSocks, I2pSocksClientConfig};
use cuprate_wire::{
    admin::{HandshakeRequest, TimedSyncRequest},
    common::PeerSupportFlags,
    AdminRequestMessage, AdminResponseMessage, BasicNodeData, CoreSyncData, I2pAddr, Message,
    NetworkAddress,
};
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;

#[tokio::test]
#[ignore = "requires CUPRATE_I2P_PEER and a real i2pd SOCKS proxy; contacts that mainnet I2P peer"]
async fn real_i2pd_handshake_and_timed_sync() {
    let address: I2pAddr = env::var("CUPRATE_I2P_PEER")
        .expect("set CUPRATE_I2P_PEER to a controlled monerod or Cuprate I2P P2P destination")
        .parse()
        .unwrap();
    let config = I2pSocksClientConfig {
        proxy: env::var("CUPRATE_I2P_SOCKS_PROXY")
            .unwrap_or_else(|_| "127.0.0.1:4447".into())
            .parse()
            .unwrap(),
    };
    let (mut stream, mut sink) = timeout(
        Duration::from_secs(120),
        I2pSocks::connect_to_peer(address, &config),
    )
    .await
    .expect("I2P connection timed out")
    .expect("I2P SOCKS connection failed");

    // Mainnet network ID and genesis payload. Anonymity zones do not block-sync.
    let network_id = [
        0x12, 0x30, 0xf1, 0x71, 0x61, 0x04, 0x41, 0x61, 0x17, 0x31, 0x00, 0x82, 0x16, 0xa1, 0xa1,
        0x10,
    ];
    let payload_data = CoreSyncData {
        cumulative_difficulty: 1,
        cumulative_difficulty_top64: 0,
        current_height: 1,
        pruning_seed: 0,
        top_id: [
            0x41, 0x80, 0x15, 0xbb, 0x9a, 0xe9, 0x82, 0xa1, 0x97, 0x5d, 0xa7, 0xd7, 0x92, 0x77,
            0xc2, 0x70, 0x57, 0x27, 0xa5, 0x68, 0x94, 0xba, 0x0f, 0xb2, 0x46, 0xad, 0xaa, 0xbb,
            0x1f, 0x46, 0x32, 0xe3,
        ],
        top_version: 1,
    };
    sink.send(
        Message::Request(AdminRequestMessage::Handshake(HandshakeRequest {
            node_data: BasicNodeData {
                my_port: 0,
                network_id,
                peer_id: 1,
                support_flags: PeerSupportFlags::FLUFFY_BLOCKS,
                rpc_port: 0,
                rpc_credits_per_hash: 0,
            },
            payload_data: payload_data.clone(),
        }))
        .into(),
    )
    .await
    .unwrap();
    let response = timeout(Duration::from_secs(45), stream.next())
        .await
        .expect("handshake timed out")
        .expect("peer closed before handshake")
        .expect("invalid handshake framing");
    let Message::Response(AdminResponseMessage::Handshake(handshake)) = response else {
        panic!("expected handshake response, got {response:?}");
    };
    assert_eq!(handshake.node_data.network_id, network_id);
    assert!(handshake
        .local_peerlist_new
        .iter()
        .all(|peer| { matches!(peer.adr, NetworkAddress::I2p(address) if address.port() == 1) }));

    sink.send(
        Message::Request(AdminRequestMessage::TimedSync(TimedSyncRequest {
            payload_data,
        }))
        .into(),
    )
    .await
    .unwrap();
    let response = timeout(Duration::from_secs(45), stream.next())
        .await
        .expect("timed sync timed out")
        .expect("peer closed before timed sync")
        .expect("invalid timed-sync framing");
    let Message::Response(AdminResponseMessage::TimedSync(sync)) = response else {
        panic!("expected timed-sync response, got {response:?}");
    };
    assert!(sync
        .local_peerlist_new
        .iter()
        .all(|peer| { matches!(peer.adr, NetworkAddress::I2p(address) if address.port() == 1) }));
    println!(
        "real i2pd P2P handshake and timed sync passed: peer={address}, advertised_peers={}",
        sync.local_peerlist_new.len()
    );
}
