#![expect(unused_crate_dependencies, reason = "external test module")]

use std::time::Duration;

use tokio::io::{duplex, split};
use tokio_util::codec::{FramedRead, FramedWrite};
use tower::{Service, ServiceExt};

use cuprate_p2p_core::{
    client::{DoHandshakeRequest, HandshakerBuilder, InternalPeerID},
    transports::DummyTransport,
    BroadcastMessage, ClearNet, ConnectionDirection, I2p, NetworkZone, Tor, TransactionCounters,
};
use cuprate_wire::{
    common::PeerSupportFlags, protocol::NewTransactions, BasicNodeData, MoneroWireCodec,
};

async fn exercise_zone<Z: NetworkZone>(sender_address: Z::Addr, receiver_address: Z::Addr) {
    let sending = TransactionCounters::default();
    let receiving = TransactionCounters::default();
    let data = BasicNodeData {
        my_port: 0,
        network_id: cuprate_helper::network::Network::Mainnet.network_id(),
        peer_id: 101,
        support_flags: PeerSupportFlags::FLUFFY_BLOCKS,
        rpc_port: 0,
        rpc_credits_per_hash: 0,
    };
    let mut other = data.clone();
    other.peer_id = 102;
    // Exercise both the diffusion broadcast stream and directed stem sends.
    let broadcast = NewTransactions {
        txs: vec![vec![3; 64].into()],
        dandelionpp_fluff: true,
        padding: Default::default(),
    };
    let sender = HandshakerBuilder::<Z, DummyTransport>::new(data, ())
        .with_transaction_counters(sending.clone())
        .with_broadcast_stream_maker(move |_| {
            futures::stream::iter([BroadcastMessage::NewTransactions(broadcast.clone())])
                .chain(futures::stream::pending())
        })
        .build();
    let receiver = HandshakerBuilder::<Z, DummyTransport>::new(other, ())
        .with_transaction_counters(receiving.clone())
        .build();
    let (left, right) = duplex(50_000);
    let (left_read, left_write) = split(left);
    let (right_read, right_write) = split(right);
    let (sender, receiver) = tokio::join!(
        sender.oneshot(DoHandshakeRequest {
            addr: InternalPeerID::KnownAddr(receiver_address),
            peer_stream: FramedRead::new(left_read, MoneroWireCodec::default()),
            peer_sink: FramedWrite::new(left_write, MoneroWireCodec::default()),
            direction: ConnectionDirection::Outbound,
            permit: None,
        }),
        receiver.oneshot(DoHandshakeRequest {
            addr: InternalPeerID::KnownAddr(sender_address),
            peer_stream: FramedRead::new(right_read, MoneroWireCodec::default()),
            peer_sink: FramedWrite::new(right_write, MoneroWireCodec::default()),
            direction: ConnectionDirection::Inbound,
            permit: None,
        }),
    );
    let mut sender = sender.unwrap();
    let receiver = receiver.unwrap();
    for fluff in [false, true] {
        sender
            .ready_broadcast()
            .await
            .unwrap()
            .call(BroadcastMessage::NewTransactions(NewTransactions {
                txs: vec![vec![1; 64].into(), vec![1; 64].into()],
                dandelionpp_fluff: fluff,
                padding: Default::default(),
            }))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while receiving.snapshot().received != 5 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sending.snapshot().relayed, 5);
    assert_eq!(sending.snapshot().received, 0);
    assert_eq!(receiving.snapshot().relayed, 0);
    assert_eq!(
        receiving.snapshot().accepted,
        0,
        "wire receipt alone is not txpool acceptance"
    );
    assert_eq!(
        sender.info.transaction_counters.snapshot(),
        sending.snapshot()
    );
    sender.info.handle.send_close_signal();
    receiver.info.handle.send_close_signal();
    drop((sender, receiver));
    assert_eq!(sending.snapshot().relayed, 5);
    assert_eq!(receiving.snapshot().received, 5);
}

use futures::StreamExt;

#[tokio::test]
async fn real_wire_io_counts_each_zone_without_counting_handshakes_as_transactions() {
    exercise_zone::<ClearNet>(
        "127.0.0.1:18080".parse().unwrap(),
        "127.0.0.1:18081".parse().unwrap(),
    )
    .await;
    exercise_zone::<I2p>(
        format!("{}.b32.i2p", "a".repeat(52)).parse().unwrap(),
        format!("{}.b32.i2p", "b".repeat(52)).parse().unwrap(),
    )
    .await;
    exercise_zone::<Tor>(
        cuprate_wire::OnionAddr::new(&format!("{}.onion", "a".repeat(56)), 18083).unwrap(),
        cuprate_wire::OnionAddr::new(&format!("{}.onion", "b".repeat(56)), 18083).unwrap(),
    )
    .await;
}
