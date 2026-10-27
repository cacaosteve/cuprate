//! Force the timeout-monitor cancellation race instead of relying on shutdown timing.

use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{channel::oneshot, stream, StreamExt};
use tokio::{
    io::{duplex, split},
    sync::Semaphore,
};
use tokio_util::codec::FramedWrite;

use cuprate_pruning::PruningSeed;
use cuprate_wire::{
    common::PeerSupportFlags, AdminRequestMessage, BasicNodeData, CoreSyncData, MoneroWireCodec,
};

use crate::{
    client::{
        handshaker::builder::{DummyAddressBook, DummyCoreSyncSvc, DummyProtocolRequestHandler},
        InternalPeerID, PeerInformation,
    },
    handles::HandleBuilder,
    ClearNet, I2p, NetworkZone, Tor,
};

use super::*;

#[derive(Clone, Copy)]
enum MonitorEnd {
    Cancelled,
    Finished,
    Failed,
    #[cfg(panic = "unwind")]
    Panicked,
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "connection is moved into the spawned run future; its resource release is asserted below"
)]
async fn exercise<Z: NetworkZone>(waiting_for_response: bool, end: MonitorEnd, close_peer: bool) {
    let monitor = tokio::spawn(async move {
        match end {
            MonitorEnd::Cancelled => pending::<Result<(), tower::BoxError>>().await,
            MonitorEnd::Finished => Ok(()),
            MonitorEnd::Failed => Err("synthetic timeout-monitor failure".into()),
            #[cfg(panic = "unwind")]
            MonitorEnd::Panicked => panic!("synthetic timeout-monitor panic"),
        }
    });
    if matches!(end, MonitorEnd::Cancelled) {
        monitor.abort();
    }
    // Crucially, do not await/consume the handle: the connection must receive the
    // ready JoinError itself, with no peer-close signal to mask the old unwrap.
    timeout(Duration::from_secs(5), async {
        while !monitor.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let (guard, handle) = HandleBuilder::new().build();
    let data = BasicNodeData {
        my_port: 0,
        network_id: crate::Network::Mainnet.network_id(),
        peer_id: 1,
        support_flags: PeerSupportFlags::FLUFFY_BLOCKS,
        rpc_port: 0,
        rpc_credits_per_hash: 0,
    };
    let info = PeerInformation {
        transaction_counters: Default::default(),
        connected_at: std::time::Instant::now(),
        id: InternalPeerID::Unknown([0; 16]),
        handle: handle.clone(),
        direction: crate::ConnectionDirection::Outbound,
        pruning_seed: PruningSeed::NotPruned,
        basic_node_data: data.clone(),
        core_sync_data: Arc::new(Mutex::new(CoreSyncData {
            current_height: 1,
            cumulative_difficulty: 0,
            cumulative_difficulty_top64: 0,
            pruning_seed: 0,
            top_id: [0; 32],
            top_version: 0,
        })),
    };
    let request_handler = PeerRequestHandler::<Z, _, _, _> {
        address_book_svc: DummyAddressBook,
        our_sync_svc: DummyCoreSyncSvc::static_mainnet_genesis(),
        protocol_request_handler: DummyProtocolRequestHandler,
        our_basic_node_data: data,
        peer_info: info,
        on_peer_sync: None,
    };
    let (local, _remote) = duplex(4096);
    let (_read, write) = split(local);
    let (client_tx, client_rx) = mpsc::channel(4);
    let mut connection = Connection::<Z, crate::transports::DummyTransport, _, _, _, _>::new(
        FramedWrite::new(write, MoneroWireCodec::default()),
        client_rx,
        stream::pending::<BroadcastMessage>(),
        request_handler,
        guard,
        monitor,
    );
    let permits = Arc::new(Semaphore::new(3));
    let mut responses = Vec::new();
    if waiting_for_response {
        let (tx, rx) = oneshot::channel();
        responses.push(rx);
        connection.state = State::WaitingForResponse {
            request_id: MessageID::Ping,
            tx,
            _req_permit: Arc::clone(&permits).acquire_owned().await.unwrap(),
        };
        connection.request_timeout = Some(Box::pin(sleep(Duration::from_mins(1))));
    }
    for _ in 0..2 {
        let (tx, rx) = oneshot::channel();
        responses.push(rx);
        client_tx
            .send(ConnectionTaskRequest {
                request: PeerRequest::Admin(AdminRequestMessage::Ping),
                response_channel: tx,
                permit: Some(Arc::clone(&permits).acquire_owned().await.unwrap()),
            })
            .await
            .unwrap();
    }
    if close_peer {
        handle.send_close_signal();
    }
    let task = tokio::spawn(async move {
        connection
            .run(
                stream::pending::<Result<Message, cuprate_wire::BucketError>>().fuse(),
                vec![],
            )
            .await;
    });
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .expect("connection must close without a second panic");
    assert!(handle.is_closed());
    assert!(client_tx.is_closed());
    assert_eq!(
        permits.available_permits(),
        3,
        "release both in-flight and queued permits"
    );
    let expected = if close_peer {
        "The connection was closed."
    } else {
        match end {
            MonitorEnd::Cancelled | MonitorEnd::Finished => "The connection was closed.",
            MonitorEnd::Failed => "synthetic timeout-monitor failure",
            #[cfg(panic = "unwind")]
            MonitorEnd::Panicked => "synthetic timeout-monitor panic",
        }
    };
    for response in responses {
        let error = response
            .await
            .expect("send an explicit shutdown error, not a dropped channel")
            .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected close reason: {error}"
        );
    }
}

async fn all_zones(waiting: bool, end: MonitorEnd, close_peer: bool) {
    exercise::<ClearNet>(waiting, end, close_peer).await;
    exercise::<I2p>(waiting, end, close_peer).await;
    exercise::<Tor>(waiting, end, close_peer).await;
}

#[tokio::test]
async fn cancelled_monitor_closes_idle_peer_and_drains_requests() {
    all_zones(false, MonitorEnd::Cancelled, false).await;
}

#[tokio::test]
async fn cancelled_monitor_closes_busy_peer_and_drains_requests() {
    all_zones(true, MonitorEnd::Cancelled, false).await;
}

#[tokio::test]
async fn simultaneous_peer_close_and_monitor_cancellation_is_clean() {
    for waiting in [false, true] {
        all_zones(waiting, MonitorEnd::Cancelled, true).await;
    }
}

#[tokio::test]
async fn normal_monitor_exit_still_closes_the_connection() {
    for waiting in [false, true] {
        all_zones(waiting, MonitorEnd::Finished, false).await;
    }
}

#[tokio::test]
async fn real_monitor_errors_are_preserved_not_treated_as_cancellation() {
    for waiting in [false, true] {
        all_zones(waiting, MonitorEnd::Failed, false).await;
    }
}

#[cfg(panic = "unwind")]
#[tokio::test]
async fn monitor_panics_are_reported_without_a_secondary_connection_panic() {
    for waiting in [false, true] {
        all_zones(waiting, MonitorEnd::Panicked, false).await;
    }
}
