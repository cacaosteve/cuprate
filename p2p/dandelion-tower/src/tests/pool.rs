use std::time::Duration;

use tokio::time::timeout;

use super::*;
use crate::{
    pool::{
        start_dandelion_pool_manager, start_dandelion_pool_manager_with_local_tx_embargo_policy,
        IncomingTx, LocalTxEmbargoPolicy,
    },
    traits::{DiffuseRequest, StemRequest},
    DandelionConfig, DandelionRouter, Graph, TxState,
};

#[tokio::test]
async fn basic_functionality() {
    let config = DandelionConfig {
        time_between_hop: Duration::from_millis(175),
        epoch_duration: Duration::from_secs(0), // make every poll ready change state
        fluff_probability: 0.2,
        graph: Graph::FourRegular,
    };

    let (broadcast_svc, mut broadcast_rx) = mock_broadcast_svc();
    let (outbound_peer_svc, _outbound_rx) = mock_discover_svc();

    let router = DandelionRouter::new(broadcast_svc, outbound_peer_svc, config);

    let (pool_svc, pool) = mock_in_memory_backing_pool();
    pool.lock().unwrap().insert(1, (0, State::Stem));

    let (mut pool_svc, pool_task) = start_dandelion_pool_manager(15, router, pool_svc, config);

    pool_svc
        .ready()
        .await
        .unwrap()
        .call(IncomingTx {
            tx: 0_usize,
            tx_id: 1_usize,
            routing_state: TxState::Fluff,
            local_embargo_policy: None,
        })
        .await
        .unwrap();

    // TODO: the DandelionPoolManager doesn't handle adding txs to the pool, add more tests here to test
    // all functionality.
    //assert!(pool.lock().unwrap().contains_key(&1));
    assert!(broadcast_rx.try_recv().is_ok());

    drop(pool_svc);
    pool_task.await.unwrap();
}

#[tokio::test]
async fn retry_local_policy_keeps_local_tx_private_after_embargo() {
    let config = DandelionConfig {
        time_between_hop: Duration::from_millis(1),
        epoch_duration: Duration::from_secs(30),
        fluff_probability: 0.2,
        graph: Graph::FourRegular,
    };

    let (broadcast_svc, _broadcast_rx) = mock_broadcast_svc::<DiffuseRequest<usize>>();
    let (outbound_peer_svc, mut outbound_rx) = mock_discover_svc::<StemRequest<usize>>();
    let router = DandelionRouter::new(broadcast_svc, outbound_peer_svc, config);
    let (backing_pool, txs) = mock_in_memory_backing_pool::<usize, usize>();
    txs.lock().unwrap().insert(1, (42, State::Stem));

    let (mut pool_svc, pool_task) = start_dandelion_pool_manager_with_local_tx_embargo_policy(
        15,
        router,
        backing_pool,
        config,
        LocalTxEmbargoPolicy::RetryLocal,
    );

    pool_svc
        .ready()
        .await
        .unwrap()
        .call(IncomingTx {
            tx: 42,
            tx_id: 1,
            routing_state: TxState::Local,
            local_embargo_policy: None,
        })
        .await
        .unwrap();

    timeout(Duration::from_secs(3), outbound_rx.recv())
        .await
        .expect("initial local stem send")
        .expect("stem request");
    timeout(Duration::from_secs(3), outbound_rx.recv())
        .await
        .expect("local tx should be retried after the embargo")
        .expect("stem retry request");

    assert_eq!(txs.lock().unwrap().get(&1), Some(&(42, State::Stem)));

    drop(pool_svc);
    pool_task.await.unwrap();
}

fn test_config() -> DandelionConfig {
    DandelionConfig {
        time_between_hop: Duration::from_millis(1),
        epoch_duration: Duration::from_secs(30),
        fluff_probability: 0.2,
        graph: Graph::FourRegular,
    }
}

#[tokio::test(start_paused = true)]
async fn clearnet_fallback_fluffs_at_embargo_even_after_stale_local_resubmission() {
    use crate::{traits::StoredTx, RouteOutcome};
    use std::sync::atomic::{AtomicBool, Ordering};
    let fallback = Arc::new(AtomicBool::new(false));
    let stored_fallback = Arc::clone(&fallback);
    let txs = Arc::new(std::sync::Mutex::new(HashMap::from([(
        1,
        (42, State::Stem),
    )])));
    let stored_txs = Arc::clone(&txs);
    let (recorded, mut recorded_rx) = mpsc::unbounded_channel();
    let backing = service_fn(move |req: TxStoreRequest<usize>| {
        let response = match req {
            TxStoreRequest::Get(id) => {
                TxStoreResponse::Transaction(stored_txs.lock().unwrap().get(&id).copied().map(
                    |(tx, state)| StoredTx {
                        tx,
                        state,
                        local_embargo_policy: Some(if stored_fallback.load(Ordering::SeqCst) {
                            LocalTxEmbargoPolicy::Fluff
                        } else {
                            LocalTxEmbargoPolicy::RetryLocal
                        }),
                    },
                ))
            }
            TxStoreRequest::RecordClearnetFallback(id) => {
                assert_eq!(id, 1);
                stored_fallback.store(true, Ordering::SeqCst);
                recorded.send(()).unwrap();
                TxStoreResponse::Ok
            }
            TxStoreRequest::Promote(id) => {
                assert!(
                    stored_fallback.load(Ordering::SeqCst),
                    "persist fallback before public promotion"
                );
                stored_txs.lock().unwrap().get_mut(&id).unwrap().1 = State::Fluff;
                TxStoreResponse::Ok
            }
        };
        std::future::ready(Ok::<_, tower::BoxError>(response))
    });
    let (broadcast, mut broadcasts) = mock_broadcast_svc::<DiffuseRequest<usize>>();
    let (peers, mut stems) = mock_discover_svc::<StemRequest<usize>>();
    let clear = Arc::new(tokio::sync::Mutex::new(DandelionRouter::new(
        broadcast,
        peers,
        test_config(),
    )));
    // Exactly the unavailable-I2P fallback path: a live clearnet peer accepts
    // the write but never forwards it. The actual router supplies the stem/fluff state.
    let router = service_fn(move |req: crate::DandelionRouteReq<usize, usize>| {
        let clear = Arc::clone(&clear);
        let is_fallback = !fallback.load(Ordering::SeqCst);
        async move {
            let future = clear.lock().await.ready().await?.call(req);
            future.await.map(|state| {
                if is_fallback {
                    RouteOutcome::LocalClearnetFallback(state)
                } else {
                    state.into()
                }
            })
        }
    });
    let (mut svc, task) = start_dandelion_pool_manager(8, router, backing, test_config());
    let incoming = || {
        crate::pool::IncomingTxBuilder::new(42, 1)
            .with_local_tx_embargo_policy(LocalTxEmbargoPolicy::RetryLocal)
            .with_routing_state(TxState::Local)
            .with_state_in_db(Some(State::Stem))
            .build()
            .unwrap()
    };
    svc.ready().await.unwrap().call(incoming()).await.unwrap();
    stems.recv().await.unwrap();
    recorded_rx.recv().await.unwrap();
    // The request was constructed under the old policy. Storage must win.
    svc.ready().await.unwrap().call(incoming()).await.unwrap();
    stems.recv().await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(10_000), broadcasts.recv())
            .await
            .unwrap()
            .unwrap()
            .0,
        42
    );
    assert_eq!(txs.lock().unwrap().get(&1), Some(&(42, State::Fluff)));
    assert!(
        recorded_rx.try_recv().is_err(),
        "fallback is recorded only once"
    );
    drop(svc);
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn ordinary_local_policy_still_fluffs_at_embargo() {
    let (backing, txs) = mock_in_memory_backing_pool::<usize, usize>();
    txs.lock().unwrap().insert(1, (42, State::Stem));
    let (routes, mut route_rx) = mpsc::unbounded_channel();
    let router = service_fn(move |req: crate::DandelionRouteReq<usize, usize>| {
        let state = if req.state == TxState::Local {
            State::Stem
        } else {
            State::Fluff
        };
        routes.send(req.state).unwrap();
        std::future::ready(Ok::<_, crate::DandelionRouterError>(state))
    });
    let (mut svc, task) = start_dandelion_pool_manager_with_local_tx_embargo_policy(
        8,
        router,
        backing,
        test_config(),
        LocalTxEmbargoPolicy::RetryLocal,
    );
    let incoming = crate::pool::IncomingTxBuilder::new(42, 1)
        .with_local_tx_embargo_policy(LocalTxEmbargoPolicy::Fluff)
        .with_routing_state(TxState::Local)
        .with_state_in_db(Some(State::Stem))
        .build()
        .unwrap();
    svc.ready().await.unwrap().call(incoming).await.unwrap();
    assert_eq!(route_rx.recv().await.unwrap(), TxState::Local);
    assert_eq!(
        timeout(Duration::from_secs(10_000), route_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        TxState::Fluff
    );
    assert_eq!(txs.lock().unwrap().get(&1), Some(&(42, State::Fluff)));
    drop(svc);
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn peer_stem_and_fluff_echoes_cannot_override_private_local_policy() {
    let (backing, txs) = mock_in_memory_backing_pool::<usize, usize>();
    txs.lock()
        .unwrap()
        .extend([(1, (42, State::Stem)), (2, (43, State::Stem))]);
    let (routes, mut route_rx) = mpsc::unbounded_channel();
    let router = service_fn(move |req: crate::DandelionRouteReq<usize, usize>| {
        let result = if req.state == TxState::Local {
            State::Stem
        } else {
            State::Fluff
        };
        routes.send((req.tx, req.state)).unwrap();
        std::future::ready(Ok::<_, crate::DandelionRouterError>(result))
    });
    // Default remains ordinary Dandelion; this one local transaction overrides it.
    let (mut svc, task) = start_dandelion_pool_manager(8, router, backing, test_config());
    let local = crate::pool::IncomingTxBuilder::new(42, 1)
        .with_local_tx_embargo_policy(LocalTxEmbargoPolicy::RetryLocal)
        .with_routing_state(TxState::Local)
        .with_state_in_db(Some(State::Stem))
        .build()
        .unwrap();
    svc.ready().await.unwrap().call(local).await.unwrap();
    assert_eq!(route_rx.recv().await.unwrap(), (42, TxState::Local));
    for state in [
        TxState::Stem { from: 9 },
        TxState::Stem { from: 9 },
        TxState::Fluff,
    ] {
        let echo = crate::pool::IncomingTxBuilder::new(42, 1)
            .with_routing_state(state)
            .with_state_in_db(Some(State::Stem))
            .build()
            .unwrap();
        svc.ready().await.unwrap().call(echo).await.unwrap();
    }
    assert!(
        route_rx.try_recv().is_err(),
        "peer echoes must not invoke a clearnet route"
    );
    assert_eq!(txs.lock().unwrap().get(&1), Some(&(42, State::Stem)));

    // Unrelated peer-origin transactions still take the ordinary route.
    let peer = crate::pool::IncomingTxBuilder::new(43, 2)
        .with_routing_state(TxState::Fluff)
        .with_state_in_db(Some(State::Stem))
        .build()
        .unwrap();
    svc.ready().await.unwrap().call(peer).await.unwrap();
    assert_eq!(route_rx.recv().await.unwrap(), (43, TxState::Fluff));
    drop(svc);
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn removed_transaction_cancels_waiting_route_and_can_be_resubmitted() {
    cancellation_test(true).await;
}

#[tokio::test(start_paused = true)]
async fn missing_backing_transaction_cancels_route_at_embargo() {
    cancellation_test(false).await;
}

async fn cancellation_test(explicit_removal: bool) {
    struct Dropped(mpsc::UnboundedSender<()>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.send(()).unwrap();
        }
    }
    let (backing, txs) = mock_in_memory_backing_pool::<usize, usize>();
    txs.lock().unwrap().insert(1, (42, State::Stem));
    let gate = Arc::new(tokio::sync::Notify::new());
    let route_gate = Arc::clone(&gate);
    let (started, mut started_rx) = mpsc::unbounded_channel();
    let (dropped, mut dropped_rx) = mpsc::unbounded_channel();
    let (sent, mut sent_rx) = mpsc::unbounded_channel();
    let router = service_fn(move |req: crate::DandelionRouteReq<usize, usize>| {
        let gate = Arc::clone(&route_gate);
        let sent = sent.clone();
        let dropped = Dropped(dropped.clone());
        started.send(req.tx).unwrap();
        async move {
            let _guard = dropped;
            gate.notified().await;
            sent.send(req.tx).unwrap();
            Ok::<_, crate::DandelionRouterError>(State::Stem)
        }
    });
    let (mut svc, task) = start_dandelion_pool_manager_with_local_tx_embargo_policy(
        8,
        router,
        backing,
        test_config(),
        LocalTxEmbargoPolicy::RetryLocal,
    );
    let incoming = || {
        crate::pool::IncomingTxBuilder::new(42, 1)
            .with_routing_state(TxState::Local)
            .with_state_in_db(Some(State::Stem))
            .build()
            .unwrap()
    };
    svc.ready().await.unwrap().call(incoming()).await.unwrap();
    assert_eq!(started_rx.recv().await.unwrap(), 42);
    txs.lock().unwrap().remove(&1);
    if explicit_removal {
        svc.remove_transaction(1).await.unwrap();
    }
    // With paused time this waits for the real embargo to detect the missing row.
    timeout(Duration::from_secs(10_000), dropped_rx.recv())
        .await
        .unwrap()
        .unwrap();
    gate.notify_one();
    assert!(sent_rx.try_recv().is_err());

    // An announcement queued after removal cannot resurrect the old route.
    svc.ready().await.unwrap().call(incoming()).await.unwrap();
    assert!(started_rx.try_recv().is_err());

    // The same hash can be legitimately submitted again; stale task completions
    // must not interfere with its new route.
    txs.lock().unwrap().insert(1, (42, State::Stem));
    svc.ready().await.unwrap().call(incoming()).await.unwrap();
    assert_eq!(started_rx.recv().await.unwrap(), 42);
    assert_eq!(sent_rx.recv().await.unwrap(), 42);
    drop(svc);
    task.await.unwrap();
}
