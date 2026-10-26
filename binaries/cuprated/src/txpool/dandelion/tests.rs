use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[tokio::test]
async fn successful_i2p_does_not_call_eager_clearnet_service() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let router = Arc::new(Mutex::new(tower::service_fn(move |()| {
        // Like the real peer client, this has a side effect at call, not poll.
        count.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok::<_, DandelionRouterError>(State::Fluff))
    })));
    let future = i2p_first_then(async { Ok(()) }, move || route_when_ready(router, ()));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(future.await.unwrap(), RouteOutcome::Standard(State::Stem));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn failed_or_timed_out_i2p_calls_fallback_once() {
    for fail_immediately in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let attempt = async move {
            if fail_immediately {
                return Err(i2p_unavailable_error());
            }
            std::future::pending().await
        };
        let future = i2p_first_then(attempt, move || {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(State::Fluff))
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            future.await.unwrap(),
            RouteOutcome::LocalClearnetFallback(State::Fluff)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancelled_interface_wait_does_not_lose_receiver() {
    let (sender, receiver) = oneshot::channel();
    let state = I2pInterfaceState::new(receiver);
    // Poll a waiter far enough to acquire the mutex and wait on the channel.
    let mut waiter = Box::pin(state.get_or_wait());
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    drop(waiter);
    assert!(
        !sender.is_closed(),
        "cancellation must not discard the shared receiver"
    );
    assert!(state.0.lock().await.receiver.is_some());
    let mut second_waiter = Box::pin(state.get_or_wait());
    assert!(futures::poll!(second_waiter.as_mut()).is_pending());
    drop(sender);
    assert!(second_waiter.await.is_err());
    drop(state);
}

#[tokio::test]
async fn cancelled_i2p_first_attempt_never_starts_fallback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let mut future = Box::pin(i2p_first_then(std::future::pending(), move || {
        count.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(State::Fluff))
    }));
    assert!(futures::poll!(future.as_mut()).is_pending());
    drop(future);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn persisted_i2p_only_policy_overrides_peer_echo_routing_state() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let clear = tower::service_fn(
        move |_: DandelionRouteReq<DandelionTx, CrossNetworkInternalPeerId>| {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, DandelionRouterError>(State::Fluff))
        },
    );
    let (sender, receiver) = oneshot::channel();
    let mut router = MainDandelionRouter::new(clear, None, receiver);
    for state in [
        TxState::Local,
        TxState::Fluff,
        TxState::Stem {
            from: CrossNetworkInternalPeerId::I2p(InternalPeerID::Unknown([9; 16])),
        },
    ] {
        let mut future = router.ready().await.unwrap().call(DandelionRouteReq {
            tx: DandelionTx(Bytes::from_static(b"transaction"), TxRelay::LocalI2pOnly),
            state,
        });
        assert!(futures::poll!(future.as_mut()).is_pending());
        tokio::time::advance(Duration::from_mins(1)).await;
        assert!(futures::poll!(future.as_mut()).is_pending());
        drop(future);
        assert!(!sender.is_closed());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn i2p_first_unavailable_really_falls_back_and_peer_origin_remains_normal() {
    let (sent, mut sent_rx) = mpsc::unbounded_channel();
    let clear = tower::service_fn(
        move |req: DandelionRouteReq<DandelionTx, CrossNetworkInternalPeerId>| {
            sent.send((req.tx.1, req.state)).unwrap();
            std::future::ready(Ok::<_, DandelionRouterError>(State::Stem))
        },
    );
    let (_sender, receiver) = oneshot::channel();
    let mut router = MainDandelionRouter::new(clear, None, receiver);
    for (relay, state) in [
        (TxRelay::LocalI2pThenClearnet, TxState::Local),
        (TxRelay::Peer, TxState::Fluff),
        (TxRelay::Local, TxState::Local),
    ] {
        let future = router.ready().await.unwrap().call(DandelionRouteReq {
            tx: DandelionTx(Bytes::new(), relay),
            state: state.clone(),
        });
        assert!(
            sent_rx.try_recv().is_err(),
            "call must not eagerly start fallback"
        );
        let expected = if relay == TxRelay::LocalI2pThenClearnet {
            RouteOutcome::LocalClearnetFallback(State::Stem)
        } else {
            State::Stem.into()
        };
        assert_eq!(future.await.unwrap(), expected);
        assert_eq!(sent_rx.recv().await.unwrap(), (relay, state));
    }
}

#[tokio::test(start_paused = true)]
async fn recorded_clearnet_fallback_bypasses_anonymous_routes_and_preserves_routing_state() {
    let (sent, mut sent_rx) = mpsc::unbounded_channel();
    let clear = tower::service_fn(
        move |req: DandelionRouteReq<DandelionTx, CrossNetworkInternalPeerId>| {
            let result = if req.state == TxState::Fluff {
                State::Fluff
            } else {
                State::Stem
            };
            sent.send((req.tx.1, req.state)).unwrap();
            std::future::ready(Ok::<_, DandelionRouterError>(result))
        },
    );
    let (_sender, receiver) = oneshot::channel();
    let mut router = MainDandelionRouter::new(clear, None, receiver);
    // If a saved fallback tries either anonymous route again, it will stall.
    let tor_guard = Arc::clone(&router.tor).lock_owned().await;
    let i2p_guard = Arc::clone(&router.i2p_interface.0).lock_owned().await;
    for state in [
        TxState::Local,
        TxState::Stem {
            from: CrossNetworkInternalPeerId::I2p(InternalPeerID::Unknown([9; 16])),
        },
        TxState::Fluff,
    ] {
        let expected = if state == TxState::Fluff {
            State::Fluff
        } else {
            State::Stem
        };
        let result = timeout(
            Duration::from_secs(1),
            router.ready().await.unwrap().call(DandelionRouteReq {
                tx: DandelionTx(Bytes::new(), TxRelay::LocalI2pClearnet),
                state: state.clone(),
            }),
        )
        .await
        .expect("recorded fallback must not wait on anonymous routes")
        .unwrap();
        assert_eq!(result, RouteOutcome::Standard(expected));
        assert_eq!(
            sent_rx.recv().await.unwrap(),
            (TxRelay::LocalI2pClearnet, state)
        );
    }
    drop((tor_guard, i2p_guard));
}
