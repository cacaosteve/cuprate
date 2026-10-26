use std::{sync::Arc, task::Poll, time::Duration};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use tower::{Service, ServiceExt};

use tokio::{
    sync::{mpsc, oneshot, Mutex},
    time::{sleep, timeout},
};

use cuprate_dandelion_tower::{
    pool::{DandelionPoolService, LocalTxEmbargoPolicy},
    traits::StemRequest,
    DandelionConfig, DandelionRouteReq, DandelionRouter, DandelionRouterError, Graph, RouteOutcome,
    State, TxState,
};
use cuprate_p2p::{NetworkInterface, PeerSetRequest, PeerSetResponse};
use cuprate_p2p_core::{client::InternalPeerID, BroadcastMessage, ClearNet, I2p, NetworkZone, Tor};
use cuprate_txpool::{
    service::{TxpoolReadHandle, TxpoolWriteHandle},
    TxRelay,
};
use cuprate_wire::protocol::NewTransactions;

use crate::{
    config::LocalTxRelayMode,
    p2p::CrossNetworkInternalPeerId,
    txpool::incoming_tx::{DandelionTx, TxId},
};

mod anon_net_service;
mod diffuse_service;
mod stem_service;
pub(super) mod tx_store;

#[cfg(test)]
mod tests;

pub(crate) use anon_net_service::AnonTxService;
pub(crate) use diffuse_service::DiffuseService;

/// The configuration used for [`cuprate_dandelion_tower`].
///
/// TODO: should we expose this to users of cuprated? probably not.
const DANDELION_CONFIG: DandelionConfig = DandelionConfig {
    time_between_hop: Duration::from_millis(175),
    epoch_duration: Duration::from_mins(10),
    fluff_probability: 0.12,
    graph: Graph::FourRegular,
};

const I2P_FIRST_SEND_TIMEOUT: Duration = Duration::from_secs(15);
const I2P_ONLY_RETRY_INTERVAL: Duration = Duration::from_secs(20);

/// A [`DandelionRouter`] with all generic types defined.
pub(super) type ConcreteDandelionRouter<Z> = DandelionRouter<
    stem_service::OutboundPeerStream<Z>,
    DiffuseService<Z>,
    CrossNetworkInternalPeerId,
    stem_service::StemPeerService<Z>,
    DandelionTx,
>;

/// The dandelion router used to send transactions to the network.
pub(super) struct MainDandelionRouter<R = ConcreteDandelionRouter<ClearNet>> {
    clearnet_router: Arc<Mutex<R>>,
    tor: Arc<Mutex<TorRouterState>>,
    i2p_interface: I2pInterfaceState,
}

impl<R> MainDandelionRouter<R> {
    pub(crate) fn new(
        clearnet_router: R,
        tor_net_rx: Option<oneshot::Receiver<NetworkInterface<Tor>>>,
        i2p_net_rx: oneshot::Receiver<NetworkInterface<I2p>>,
    ) -> Self {
        Self {
            clearnet_router: Arc::new(Mutex::new(clearnet_router)),
            tor: Arc::new(Mutex::new(TorRouterState {
                router: None,
                receiver: tor_net_rx,
            })),
            i2p_interface: I2pInterfaceState::new(i2p_net_rx),
        }
    }
}

impl<R> Service<DandelionRouteReq<DandelionTx, CrossNetworkInternalPeerId>>
    for MainDandelionRouter<R>
where
    R: Service<
            DandelionRouteReq<DandelionTx, CrossNetworkInternalPeerId>,
            Response = State,
            Error = DandelionRouterError,
        > + Send
        + 'static,
    R::Future: Send,
{
    type Response = RouteOutcome;
    type Error = DandelionRouterError;
    type Future = BoxFuture<'static, Result<RouteOutcome, DandelionRouterError>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // A blocked Tor/clearnet route must not block I2P-only transactions.
        // Each underlying service acquires readiness only when actually used.
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        mut req: DandelionRouteReq<DandelionTx, CrossNetworkInternalPeerId>,
    ) -> Self::Future {
        if req.tx.1 == TxRelay::LocalI2pClearnet {
            return route_when_ready(Arc::clone(&self.clearnet_router), req)
                .map_ok(Into::into)
                .boxed();
        }
        // Origin/policy is snapshotted with the transaction, not inferred from a peer echo
        // or the node's current configuration after a restart.
        let local_mode = match req.tx.1 {
            TxRelay::LocalI2pThenClearnet => LocalTxRelayMode::I2pThenClearnet,
            TxRelay::LocalI2pOnly => LocalTxRelayMode::I2pOnly,
            TxRelay::Unknown | TxRelay::Peer | TxRelay::Local | TxRelay::LocalI2pClearnet => {
                LocalTxRelayMode::Clearnet
            }
        };
        if req.tx.1.retry_local() {
            req.state = TxState::Local;
        }
        if req.state == TxState::Local {
            match local_mode {
                LocalTxRelayMode::Clearnet => {
                    let tor = Arc::clone(&self.tor);
                    let clearnet = Arc::clone(&self.clearnet_router);
                    return async move {
                        let peer = tor.lock().await.ready_peer().await?;
                        match peer {
                            Some(mut peer) => {
                                peer.call(StemRequest(req.tx))
                                    .map_ok(|_| State::Stem)
                                    .map_err(DandelionRouterError::PeerError)
                                    .await
                            }
                            None => route_when_ready(clearnet, req).await,
                        }
                    }
                    .map_ok(Into::into)
                    .boxed();
                }
                LocalTxRelayMode::I2pThenClearnet => {
                    let clearnet = Arc::clone(&self.clearnet_router);
                    let i2p_interface = self.i2p_interface.clone();
                    let tx = req.tx.clone();
                    return i2p_first_then(
                        async move {
                            let interface = i2p_interface.get_if_ready().await?;
                            send_i2p_stem_once(interface, tx).await
                        },
                        move || route_when_ready(clearnet, req),
                    )
                    .boxed();
                }
                LocalTxRelayMode::I2pOnly => {
                    let i2p_interface = self.i2p_interface.clone();
                    return async move {
                        loop {
                            let result = async {
                                let send = send_i2p_stem_once(
                                    i2p_interface.get_or_wait().await?, req.tx.clone(),
                                );
                                timeout(I2P_FIRST_SEND_TIMEOUT, send)
                                    .await
                                    .map_err(|_| i2p_timeout_error())?
                            }
                            .await;

                            match result {
                                Ok(()) => return Ok(State::Stem.into()),
                                Err(error) => {
                                    tracing::warn!(%error, "I2P-only transaction remains queued; retrying without clearnet fallback");
                                    sleep(I2P_ONLY_RETRY_INTERVAL).await;
                                }
                            }
                        }
                    }
                    .boxed();
                }
            }
        }

        // Transactions received from any peer zone retain the existing relay path.
        route_when_ready(Arc::clone(&self.clearnet_router), req)
            .map_ok(Into::into)
            .boxed()
    }
}

struct TorRouterState {
    router: Option<AnonTxService<Tor>>,
    receiver: Option<oneshot::Receiver<NetworkInterface<Tor>>>,
}

impl TorRouterState {
    async fn ready_peer(
        &mut self,
    ) -> Result<Option<stem_service::StemPeerService<Tor>>, DandelionRouterError> {
        if let Some(rx) = self.receiver.as_mut() {
            match rx.try_recv() {
                Ok(interface) => {
                    self.router = Some(AnonTxService::new(interface));
                    self.receiver = None;
                    tracing::info!("Tor P2P zone is now available.");
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => self.receiver = None,
            }
        }
        if let Some(router) = self.router.as_mut() {
            futures::future::poll_fn(|cx| router.poll_ready(cx)).await?;
            return Ok(router.peer.take());
        }
        Ok(None)
    }
}

/// Do not even call the peer service until fallback is required: `Service::call` may
/// synchronously enqueue a network write before its returned future is polled.
async fn i2p_first_then<A, F, Fut>(
    attempt: A,
    fallback: F,
) -> Result<RouteOutcome, DandelionRouterError>
where
    A: std::future::Future<Output = Result<(), DandelionRouterError>>,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<State, DandelionRouterError>>,
{
    match timeout(I2P_FIRST_SEND_TIMEOUT, attempt).await {
        Ok(Ok(())) => Ok(State::Stem.into()),
        result => {
            tracing::warn!(
                ?result,
                "I2P transaction send failed or timed out; falling back to the normal route"
            );
            fallback().await.map(RouteOutcome::LocalClearnetFallback)
        }
    }
}

async fn route_when_ready<S, Req>(service: Arc<Mutex<S>>, req: Req) -> Result<S::Response, S::Error>
where
    S: Service<Req>,
{
    // Keep readiness and call together, but release the mutex while the route is in flight.
    let future = service.lock().await.ready().await?.call(req);
    future.await
}

#[derive(Clone)]
struct I2pInterfaceState(Arc<Mutex<I2pInterfaceStateInner>>);

struct I2pInterfaceStateInner {
    receiver: Option<oneshot::Receiver<NetworkInterface<I2p>>>,
    interface: Option<NetworkInterface<I2p>>,
}

impl I2pInterfaceState {
    fn new(receiver: oneshot::Receiver<NetworkInterface<I2p>>) -> Self {
        Self(Arc::new(Mutex::new(I2pInterfaceStateInner {
            receiver: Some(receiver),
            interface: None,
        })))
    }

    async fn get_if_ready(&self) -> Result<NetworkInterface<I2p>, DandelionRouterError> {
        let mut state = self.0.lock().await;
        if let Some(interface) = &state.interface {
            return Ok(interface.clone());
        }

        let Some(receiver) = state.receiver.as_mut() else {
            return Err(i2p_unavailable_error());
        };

        match receiver.try_recv() {
            Ok(interface) => {
                state.receiver = None;
                state.interface = Some(interface.clone());
                Ok(interface)
            }
            Err(oneshot::error::TryRecvError::Empty) => Err(i2p_unavailable_error()),
            Err(oneshot::error::TryRecvError::Closed) => {
                state.receiver = None;
                Err(i2p_unavailable_error())
            }
        }
    }

    async fn get_or_wait(&self) -> Result<NetworkInterface<I2p>, DandelionRouterError> {
        let mut state = self.0.lock().await;
        if let Some(interface) = &state.interface {
            return Ok(interface.clone());
        }

        // Keep the receiver in shared state while awaiting it. Cancelling one queued
        // transaction must not discard the interface for every other transaction.
        let receiver = state.receiver.as_mut().ok_or_else(i2p_unavailable_error)?;
        let result = receiver.await;
        state.receiver = None;
        let interface = result.map_err(|_| i2p_unavailable_error())?;
        state.interface = Some(interface.clone());
        Ok(interface)
    }
}

fn i2p_unavailable_error() -> DandelionRouterError {
    DandelionRouterError::PeerError(Box::new(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "I2P P2P zone is not available",
    )))
}

fn i2p_timeout_error() -> DandelionRouterError {
    DandelionRouterError::PeerError(Box::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "I2P transaction send timed out",
    )))
}

async fn send_i2p_stem_once(
    mut interface: NetworkInterface<I2p>,
    tx: DandelionTx,
) -> Result<(), DandelionRouterError> {
    let mut peer = {
        let peer_set = interface.peer_set();
        let response = peer_set
            .ready()
            .await
            .map_err(DandelionRouterError::OutboundPeerStreamError)?
            .call(PeerSetRequest::StemPeer)
            .await
            .map_err(DandelionRouterError::OutboundPeerStreamError)?;
        match response {
            PeerSetResponse::StemPeer(Some(peer)) => peer,
            PeerSetResponse::StemPeer(None)
            | PeerSetResponse::PeersWithMorePoW(_)
            | PeerSetResponse::MostPoWSeen { .. } => {
                return Err(DandelionRouterError::PeerError(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "no I2P stem peer available",
                    ),
                )));
            }
        }
    };

    ServiceExt::<BroadcastMessage>::ready(&mut *peer)
        .await
        .map_err(DandelionRouterError::PeerError)?
        .call(BroadcastMessage::NewTransactions(NewTransactions {
            txs: vec![tx.0],
            dandelionpp_fluff: false,
            padding: Bytes::new(),
        }))
        .await
        .map_err(DandelionRouterError::PeerError)?;

    Ok(())
}

/// Starts the dandelion pool manager and returns its service and task handles.
pub(crate) fn start_dandelion_pool_manager(
    router: MainDandelionRouter,
    txpool_read_handle: TxpoolReadHandle,
    txpool_write_handle: TxpoolWriteHandle,
    promote_tx: mpsc::UnboundedSender<[u8; 32]>,
) -> (
    DandelionPoolService<DandelionTx, TxId, CrossNetworkInternalPeerId>,
    tokio::task::JoinHandle<()>,
) {
    cuprate_dandelion_tower::pool::start_dandelion_pool_manager_with_local_tx_embargo_policy(
        // TODO: make this constant configurable?
        32,
        router,
        tx_store::TxStoreService {
            txpool_read_handle,
            txpool_write_handle,
            promote_tx,
        },
        DANDELION_CONFIG,
        LocalTxEmbargoPolicy::Fluff,
    )
}

/// Creates a [`DandelionRouter`] from a [`NetworkInterface`].
pub(crate) fn dandelion_router<Z: NetworkZone>(
    network_interface: NetworkInterface<Z>,
) -> ConcreteDandelionRouter<Z>
where
    InternalPeerID<Z::Addr>: Into<CrossNetworkInternalPeerId>,
{
    DandelionRouter::new(
        DiffuseService {
            clear_net_broadcast_service: network_interface.broadcast_svc(),
        },
        stem_service::OutboundPeerStream::<Z>::new(network_interface),
        DANDELION_CONFIG,
    )
}
