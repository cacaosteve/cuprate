use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    time::Duration,
};

use futures::{FutureExt, StreamExt};
use rand::prelude::*;
use rand_distr::Exp;
use tokio::{
    sync::{mpsc, oneshot},
    task::{AbortHandle, Id, JoinSet},
};
use tokio_util::time::{delay_queue, DelayQueue};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use crate::{
    pool::IncomingTx,
    traits::{StoredTx, TxStoreRequest, TxStoreResponse},
    DandelionConfig, DandelionRouteReq, DandelionRouterError, RouteOutcome, State, TxState,
};

#[derive(Copy, Clone, Debug, thiserror::Error)]
#[error("The dandelion pool was shutdown")]
pub struct DandelionPoolShutDown;

/// The dandelion++ pool manager.
///
/// See the [module docs](super) for more.
pub struct DandelionPoolManager<P, R, Tx, TxId, PeerId> {
    /// The dandelion++ router
    pub(crate) dandelion_router: R,
    /// The backing tx storage.
    pub(crate) backing_pool: P,
    /// The set of tasks that are running the future returned from `dandelion_router`.
    pub(crate) routing_set: JoinSet<(TxId, Result<RouteOutcome, TxState<PeerId>>)>,

    /// The origin of stem transactions.
    pub(crate) stem_origins: HashMap<TxId, HashSet<PeerId>>,

    /// Protected local transactions, used to keep their configured relay policy
    /// across Dandelion embargo retries.
    pub(crate) local_tx_ids: HashSet<TxId>,

    /// Locally submitted transactions with an active route future. Prevents an
    /// I2P-only transaction waiting for a peer from spawning another waiter on
    /// each embargo tick.
    pub(crate) local_routing_pending: HashSet<TxId>,
    /// Task IDs also distinguish stale completions from a new submission with the same hash.
    pub(crate) routing_tasks: HashMap<TxId, HashMap<Id, AbortHandle>>,

    /// Current stem pool embargo timers.
    pub(crate) embargo_timers: DelayQueue<TxId>,
    pub(crate) embargo_timer_keys: HashMap<TxId, delay_queue::Key>,
    /// The distrobution to sample to get embargo timers.
    pub(crate) embargo_dist: Exp<f64>,

    /// The d++ config.
    pub(crate) config: DandelionConfig,

    /// Policy for locally submitted transactions when an embargo expires.
    pub(crate) local_tx_embargo_policy: super::LocalTxEmbargoPolicy,

    pub(crate) _tx: PhantomData<Tx>,
}

impl<P, R, Tx, TxId, PeerId> DandelionPoolManager<P, R, Tx, TxId, PeerId>
where
    Tx: Clone + Send,
    TxId: Hash + Eq + Clone + Send + 'static,
    PeerId: Hash + Eq + Clone + Send + 'static,
    P: Service<TxStoreRequest<TxId>, Response = TxStoreResponse<Tx>, Error = tower::BoxError>,
    P::Future: Send + 'static,
    R: Service<DandelionRouteReq<Tx, PeerId>, Error = DandelionRouterError>,
    R::Future: Send + 'static,
    R::Response: Into<RouteOutcome>,
{
    /// Adds a new embargo timer to the running timers, with a duration pulled from [`Self::embargo_dist`]
    fn add_embargo_timer_for_tx(&mut self, tx_id: TxId) {
        if self.embargo_timer_keys.contains_key(&tx_id) {
            return;
        }
        let embargo_timer = self.embargo_dist.sample(&mut thread_rng());
        tracing::debug!(
            "Setting embargo timer for stem tx: {} seconds.",
            embargo_timer
        );

        let key = self
            .embargo_timers
            .insert(tx_id.clone(), Duration::from_secs_f64(embargo_timer));
        self.embargo_timer_keys.insert(tx_id, key);
    }

    /// Stems the tx, setting the stem origin, if it wasn't already set.
    ///
    /// This function does not add the tx to the backing pool.
    async fn stem_tx(
        &mut self,
        tx: Tx,
        tx_id: TxId,
        from: Option<PeerId>,
    ) -> Result<(), tower::BoxError> {
        if from.is_none()
            && self.local_tx_ids.contains(&tx_id)
            && !self.local_routing_pending.insert(tx_id.clone())
        {
            return Ok(());
        }

        if let Some(peer) = &from {
            self.stem_origins
                .entry(tx_id.clone())
                .or_default()
                .insert(peer.clone());
        }

        let state = from.map_or(TxState::Local, |from| TxState::Stem { from });

        let fut = self
            .dandelion_router
            .ready()
            .await?
            .call(DandelionRouteReq {
                tx,
                state: state.clone(),
            });

        let task_tx_id = tx_id.clone();
        let handle = self
            .routing_set
            .spawn(fut.map(|res| (task_tx_id, res.map(Into::into).map_err(|_| state))));
        self.routing_tasks
            .entry(tx_id)
            .or_default()
            .insert(handle.id(), handle);
        Ok(())
    }

    /// Fluffs a tx, does not add the tx to the tx pool.
    async fn fluff_tx(&mut self, tx: Tx, tx_id: TxId) -> Result<(), tower::BoxError> {
        let fut = self
            .dandelion_router
            .ready()
            .await?
            .call(DandelionRouteReq {
                tx,
                state: TxState::Fluff,
            });

        let task_tx_id = tx_id.clone();
        let handle = self
            .routing_set
            .spawn(fut.map(|res| (task_tx_id, res.map(Into::into).map_err(|_| TxState::Fluff))));
        self.routing_tasks
            .entry(tx_id)
            .or_default()
            .insert(handle.id(), handle);
        Ok(())
    }

    /// Function to handle an [`IncomingTx`].
    async fn handle_incoming_tx(
        &mut self,
        tx: Tx,
        tx_state: TxState<PeerId>,
        tx_id: TxId,
        local_embargo_policy: Option<super::LocalTxEmbargoPolicy>,
    ) -> Result<(), tower::BoxError> {
        // Re-check storage: a queued announcement may outlive removal from the pool.
        drop(tx);
        let Some(stored) = self.get_stored_tx(tx_id.clone()).await? else {
            self.forget_tx(&tx_id);
            return Ok(());
        };
        let tx = stored.tx;
        let local_embargo_policy = stored.local_embargo_policy.or(local_embargo_policy);
        if local_embargo_policy == Some(super::LocalTxEmbargoPolicy::Fluff) {
            self.local_tx_ids.remove(&tx_id);
        }
        // A peer echo does not change the origin or authorize clearnet relay.
        if self.local_tx_ids.contains(&tx_id) && tx_state != TxState::Local {
            return Ok(());
        }
        match tx_state {
            TxState::Stem { from } => {
                if self
                    .stem_origins
                    .get(&tx_id)
                    .is_some_and(|peers| peers.contains(&from))
                {
                    tracing::debug!("Received stem tx twice from same peer, fluffing it");
                    // The same peer sent us a tx twice, fluff it.
                    self.promote_and_fluff_tx(tx_id).await?;
                } else {
                    // This could be a new tx or it could have already been stemed, but we still stem it again
                    // unless the same peer sends us a tx twice.
                    tracing::debug!("Steming incoming tx");
                    self.stem_tx(tx, tx_id.clone(), Some(from)).await?;
                    self.add_embargo_timer_for_tx(tx_id);
                }
            }
            TxState::Fluff => {
                tracing::debug!("Fluffing incoming tx");
                self.fluff_tx(tx, tx_id).await?;
            }
            TxState::Local => {
                tracing::debug!("Steming local transaction");
                if local_embargo_policy.unwrap_or(self.local_tx_embargo_policy)
                    == super::LocalTxEmbargoPolicy::RetryLocal
                {
                    self.local_tx_ids.insert(tx_id.clone());
                }
                self.stem_tx(tx, tx_id.clone(), None).await?;
                self.add_embargo_timer_for_tx(tx_id);
            }
        }

        Ok(())
    }

    /// Persist the explicit I2P-first fallback transition.
    async fn record_clearnet_fallback(&mut self, tx_id: TxId) -> Result<(), tower::BoxError> {
        self.backing_pool
            .ready()
            .await?
            .call(TxStoreRequest::RecordClearnetFallback(tx_id))
            .await?;
        Ok(())
    }

    /// Promotes a tx to the clear pool.
    async fn promote_tx(&mut self, tx_id: TxId) -> Result<(), tower::BoxError> {
        self.forget_tx(&tx_id);

        self.backing_pool
            .ready()
            .await?
            .call(TxStoreRequest::Promote(tx_id))
            .await?;

        Ok(())
    }

    /// Forget a transaction and abort every outstanding route, including stale retries.
    fn forget_tx(&mut self, tx_id: &TxId) {
        if let Some(tasks) = self.routing_tasks.remove(tx_id) {
            for task in tasks.into_values() {
                task.abort();
            }
        }
        self.local_routing_pending.remove(tx_id);
        // Remove the tx from the maps used during the stem phase.
        self.stem_origins.remove(tx_id);
        self.local_tx_ids.remove(tx_id);

        if let Some(key) = self.embargo_timer_keys.remove(tx_id) {
            self.embargo_timers.try_remove(&key);
        }
    }

    /// Promotes a tx to the public fluff pool and fluffs the tx.
    async fn promote_and_fluff_tx(&mut self, tx_id: TxId) -> Result<(), tower::BoxError> {
        tracing::debug!("Promoting transaction to public pool and fluffing it.");

        let TxStoreResponse::Transaction(tx) = self
            .backing_pool
            .ready()
            .await?
            .call(TxStoreRequest::Get(tx_id.clone()))
            .await?
        else {
            panic!("Backing tx pool responded with wrong response for request.");
        };

        let Some(StoredTx { tx, state, .. }) = tx else {
            self.forget_tx(&tx_id);
            tracing::debug!("Could not find tx, skipping.");
            return Ok(());
        };

        if state == State::Fluff {
            tracing::debug!("Transaction already fluffed, skipping.");
            return Ok(());
        }

        self.promote_tx(tx_id.clone()).await?;
        self.fluff_tx(tx, tx_id).await
    }

    /// Returns a tx stored in the fluff _OR_ stem pool.
    async fn get_stored_tx(
        &mut self,
        tx_id: TxId,
    ) -> Result<Option<StoredTx<Tx>>, tower::BoxError> {
        let TxStoreResponse::Transaction(tx) = self
            .backing_pool
            .ready()
            .await?
            .call(TxStoreRequest::Get(tx_id))
            .await?
        else {
            panic!("Backing tx pool responded with wrong response for request.");
        };

        Ok(tx)
    }

    async fn get_tx_from_pool(&mut self, tx_id: TxId) -> Result<Option<Tx>, tower::BoxError> {
        Ok(self.get_stored_tx(tx_id).await?.map(|stored| stored.tx))
    }

    #[expect(clippy::type_complexity)]
    /// Starts the [`DandelionPoolManager`].
    pub(crate) async fn run(
        mut self,
        mut rx: mpsc::Receiver<(
            (IncomingTx<Tx, TxId, PeerId>, tracing::Span),
            oneshot::Sender<()>,
        )>,
        mut remove_rx: mpsc::Receiver<(TxId, oneshot::Sender<()>)>,
    ) {
        tracing::debug!("Starting dandelion++ tx-pool, config: {:?}", self.config);

        loop {
            tokio::select! {
                // biased to handle current txs before routing new ones.
                biased;
                Some((tx_id, ack)) = remove_rx.recv() => {
                    self.forget_tx(&tx_id);
                    #[expect(clippy::let_underscore_must_use)]
                    let _ = ack.send(());
                }
                Some(fired) = self.embargo_timers.next() => {
                    let span = tracing::debug_span!("embargo_timer_fired");
                    tracing::debug!(parent: &span,"Embargo timer fired, did not see stem tx in time.");

                    let tx_id = fired.into_inner();
                    self.embargo_timer_keys.remove(&tx_id);
                    let result = async {
                        if self.local_tx_ids.contains(&tx_id) {
                            if let Some(stored) = self.get_stored_tx(tx_id.clone()).await? {
                                if stored.local_embargo_policy == Some(super::LocalTxEmbargoPolicy::Fluff) {
                                    self.local_tx_ids.remove(&tx_id);
                                    self.promote_and_fluff_tx(tx_id).await?;
                                } else {
                                    self.stem_tx(stored.tx, tx_id.clone(), None).await?;
                                    self.add_embargo_timer_for_tx(tx_id);
                                }
                            } else {
                                self.forget_tx(&tx_id);
                            }
                            Ok(())
                        } else {
                            self.promote_and_fluff_tx(tx_id).await
                        }
                    }
                    .instrument(span.clone())
                    .await;

                    if let Err(e) = result {
                        tracing::error!(parent: &span, "Error handling fired embargo timer: {e}");
                        return;
                    }
                }
                Some(result) = self.routing_set.join_next_with_id() => {
                    let (task_id, (tx_id, res)) = match result {
                        Ok(result) => result,
                        Err(error) if error.is_cancelled() => continue,
                        Err(error) => {
                            tracing::error!(%error, "Transaction routing task panicked");
                            return;
                        }
                    };
                    let Some(tasks) = self.routing_tasks.get_mut(&tx_id) else {
                        continue;
                    };
                    if tasks.remove(&task_id).is_none() {
                        continue;
                    }
                    if tasks.is_empty() {
                        self.routing_tasks.remove(&tx_id);
                    }
                    self.local_routing_pending.remove(&tx_id);
                    let span = tracing::debug_span!("dandelion_routing_result");

                    // Only a router's explicit fallback result may relax the local
                    // embargo policy, and storage must acknowledge it first.
                    let res = match res {
                        Ok(RouteOutcome::LocalClearnetFallback(state)) => {
                            if let Err(error) = self.record_clearnet_fallback(tx_id.clone()).await {
                                tracing::error!(%error, "Failed to record clearnet fallback");
                                return;
                            }
                            self.local_tx_ids.remove(&tx_id);
                            Ok(state)
                        }
                        Ok(RouteOutcome::Standard(state)) => Ok(state),
                        Err(state) => Err(state),
                    };

                    let res = match res {
                        Ok(State::Fluff) => {
                            tracing::debug!(parent: &span, "Transaction was fluffed upgrading it to the public pool.");
                            self.promote_tx(tx_id).instrument(span.clone()).await
                        }
                        Err(tx_state) => {
                            tracing::debug!(parent: &span, "Error routing transaction, trying again.");

                            match self.get_tx_from_pool(tx_id.clone()).instrument(span.clone()).await {
                                Ok(Some(tx)) => match tx_state {
                                    TxState::Fluff => self.fluff_tx(tx, tx_id).instrument(span.clone()).await,
                                    TxState::Stem { from } => self.stem_tx(tx, tx_id, Some(from)).instrument(span.clone()).await,
                                    TxState::Local => self.stem_tx(tx, tx_id, None).instrument(span.clone()).await,
                                }
                                Err(e) => Err(e),
                                _ => {
                                    self.forget_tx(&tx_id);
                                    continue;
                                },
                            }
                        }
                        Ok(State::Stem) => continue,
                    };

                    if let Err(e) = res {
                        tracing::error!(parent: &span, "Error handling transaction routing return: {e}");
                        return;
                    }
                }
                req = rx.recv() => {
                    let Some(((IncomingTx { tx, tx_id, routing_state, local_embargo_policy }, span), res_tx)) = req else {
                        return;
                    };

                    let span = tracing::debug_span!(parent: &span, "dandelion_pool_manager");

                    tracing::debug!(parent: &span, "Received new tx to route.");

                    if let Err(e) = self.handle_incoming_tx(tx, routing_state, tx_id, local_embargo_policy).instrument(span.clone()).await {
                        #[expect(clippy::let_underscore_must_use, reason = "dropped receivers can be ignored")]
                        let _ = res_tx.send(());

                        tracing::error!(parent: &span, "Error handling transaction in dandelion pool: {e}");
                        return;
                    }

                    #[expect(clippy::let_underscore_must_use)]
                    let _ = res_tx.send(());
                }
            }
        }
    }
}
