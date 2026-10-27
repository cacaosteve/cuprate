use std::sync::Arc;

use cuprate_p2p::BroadcastSvc;
use cuprate_test_utils::data::TX_V2_RCT3;
use cuprate_txpool::service::init_with_pool;
use tokio::time::timeout;

use super::*;
use crate::txpool::dandelion::tx_store::TxStoreService;

#[tokio::test]
async fn acceptance_requires_new_pool_insertion_and_preserves_source_even_for_fluff() {
    for local in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let database = fjall::Database::builder(directory.path()).open().unwrap();
        let workers = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let (read, write) = init_with_pool(database, workers).unwrap();
        let (promote_tx, promote_rx) = mpsc::unbounded_channel();
        let router = tower::service_fn(
            |_: cuprate_dandelion_tower::DandelionRouteReq<
                DandelionTx,
                CrossNetworkInternalPeerId,
            >| {
                std::future::ready(Ok::<_, cuprate_dandelion_tower::DandelionRouterError>(
                    State::Stem,
                ))
            },
        );
        let (dandelion, task) = cuprate_dandelion_tower::pool::start_dandelion_pool_manager(
            8,
            router,
            TxStoreService {
                txpool_read_handle: read.clone(),
                txpool_write_handle: write.clone(),
                promote_tx,
            },
            cuprate_dandelion_tower::DandelionConfig {
                time_between_hop: Duration::from_secs(175),
                epoch_duration: Duration::from_mins(10),
                fluff_probability: 0.12,
                graph: cuprate_dandelion_tower::Graph::FourRegular,
            },
        );
        let executor = TaskExecutor::new();
        let manager = start_txpool_manager(
            write,
            read,
            promote_rx,
            DiffuseService {
                clear_net_broadcast_service: BroadcastSvc::mock(),
            },
            dandelion.clone(),
            TxpoolConfig::default(),
            executor.clone(),
            LocalTxRelayMode::Clearnet,
        )
        .await
        .unwrap();
        let source = cuprate_p2p_core::TransactionCounters::default();
        let echo_source = cuprate_p2p_core::TransactionCounters::default();
        // Fluff has no peer ID in TxState, so attribution must use the explicit origin handle.
        let state = if local {
            TxState::Local
        } else {
            TxState::Fluff
        };
        let origin = if local { None } else { Some(source.clone()) };
        manager
            .command_tx
            .send(TxpoolManagerCommand::IncomingTx(
                TX_V2_RCT3.clone().try_into().unwrap(),
                state,
                origin,
            ))
            .await
            .unwrap();
        for origin in [source.clone(), echo_source.clone()] {
            // Concurrent verification/peer echoes can queue the same tx again. The DB must
            // reject that insertion, and neither source may gain another accepted count.
            manager
                .command_tx
                .send(TxpoolManagerCommand::IncomingTx(
                    TX_V2_RCT3.clone().try_into().unwrap(),
                    TxState::Fluff,
                    Some(origin),
                ))
                .await
                .unwrap();
        }
        // FIFO command barrier: count only after all DB writes have actually completed.
        let (response_tx, response_rx) = oneshot::channel();
        manager
            .command_tx
            .send(TxpoolManagerCommand::PoolInfoSince {
                since: 0,
                response_tx,
            })
            .await
            .unwrap();
        drop(response_rx.await.unwrap());
        assert_eq!(source.snapshot().accepted, u64::from(!local));
        assert_eq!(echo_source.snapshot().accepted, 0);
        assert_eq!(
            source.snapshot().relayed,
            0,
            "queueing a route is not a wire send"
        );
        executor.trigger_shutdown();
        drop(executor.wait_for_shutdown().await);
        drop((manager, dandelion));
        task.await.unwrap();
    }
}

// Reopen a real transaction pool and recover it with a different current mode.
// No running node, wallet, blockchain data, or real peer is used by this test.
#[tokio::test]
async fn restart_preserves_local_policy_and_does_not_reclassify_peer_transactions() {
    struct RouteDropped(mpsc::UnboundedSender<()>);
    impl Drop for RouteDropped {
        fn drop(&mut self) {
            self.0.send(()).unwrap();
        }
    }
    for relay in [
        TxRelay::Peer,
        TxRelay::Local,
        TxRelay::LocalI2pThenClearnet,
        TxRelay::LocalI2pClearnet,
        TxRelay::LocalI2pOnly,
        TxRelay::Unknown,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let tx: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
        let tx_id = tx.tx_hash;
        let spent_key_images = tx
            .tx
            .prefix()
            .inputs
            .iter()
            .map(|input| match input {
                monero_oxide::transaction::Input::ToKey { key_image, .. } => key_image.to_bytes(),
                monero_oxide::transaction::Input::Gen(_) => unreachable!(),
            })
            .collect();
        {
            let database = fjall::Database::builder(directory.path()).open().unwrap();
            let pool = Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(1)
                    .build()
                    .unwrap(),
            );
            let (read, mut write) = init_with_pool(database.clone(), Arc::clone(&pool)).unwrap();
            let result = write
                .ready()
                .await
                .unwrap()
                .call(TxpoolWriteRequest::AddTransaction {
                    tx: Box::new(tx),
                    state_stem: true,
                    relay,
                })
                .await
                .unwrap();
            assert!(matches!(result, TxpoolWriteResponse::AddTransaction(None)));
            database.persist(fjall::PersistMode::SyncAll).unwrap();
            drop(read);
            drop(write);
            // Drop the worker pool before reopening so the write closure also releases its database.
            drop(pool);
        }
        let database = fjall::Database::builder(directory.path()).open().unwrap();
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let (mut read, write) = init_with_pool(database, pool).unwrap();
        let (promote_tx, promote_rx) = mpsc::unbounded_channel();
        let (routes, mut route_rx) = mpsc::unbounded_channel();
        let (dropped, mut dropped_rx) = mpsc::unbounded_channel();
        let router = tower::service_fn(
            move |req: cuprate_dandelion_tower::DandelionRouteReq<
                DandelionTx,
                CrossNetworkInternalPeerId,
            >| {
                routes.send((req.tx.1, req.state)).unwrap();
                let guard = RouteDropped(dropped.clone());
                async move {
                    let _guard = guard;
                    if req.tx.1 == TxRelay::LocalI2pOnly {
                        std::future::pending::<()>().await;
                    }
                    Ok::<_, cuprate_dandelion_tower::DandelionRouterError>(State::Stem)
                }
            },
        );
        let (dandelion, task) = cuprate_dandelion_tower::pool::start_dandelion_pool_manager(
            8,
            router,
            TxStoreService {
                txpool_read_handle: read.clone(),
                txpool_write_handle: write.clone(),
                promote_tx,
            },
            cuprate_dandelion_tower::DandelionConfig {
                time_between_hop: Duration::from_secs(175),
                epoch_duration: Duration::from_mins(10),
                fluff_probability: 0.12,
                graph: cuprate_dandelion_tower::Graph::FourRegular,
            },
        );
        let executor = TaskExecutor::new();
        // In particular, a queued I2P-only tx must stay I2P-only even after changing to clearnet.
        // Conversely, switching to I2P-only must not turn old peer txs into local submissions.
        let current_mode = if relay.retry_local() {
            LocalTxRelayMode::Clearnet
        } else {
            LocalTxRelayMode::I2pOnly
        };
        let manager = start_txpool_manager(
            write,
            read.clone(),
            promote_rx,
            DiffuseService {
                clear_net_broadcast_service: BroadcastSvc::mock(),
            },
            dandelion.clone(),
            TxpoolConfig::default(),
            executor.clone(),
            current_mode,
        )
        .await
        .unwrap();
        let TxpoolReadResponse::TxBlob {
            state_stem,
            relay: stored,
            ..
        } = read
            .ready()
            .await
            .unwrap()
            .call(TxpoolReadRequest::TxBlob(tx_id))
            .await
            .unwrap()
        else {
            panic!("wrong response")
        };
        assert_eq!(stored, relay);
        match relay {
            TxRelay::LocalI2pThenClearnet | TxRelay::LocalI2pOnly => {
                assert!(state_stem);
                assert_eq!(
                    timeout(Duration::from_secs(2), route_rx.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                    (relay, TxState::Local)
                );
            }
            TxRelay::Peer | TxRelay::Local | TxRelay::LocalI2pClearnet => {
                assert!(
                    !state_stem,
                    "ordinary transactions retain standard restart recovery"
                );
                assert!(route_rx.try_recv().is_err());
            }
            TxRelay::Unknown => {
                assert!(
                    state_stem,
                    "legacy unknown origin must not be guessed or publicly promoted"
                );
                assert!(route_rx.try_recv().is_err());
            }
        }
        if relay == TxRelay::LocalI2pOnly {
            // Block inclusion removes the row and cancels the pending route immediately,
            // without waiting for an embargo or an I2P peer to become available.
            manager.new_block(spent_key_images).await.unwrap();
            timeout(Duration::from_secs(2), dropped_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                read.ready()
                    .await
                    .unwrap()
                    .call(TxpoolReadRequest::TxBlob(tx_id))
                    .await,
                Err(cuprate_txpool::TxPoolError::NotFound)
            ));
        }
        executor.trigger_shutdown();
        assert!(matches!(
            executor.wait_for_shutdown().await,
            crate::monitor::ShutdownReason::Requested
        ));
        drop(manager);
        drop(dandelion);
        task.await.unwrap();
    }
}
