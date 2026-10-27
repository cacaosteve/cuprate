use std::sync::Arc;

use cuprate_p2p::BroadcastSvc;
use cuprate_test_utils::data::TX_V2_RCT3;
use cuprate_txpool::service::init_with_pool;
use tokio::time::timeout;

use super::*;
use crate::txpool::dandelion::tx_store::TxStoreService;

#[tokio::test]
async fn submission_waits_for_admission_and_never_turns_failure_into_success() {
    for outcome in [
        Ok(TxAdmission::Inserted),
        Ok(TxAdmission::AlreadyKnown),
        Ok(TxAdmission::DoubleSpend),
        Ok(TxAdmission::Full),
        Err("injected storage failure".to_owned()),
    ] {
        let (command_tx, mut commands) = mpsc::channel(1);
        let (spent_kis_tx, _) = mpsc::channel(1);
        let handle = TxpoolManagerHandle {
            command_tx,
            spent_kis_tx,
        };
        let mut pending =
            Box::pin(handle.submit(TX_V2_RCT3.clone().try_into().unwrap(), TxState::Local, None));
        assert!(futures::poll!(&mut pending).is_pending());
        let TxpoolManagerCommand::IncomingTx(_, _, _, response) = commands.recv().await.unwrap()
        else {
            panic!("wrong command")
        };
        assert!(
            futures::poll!(&mut pending).is_pending(),
            "enqueueing is not acceptance"
        );
        let expected = outcome.clone();
        response.send(outcome).unwrap();
        let result = pending.await;
        match expected {
            Ok(TxAdmission::Inserted) => assert_eq!(result.unwrap(), TxAdmission::Inserted),
            Ok(TxAdmission::AlreadyKnown) => assert_eq!(result.unwrap(), TxAdmission::AlreadyKnown),
            Ok(TxAdmission::DoubleSpend) => {
                assert!(matches!(result, Err(IncomingTxError::PoolDoubleSpend)));
            }
            Ok(TxAdmission::Full) => assert!(matches!(result, Err(IncomingTxError::PoolFull))),
            Err(_) => assert!(matches!(
                result,
                Err(IncomingTxError::PoolSubmissionFailed(_))
            )),
        }
    }
    let (command_tx, mut commands) = mpsc::channel(1);
    let (spent_kis_tx, _) = mpsc::channel(1);
    let handle = TxpoolManagerHandle {
        command_tx,
        spent_kis_tx,
    };
    let mut pending =
        Box::pin(handle.submit(TX_V2_RCT3.clone().try_into().unwrap(), TxState::Local, None));
    assert!(futures::poll!(&mut pending).is_pending());
    drop(commands.recv().await.unwrap());
    assert!(matches!(pending.await, Err(IncomingTxError::ChannelClosed)));
}

// The manager receives already-verified transactions. Use real serialized fixtures
// and isolated databases; no transaction is sent to a live chain or peer.
struct PoolFixture {
    _directory: tempfile::TempDir,
    manager: TxpoolManagerHandle,
    read: TxpoolReadHandle,
    executor: TaskExecutor,
    dandelion: DandelionPoolService<DandelionTx, TxId, CrossNetworkInternalPeerId>,
    task: tokio::task::JoinHandle<()>,
}

impl PoolFixture {
    async fn new(
        cap: usize,
        mode: LocalTxRelayMode,
        seed: Option<(TransactionVerificationData, TxRelay)>,
    ) -> Result<Self, FatalError> {
        let directory = tempfile::tempdir().unwrap();
        let database = fjall::Database::builder(directory.path()).open().unwrap();
        let workers = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let (read, mut write) = init_with_pool(database, workers).unwrap();
        if let Some((tx, relay)) = seed {
            write
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
        }
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
        let result = start_txpool_manager(
            write,
            read.clone(),
            promote_rx,
            DiffuseService {
                clear_net_broadcast_service: BroadcastSvc::mock(),
            },
            dandelion.clone(),
            TxpoolConfig {
                max_txpool_byte_size: cap,
                ..Default::default()
            },
            executor.clone(),
            mode,
        )
        .await;
        let manager = match result {
            Ok(manager) => manager,
            Err(error) => {
                drop(dandelion);
                task.await.unwrap();
                return Err(error);
            }
        };
        Ok(Self {
            _directory: directory,
            manager,
            read,
            executor,
            dandelion,
            task,
        })
    }

    async fn close(self) {
        self.executor.trigger_shutdown();
        assert!(matches!(
            self.executor.wait_for_shutdown().await,
            crate::monitor::ShutdownReason::Requested
        ));
        drop((self.manager, self.dandelion));
        self.task.await.unwrap();
    }
}

#[tokio::test]
async fn real_pool_conflict_is_reported_and_does_not_increment_acceptance() {
    let pool = PoolFixture::new(100_000_000, LocalTxRelayMode::Clearnet, None)
        .await
        .unwrap();
    let original: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
    let source = cuprate_p2p_core::TransactionCounters::default();
    assert_eq!(
        pool.manager
            .submit(original.clone(), TxState::Fluff, Some(source.clone()))
            .await
            .unwrap(),
        TxAdmission::Inserted
    );
    // Simulate a distinct verified transaction using the same key images. Only
    // the manager layer is under test, not cryptographic transaction validation.
    let mut conflict = original.clone();
    conflict.tx_hash[0] ^= 1;
    assert!(matches!(
        pool.manager
            .submit(conflict, TxState::Fluff, Some(source.clone()))
            .await,
        Err(IncomingTxError::PoolDoubleSpend)
    ));
    assert_eq!(source.snapshot().accepted, 1);
    let TxpoolReadResponse::Backlog(backlog) = pool
        .read
        .clone()
        .oneshot(TxpoolReadRequest::Backlog)
        .await
        .unwrap()
    else {
        panic!("wrong response")
    };
    assert_eq!(backlog.len(), 1);
    assert_eq!(backlog[0].blob_size, original.tx_blob.len());
    pool.close().await;
}

#[tokio::test]
async fn capacity_counts_private_transactions_and_is_freed_by_block_inclusion() {
    let original: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
    let next: TransactionVerificationData = cuprate_test_utils::data::TX_V1_SIG2
        .clone()
        .try_into()
        .unwrap();
    let cap = original.tx_blob.len().max(next.tx_blob.len());
    for mode in [
        LocalTxRelayMode::Clearnet,
        LocalTxRelayMode::I2pOnly,
        LocalTxRelayMode::I2pThenClearnet,
    ] {
        let pool = PoolFixture::new(cap, mode, None).await.unwrap();
        assert_eq!(
            pool.manager
                .submit(original.clone(), TxState::Local, None)
                .await
                .unwrap(),
            TxAdmission::Inserted
        );
        assert_eq!(
            pool.manager
                .submit(original.clone(), TxState::Local, None)
                .await
                .unwrap(),
            TxAdmission::AlreadyKnown
        );
        assert!(matches!(
            pool.manager
                .submit(next.clone(), TxState::Local, None)
                .await,
            Err(IncomingTxError::PoolFull)
        ));
        let TxpoolReadResponse::TxBlob { relay, .. } = pool
            .read
            .clone()
            .oneshot(TxpoolReadRequest::TxBlob(original.tx_hash))
            .await
            .unwrap()
        else {
            panic!("wrong response")
        };
        assert_eq!(
            relay,
            match mode {
                LocalTxRelayMode::Clearnet => TxRelay::Local,
                LocalTxRelayMode::I2pOnly => TxRelay::LocalI2pOnly,
                LocalTxRelayMode::I2pThenClearnet => TxRelay::LocalI2pThenClearnet,
            }
        );
        pool.manager
            .new_block(
                original
                    .tx
                    .prefix()
                    .inputs
                    .iter()
                    .map(|input| match input {
                        monero_oxide::transaction::Input::ToKey { key_image, .. } => {
                            key_image.to_bytes()
                        }
                        monero_oxide::transaction::Input::Gen(_) => unreachable!(),
                    })
                    .collect(),
            )
            .await
            .unwrap();
        assert_eq!(
            pool.manager
                .submit(next.clone(), TxState::Local, None)
                .await
                .unwrap(),
            TxAdmission::Inserted
        );
        pool.close().await;
    }
}

#[tokio::test]
async fn concurrent_submissions_cannot_overbook_the_pool() {
    let first: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
    let second: TransactionVerificationData = cuprate_test_utils::data::TX_V1_SIG2
        .clone()
        .try_into()
        .unwrap();
    let cap = first.tx_blob.len().max(second.tx_blob.len());
    let pool = PoolFixture::new(cap, LocalTxRelayMode::I2pOnly, None)
        .await
        .unwrap();
    let source = cuprate_p2p_core::TransactionCounters::default();
    let (a, b) = tokio::join!(
        pool.manager
            .submit(first, TxState::Local, Some(source.clone())),
        pool.manager
            .submit(second, TxState::Local, Some(source.clone())),
    );
    assert!(matches!(
        (&a, &b),
        (Ok(TxAdmission::Inserted), Err(IncomingTxError::PoolFull))
            | (Err(IncomingTxError::PoolFull), Ok(TxAdmission::Inserted))
    ));
    let TxpoolReadResponse::Backlog(backlog) = pool
        .read
        .clone()
        .oneshot(TxpoolReadRequest::Backlog)
        .await
        .unwrap()
    else {
        panic!("wrong response")
    };
    assert!(backlog.iter().map(|tx| tx.blob_size).sum::<usize>() <= cap);
    assert_eq!(backlog.len(), 1);
    assert_eq!(backlog[0].relay, TxRelay::LocalI2pOnly);
    assert_eq!(source.snapshot().accepted, 1);
    pool.close().await;
}

#[tokio::test]
async fn zero_and_exact_capacity_and_saved_private_pool_are_enforced() {
    let tx: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
    for cap in [0, tx.tx_blob.len() - 1, tx.tx_blob.len()] {
        let pool = PoolFixture::new(cap, LocalTxRelayMode::I2pOnly, None)
            .await
            .unwrap();
        let result = pool.manager.submit(tx.clone(), TxState::Local, None).await;
        if cap == tx.tx_blob.len() {
            assert_eq!(result.unwrap(), TxAdmission::Inserted);
        } else {
            assert!(matches!(result, Err(IncomingTxError::PoolFull)));
        }
        pool.close().await;
    }
    let pool = PoolFixture::new(
        tx.tx_blob.len(),
        LocalTxRelayMode::Clearnet,
        Some((tx.clone(), TxRelay::LocalI2pOnly)),
    )
    .await
    .unwrap();
    assert_eq!(
        pool.manager
            .submit(tx.clone(), TxState::Local, None)
            .await
            .unwrap(),
        TxAdmission::AlreadyKnown
    );
    let next: TransactionVerificationData = cuprate_test_utils::data::TX_V1_SIG2
        .clone()
        .try_into()
        .unwrap();
    assert!(matches!(
        pool.manager.submit(next, TxState::Local, None).await,
        Err(IncomingTxError::PoolFull)
    ));
    pool.close().await;
    let result = PoolFixture::new(
        tx.tx_blob.len() - 1,
        LocalTxRelayMode::Clearnet,
        Some((tx, TxRelay::LocalI2pOnly)),
    )
    .await;
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("persisted transaction pool"));
}

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
            .submit(TX_V2_RCT3.clone().try_into().unwrap(), state, origin)
            .await
            .unwrap();
        for origin in [source.clone(), echo_source.clone()] {
            // Already-known transactions succeed idempotently, but never gain another count.
            assert_eq!(
                manager
                    .submit(
                        TX_V2_RCT3.clone().try_into().unwrap(),
                        TxState::Fluff,
                        Some(origin),
                    )
                    .await
                    .unwrap(),
                TxAdmission::AlreadyKnown
            );
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
