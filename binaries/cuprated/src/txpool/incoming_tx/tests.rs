use cuprate_test_utils::data::TX_V2_RCT3;
use cuprate_txpool::service::{init_with_pool, interface::TxpoolWriteRequest};

use super::*;
use crate::txpool::dandelion::tx_store::TxStoreService;

#[tokio::test]
async fn stored_private_origin_blocks_echo_before_any_in_memory_local_marker() {
    for relay in [
        TxRelay::LocalI2pOnly,
        TxRelay::LocalI2pThenClearnet,
        TxRelay::Unknown,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let database = fjall::Database::builder(directory.path()).open().unwrap();
        let workers = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let (mut read, mut write) = init_with_pool(database, workers).unwrap();
        let tx: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
        let tx_id = tx.tx_hash;
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
        let (routes, mut route_rx) = mpsc::unbounded_channel();
        let router = tower::service_fn(
            move |req: cuprate_dandelion_tower::DandelionRouteReq<
                DandelionTx,
                CrossNetworkInternalPeerId,
            >| {
                routes.send((req.tx.1, req.state)).unwrap();
                std::future::ready(Ok::<_, cuprate_dandelion_tower::DandelionRouterError>(
                    State::Stem,
                ))
            },
        );
        let (promote_tx, _promote_rx) = mpsc::unbounded_channel();
        let (mut pool, task) = cuprate_dandelion_tower::pool::start_dandelion_pool_manager(
            8,
            router,
            TxStoreService {
                txpool_read_handle: read.clone(),
                txpool_write_handle: write.clone(),
                promote_tx,
            },
            cuprate_dandelion_tower::DandelionConfig {
                time_between_hop: std::time::Duration::from_millis(175),
                epoch_duration: std::time::Duration::from_mins(10),
                fluff_probability: 0.12,
                graph: cuprate_dandelion_tower::Graph::FourRegular,
            },
        );
        for state in [
            TxState::Stem {
                from: CrossNetworkInternalPeerId::I2p(
                    cuprate_p2p_core::client::InternalPeerID::Unknown([9; 16]),
                ),
            },
            TxState::Fluff,
        ] {
            rerelay_stem_tx(&tx_id, state, &mut read, &mut pool)
                .await
                .unwrap();
            assert!(
                route_rx.try_recv().is_err(),
                "peer echo must not create a local retry or fallback"
            );
        }
        // A genuine local resubmission preserves the original saved policy, not
        // the node's current configuration. Legacy unknowns remain quarantined.
        rerelay_stem_tx(&tx_id, TxState::Local, &mut read, &mut pool)
            .await
            .unwrap();
        if relay == TxRelay::Unknown {
            assert!(route_rx.try_recv().is_err());
        } else {
            assert_eq!(route_rx.try_recv().unwrap(), (relay, TxState::Local));
        }
        drop(pool);
        task.await.unwrap();
    }
}
