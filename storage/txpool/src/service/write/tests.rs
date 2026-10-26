use super::*;
use crate::TxRelay;
use cuprate_test_utils::data::TX_V2_RCT3;
use std::sync::{Arc, Barrier};

#[test]
fn recorded_fallback_survives_reopen_and_never_downgrades_i2p_only() {
    for relay in [
        TxRelay::LocalI2pThenClearnet,
        TxRelay::LocalI2pClearnet,
        TxRelay::LocalI2pOnly,
        TxRelay::Local,
        TxRelay::Peer,
        TxRelay::Unknown,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let tx: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
        let id = tx.tx_hash;
        let allowed = matches!(
            relay,
            TxRelay::LocalI2pThenClearnet | TxRelay::LocalI2pClearnet
        );
        {
            let db = TxpoolDatabase::open_with_database(
                fjall::Database::builder(directory.path()).open().unwrap(),
            )
            .unwrap();
            assert_eq!(
                handle_txpool_request(
                    &db,
                    TxpoolWriteRequest::AddTransaction {
                        tx: Box::new(tx),
                        state_stem: true,
                        relay,
                    }
                )
                .unwrap(),
                TxpoolWriteResponse::AddTransaction(None)
            );
            for _ in 0..2 {
                let result =
                    handle_txpool_request(&db, TxpoolWriteRequest::RecordClearnetFallback(id));
                if allowed {
                    assert_eq!(result.unwrap(), TxpoolWriteResponse::Ok);
                } else {
                    assert!(matches!(result, Err(TxPoolError::InvalidRelayTransition)));
                }
            }
            db.fjall_database
                .persist(fjall::PersistMode::SyncAll)
                .unwrap();
        }
        let db = TxpoolDatabase::open_with_database(
            fjall::Database::builder(directory.path()).open().unwrap(),
        )
        .unwrap();
        let info = db.tx_infos.get(id).unwrap().unwrap();
        let info: TransactionInfo = bytemuck::pod_read_unaligned(info.as_ref());
        assert_eq!(
            info.flags.relay(),
            if allowed {
                TxRelay::LocalI2pClearnet
            } else {
                relay
            }
        );
        assert!(
            info.flags.private(),
            "fallback must not prematurely expose a stem transaction"
        );
    }
}

#[test]
fn fallback_racing_with_eviction_or_new_block_does_not_resurrect_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(
        TxpoolDatabase::open_with_database(
            fjall::Database::builder(directory.path()).open().unwrap(),
        )
        .unwrap(),
    );
    for remove_for_new_block in [false, true] {
        for _ in 0..10 {
            let tx: TransactionVerificationData = TX_V2_RCT3.clone().try_into().unwrap();
            let id = tx.tx_hash;
            let removal = if remove_for_new_block {
                TxpoolWriteRequest::NewBlock {
                    spent_key_images: tx
                        .tx
                        .prefix()
                        .inputs
                        .iter()
                        .map(|input| match input {
                            Input::ToKey { key_image, .. } => key_image.to_bytes(),
                            Input::Gen(_) => unreachable!(),
                        })
                        .collect(),
                }
            } else {
                TxpoolWriteRequest::RemoveTransaction(id)
            };
            handle_txpool_request(
                &db,
                TxpoolWriteRequest::AddTransaction {
                    tx: Box::new(tx),
                    state_stem: true,
                    relay: TxRelay::LocalI2pThenClearnet,
                },
            )
            .unwrap();
            let start = Arc::new(Barrier::new(3));
            let fallback_db = Arc::clone(&db);
            let fallback_start = Arc::clone(&start);
            let fallback = std::thread::spawn(move || {
                fallback_start.wait();
                handle_txpool_request(&fallback_db, TxpoolWriteRequest::RecordClearnetFallback(id))
                    .unwrap();
            });
            let remove_db = Arc::clone(&db);
            let remove_start = Arc::clone(&start);
            let remove = std::thread::spawn(move || {
                remove_start.wait();
                handle_txpool_request(&remove_db, removal).unwrap();
            });
            start.wait();
            fallback.join().unwrap();
            remove.join().unwrap();
            assert!(db.tx_infos.get(id).unwrap().is_none());
            assert!(db.tx_blobs.get(id).unwrap().is_none());
            // A delayed fallback is also a no-op after removal.
            handle_txpool_request(&db, TxpoolWriteRequest::RecordClearnetFallback(id)).unwrap();
            assert!(db.tx_infos.get(id).unwrap().is_none());
        }
    }
}
