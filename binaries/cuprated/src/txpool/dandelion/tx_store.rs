use std::{
    future::ready,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;
use tower::{Service, ServiceExt};

use cuprate_dandelion_tower::{
    traits::{StoredTx, TxStoreRequest, TxStoreResponse},
    State,
};
use cuprate_txpool::{
    service::{
        interface::{TxpoolReadRequest, TxpoolReadResponse, TxpoolWriteRequest},
        TxpoolReadHandle, TxpoolWriteHandle,
    },
    TxPoolError,
};

use super::{DandelionTx, TxId};

/// The dandelion tx-store service.
///
/// This is just mapping the interface [`cuprate_dandelion_tower`] wants to what [`cuprate_txpool`] provides.
pub(crate) struct TxStoreService {
    pub txpool_read_handle: TxpoolReadHandle,
    pub txpool_write_handle: TxpoolWriteHandle,
    pub promote_tx: mpsc::UnboundedSender<[u8; 32]>,
}

impl Service<TxStoreRequest<TxId>> for TxStoreService {
    type Response = TxStoreResponse<DandelionTx>;
    type Error = tower::BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: TxStoreRequest<TxId>) -> Self::Future {
        match req {
            TxStoreRequest::RecordClearnetFallback(tx_id) => self
                .txpool_write_handle
                .clone()
                .oneshot(TxpoolWriteRequest::RecordClearnetFallback(tx_id))
                .map(|res| res.map(|_| TxStoreResponse::Ok).map_err(Into::into))
                .boxed(),
            TxStoreRequest::Get(tx_id) => self
                .txpool_read_handle
                .clone()
                .oneshot(TxpoolReadRequest::TxBlob(tx_id))
                .map(|res| match res {
                    Ok(TxpoolReadResponse::TxBlob {
                        tx_blob,
                        state_stem,
                        relay,
                    }) => {
                        let state = if state_stem {
                            State::Stem
                        } else {
                            State::Fluff
                        };

                        let tx = DandelionTx(Bytes::from(tx_blob), relay);
                        let local_embargo_policy = Some(tx.embargo_policy());
                        Ok(TxStoreResponse::Transaction(Some(StoredTx {
                            tx,
                            state,
                            local_embargo_policy,
                        })))
                    }
                    Err(TxPoolError::NotFound) => Ok(TxStoreResponse::Transaction(None)),
                    Err(e) => Err(e.into()),
                    Ok(_) => unreachable!(),
                })
                .boxed(),
            TxStoreRequest::Promote(tx_id) => ready(
                self.promote_tx
                    .send(tx_id)
                    .map_err(Into::into)
                    .map(|()| TxStoreResponse::Ok),
            )
            .boxed(),
        }
    }
}
