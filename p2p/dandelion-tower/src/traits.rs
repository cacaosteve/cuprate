/// A request to diffuse a transaction to all connected peers.
///
/// This crate does not handle diffusion it is left to implementers.
pub struct DiffuseRequest<Tx>(pub Tx);

/// A request sent to a single peer to stem this transaction.
pub struct StemRequest<Tx>(pub Tx);

#[cfg(feature = "txpool")]
/// A request sent to the backing transaction pool storage.
pub enum TxStoreRequest<TxId> {
    /// A request to retrieve a `Tx` with the given Id from the pool, should not remove that tx from the pool.
    ///
    /// Must return [`TxStoreResponse::Transaction`]
    Get(TxId),
    /// Promote a transaction from the stem pool to the public pool.
    ///
    /// If the tx is already in the fluff pool do nothing.
    ///
    /// This should not error if the tx isn't in the pool at all.
    Promote(TxId),
    /// Record an I2P-first transaction's actual clearnet fallback before enabling
    /// standard embargo recovery. Must not downgrade a strict I2P-only policy.
    RecordClearnetFallback(TxId),
}

#[cfg(feature = "txpool")]
/// An authoritative transaction snapshot returned by the backing pool.
pub struct StoredTx<Tx> {
    pub tx: Tx,
    pub state: crate::State,
    /// Persisted policy takes precedence over a possibly stale incoming request.
    /// `None` leaves policy selection to the incoming request / pool default.
    pub local_embargo_policy: Option<crate::pool::LocalTxEmbargoPolicy>,
}

#[cfg(feature = "txpool")]
/// A response sent back from the backing transaction pool.
pub enum TxStoreResponse<Tx> {
    /// A generic ok response.
    Ok,
    /// A response containing a requested transaction.
    Transaction(Option<StoredTx<Tx>>),
}
