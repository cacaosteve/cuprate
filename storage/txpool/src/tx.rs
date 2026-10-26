//! Transaction metadata.

/// Persisted origin and local relay policy. Independent of the current node configuration.
#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum TxRelay {
    /// A record written before origin metadata was available. Never infer local origin.
    #[default]
    Unknown,
    /// Received from a peer; use the ordinary Dandelion relay path.
    Peer,
    /// Submitted locally with the normal relay policy.
    Local,
    /// Submitted locally with I2P preferred, allowing explicit clearnet fallback.
    LocalI2pThenClearnet,
    /// An I2P-first local transaction which has actually used clearnet fallback.
    /// Resume ordinary clearnet Dandelion recovery, including after restart.
    LocalI2pClearnet,
    /// Submitted locally with no clearnet fallback.
    LocalI2pOnly,
}

impl TxRelay {
    /// Whether local routing must be retried rather than fluffed at embargo expiry.
    pub const fn retry_local(self) -> bool {
        matches!(self, Self::LocalI2pThenClearnet | Self::LocalI2pOnly)
    }
}

/// Data about a transaction in the pool.
///
/// Used in [`TxpoolReadResponse::Backlog`](crate::service::interface::TxpoolReadResponse::Backlog).
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TxEntry {
    /// The transaction's ID (hash).
    pub id: [u8; 32],
    /// The transaction's weight.
    pub weight: usize,
    /// The transaction's fee.
    pub fee: u64,
    /// If the tx is in the private pool.
    pub private: bool,
    /// The original submission source and snapshotted relay policy.
    pub relay: TxRelay,
    /// The UNIX timestamp when the transaction was received.
    pub received_at: u64,
}
