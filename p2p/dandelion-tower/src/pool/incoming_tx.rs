//! Contains [`IncomingTx`] and [`IncomingTxBuilder`]
use crate::{State, TxState};

/// An incoming transaction that has gone through the preprocessing stage.
pub struct IncomingTx<Tx, TxId, PeerId> {
    /// The transaction.
    pub(crate) tx: Tx,
    /// The transaction ID.
    pub(crate) tx_id: TxId,
    /// The routing state of the transaction.
    pub(crate) routing_state: TxState<PeerId>,
    pub(crate) local_embargo_policy: Option<super::LocalTxEmbargoPolicy>,
}

/// An [`IncomingTx`] builder.
///
/// The const generics here are used to restrict what methods can be called.
///
/// - `RS`: routing state; a `bool` for if the routing state is set
/// - `DBS`: database state; a `bool` for if the state in the DB is set
pub struct IncomingTxBuilder<const RS: bool, const DBS: bool, Tx, TxId, PeerId> {
    /// The transaction.
    tx: Tx,
    /// The transaction ID.
    tx_id: TxId,
    /// The routing state of the transaction.
    routing_state: Option<TxState<PeerId>>,
    /// The state of this transaction in the DB.
    state_in_db: Option<State>,
    local_embargo_policy: Option<super::LocalTxEmbargoPolicy>,
}

impl<Tx, TxId, PeerId> IncomingTxBuilder<false, false, Tx, TxId, PeerId> {
    /// Creates a new [`IncomingTxBuilder`].
    pub const fn new(tx: Tx, tx_id: TxId) -> Self {
        Self {
            tx,
            tx_id,
            routing_state: None,
            state_in_db: None,
            local_embargo_policy: None,
        }
    }
}

impl<const DBS: bool, Tx, TxId, PeerId> IncomingTxBuilder<false, DBS, Tx, TxId, PeerId> {
    /// Adds the routing state to the builder.
    ///
    /// The routing state is the origin of this transaction from our perspective.
    pub fn with_routing_state(
        self,
        state: TxState<PeerId>,
    ) -> IncomingTxBuilder<true, DBS, Tx, TxId, PeerId> {
        IncomingTxBuilder {
            tx: self.tx,
            tx_id: self.tx_id,
            routing_state: Some(state),
            state_in_db: self.state_in_db,
            local_embargo_policy: self.local_embargo_policy,
        }
    }
}

impl<const RS: bool, Tx, TxId, PeerId> IncomingTxBuilder<RS, false, Tx, TxId, PeerId> {
    /// Adds the database state to the builder.
    ///
    /// If the transaction is not in the DB already then the state should be [`None`].
    pub fn with_state_in_db(
        self,
        state: Option<State>,
    ) -> IncomingTxBuilder<RS, true, Tx, TxId, PeerId> {
        IncomingTxBuilder {
            tx: self.tx,
            tx_id: self.tx_id,
            routing_state: self.routing_state,
            state_in_db: state,
            local_embargo_policy: self.local_embargo_policy,
        }
    }
}

impl<const RS: bool, const DBS: bool, Tx, TxId, PeerId>
    IncomingTxBuilder<RS, DBS, Tx, TxId, PeerId>
{
    /// Override the default embargo policy with this transaction's persisted policy.
    #[must_use]
    pub const fn with_local_tx_embargo_policy(
        mut self,
        policy: super::LocalTxEmbargoPolicy,
    ) -> Self {
        self.local_embargo_policy = Some(policy);
        self
    }
}

impl<Tx, TxId, PeerId> IncomingTxBuilder<true, true, Tx, TxId, PeerId> {
    /// Builds the [`IncomingTx`].
    ///
    /// If this returns [`None`] then the transaction does not need to be given to the dandelion pool
    /// manager.
    pub fn build(self) -> Option<IncomingTx<Tx, TxId, PeerId>> {
        let routing_state = self.routing_state.unwrap();

        if self.state_in_db == Some(State::Fluff) {
            return None;
        }

        Some(IncomingTx {
            tx: self.tx,
            tx_id: self.tx_id,
            routing_state,
            local_embargo_policy: self.local_embargo_policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        IncomingTxBuilder::new(1, 2)
            .with_routing_state(TxState::Stem { from: 3 })
            .with_state_in_db(None)
            .build();

        IncomingTxBuilder::new(1, 2)
            .with_state_in_db(None)
            .with_routing_state(TxState::Stem { from: 3 })
            .build();
    }
}
