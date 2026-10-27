use cuprate_consensus_rules::{transactions::TransactionError, ConsensusError};
use tower::{Service, ServiceExt};

use cuprate_types::TxRelayChecks;

use crate::txpool::{IncomingTxError, IncomingTxHandler, IncomingTxs, RelayRuleError};

pub(crate) async fn handle_incoming_txs(
    tx_handler: &mut IncomingTxHandler,
    incoming_txs: IncomingTxs,
) -> Result<TxRelayChecks, IncomingTxError> {
    let resp = tx_handler.ready().await?.call(incoming_txs).await;
    relay_checks(resp)
}

fn relay_checks(resp: Result<(), IncomingTxError>) -> Result<TxRelayChecks, IncomingTxError> {
    Ok(match resp {
        Ok(()) => TxRelayChecks::empty(),
        Err(IncomingTxError::Consensus(ConsensusError::Transaction(tx_err))) => match &tx_err {
            TransactionError::TooBig => TxRelayChecks::TOO_BIG,
            TransactionError::KeyImageSpent => TxRelayChecks::DOUBLE_SPEND,

            TransactionError::OutputNotValidPoint
            | TransactionError::OutputTypeInvalid
            | TransactionError::ZeroOutputForV1
            | TransactionError::NonZeroOutputForV2
            | TransactionError::OutputsOverflow
            | TransactionError::OutputsTooHigh => TxRelayChecks::INVALID_OUTPUT,

            TransactionError::MoreThanOneMixableInputWithUnmixable
            | TransactionError::InvalidNumberOfOutputs
            | TransactionError::InputDoesNotHaveExpectedNumbDecoys
            | TransactionError::IncorrectInputType
            | TransactionError::InputsAreNotOrdered
            | TransactionError::InputsOverflow
            | TransactionError::NoInputs => TxRelayChecks::INVALID_INPUT,

            TransactionError::KeyImageIsNotInPrimeSubGroup
            | TransactionError::AmountNotDecomposed
            | TransactionError::DuplicateRingMember
            | TransactionError::OneOrMoreRingMembersLocked
            | TransactionError::RingMemberNotFoundOrInvalid
            | TransactionError::RingSignatureIncorrect
            | TransactionError::TransactionVersionInvalid
            | TransactionError::BatchVerificationFailed
            | TransactionError::RingCTError(_) => {
                return Err(IncomingTxError::Consensus(ConsensusError::Transaction(
                    tx_err,
                )))
            }
        },
        Err(IncomingTxError::RelayRule(e)) => match e {
            RelayRuleError::NonZeroTimelock => TxRelayChecks::NONZERO_UNLOCK_TIME,
            RelayRuleError::ExtraFieldTooLarge => TxRelayChecks::TX_EXTRA_TOO_BIG,
            RelayRuleError::FeeBelowMinimum => TxRelayChecks::FEE_TOO_LOW,
        },
        Err(IncomingTxError::DuplicateTransaction | IncomingTxError::PoolDoubleSpend) => {
            TxRelayChecks::DOUBLE_SPEND
        }
        Err(e) => return Err(e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_admission_failures_never_map_to_rpc_success() {
        assert_eq!(
            relay_checks(Err(IncomingTxError::PoolDoubleSpend))
                .unwrap()
                .bits(),
            TxRelayChecks::DOUBLE_SPEND.bits()
        );
        for error in [
            IncomingTxError::PoolFull,
            IncomingTxError::InProgress,
            IncomingTxError::PoolSubmissionFailed("injected failure".to_owned()),
            IncomingTxError::ChannelClosed,
            IncomingTxError::DoNotRelayUnsupported,
        ] {
            assert!(relay_checks(Err(error)).is_err());
        }
        assert!(relay_checks(Ok(())).unwrap().is_empty());
    }
}
