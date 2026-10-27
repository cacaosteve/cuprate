//! Bounded, process-lifetime transaction diagnostics for a single network zone.

use std::sync::{Arc, Mutex};

use cuprate_wire::{Message, ProtocolMessage};

/// A consistent snapshot of one zone's transaction counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionCounterSnapshot {
    /// Transaction blobs in decoded `NewTransactions` messages, including duplicates.
    pub received: u64,
    /// New transactions successfully inserted into the txpool from this zone.
    pub accepted: u64,
    /// Transaction blobs successfully written to peer sockets in this zone.
    /// Fan-out and successful re-relays count again; this is not a remote acknowledgement.
    pub relayed: u64,
}

/// Shared zone totals, independent of individual connections and txpool contents.
/// No transaction hashes, peer identities, or per-transaction timestamps are retained.
#[derive(Debug, Clone, Default)]
pub struct TransactionCounters(Arc<Mutex<TransactionCounterSnapshot>>);

impl TransactionCounters {
    /// Read all three totals under one lock. Counters saturate rather than wrap.
    pub fn snapshot(&self) -> TransactionCounterSnapshot {
        *self.0.lock().unwrap()
    }

    /// Record a new txpool insertion, not verification, queueing, or a duplicate.
    pub fn record_accepted(&self) {
        let mut totals = self.0.lock().unwrap();
        totals.accepted = totals.accepted.saturating_add(1);
    }

    /// Record decoded P2P traffic before validation, including ignored messages.
    pub(crate) fn record_received(&self, message: &Message) {
        let count = transaction_count(message);
        if count != 0 {
            let mut totals = self.0.lock().unwrap();
            totals.received = totals.received.saturating_add(count);
        }
    }

    /// Called only after the transport sink successfully sends and flushes a message.
    pub(crate) fn record_relayed(&self, count: u64) {
        if count != 0 {
            let mut totals = self.0.lock().unwrap();
            totals.relayed = totals.relayed.saturating_add(count);
        }
    }
}

pub(crate) const fn transaction_count(message: &Message) -> u64 {
    match message {
        Message::Protocol(ProtocolMessage::NewTransactions(txs)) => {
            cuprate_helper::cast::usize_to_u64(txs.txs.len())
        }
        Message::Request(_) | Message::Response(_) | Message::Protocol(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_totals_survive_handle_drops_and_saturate() {
        let counters = TransactionCounters::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let counters = counters.clone();
                scope.spawn(move || {
                    for _ in 0..1000 {
                        counters.record_accepted();
                    }
                });
            }
        });
        assert_eq!(counters.snapshot().accepted, 4000);
        counters.0.lock().unwrap().accepted = u64::MAX;
        counters.record_accepted();
        assert_eq!(counters.snapshot().accepted, u64::MAX);
        assert_eq!(
            TransactionCounters::default().snapshot(),
            TransactionCounterSnapshot::default()
        );
    }
}
