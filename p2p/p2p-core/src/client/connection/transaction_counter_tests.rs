use std::{convert::Infallible, future::pending};

use cuprate_wire::{
    protocol::NewTransactions, AdminRequestMessage, BucketError, Message, ProtocolMessage,
};
use futures::{sink, FutureExt, SinkExt};

use super::send_message;
use crate::TransactionCounters;

fn transactions() -> Message {
    Message::Protocol(ProtocolMessage::NewTransactions(NewTransactions {
        // Count blobs, even duplicates or malformed transactions, not message envelopes.
        txs: vec![vec![1].into(), vec![1].into(), vec![2].into()],
        dandelionpp_fluff: false,
        padding: Default::default(),
    }))
}

fn never(error: Infallible) -> BucketError {
    match error {}
}

#[tokio::test]
async fn only_successful_flushed_notifications_count_as_relayed() {
    let counters = TransactionCounters::default();
    let mut good = sink::drain().sink_map_err(never);
    send_message(&mut good, transactions(), &counters)
        .await
        .unwrap();
    send_message(&mut good, transactions(), &counters)
        .await
        .unwrap();
    send_message(
        &mut good,
        Message::Request(AdminRequestMessage::Ping),
        &counters,
    )
    .await
    .unwrap();
    assert_eq!(counters.snapshot().relayed, 6);
    assert_eq!(counters.snapshot().received, 0);
    assert_eq!(counters.snapshot().accepted, 0);

    let mut bad = sink::unfold((), |(), _| {
        std::future::ready(Err::<(), _>(BucketError::IO(
            std::io::ErrorKind::BrokenPipe.into(),
        )))
    });
    assert!(send_message(&mut bad, transactions(), &counters)
        .await
        .is_err());
    assert_eq!(counters.snapshot().relayed, 6);

    // Cancellation/timeout while flushing must not count a queued/partially written message.
    let mut blocked = Box::pin(sink::unfold((), |(), _| {
        pending::<Result<(), BucketError>>()
    }));
    assert!(send_message(&mut blocked, transactions(), &counters)
        .now_or_never()
        .is_none());
    assert_eq!(counters.snapshot().relayed, 6);
}

#[test]
fn receipt_counts_blobs_not_envelopes_and_does_not_imply_acceptance() {
    let counters = TransactionCounters::default();
    counters.record_received(&transactions());
    counters.record_received(&Message::Request(AdminRequestMessage::Ping));
    assert_eq!(counters.snapshot().received, 3);
    assert_eq!(counters.snapshot().accepted, 0);
    assert_eq!(counters.snapshot().relayed, 0);
}
