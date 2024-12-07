//! Handles processing transactions and events from Radix DLT.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;
mod event;
mod stream_handler;
mod transaction;

pub use error::Error;
pub use event::Event;
pub use stream_handler::Handler;
pub use transaction::Transaction;

use std::sync::Arc;
use std::sync::LazyLock;

use proven_radix_gateway_sdk::types::{
    LedgerStateSelector, LedgerStateSelectorInner, StreamTransactionsRequest,
    StreamTransactionsRequestKindFilter, StreamTransactionsRequestOrder, TransactionDetailsOptIns,
};
use proven_radix_gateway_sdk::{build_client, Client};
use proven_stream::Stream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, trace};

static GATEWAY_OPT_INS: LazyLock<TransactionDetailsOptIns> =
    LazyLock::new(|| TransactionDetailsOptIns {
        affected_global_entities: true,
        balance_changes: true,
        raw_hex: true,
        manifest_instructions: true,
        receipt_costing_parameters: true,
        receipt_events: true,
        receipt_fee_destination: true,
        receipt_fee_source: true,
        receipt_fee_summary: true,
        receipt_output: true,
        receipt_state_changes: true,
    });

/// A Radix Stream that processes transactions and events from Radix DLT.
pub struct RadixStream<TS, ES>
where
    ES: Stream<Handler>,
    TS: Stream<Handler>,
{
    client: Client,
    event_stream: ES,
    last_state_version: Arc<Mutex<Option<u64>>>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
    transaction_stream: TS,
}

/// Options for creating a new `RadixStream`.
pub struct RadixStreamOptions<TS, ES>
where
    ES: Stream<Handler>,
    TS: Stream<Handler>,
{
    /// The event stream to publish events to.
    pub event_stream: ES,

    /// The origin of the Radix Gateway.
    pub radix_gateway_origin: &'static str,

    /// The transaction stream to publish transactions to.
    pub transaction_stream: TS,
}

type StartResult<TSE, ESE> = Result<JoinHandle<Result<(), Error<TSE, ESE>>>, Error<TSE, ESE>>;

impl<TS, ES> RadixStream<TS, ES>
where
    ES: Stream<Handler>,
    TS: Stream<Handler>,
{
    /// Creates a new `RadixStream`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the transaction stream fails to provide the last message
    /// or if the last transaction cannot be converted into a `Transaction`.
    pub async fn new(
        RadixStreamOptions {
            event_stream,
            radix_gateway_origin,
            transaction_stream,
        }: RadixStreamOptions<TS, ES>,
    ) -> Result<Self, Error<TS::Error, ES::Error>> {
        let client = build_client(radix_gateway_origin, None, None);

        let last_state_version = if let Some(last_transaction) = transaction_stream
            .last_message()
            .await
            .map_err(Error::TransactionStream)?
        {
            let transaction = Transaction::try_from(last_transaction)?;
            let state_version = transaction.state_version();
            info!("Starting from state version: {}", state_version);
            Some(state_version)
        } else {
            None
        };

        Ok(Self {
            client,
            event_stream,
            last_state_version: Arc::new(Mutex::new(last_state_version)),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            transaction_stream,
        })
    }

    /// Returns the current state version.
    ///
    /// This function returns the current state version of the Radix stream, if available.
    pub async fn current_state_version(&self) -> Option<u64> {
        *self.last_state_version.lock().await
    }

    async fn poll_transactions(
        client: Client,
        event_stream: ES,
        transaction_stream: TS,
        last_state_version: Arc<Mutex<Option<u64>>>,
        shutdown_token: CancellationToken,
    ) -> Result<(), Error<TS::Error, ES::Error>> {
        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            let current_version = *last_state_version.lock().await;
            if let Some(state_version) = current_version {
                info!(
                    "Polling for transactions from state version: {}",
                    state_version
                );
            } else {
                info!("Polling for transactions ");
            }

            let ledger_state_version_selector = if let Some(state_version) = current_version {
                let state_version_i64: i64 = state_version
                    .try_into()
                    .map_err(|e| Error::TryFromInt("state_version", e))?;

                LedgerStateSelector(Some(LedgerStateSelectorInner {
                    epoch: None,
                    round: None,
                    state_version: Some(state_version_i64),
                    timestamp: None,
                }))
            } else {
                LedgerStateSelector(None)
            };

            let body: proven_radix_gateway_sdk::types::builder::StreamTransactionsRequest =
                StreamTransactionsRequest::builder()
                    .kind_filter(StreamTransactionsRequestKindFilter::User)
                    .opt_ins(GATEWAY_OPT_INS.clone())
                    .from_ledger_state(ledger_state_version_selector)
                    .limit_per_page(100)
                    .order(StreamTransactionsRequestOrder::Asc);

            match client.stream_transactions().body(body).send().await {
                Ok(response) => {
                    let transactions: Vec<Transaction> = response
                        .clone()
                        .items
                        .into_iter()
                        .map(Transaction::from)
                        .collect();

                    debug!("Received {} new transactions", transactions.len());
                    let is_at_head = transactions.len() < 100;

                    if let Some(last_transaction) = transactions.last() {
                        *last_state_version.lock().await = Some(last_transaction.state_version());
                    } else {
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                        continue;
                    }

                    Self::process_transactions(
                        transactions,
                        event_stream.clone(),
                        transaction_stream.clone(),
                        last_state_version.clone(),
                    )
                    .await?;

                    if is_at_head {
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    } else {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
                Err(e) => {
                    info!("Failed to poll for transactions: {:?}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            }
        }
        Ok(())
    }

    async fn process_transactions(
        transactions: Vec<Transaction>,
        event_stream: ES,
        transaction_stream: TS,
        last_state_version: Arc<Mutex<Option<u64>>>,
    ) -> Result<(), Error<TS::Error, ES::Error>> {
        for transaction in &transactions {
            trace!("Publishing transaction: {:?}", transaction);
            transaction_stream
                .publish(transaction.clone().try_into().unwrap())
                .await
                .map_err(Error::TransactionStream)?;

            for event in &transaction.events() {
                trace!("Publishing event: {:?}", event);
                event_stream
                    .publish(event.clone().try_into().unwrap())
                    .await
                    .map_err(Error::EventStream)?;
            }

            *last_state_version.lock().await = Some(transaction.state_version());
        }

        Ok(())
    }

    /// Starts the `RadixStream`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the `RadixStream` has already been started.
    pub fn start(&self) -> StartResult<TS::Error, ES::Error> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let client = self.client.clone();
        let event_stream = self.event_stream.clone();
        let shutdown_token = self.shutdown_token.clone();
        let transaction_stream = self.transaction_stream.clone();
        let last_state_version = self.last_state_version.clone();
        let handle = self.task_tracker.spawn(Self::poll_transactions(
            client,
            event_stream,
            transaction_stream,
            last_state_version,
            shutdown_token,
        ));

        self.task_tracker.close();

        Ok(handle)
    }

    /// Shuts down the `RadixStream`.
    pub async fn shutdown(&self) {
        info!("radix-stream shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("radix-stream shutdown complete");
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use proven_stream_memory::MemoryStream;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_new_radix_stream() {
        let event_stream = MemoryStream::new();
        let transaction_stream = MemoryStream::new();
        let options = RadixStreamOptions {
            event_stream,
            radix_gateway_origin: "https://mainnet.radixdlt.com",
            transaction_stream,
        };

        let radix_stream = RadixStream::new(options).await.unwrap();
        assert!(radix_stream.current_state_version().await.is_none());
    }

    #[tokio::test]
    async fn test_start_radix_stream() {
        let event_stream = MemoryStream::new();
        let transaction_stream = MemoryStream::new();
        let options = RadixStreamOptions {
            event_stream,
            radix_gateway_origin: "https://mainnet.radixdlt.com",
            transaction_stream,
        };

        let radix_stream = RadixStream::new(options).await.unwrap();
        let handle = radix_stream.start().unwrap();
        assert!(!handle.is_finished());
    }

    #[tokio::test]
    async fn test_current_state_version() {
        let event_stream = MemoryStream::new();
        let transaction_stream = MemoryStream::new();
        let options = RadixStreamOptions {
            event_stream,
            radix_gateway_origin: "https://mainnet.radixdlt.com",
            transaction_stream,
        };

        let radix_stream = RadixStream::new(options).await.unwrap();
        assert!(radix_stream.current_state_version().await.is_none());

        // Simulate a transaction to update the state version
        let mut last_state_version = radix_stream.last_state_version.lock().await;
        *last_state_version = Some(42);
        drop(last_state_version);

        assert_eq!(radix_stream.current_state_version().await, Some(42));
    }

    #[tokio::test]
    async fn test_shutdown_radix_stream() {
        let event_stream = MemoryStream::new();
        let transaction_stream = MemoryStream::new();
        let options = RadixStreamOptions {
            event_stream,
            radix_gateway_origin: "https://mainnet.radixdlt.com",
            transaction_stream,
        };

        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let radix_stream = RadixStream::new(options).await.unwrap();
            let handle = radix_stream.start().unwrap();

            radix_stream.shutdown().await;
            assert!(handle.is_finished());
        })
        .await;

        assert!(result.is_ok(), "Shutdown test timed out");
    }
}
