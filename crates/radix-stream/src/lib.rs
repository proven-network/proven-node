mod error;
mod stream_handler;
mod transaction;

pub use error::Error;
pub use stream_handler::Handler;
pub use transaction::Transaction;

use std::sync::Arc;

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

pub struct RadixStreamOptions<TS, ES>
where
    ES: Stream<Handler>,
    TS: Stream<Handler>,
{
    pub event_stream: ES,
    pub radix_gateway_origin: &'static str,
    pub transaction_stream: TS,
}

impl<TS, ES> RadixStream<TS, ES>
where
    ES: Stream<Handler>,
    TS: Stream<Handler>,
{
    pub async fn new(
        RadixStreamOptions {
            event_stream,
            radix_gateway_origin,
            transaction_stream,
        }: RadixStreamOptions<TS, ES>,
    ) -> Result<Self, Error<TS::Error>> {
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

    pub async fn current_state_version(&self) -> Option<u64> {
        *self.last_state_version.lock().await
    }

    pub async fn start(&self) -> Result<JoinHandle<()>, Error<TS::Error>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let client = self.client.clone();
        let event_stream = self.event_stream.clone();
        let shutdown_token = self.shutdown_token.clone();
        let transaction_stream = self.transaction_stream.clone();
        let last_state_version = self.last_state_version.clone();
        let handle = self.task_tracker.spawn(async move {
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

                let opt_ins = TransactionDetailsOptIns {
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
                };

                let body = StreamTransactionsRequest::builder()
                    .kind_filter(StreamTransactionsRequestKindFilter::User)
                    .opt_ins(opt_ins)
                    .from_ledger_state(LedgerStateSelector(current_version.map(|i| {
                        LedgerStateSelectorInner {
                            epoch: None,
                            round: None,
                            state_version: Some(i as i64),
                            timestamp: None,
                        }
                    })))
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

                        let event_stream = event_stream.clone();
                        let transaction_stream = transaction_stream.clone();
                        let last_state_version = last_state_version.clone();

                        // Don't spawn a task if there are no transactions
                        if let Some(last_transaction) = transactions.last() {
                            *last_state_version.lock().await =
                                Some(last_transaction.state_version());
                        } else {
                            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                            continue;
                        }

                        tokio::spawn(async move {
                            for transaction in transactions.iter() {
                                trace!("Publishing event: {:?}", transaction);
                                transaction_stream
                                    .publish(transaction.clone().try_into().unwrap())
                                    .await
                                    .unwrap();

                                for event in transaction.events().iter() {
                                    trace!("Publishing event: {:?}", event);
                                    event_stream
                                        .publish(event.clone().try_into().unwrap())
                                        .await
                                        .unwrap();
                                }

                                *last_state_version.lock().await =
                                    Some(transaction.state_version());
                            }
                        });

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
        });

        Ok(handle)
    }
}
