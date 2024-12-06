mod error;
mod stream_handler;
mod transaction;

pub use error::{Error, Result};
pub use stream_handler::Handler;
pub use transaction::Transaction;

use proven_radix_gateway_sdk::types::{
    StreamTransactionsRequest, StreamTransactionsRequestKindFilter, StreamTransactionsRequestOrder,
    TransactionDetailsOptIns,
};
use proven_radix_gateway_sdk::{build_client, Client};
use proven_stream::Stream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub struct RadixStream<TS, ES>
where
    ES: Stream<Handler>,
    TS: Stream<Handler>,
{
    client: Client,
    event_stream: ES,
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

impl<ST, ET> RadixStream<ST, ET>
where
    ET: Stream<Handler>,
    ST: Stream<Handler>,
{
    pub fn new(
        RadixStreamOptions {
            event_stream,
            radix_gateway_origin,
            transaction_stream,
        }: RadixStreamOptions<ST, ET>,
    ) -> Self {
        let client = build_client(radix_gateway_origin, None, None);

        Self {
            client,
            event_stream,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            transaction_stream,
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<()>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let client = self.client.clone();
        let event_stream = self.event_stream.clone();
        let shutdown_token = self.shutdown_token.clone();
        let transaction_stream = self.transaction_stream.clone();
        let handle = self.task_tracker.spawn(async move {
            loop {
                if shutdown_token.is_cancelled() {
                    break;
                }

                info!("Polling for transactions");

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
                    .limit_per_page(1)
                    .order(StreamTransactionsRequestOrder::Asc);

                match client.stream_transactions().body(body).send().await {
                    Ok(response) => {
                        let transactions: Vec<Transaction> = response
                            .clone()
                            .items
                            .into_iter()
                            .map(Transaction::from)
                            .collect();

                        let event_stream = event_stream.clone();
                        let transaction_stream = transaction_stream.clone();

                        tokio::spawn(async move {
                            for transaction in transactions.iter() {
                                info!("Publishing event: {:?}", transaction);
                                transaction_stream
                                    .publish(transaction.clone().try_into().unwrap())
                                    .await
                                    .unwrap();

                                for event in transaction.events().iter() {
                                    info!("Publishing event: {:?}", event);
                                    event_stream
                                        .publish(event.clone().try_into().unwrap())
                                        .await
                                        .unwrap();
                                }
                            }
                        });
                    }
                    Err(e) => {
                        info!("Failed to poll for transactions: {:?}", e);
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });

        Ok(handle)
    }
}
