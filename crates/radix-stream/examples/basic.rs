use proven_radix_gateway_sdk::*;

#[tokio::main]
async fn main() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "User-Agent",
        "proven-radix-gateway-sdk/0.1.0".parse().unwrap(),
    );

    let gateway_sdk = Client::new_with_client(
        "https://mainnet.radixdlt.com",
        reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap(),
    );

    let opt_ins = types::TransactionDetailsOptIns {
        affected_global_entities: true,
        balance_changes: false,
        raw_hex: false,
        manifest_instructions: false,
        receipt_costing_parameters: false,
        receipt_events: true,
        receipt_fee_destination: false,
        receipt_fee_source: false,
        receipt_fee_summary: false,
        receipt_output: false,
        receipt_state_changes: false,
    };

    let body = types::StreamTransactionsRequest::builder()
        .kind_filter(types::StreamTransactionsRequestKindFilter::User)
        .opt_ins(opt_ins)
        .order(types::StreamTransactionsRequestOrder::Asc);

    let wat = gateway_sdk
        .stream_transactions()
        .body(body)
        .send()
        .await
        .unwrap();

    println!("{:?}", wat);
}
