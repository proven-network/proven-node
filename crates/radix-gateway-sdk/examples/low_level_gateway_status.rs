use proven_radix_gateway_sdk::Client;

#[tokio::main]
async fn main() {
    let client = Client::new("https://mainnet.radixdlt.com", None, None).unwrap();
    let status = client.get_inner_client().gateway_status().await.unwrap();
    println!("{:?}", status);
}
