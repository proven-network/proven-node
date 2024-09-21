use proven_radix_gateway_sdk::Client;
use proven_radix_gateway_sdk::Network;

#[tokio::main]
async fn main() {
    let client = Client::new(Network::Mainnet, None, None).unwrap();
    let status = client.get_inner_client().gateway_status().await.unwrap();
    println!("{:?}", status);
}
