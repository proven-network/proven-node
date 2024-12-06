mod codegen;

pub use codegen::*;

pub fn build_client<O: Into<String>>(
    gateway_origin: O,
    dapp_definition: Option<&str>,
    application_name: Option<&str>,
) -> Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "User-Agent",
        "proven-radix-gateway-sdk/0.1.0".parse().unwrap(),
    );

    if let Some(dapp_definition) = dapp_definition {
        headers.insert("Rdx-App-Dapp-Definition", dapp_definition.parse().unwrap());
    }

    if let Some(application_name) = application_name {
        headers.insert("Rdx-App-Name", application_name.parse().unwrap());
    }

    Client::new_with_client(
        gateway_origin.into().as_str(),
        reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap(),
    )
}
