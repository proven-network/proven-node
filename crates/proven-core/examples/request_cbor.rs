use proven_core::rpc::Request;

fn main() {
    let requests = vec![
        Request::WhoAmI,
        Request::Execute(
            "export function handler () { console.log('Hello, world!') }".to_string(),
            "handler".to_string(),
        ),
        Request::Watch("resource".to_string()),
    ];

    for request in requests {
        let cbor_bytes = serde_cbor::to_vec(&request).unwrap();
        let hex_string = hex::encode(&cbor_bytes);
        println!("{:?}: {:?}", request, hex_string);

        // Check that the deserialized value is the same as the original value
        let deserialized: Request = serde_cbor::from_slice(&cbor_bytes).unwrap();
        assert_eq!(request, deserialized);
    }
}
