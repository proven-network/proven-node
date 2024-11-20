use proven_core::rpc::Request;
use serde_json::json;

fn main() {
    let requests = vec![
        Request::WhoAmI,
        Request::Execute(
            "export function handler (a, b) { console.log(a + b) }".to_string(),
            "handler".to_string(),
            vec![json!(10), json!(20), json!("hello"), json!(true)],
        ),
        Request::Watch("resource".to_string()),
    ];

    for request in requests {
        let mut cbor_bytes = Vec::new();
        ciborium::ser::into_writer(&request, &mut cbor_bytes).unwrap();
        let hex_string = hex::encode(&cbor_bytes);
        println!("{:?}: {:?}", request, hex_string);

        // Check that the deserialized value is the same as the original value
        let deserialized: Request = ciborium::de::from_reader(&cbor_bytes[..]).unwrap();
        assert_eq!(request, deserialized);
    }
}
