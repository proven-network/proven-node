use proven_core::rpc::Request;

fn main() {
    who_am_i();
    watch();
}

fn who_am_i() {
    let who_am_i = Request::WhoAmI;
    let cbor = serde_cbor::to_vec(&who_am_i).unwrap();
    let hex = hex::encode(cbor);
    println!("who_am_i: {}", hex);
}

fn watch() {
    let watch = Request::Watch("test:example".to_string());
    let cbor = serde_cbor::to_vec(&watch).unwrap();
    let hex = hex::encode(cbor);
    println!("watch: {}", hex);
}
