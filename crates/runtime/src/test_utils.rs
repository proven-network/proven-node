use crate::runtime::RuntimeOptions;

use proven_radix_nft_verifier_mock::RadixNftVerifierMock;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore2, MemoryStore3};
use radix_common::network::NetworkDefinition;
use tempfile::tempdir;

pub fn create_runtime_options(
    script_name: &str,
    handler_name: &str,
) -> RuntimeOptions<
    MemoryStore2,
    MemoryStore3,
    MemoryStore3,
    DirectSqlStore2,
    DirectSqlStore3,
    DirectSqlStore3,
    RadixNftVerifierMock,
> {
    let module = std::fs::read_to_string(format!("./test_esm/{script_name}.ts")).unwrap();
    RuntimeOptions {
        application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
        application_store: MemoryStore2::new(),
        handler_name: Some(handler_name.to_string()),
        module,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        personal_store: MemoryStore3::new(),
        radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
        radix_network_definition: NetworkDefinition::stokenet(),
        radix_nft_verifier: RadixNftVerifierMock::new(),
    }
}
