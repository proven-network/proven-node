use crate::import_replacements::replace_esm_imports;
use crate::runtime::RuntimeOptions;

use deno_core::url::Url;
use deno_graph::source::{MemoryLoader, Source};
use deno_graph::{BuildOptions, GraphKind, ModuleGraph, ModuleSpecifier};
use futures::executor::block_on;
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore2, MemoryStore3};
use radix_common::network::NetworkDefinition;
use tempfile::tempdir;

pub fn create_test_module_graph(script_name: &str, handler_name: &str) -> (Url, ModuleGraph) {
    let module = std::fs::read_to_string(format!("./test_esm/{script_name}.ts")).unwrap();
    let import_replaced_module = replace_esm_imports(&module);

    let specifier = format!("file:///{script_name}.ts#{handler_name}");

    let loader = MemoryLoader::new(
        vec![
            (
                specifier.clone(),
                Source::Module {
                    specifier: specifier.clone(),
                    maybe_headers: None,
                    content: import_replaced_module,
                },
            ),
            (
                "proven:crypto".to_string(),
                Source::External("proven:crypto".to_string()),
            ),
            (
                "proven:handler".to_string(),
                Source::External("proven:handler".to_string()),
            ),
            (
                "proven:kv".to_string(),
                Source::External("proven:kv".to_string()),
            ),
            (
                "proven:session".to_string(),
                Source::External("proven:session".to_string()),
            ),
            (
                "proven:sql".to_string(),
                Source::External("proven:sql".to_string()),
            ),
        ],
        Vec::new(),
    );
    let module_root = ModuleSpecifier::parse(&specifier).unwrap();
    let roots = vec![module_root.clone()];

    let future = async move {
        let mut module_graph = ModuleGraph::new(GraphKind::CodeOnly);
        module_graph
            .build(roots, &loader, BuildOptions::default())
            .await;
        module_graph
    };

    let module_graph = block_on(future);

    (module_root, module_graph)
}

pub fn create_test_runtime_options(
    script_name: &str,
    handler_name: &str,
) -> RuntimeOptions<
    MemoryStore2,
    MemoryStore3,
    MemoryStore3,
    DirectSqlStore2,
    DirectSqlStore3,
    DirectSqlStore3,
    MockRadixNftVerifier,
> {
    let (module_root, module_graph) = create_test_module_graph(script_name, handler_name);

    RuntimeOptions {
        application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
        application_store: MemoryStore2::new(),
        handler_name: Some(handler_name.to_string()),
        module_graph,
        module_root,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        personal_store: MemoryStore3::new(),
        radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
        radix_network_definition: NetworkDefinition::stokenet(),
        radix_nft_verifier: MockRadixNftVerifier::new(),
    }
}
