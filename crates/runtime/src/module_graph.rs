use crate::import_replacements::replace_esm_imports;

use deno_core::url::Url;
use deno_graph::source::{MemoryLoader, Source};
use deno_graph::{BuildOptions, GraphKind, ModuleGraph, ModuleSpecifier};
use futures::executor::block_on;

#[must_use]
/// Creates a module graph from the given module source.
///
/// # Arguments
///
/// * `module_source` - A string that represents the source code of the module.
///
/// # Returns
///
/// A tuple containing the URL of the module root and the constructed module graph.
///
/// # Panics
///
/// This function will panic if the module root URL cannot be parsed.
pub fn create_module_graph<S>(module_source: S) -> (Url, ModuleGraph)
where
    S: Into<String>,
{
    let import_replaced_module = replace_esm_imports(module_source.into().as_str());

    let loader = MemoryLoader::new(
        vec![
            (
                "file:///main.ts",
                Source::Module {
                    specifier: "file:///main.ts",
                    maybe_headers: None,
                    content: import_replaced_module.as_str(),
                },
            ),
            ("proven:crypto", Source::External("proven:crypto")),
            ("proven:handler", Source::External("proven:handler")),
            ("proven:kv", Source::External("proven:kv")),
            ("proven:session", Source::External("proven:session")),
            ("proven:sql", Source::External("proven:sql")),
        ],
        Vec::new(),
    );
    let module_root = ModuleSpecifier::parse("file:///main.ts").unwrap();
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
