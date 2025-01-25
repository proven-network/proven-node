//! A library for creating and working with code packages runnable in the Proven runtime.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod npm_resolver;

pub use deno_core::ModuleSpecifier;
pub use error::Error;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use deno_ast::{EmitOptions, TranspileOptions};
use deno_graph::source::{MemoryLoader, NullFileSystem, Source};
use deno_graph::{BuildOptions, GraphKind, ModuleGraph};
use deno_graph::{CapturingEsParser, DefaultParsedSourceStore};
use eszip::{EszipV2, FromGraphOptions};
use futures::executor::block_on;
use futures::io::BufReader;
use sha2::{Digest, Sha256};

/// Represents a package of code that can be executed on a runtime. Can be serialized to and from bytes.
pub struct CodePackage {
    /// The `ESZip` representation of the code package.
    eszip: Arc<Mutex<Option<EszipV2>>>,

    /// The hash of the code package bytes.
    pub hash: String,
}

impl CodePackage {
    /// Retrieves the source code of a module by its specifier and processing mode.
    ///
    /// # Arguments
    ///
    /// * `module_specifer` - The specifier of the module to retrieve.
    /// * `processing_mode` - The mode in which the module is being processed.
    ///
    /// # Returns
    ///
    /// An `Option` containing the source code of the module if found, otherwise `None`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `eszip` mutex is poisoned or if the `eszip` is `None`.
    #[must_use]
    pub fn get_module_source(&self, module_specifer: &ModuleSpecifier) -> Option<String> {
        #[allow(clippy::significant_drop_in_scrutinee)]
        self.eszip
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .get_module(module_specifer.as_str())
            .and_then(|module| {
                block_on(async { module.source().await })
                    .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
            })
    }

    /// Creates a `CodePackage` from a map of module sources and a list of module roots.
    ///
    /// # Arguments
    ///
    /// * `module_sources` - A map containing the module sources.
    /// * `module_roots` - A list of module specifiers representing the roots of the modules.
    ///
    /// # Errors
    ///
    /// This function will return an error if the module graph cannot be built or if the `EszipV2::from_graph` function fails.
    pub fn from_map(
        module_sources: &HashMap<ModuleSpecifier, String>,
        module_roots: impl IntoIterator<Item = ModuleSpecifier>,
    ) -> Result<Self, Error> {
        let mut sources = module_sources
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str(),
                    Source::Module {
                        specifier: k.as_str(),
                        maybe_headers: None,
                        content: v.as_str(),
                    },
                )
            })
            .collect::<Vec<_>>();

        sources.extend(vec![
            ("proven:crypto", Source::External("proven:crypto")),
            ("proven:handler", Source::External("proven:handler")),
            ("proven:kv", Source::External("proven:kv")),
            ("proven:session", Source::External("proven:session")),
            ("proven:sql", Source::External("proven:sql")),
            (
                "proven:babylon_gateway_api",
                Source::External("proven:babylon_gateway_api"),
            ),
            (
                "proven:radix_engine_toolkit",
                Source::External("proven:radix_engine_toolkit"),
            ),
            ("proven:zod", Source::External("proven:zod")),
        ]);

        let loader = MemoryLoader::new(sources, Vec::new());

        let module_graph_future = async move {
            let mut graph = ModuleGraph::new(GraphKind::All);

            graph
                .build(
                    module_roots.into_iter().collect(),
                    &loader,
                    BuildOptions {
                        is_dynamic: true,
                        imports: Vec::default(),
                        executor: Default::default(),
                        locker: None,
                        file_system: &NullFileSystem,
                        jsr_url_provider: Default::default(),
                        passthrough_jsr_specifiers: false,
                        module_analyzer: Default::default(),
                        npm_resolver: None,
                        reporter: None,
                        resolver: None,
                    },
                )
                .await;

            graph
        };

        let module_graph = block_on(module_graph_future);

        Self::from_module_graph(module_graph)
    }

    /// Creates a `CodePackage` from a string containing module source code.
    ///
    /// # Errors
    ///
    /// This function will return an error if the module source cannot be processed.
    ///
    /// # Panics
    ///
    /// This function will panic if the module root cannot be parsed.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(module_source: &str) -> Result<Self, Error> {
        let module_specifier = ModuleSpecifier::parse("file:///main.ts").unwrap();
        let module_sources = HashMap::from([(module_specifier.clone(), module_source.to_string())]);
        Self::from_map(&module_sources, vec![module_specifier])
    }

    /// Creates a `CodePackage` from a `ModuleGraph`.
    ///
    /// # Arguments
    ///
    /// * `module_graph` - The module graph to create the code package from.
    ///
    /// # Errors
    ///
    /// This function will return an error if the `EszipV2::from_graph` function fails.
    pub fn from_module_graph(module_graph: ModuleGraph) -> Result<Self, Error> {
        let eszip = EszipV2::from_graph(FromGraphOptions {
            graph: module_graph.clone(),
            parser: CapturingEsParser::new(None, &DefaultParsedSourceStore::default()),
            module_kind_resolver: Default::default(),
            transpile_options: TranspileOptions::default(),
            emit_options: EmitOptions::default(),
            relative_file_base: None,
            npm_packages: None,
        })
        .map_err(|e| Error::CodePackage(e.to_string()))?;

        // First get a hash of the bytes
        let mut hasher = Sha256::new();
        hasher.update(eszip.into_bytes());
        let hash_bytes = hasher.finalize();

        // Convert the hash result to a hexadecimal string
        let hash = format!("{hash_bytes:x}");

        // Slightly inefficient to parse the graph again, but it's infrequent
        let eszip = EszipV2::from_graph(FromGraphOptions {
            graph: module_graph,
            parser: CapturingEsParser::new(None, &DefaultParsedSourceStore::default()),
            module_kind_resolver: Default::default(),
            transpile_options: TranspileOptions::default(),
            emit_options: EmitOptions::default(),
            relative_file_base: None,
            npm_packages: None,
        })
        .map_err(|e| Error::CodePackage(e.to_string()))?;

        Ok(Self {
            eszip: Arc::new(Mutex::new(Some(eszip))),
            hash,
        })
    }

    /// Converts the `CodePackage` into bytes.
    ///
    /// # Panics
    ///
    /// This function will panic if the `eszip` mutex is poisoned or if the `eszip` is `None`.
    #[must_use]
    pub fn into_bytes(&self) -> Bytes {
        let mut eszip_guard = self.eszip.lock().unwrap();

        // Take ownership of current eszip using Option
        let current_eszip = eszip_guard.take().expect("eszip should be initialized");
        let vec_u8 = current_eszip.into_bytes();

        let reader = BufReader::new(&vec_u8[..]);
        let (new_eszip, loaded_future) = block_on(async { EszipV2::parse(reader).await }).unwrap();
        block_on(loaded_future).unwrap();

        // Put the new eszip back
        eszip_guard.replace(new_eszip);
        drop(eszip_guard);

        Bytes::from(vec_u8)
    }

    /// Returns a list of module specifiers.
    ///
    /// # Panics
    ///
    /// This function will panic if the `eszip` mutex is poisoned or if the `eszip` is `None`.
    #[must_use]
    pub fn specifiers(&self) -> Vec<String> {
        self.eszip.lock().unwrap().as_mut().unwrap().specifiers()
    }
}

impl Clone for CodePackage {
    fn clone(&self) -> Self {
        let bytes = self.into_bytes();

        let reader = BufReader::new(&bytes[..]);
        let (eszip, loaded_future) = block_on(async { EszipV2::parse(reader).await }).unwrap();

        block_on(loaded_future).unwrap();

        Self {
            eszip: Arc::new(Mutex::new(Some(eszip))),
            hash: self.hash.clone(),
        }
    }
}

impl Debug for CodePackage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodePackage")
            .field("hash", &self.hash)
            .finish_non_exhaustive()
    }
}

impl TryFrom<Bytes> for CodePackage {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        // First get a hash of the bytes
        let mut hasher = Sha256::new();
        hasher.update(bytes.as_ref());
        let hash_bytes = hasher.finalize();

        // Convert the hash result to a hexadecimal string
        let hash = format!("{hash_bytes:x}");

        let reader = BufReader::new(&bytes[..]);
        let (eszip, loaded_future) = block_on(async { EszipV2::parse(reader).await }).unwrap();

        // TODO: Handle error
        block_on(loaded_future).unwrap();

        Ok(Self {
            eszip: Arc::new(Mutex::new(Some(eszip))),
            hash,
        })
    }
}

impl From<CodePackage> for Bytes {
    fn from(code_package: CodePackage) -> Self {
        code_package.into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_code_package_from_str() {
        let code_package = CodePackage::from_str("export default = 'Hello, world!'").unwrap();

        assert_eq!(code_package.specifiers().len(), 1);
    }
}
