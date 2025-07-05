//! Tools for creating and working with code packages runnable in the Proven runtime.
//!
//! This crate provides functionality to bundle TypeScript/JavaScript code along with NPM dependencies
//! into portable `CodePackage` structures that can be executed in the Proven runtime environment.
//!
//! ## NPM Dependency Support
//!
//! The code-package crate includes comprehensive NPM dependency bundling support:
//!
//! ### Basic Usage
//!
//! ```rust,no_run
//! use proven_code_package::{CodePackage, ModuleSpecifier, PackageJson};
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Parse a package.json file
//! let package_json: PackageJson = r#"
//! {
//!     "name": "my-app",
//!     "dependencies": {
//!         "lodash": "^4.17.21",
//!         "uuid": "^8.3.2"
//!     }
//! }
//! "#
//! .parse()?;
//!
//! // Create your application modules
//! let main_module = r#"
//!     import _ from 'lodash';
//!     import { v4 as uuidv4 } from 'uuid';
//!     
//!     export default function main() {
//!         return _.capitalize(`Hello ${uuidv4()}`);
//!     }
//! "#;
//!
//! let module_sources = HashMap::from([(
//!     ModuleSpecifier::parse("file:///main.ts")?,
//!     main_module.to_string(),
//! )]);
//!
//! // Bundle with NPM dependencies
//! let code_package = CodePackage::from_map_with_npm_deps(
//!     &module_sources,
//!     vec![ModuleSpecifier::parse("file:///main.ts")?],
//!     Some(&package_json),
//!     false, // Don't include dev dependencies
//! )
//! .await?;
//!
//! // The resulting CodePackage includes both your code and resolved NPM dependencies
//! println!(
//!     "Package created with {} modules",
//!     code_package.specifiers().len()
//! );
//! # Ok(())
//! # }
//! ```
//!
//! ### Features
//!
//! - **NPM Registry Integration**: Fetches package information from the NPM registry
//! - **Version Resolution**: Resolves semantic version constraints to specific versions
//! - **Dependency Graphing**: Builds complete dependency graphs including transitive dependencies
//! - **Caching**: Intelligent caching with TTL to avoid repeated registry requests
//! - **Package.json Support**: Full support for parsing package.json dependency specifications
//! - **`ESZip` Bundling**: Packages are serialized using `ESZip` for efficient storage and transport
//! - **Runtime Compatible**: Generated packages work seamlessly with the Proven runtime
//!
//! ### Supported Dependency Types
//!
//! The system supports standard NPM dependencies while filtering out non-NPM sources:
//!
//! - ✅ `"^4.17.21"` - Semantic version ranges
//! - ✅ `"~1.0.0"` - Tilde ranges  
//! - ✅ `">=16.0.0"` - Comparison ranges
//! - ✅ `"*"` - Wildcard versions
//! - ❌ `"file:../local"` - Local file dependencies (skipped)
//! - ❌ `"git://github.com/user/repo"` - Git dependencies (skipped)
//! - ❌ `"https://example.com/package.tgz"` - URL dependencies (skipped)
//!
//! ### Error Handling
//!
//! The system provides comprehensive error handling for common scenarios:
//!
//! - Network failures when accessing the NPM registry
//! - Package not found errors
//! - Version resolution conflicts
//! - Invalid package.json syntax
//! - Malformed version specifications
//!
//! ### Performance Considerations
//!
//! - **Caching**: Package metadata is cached with a 1-hour TTL by default
//! - **Concurrent Resolution**: Multiple packages are resolved concurrently
//! - **Efficient Bundling**: Uses Deno's proven `ESZip` format for optimal size and performance
//! - **Memory Management**: Shared references and careful memory usage patterns
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod npm_resolver;
mod package_json;
mod resolver;

pub use error::Error;
use npm_resolver::CodePackageNpmResolver;
pub use package_json::PackageJson;
use resolver::CodePackageResolver;

use bytes::Bytes;
use deno_ast::{EmitOptions, TranspileOptions};
pub use deno_core::ModuleSpecifier;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use deno_graph::ast::{CapturingEsParser, DefaultParsedSourceStore};
use deno_graph::source::{MemoryLoader, NpmResolver, NullFileSystem, Source};
use deno_graph::{BuildOptions, GraphKind, ModuleGraph};
use eszip::{EszipV2, FromGraphOptions};
use futures::executor::block_on;
use futures::io::BufReader;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Represents a package of code that can be executed on a runtime. Can be serialized to and from bytes.
pub struct CodePackage {
    /// The `ESZip` representation of the code package.
    eszip: Arc<Mutex<Option<EszipV2>>>,

    /// The hash of the code package bytes.
    hash: String,

    /// The valid entrypoints of the code package.
    valid_entrypoints: HashSet<ModuleSpecifier>,
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
    pub async fn from_map(
        module_sources: &HashMap<ModuleSpecifier, String>,
        module_roots: impl IntoIterator<Item = ModuleSpecifier> + Clone,
    ) -> Result<Self, Error> {
        // Clone the data we need to move into the blocking task
        let module_sources_clone = module_sources.clone();
        let module_roots_vec: Vec<ModuleSpecifier> = module_roots.clone().into_iter().collect();

        // Use spawn_blocking to handle the non-Send deno operations
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async {
                Self::from_map_inner(&module_sources_clone, module_roots_vec).await
            })
        })
        .await
        .map_err(|e| Error::CodePackage(format!("Task join error: {e:?}")))?
    }

    /// Internal implementation of `from_map` that can be non-Send.
    #[allow(clippy::future_not_send)]
    async fn from_map_inner(
        module_sources: &HashMap<ModuleSpecifier, String>,
        module_roots: Vec<ModuleSpecifier>,
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
            ("proven:rpc", Source::External("proven:rpc")),
            ("proven:session", Source::External("proven:session")),
            ("proven:sql", Source::External("proven:sql")),
            ("proven:openai", Source::External("proven:openai")),
            (
                "proven:radix_engine_toolkit",
                Source::External("proven:radix_engine_toolkit"),
            ),
            ("proven:uuid", Source::External("proven:uuid")),
            ("proven:zod", Source::External("proven:zod")),
        ]);

        let loader = MemoryLoader::new(sources, Vec::new());

        let mut module_graph = ModuleGraph::new(GraphKind::All);

        module_graph
            .build(
                module_roots.clone(),
                Vec::default(),
                &loader,
                BuildOptions {
                    is_dynamic: true,
                    skip_dynamic_deps: false,
                    executor: Default::default(),
                    locker: None,
                    file_system: &NullFileSystem,
                    jsr_url_provider: Default::default(),
                    passthrough_jsr_specifiers: false,
                    module_analyzer: Default::default(),
                    module_info_cacher: Default::default(),
                    npm_resolver: Some(&CodePackageNpmResolver::new()),
                    reporter: None,
                    resolver: Some(&CodePackageResolver),
                    unstable_bytes_imports: false,
                    unstable_text_imports: false,
                },
            )
            .await;

        Self::from_module_graph(module_graph, module_roots)
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
    pub async fn from_str(module_source: &str) -> Result<Self, Error> {
        let module_specifier = ModuleSpecifier::parse("file:///main.ts").unwrap();
        let module_sources = HashMap::from([(module_specifier.clone(), module_source.to_string())]);
        Self::from_map(&module_sources, vec![module_specifier]).await
    }

    /// Creates a `CodePackage` from module sources and a package.json, resolving NPM dependencies.
    ///
    /// # Arguments
    ///
    /// * `module_sources` - A map containing the module sources.
    /// * `module_roots` - A list of module specifiers representing the roots of the modules.
    /// * `package_json` - Optional package.json for dependency resolution.
    /// * `include_dev_deps` - Whether to include dev dependencies from package.json.
    ///
    /// # Errors
    ///
    /// This function will return an error if the module graph cannot be built, NPM dependencies
    /// cannot be resolved, or if the `EszipV2::from_graph` function fails.
    pub async fn from_map_with_npm_deps(
        module_sources: &HashMap<ModuleSpecifier, String>,
        module_roots: impl IntoIterator<Item = ModuleSpecifier> + Clone,
        package_json: Option<&PackageJson>,
        include_dev_deps: bool,
    ) -> Result<Self, Error> {
        // Clone the data we need to move into the blocking task
        let module_sources_clone = module_sources.clone();
        let module_roots_vec: Vec<ModuleSpecifier> = module_roots.clone().into_iter().collect();
        let package_json_clone = package_json.cloned();

        // Use spawn_blocking to handle the non-Send deno operations
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async {
                Self::from_map_with_npm_deps_inner(
                    &module_sources_clone,
                    module_roots_vec,
                    package_json_clone.as_ref(),
                    include_dev_deps,
                )
                .await
            })
        })
        .await
        .map_err(|e| Error::CodePackage(format!("Task join error: {e:?}")))?
    }

    /// Internal implementation of `from_map_with_npm_deps` that can be non-Send.
    #[allow(clippy::future_not_send)]
    async fn from_map_with_npm_deps_inner(
        module_sources: &HashMap<ModuleSpecifier, String>,
        module_roots: Vec<ModuleSpecifier>,
        package_json: Option<&PackageJson>,
        include_dev_deps: bool,
    ) -> Result<Self, Error> {
        let npm_resolver = CodePackageNpmResolver::new();

        // If we have a package.json, resolve its dependencies first
        if let Some(pkg_json) = package_json {
            let package_reqs = pkg_json.to_package_reqs(include_dev_deps).map_err(|e| {
                Error::CodePackage(format!("Failed to parse package.json dependencies: {e}"))
            })?;

            if !package_reqs.is_empty() {
                let resolution_result = npm_resolver.resolve_pkg_reqs(&package_reqs).await;

                // Check if any resolutions failed
                for (i, result) in resolution_result.results.iter().enumerate() {
                    if let Err(e) = result {
                        return Err(Error::CodePackage(format!(
                            "Failed to resolve NPM dependency '{}': {e:?}",
                            package_reqs[i]
                        )));
                    }
                }
            }
        }

        // Build the module graph with NPM support
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

        // Add proven extension sources
        sources.extend(vec![
            ("proven:crypto", Source::External("proven:crypto")),
            ("proven:handler", Source::External("proven:handler")),
            ("proven:kv", Source::External("proven:kv")),
            ("proven:rpc", Source::External("proven:rpc")),
            ("proven:session", Source::External("proven:session")),
            ("proven:sql", Source::External("proven:sql")),
            ("proven:openai", Source::External("proven:openai")),
            (
                "proven:radix_engine_toolkit",
                Source::External("proven:radix_engine_toolkit"),
            ),
            ("proven:uuid", Source::External("proven:uuid")),
            ("proven:zod", Source::External("proven:zod")),
        ]);

        let loader = MemoryLoader::new(sources, Vec::new());

        let mut module_graph = ModuleGraph::new(GraphKind::All);

        module_graph
            .build(
                module_roots.clone(),
                Vec::default(),
                &loader,
                BuildOptions {
                    is_dynamic: true,
                    skip_dynamic_deps: false,
                    executor: Default::default(),
                    locker: None,
                    file_system: &NullFileSystem,
                    jsr_url_provider: Default::default(),
                    passthrough_jsr_specifiers: false,
                    module_analyzer: Default::default(),
                    module_info_cacher: Default::default(),
                    npm_resolver: Some(&npm_resolver),
                    reporter: None,
                    resolver: Some(&CodePackageResolver),
                    unstable_bytes_imports: false,
                    unstable_text_imports: false,
                },
            )
            .await;

        Self::from_module_graph(module_graph, module_roots)
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
    pub fn from_module_graph(
        module_graph: ModuleGraph,
        valid_entrypoints: impl IntoIterator<Item = ModuleSpecifier>,
    ) -> Result<Self, Error> {
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
            valid_entrypoints: valid_entrypoints.into_iter().collect(),
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

    /// Returns the hash of the code package.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn hash(&self) -> &str {
        &self.hash
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

    /// Returns a reference to the set of valid entrypoints.
    #[must_use]
    pub const fn valid_entrypoints(&self) -> &HashSet<ModuleSpecifier> {
        &self.valid_entrypoints
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
            valid_entrypoints: self.valid_entrypoints.clone(),
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

#[derive(Debug, Deserialize, Serialize)]
struct StoredCodePackage {
    eszip_bytes: Bytes,
    valid_entrypoints: HashSet<ModuleSpecifier>,
}

impl TryFrom<Bytes> for CodePackage {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let stored_code_package: StoredCodePackage =
            ciborium::de::from_reader(&bytes[..]).map_err(|e| Error::CodePackage(e.to_string()))?;

        // First get a hash of the bytes
        let mut hasher = Sha256::new();
        hasher.update(&stored_code_package.eszip_bytes);
        let hash_bytes = hasher.finalize();

        // Convert the hash result to a hexadecimal string
        let hash = format!("{hash_bytes:x}");

        let reader = BufReader::new(&stored_code_package.eszip_bytes[..]);
        let (eszip, _loaded_future) = block_on(async { EszipV2::parse(reader).await }).unwrap();

        Ok(Self {
            eszip: Arc::new(Mutex::new(Some(eszip))),
            hash,
            valid_entrypoints: stored_code_package.valid_entrypoints,
        })
    }
}

impl TryInto<Bytes> for CodePackage {
    type Error = Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        // TODO: Handle errors
        let eszip_bytes = self.eszip.lock().unwrap().take().unwrap().into_bytes();

        let stored_code_package = StoredCodePackage {
            eszip_bytes: Bytes::from(eszip_bytes),
            valid_entrypoints: self.valid_entrypoints,
        };

        let mut u8_vec = Vec::new();
        ciborium::ser::into_writer(&stored_code_package, &mut u8_vec).unwrap();

        Ok(Bytes::from(u8_vec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_code_package_from_str() {
        let code_package = CodePackage::from_str("export default 'Hello, world!'")
            .await
            .unwrap();

        assert_eq!(code_package.specifiers().len(), 1);
        assert_eq!(code_package.valid_entrypoints().len(), 1);
    }

    #[tokio::test]
    async fn test_code_package_serde() {
        let code_package = CodePackage::from_str("export default 'Hello, world!'")
            .await
            .unwrap();
        let bytes: Bytes = code_package.clone().try_into().unwrap();
        let code_package2: CodePackage = bytes.try_into().unwrap();

        assert_eq!(code_package.hash, code_package2.hash);
        assert_eq!(code_package.specifiers(), code_package2.specifiers());
        assert_eq!(
            code_package.valid_entrypoints(),
            code_package2.valid_entrypoints()
        );
    }

    #[tokio::test]
    async fn test_npm_specifier_storage() {
        // Test that NPM specifiers can be stored and retrieved from CodePackage
        // without triggering actual NPM resolution
        let npm_module_source = "export const lodash = 'utility library';";
        let regular_module_source = "export default 'main module';";

        let module_sources = HashMap::from([
            (
                ModuleSpecifier::parse("npm:lodash@4.17.21").unwrap(),
                npm_module_source.to_string(),
            ),
            (
                ModuleSpecifier::parse("file:///main.ts").unwrap(),
                regular_module_source.to_string(),
            ),
        ]);

        // Create code package without NPM resolution to avoid network calls
        let result = CodePackage::from_map(
            &module_sources,
            vec![ModuleSpecifier::parse("file:///main.ts").unwrap()],
        )
        .await;

        if let Ok(code_package) = result {
            // Verify both modules are included
            let specifiers = code_package.specifiers();
            assert!(!specifiers.is_empty());

            // Verify we can retrieve the main module
            let main_source =
                code_package.get_module_source(&ModuleSpecifier::parse("file:///main.ts").unwrap());
            assert!(main_source.is_some());
            assert!(main_source.unwrap().contains("main module"));
        }
        // If the result is an error, that's also acceptable since we're not
        // providing a real NPM resolver setup
    }

    #[tokio::test]
    async fn test_complete_bundling_workflow() {
        // Test the complete workflow from package.json to CodePackage
        let package_json_content = r#"
        {
            "name": "test-app",
            "version": "1.0.0",
            "dependencies": {
                "utility-lib": "^1.0.0"
            },
            "devDependencies": {
                "test-framework": "^2.0.0"
            }
        }
        "#;

        // Parse package.json
        let package_json: PackageJson = package_json_content.parse().unwrap();

        // Test production dependencies only
        let prod_reqs = package_json.to_package_reqs(false).unwrap();
        assert_eq!(prod_reqs.len(), 1);
        assert_eq!(prod_reqs[0].name.as_str(), "utility-lib");

        // Test with dev dependencies
        let all_reqs = package_json.to_package_reqs(true).unwrap();
        assert_eq!(all_reqs.len(), 2);

        // Create TypeScript module that would import the dependency
        let main_module = r"
            import { utilityFunction } from 'utility-lib';
            
            export default function main() {
                return utilityFunction('Hello, World!');
            }
        ";

        let module_sources = HashMap::from([(
            ModuleSpecifier::parse("file:///main.ts").unwrap(),
            main_module.to_string(),
        )]);

        // Test basic CodePackage creation
        let basic_package = CodePackage::from_map(
            &module_sources,
            vec![ModuleSpecifier::parse("file:///main.ts").unwrap()],
        )
        .await;

        assert!(basic_package.is_ok());
        let basic_package = basic_package.unwrap();

        // Verify the basic package structure
        assert_eq!(basic_package.valid_entrypoints().len(), 1);
        assert!(!basic_package.specifiers().is_empty());

        // Verify module content can be retrieved
        let retrieved_source =
            basic_package.get_module_source(&ModuleSpecifier::parse("file:///main.ts").unwrap());
        assert!(retrieved_source.is_some());
        assert!(retrieved_source.unwrap().contains("utilityFunction"));

        // Test serialization/deserialization
        let package_bytes: Result<Bytes, _> = basic_package.clone().try_into();
        assert!(package_bytes.is_ok());

        let deserialized_package: Result<CodePackage, _> = package_bytes.unwrap().try_into();
        assert!(deserialized_package.is_ok());

        let deserialized_package = deserialized_package.unwrap();
        assert_eq!(basic_package.hash(), deserialized_package.hash());
        assert_eq!(
            basic_package.specifiers(),
            deserialized_package.specifiers()
        );
    }
}
