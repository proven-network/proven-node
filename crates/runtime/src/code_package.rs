use crate::import_replacements::replace_esm_imports;
use crate::options::ModuleOptions;
use crate::options_parser::OptionsParser;
use crate::preprocessor::Preprocessor;
use crate::util::run_in_thread;
use crate::Error;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use deno_ast::{EmitOptions, TranspileOptions};
use deno_graph::source::{MemoryLoader, Source};
use deno_graph::{BuildOptions, GraphKind, ModuleGraph, ModuleSpecifier};
use deno_graph::{CapturingEsParser, DefaultParsedSourceStore};
use eszip::{EszipV2, FromGraphOptions};
use futures::executor::block_on;
use futures::io::BufReader;
use regex::Regex;
use rustyscript::module_loader::ImportProvider;
use rustyscript::Module;
use sha2::{Digest, Sha256};

pub enum ProcessingMode {
    Options,
    Runtime,
}

/// Represents a package of code that can be executed on a runtime. Can be serialized to and from bytes.
pub struct CodePackage {
    /// The `ESZip` representation of the code package.
    eszip: Arc<Mutex<Option<EszipV2>>>,

    /// The hash of the code package bytes.
    pub hash: String,

    /// Cache for module options.
    module_options_cache: Arc<Mutex<HashMap<ModuleSpecifier, Option<ModuleOptions>>>>,
}

impl CodePackage {
    #[must_use]
    /// Retrieves a module by its specifier and processing mode.
    ///
    /// # Arguments
    ///
    /// * `module_specifer` - The specifier of the module to retrieve.
    /// * `processing_mode` - The mode in which the module is being processed.
    ///
    /// # Returns
    ///
    /// An `Option` containing the `Module` if found, otherwise `None`.
    pub fn get_module(
        &self,
        module_specifer: &ModuleSpecifier,
        processing_mode: &ProcessingMode,
    ) -> Option<Module> {
        #[allow(clippy::significant_drop_in_scrutinee)]
        self.get_module_source(module_specifer, processing_mode)
            .map(|module_source| Module::new(module_specifer.as_str(), module_source))
    }

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
    pub fn get_module_source(
        &self,
        module_specifer: &ModuleSpecifier,
        processing_mode: &ProcessingMode,
    ) -> Option<String> {
        #[allow(clippy::significant_drop_in_scrutinee)]
        self.eszip
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .get_module(module_specifer.as_str())
            .map(|module| {
                let bytes = block_on(async { module.source().await.unwrap() });

                let module_source = String::from_utf8(bytes.to_vec()).unwrap();

                match processing_mode {
                    ProcessingMode::Options => {
                        let module_source = replace_esm_imports(&module_source);
                        let module_source = strip_comments(&module_source);
                        let module_source = name_default_export(&module_source);

                        rewrite_run_functions(&module_source)
                    }
                    ProcessingMode::Runtime => module_source,
                }
            })
    }

    /// Retrieves the module options for a given specifier.
    ///
    /// # Arguments
    ///
    /// * `specifier` - The specifier of the module to retrieve options for.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `ModuleOptions` if successful, otherwise an `Error`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the options cannot be parsed.
    ///
    /// # Panics
    ///
    /// This function will panic if the `module_options_cache` mutex is poisoned.
    pub fn get_module_options(&self, specifier: &ModuleSpecifier) -> Result<ModuleOptions, Error> {
        let mut module_options_cache = self.module_options_cache.lock().unwrap();

        if let Some(module_options) = module_options_cache.get(specifier) {
            return Ok(module_options.clone().unwrap());
        }

        let self_clone = self.clone();
        let specifier_clone = specifier.clone();

        let module_options =
            run_in_thread(move || OptionsParser::parse(&self_clone, &specifier_clone))?;

        module_options_cache.insert(specifier.clone(), Some(module_options.clone()));
        drop(module_options_cache);

        Ok(module_options)
    }

    #[allow(clippy::should_implement_trait)]
    /// Creates a `CodePackage` from a string containing module source code.
    ///
    /// # Errors
    ///
    /// This function will return an error if the module source cannot be processed.
    ///
    /// # Panics
    ///
    /// This function will panic if the module root cannot be parsed.
    pub fn from_str(module_source: &str) -> Result<Self, Error> {
        let import_replaced_module = replace_esm_imports(module_source);

        let preprocessed_module = run_in_thread(move || {
            let mut preprocessor = Preprocessor::new()?;
            preprocessor.process(import_replaced_module)
        })?;

        let loader = MemoryLoader::new(
            vec![
                (
                    "file:///main.ts",
                    Source::Module {
                        specifier: "file:///main.ts",
                        maybe_headers: None,
                        content: preprocessed_module.as_str(),
                    },
                ),
                ("proven:crypto", Source::External("proven:crypto")),
                ("proven:handler", Source::External("proven:handler")),
                ("proven:kv", Source::External("proven:kv")),
                ("proven:session", Source::External("proven:session")),
                ("proven:sql", Source::External("proven:sql")),
                // TODO: Use NPM packages for these in future
                (
                    "proven:babylon_gateway_api",
                    Source::External("proven:babylon_gateway_api"),
                ),
                (
                    "proven:radix_engine_toolkit",
                    Source::External("proven:radix_engine_toolkit"),
                ),
                ("proven:openai", Source::External("proven:openai")),
                ("proven:uuid", Source::External("proven:uuid")),
                ("proven:zod", Source::External("proven:zod")),
            ],
            Vec::new(),
        );
        let module_root = ModuleSpecifier::parse("file:///main.ts").unwrap();
        let roots = vec![module_root];

        let module_graph_future = async move {
            let mut graph = ModuleGraph::new(GraphKind::All);
            graph.build(roots, &loader, BuildOptions::default()).await;

            graph
        };

        let module_graph = block_on(module_graph_future);

        Self::from_module_graph(module_graph)
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
            module_options_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Creates a `CodePackageImportProvider` for the given processing mode.
    ///
    /// # Arguments
    ///
    /// * `processing_mode` - The mode in which the code package is being processed.
    ///
    /// # Returns
    ///
    /// A `CodePackageImportProvider` for the specified processing mode.
    #[must_use]
    pub fn import_provider(&self, processing_mode: ProcessingMode) -> CodePackageImportProvider {
        CodePackageImportProvider {
            code_package: self.clone(),
            processing_mode,
        }
    }

    #[must_use]
    /// Converts the `CodePackage` into bytes.
    ///
    /// # Panics
    ///
    /// This function will panic if the `eszip` mutex is poisoned or if the `eszip` is `None`.
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

    #[must_use]
    /// Returns a list of module specifiers.
    ///
    /// # Panics
    ///
    /// This function will panic if the `eszip` mutex is poisoned or if the `eszip` is `None`.
    pub fn specifiers(&self) -> Vec<String> {
        self.eszip.lock().unwrap().as_mut().unwrap().specifiers()
    }

    #[cfg(test)]
    #[must_use]
    /// Creates a `CodePackage` from a test code file.
    ///
    /// # Panics
    ///
    /// This function will panic if the file cannot be read.
    pub fn from_test_code(script_name: &str) -> Self {
        let esm = std::fs::read_to_string(format!("./test_esm/{script_name}.ts")).unwrap();

        Self::from_str(&esm).unwrap()
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
            module_options_cache: self.module_options_cache.clone(),
        }
    }
}

pub struct CodePackageImportProvider {
    code_package: CodePackage,
    processing_mode: ProcessingMode,
}

impl ImportProvider for CodePackageImportProvider {
    fn resolve(
        &mut self,
        specifier: &ModuleSpecifier,
        referrer: &str,
        kind: deno_core::ResolutionKind,
    ) -> Option<Result<ModuleSpecifier, anyhow::Error>> {
        match self.processing_mode {
            ProcessingMode::Options => {
                println!("options_parser resolve: {specifier}, {referrer}, {kind:?}");
            }
            ProcessingMode::Runtime => {
                println!("runtime resolve: {specifier}, {referrer}, {kind:?}");
            }
        }

        None
    }

    fn import(
        &mut self,
        specifier: &ModuleSpecifier,
        referrer: Option<&ModuleSpecifier>,
        is_dyn_import: bool,
        requested_module_type: deno_core::RequestedModuleType,
    ) -> Option<Result<String, anyhow::Error>> {
        match self.processing_mode {
            ProcessingMode::Options => {
                println!("options_parser import: {specifier}, {referrer:?}, {is_dyn_import}, {requested_module_type:?}");
            }
            ProcessingMode::Runtime => {
                println!("runtime import: {specifier}, {referrer:?}, {is_dyn_import}, {requested_module_type:?}");
            }
        }

        self.code_package
            .get_module_source(specifier, &self.processing_mode)
            .map(Ok)
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
    type Error = ciborium::de::Error<std::io::Error>;

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
            module_options_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl From<CodePackage> for Bytes {
    fn from(code_package: CodePackage) -> Self {
        code_package.into_bytes()
    }
}

fn strip_comments(module_source: &str) -> String {
    let comment_re = Regex::new(r"(?m)^\s*//.*|/\*[\s\S]*?\*/").unwrap();
    comment_re.replace_all(module_source, "").to_string()
}

fn name_default_export(module_source: &str) -> String {
    module_source.replace("export default ", "export const __default__ = ")
}

fn rewrite_run_functions(module_source: &str) -> String {
    // Define the regex to match `export const/let` declarations with the specified functions
    let re = Regex::new(r"(?m)^(\s*)export\s+(const|let)\s+(\w+)\s*=\s*(runOnHttp|runOnProvenEvent|runOnRadixEvent|runOnSchedule|runWithOptions)\(").unwrap();

    // Replace the matched string with the modified version
    let result = re.replace_all(module_source, |caps: &regex::Captures| {
        format!(
            "{}export {} {} = {}('{}', ",
            &caps[1], &caps[2], &caps[3], &caps[4], &caps[3]
        )
    });

    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::options::HandlerOptions;

    use std::collections::HashSet;

    #[test]
    fn test_code_package_from_str() {
        let source = include_str!("../test_esm/handler/test_http_handler.ts");

        let code_package = CodePackage::from_str(source).unwrap();

        assert_eq!(code_package.specifiers().len(), 1);
    }

    #[test]
    fn test_parse_module_handler_options() {
        let code_package = CodePackage::from_str(
            r"
            import { runOnHttp, runWithOptions } from '@proven-network/handler';
            import { getApplicationDb, sql } from '@proven-network/sql';

            const DB = getApplicationDb('main');
            DB.migrate('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);');

            export const handler = runOnHttp((x,y) => {
                console.log(x, y);
            }, {
                path: '/hello',
                timeout: 5000
            });

            export default runWithOptions((x,y) => {
                console.log(x, y);
            }, {
                timeout: 2000
            });
        ",
        )
        .unwrap();

        let module_options = code_package
            .get_module_options(&ModuleSpecifier::parse("file:///main.ts").unwrap())
            .unwrap();

        assert_eq!(module_options.handler_options.len(), 2);
        assert!(module_options.handler_options.contains_key("handler"));
        assert!(module_options.handler_options.contains_key("__default__"));

        assert!(module_options
            .sql_migrations
            .application
            .contains_key("main"));
        assert_eq!(
            module_options
                .sql_migrations
                .application
                .get("main")
                .unwrap(),
            &vec!["CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);".to_string()]
        );

        assert_eq!(
            module_options.handler_options.get("handler").unwrap(),
            &HandlerOptions::Http {
                allowed_web_origins: HashSet::new(),
                path: Some("/hello".to_string()),
                max_heap_mbs: None,
                timeout_millis: Some(5000),
            }
        );

        assert_eq!(
            module_options.handler_options.get("__default__").unwrap(),
            &HandlerOptions::Rpc {
                allowed_web_origins: HashSet::new(),
                max_heap_mbs: None,
                timeout_millis: Some(2000),
            }
        );
    }

    #[test]
    fn test_strip_comments() {
        let source = r"
        // This is a comment
        const x = 42;/* This is another comment */
        /* This is another comment */console.log(x);
    ";
        let expected = r"
        const x = 42;
        console.log(x);
    ";
        assert_eq!(strip_comments(source), expected);
    }

    #[test]
    fn test_name_default_export() {
        let source = r#"
        export default function() {
            console.log("Hello, world!");
        }
    "#;
        let expected = r#"
        export const __default__ = function() {
            console.log("Hello, world!");
        }
    "#;
        assert_eq!(name_default_export(source), expected);
    }

    #[test]
    fn test_rewrite_run_functions() {
        let source = r"
        export const handler = runWithOptions((x,y) => {
            console.log(x, y);
        }, {
            timeout: 5000
        });
    ";
        let expected = r"
        export const handler = runWithOptions('handler', (x,y) => {
            console.log(x, y);
        }, {
            timeout: 5000
        });
    ";
        assert_eq!(rewrite_run_functions(source), expected);
    }
}
