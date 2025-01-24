use crate::import_replacements::replace_esm_imports;
use crate::options::ModuleOptions;
use crate::options_parser::OptionsParser;
use crate::preprocessor::Preprocessor;
use crate::util::run_in_thread;
use crate::Error;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use proven_code_package::{CodePackage, ModuleSpecifier};
use regex::Regex;
use rustyscript::Module;

pub enum ProcessingMode {
    Options,
    Runtime,
}

/// Loads modules and options from a code package.
#[derive(Clone, Debug)]
pub struct ModuleLoader {
    /// The code package to import modules from.
    code_package: CodePackage,

    /// Cache for module options.
    module_options_cache: Arc<Mutex<HashMap<ModuleSpecifier, Option<ModuleOptions>>>>,
}

impl ModuleLoader {
    /// Creates a new `ModuleLoader` from a `CodePackage`.
    #[must_use]
    pub fn new(code_package: CodePackage) -> Self {
        Self {
            code_package,
            module_options_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retrieves the hash of the code package.
    #[must_use]
    pub fn code_package_hash(&self) -> String {
        self.code_package.hash.clone()
    }

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
    #[must_use]
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
        self.code_package
            .get_module_source(module_specifer)
            .map(|module_source| match processing_mode {
                ProcessingMode::Options => {
                    let module_source = replace_esm_imports(&module_source);
                    let module_source = strip_comments(&module_source);
                    let module_source = name_default_export(&module_source);

                    rewrite_run_functions(&module_source)
                }
                ProcessingMode::Runtime => {
                    let import_replaced_module = replace_esm_imports(&module_source);

                    run_in_thread(move || {
                        let mut preprocessor = Preprocessor::new()?;
                        preprocessor.process(import_replaced_module)
                    })
                    .unwrap()
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
    pub fn import_provider(&self, processing_mode: ProcessingMode) -> ImportProvider {
        ImportProvider {
            module_loader: self.clone(),
            processing_mode,
        }
    }

    #[cfg(test)]
    #[must_use]
    /// Creates a `ModuleLoader` from a test code file.
    ///
    /// # Panics
    ///
    /// This function will panic if the file cannot be read.
    pub fn from_test_code(script_name: &str) -> Self {
        let esm = std::fs::read_to_string(format!("./test_esm/{script_name}.ts")).unwrap();

        Self::new(CodePackage::from_str(&esm).unwrap())
    }
}

pub struct ImportProvider {
    module_loader: ModuleLoader,
    processing_mode: ProcessingMode,
}

impl rustyscript::module_loader::ImportProvider for ImportProvider {
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

        self.module_loader
            .get_module_source(specifier, &self.processing_mode)
            .map(Ok)
    }
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

fn strip_comments(module_source: &str) -> String {
    let comment_re = Regex::new(r"(?m)^\s*//.*|/\*[\s\S]*?\*/").unwrap();
    comment_re.replace_all(module_source, "").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::options::HandlerOptions;

    use std::collections::HashSet;

    #[test]
    fn test_parse_module_handler_options() {
        let module_loader = ModuleLoader::new(
            CodePackage::from_str(
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
            .unwrap(),
        );

        let module_options = module_loader
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
}
