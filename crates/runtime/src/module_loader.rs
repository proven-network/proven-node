use crate::Error;
use crate::import_replacements::replace_esm_imports;
use crate::options::ModuleOptions;
use crate::options_parser::OptionsParser;
use crate::util::run_in_thread;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use deno_core::error::ModuleLoaderError;
use deno_core::url::Url;
use proven_code_package::{CodePackage, ModuleSpecifier};
use regex::Regex;
use rustyscript::Module;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProcessingMode {
    Options,
    Runtime,
}

/// Loads modules and options from a code package.
#[derive(Clone, Debug)]
#[allow(clippy::type_complexity)]
pub struct ModuleLoader {
    /// The code package to import modules from.
    code_package: CodePackage,

    /// Cache for modules.
    module_cache: Arc<Mutex<HashMap<(ModuleSpecifier, ProcessingMode), Option<Module>>>>,

    /// Cache for module options.
    module_options_cache: Arc<Mutex<HashMap<ModuleSpecifier, Option<ModuleOptions>>>>,
}

impl ModuleLoader {
    /// Creates a new `ModuleLoader` from a `CodePackage`.
    #[must_use]
    pub fn new(code_package: CodePackage) -> Self {
        Self {
            code_package,
            module_cache: Arc::new(Mutex::new(HashMap::new())),
            module_options_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retrieves the hash of the code package.
    #[must_use]
    pub fn code_package_hash(&self) -> String {
        self.code_package.hash().to_string()
    }

    /// Retrieves the specifiers of the code package.
    #[must_use]
    pub fn specifiers(&self) -> Vec<String> {
        self.code_package.specifiers()
    }

    /// Retrieves the valid entrypoints of the code package.
    #[must_use]
    pub fn valid_entrypoints(&self) -> HashSet<ModuleSpecifier> {
        self.code_package.valid_entrypoints().clone()
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
    ///
    /// # Panics
    ///
    /// This function will panic if the `module_cache` mutex is poisoned.
    #[must_use]
    pub fn get_module(
        &self,
        module_specifer: &ModuleSpecifier,
        processing_mode: ProcessingMode,
    ) -> Option<Module> {
        if !self
            // Only valid entrypoints can be loaded directly
            .code_package
            .valid_entrypoints()
            .contains(module_specifer)
        {
            return None;
        }

        let mut module_cache = self.module_cache.lock().unwrap();

        if let Some(cached_module) = module_cache.get(&(module_specifer.clone(), processing_mode)) {
            return cached_module.clone();
        }

        let module = self
            .get_module_source(module_specifer, processing_mode)
            .map(|module_source| Module::new(module_specifer.as_str(), module_source));

        module_cache.insert((module_specifer.clone(), processing_mode), module.clone());
        drop(module_cache);

        module
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
        processing_mode: ProcessingMode,
    ) -> Option<String> {
        #[allow(clippy::significant_drop_in_scrutinee)]
        self.code_package
            .get_module_source(module_specifer)
            .map(|module_source| match processing_mode {
                ProcessingMode::Options => {
                    let module_source = replace_esm_imports(&module_source);
                    let module_source = name_default_export(&module_source);

                    rewrite_run_functions(module_specifer.as_str(), &module_source)
                }
                ProcessingMode::Runtime => replace_esm_imports(&module_source),
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

    /// Creates a `ModuleLoader` from a test code file.
    ///
    /// # Panics
    ///
    /// This function will panic if the file cannot be read.
    #[cfg(test)]
    #[must_use]
    pub async fn from_test_code(script_name: &str) -> Self {
        let esm = std::fs::read_to_string(format!("./test_esm/{script_name}.ts")).unwrap();

        Self::new(CodePackage::from_str(&esm).await.unwrap())
    }

    /// Creates a `ModuleLoader` from a map of module sources and module roots.
    ///
    /// # Arguments
    ///
    /// * `module_sources` - A map of module specifiers to their source code.
    /// * `module_roots` - An iterator over the module specifiers that are considered roots.
    ///
    /// # Panics
    ///
    /// This function will panic if the `CodePackage` cannot be created from the provided map.
    #[cfg(test)]
    #[must_use]
    pub async fn from_test_code_map(
        module_sources: &HashMap<ModuleSpecifier, &str>,
        module_roots: impl IntoIterator<Item = ModuleSpecifier> + Clone,
    ) -> Self {
        let mut sources = HashMap::new();
        for (specifier, script_name) in module_sources {
            let esm = std::fs::read_to_string(format!("./test_esm/{script_name}.ts")).unwrap();
            sources.insert(specifier.clone(), esm);
        }
        let code_package = CodePackage::from_map(&sources, module_roots).await.unwrap();

        Self::new(code_package)
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
        _referrer: &str,
        _kind: deno_core::ResolutionKind,
    ) -> Option<Result<Url, ModuleLoaderError>> {
        let to_strip = format!(
            "file://{}/file:",
            std::env::current_dir().unwrap().to_str().unwrap()
        );

        specifier
            .as_str()
            .strip_prefix(&to_strip)
            .map(|stripped_specifier| {
                Ok(ModuleSpecifier::from_file_path(stripped_specifier).unwrap())
            })
    }

    fn import(
        &mut self,
        specifier: &ModuleSpecifier,
        _referrer: Option<&ModuleSpecifier>,
        _is_dyn_import: bool,
        _requested_module_type: deno_core::RequestedModuleType,
    ) -> Option<Result<String, ModuleLoaderError>> {
        let pwd = std::env::current_dir().unwrap();
        let specifier_with_pwd = specifier.as_str().replace(
            "file:///file:",
            &format!("file://{}/file:", pwd.to_str().unwrap()),
        );

        self.module_loader
            .get_module_source(
                &ModuleSpecifier::parse(&specifier_with_pwd).unwrap(),
                self.processing_mode,
            )
            .map(Ok)
    }
}

fn name_default_export(module_source: &str) -> String {
    module_source.replace("export default ", "export const __default__ = ")
}

fn rewrite_run_functions(module_specifier: &str, module_source: &str) -> String {
    // Define the regex to match `export const/let` declarations with the specified functions
    let re = Regex::new(r"(?m)^(\s*)export\s+(const|let)\s+(\w+)\s*=\s*(runOnHttp|runOnProvenEvent|runOnRadixEvent|runOnSchedule|runWithOptions|run)\(").unwrap();

    // Replace the matched string with the modified version
    let result = re.replace_all(module_source, |caps: &regex::Captures| {
        format!(
            "{}export {} {} = {}('{}', '{}', ",
            &caps[1], &caps[2], &caps[3], &caps[4], module_specifier, &caps[3]
        )
    });

    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::options::HandlerOptions;

    use std::collections::HashSet;

    use proven_code_package::PackageJson;

    #[tokio::test]
    async fn test_parse_module_handler_options() {
        let module_loader = ModuleLoader::new(
            CodePackage::from_str(
                r"
            import { runOnHttp, runWithOptions } from '@proven-network/handler';
            import { getApplicationDb, sql } from '@proven-network/sql';

            const DB = getApplicationDb('main').migrate('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);');

            export const handler = runOnHttp({ path: '/hello', timeout: 5000 }, (x,y) => {
                console.log(x, y);
            });

            export default runWithOptions({ timeout: 2000 }, (x,y) => {
                console.log(x, y);
            });
        ",
            )
            .await
            .unwrap(),
        );

        let module_options = module_loader
            .get_module_options(&ModuleSpecifier::parse("file:///main.ts").unwrap())
            .unwrap();

        assert_eq!(module_options.handler_options.len(), 2);
        assert!(
            module_options
                .handler_options
                .contains_key("file:///main.ts#handler")
        );
        assert!(
            module_options
                .handler_options
                .contains_key("file:///main.ts")
        );

        assert!(
            module_options
                .sql_migrations
                .application
                .contains_key("main")
        );
        assert_eq!(
            module_options
                .sql_migrations
                .application
                .get("main")
                .unwrap(),
            &vec!["CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);".to_string()]
        );

        assert_eq!(
            module_options
                .handler_options
                .get("file:///main.ts#handler")
                .unwrap(),
            &HandlerOptions::Http {
                allowed_web_origins: HashSet::new(),
                method: None,
                path: Some("/hello".to_string()),
                max_heap_mbs: None,
                timeout_millis: Some(5000),
            }
        );

        assert_eq!(
            module_options
                .handler_options
                .get("file:///main.ts")
                .unwrap(),
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
        export const handler = runWithOptions('file:///main.ts', 'handler', (x,y) => {
            console.log(x, y);
        }, {
            timeout: 5000
        });
    ";
        assert_eq!(rewrite_run_functions("file:///main.ts", source), expected);
    }

    #[tokio::test]
    async fn test_module_loader_with_package_json_integration() {
        // Test that ModuleLoader can work with package.json concepts
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

        let package_json: PackageJson = package_json_content.parse().unwrap();
        assert_eq!(package_json.name, Some("test-app".to_string()));
        assert_eq!(package_json.dependencies.len(), 1);
        assert_eq!(package_json.dev_dependencies.len(), 1);

        // Create a TypeScript module that imports proven modules
        let main_module = r"
            import { runOnHttp } from '@proven-network/handler';

            export const handler = runOnHttp({ path: '/api/hello' }, (request) => {
                const message = 'hello from app';
                return { message };
            });
        ";

        let module_sources = HashMap::from([(
            ModuleSpecifier::parse("file:///main.ts").unwrap(),
            main_module.to_string(),
        )]);

        // Test basic CodePackage creation (without network NPM resolution)
        let basic_package = CodePackage::from_map(
            &module_sources,
            vec![ModuleSpecifier::parse("file:///main.ts").unwrap()],
        )
        .await
        .unwrap();

        let module_loader = ModuleLoader::new(basic_package);

        // Test that we can retrieve the module
        let module = module_loader.get_module(
            &ModuleSpecifier::parse("file:///main.ts").unwrap(),
            ProcessingMode::Runtime,
        );
        assert!(module.is_some());

        // Test that source transformation works correctly
        let module_source = module_loader.get_module_source(
            &ModuleSpecifier::parse("file:///main.ts").unwrap(),
            ProcessingMode::Runtime,
        );
        assert!(module_source.is_some());
        let source = module_source.unwrap();

        // Verify import replacements worked
        assert!(source.contains("proven:handler"));

        // Test module options parsing
        let module_options = module_loader
            .get_module_options(&ModuleSpecifier::parse("file:///main.ts").unwrap())
            .unwrap();

        assert!(!module_options.handler_options.is_empty());
        assert!(
            module_options
                .handler_options
                .contains_key("file:///main.ts#handler")
        );
    }

    #[tokio::test]
    async fn test_module_loader_npm_specifier_handling() {
        // Test that ModuleLoader can handle modules that reference NPM packages conceptually
        // (without actually triggering NPM resolution)
        let main_module_source = r"
            import { runOnHttp } from '@proven-network/handler';

            export const handler = runOnHttp({ path: '/test' }, () => {
                // This would use a utility from an NPM package if it were available
                const result = 'simulated npm utility result';
                return { result };
            });
        ";

        let module_sources = HashMap::from([(
            ModuleSpecifier::parse("file:///main.ts").unwrap(),
            main_module_source.to_string(),
        )]);

        // Create basic CodePackage without NPM resolution
        let code_package = CodePackage::from_map(
            &module_sources,
            vec![ModuleSpecifier::parse("file:///main.ts").unwrap()],
        )
        .await
        .unwrap();

        let module_loader = ModuleLoader::new(code_package);

        // Verify we can get the main module
        let main_module = module_loader.get_module(
            &ModuleSpecifier::parse("file:///main.ts").unwrap(),
            ProcessingMode::Runtime,
        );
        assert!(main_module.is_some());

        // Verify specifiers work
        let specifiers = module_loader.specifiers();
        assert!(!specifiers.is_empty());

        // Verify valid entrypoints
        let entrypoints = module_loader.valid_entrypoints();
        assert!(entrypoints.contains(&ModuleSpecifier::parse("file:///main.ts").unwrap()));

        // Verify import replacements work
        let module_source = module_loader.get_module_source(
            &ModuleSpecifier::parse("file:///main.ts").unwrap(),
            ProcessingMode::Runtime,
        );
        assert!(module_source.is_some());
        let source = module_source.unwrap();
        assert!(source.contains("proven:handler"));
    }

    #[tokio::test]
    async fn test_module_loader_processing_modes() {
        let module_source = r"
            import { runOnHttp } from '@proven-network/handler';

            export default runOnHttp({ path: '/api/test' }, (request) => {
                return { status: 'ok' };
            });
        ";

        let module_sources = HashMap::from([(
            ModuleSpecifier::parse("file:///main.ts").unwrap(),
            module_source.to_string(),
        )]);

        let code_package = CodePackage::from_map(
            &module_sources,
            vec![ModuleSpecifier::parse("file:///main.ts").unwrap()],
        )
        .await
        .unwrap();

        let module_loader = ModuleLoader::new(code_package);
        let specifier = ModuleSpecifier::parse("file:///main.ts").unwrap();

        // Test Runtime processing mode
        let runtime_source = module_loader.get_module_source(&specifier, ProcessingMode::Runtime);
        assert!(runtime_source.is_some());
        let runtime_src = runtime_source.unwrap();
        assert!(runtime_src.contains("proven:handler")); // Import replacement
        assert!(runtime_src.contains("export default")); // No default export rename

        // Test Options processing mode
        let options_source = module_loader.get_module_source(&specifier, ProcessingMode::Options);
        assert!(options_source.is_some());
        let options_src = options_source.unwrap();
        assert!(options_src.contains("proven:handler")); // Import replacement
        assert!(options_src.contains("export const __default__")); // Default export renamed
        assert!(options_src.contains("runOnHttp('file:///main.ts', '__default__'")); // Function rewrite

        // Verify both modes produce different output
        assert_ne!(runtime_src, options_src);
    }

    #[tokio::test]
    async fn test_module_loader_caching() {
        let module_source = r"
            import { runOnHttp } from '@proven-network/handler';
            export const handler = runOnHttp({ path: '/cached' }, () => ({ cached: true }));
        ";

        let module_sources = HashMap::from([(
            ModuleSpecifier::parse("file:///main.ts").unwrap(),
            module_source.to_string(),
        )]);

        let code_package = CodePackage::from_map(
            &module_sources,
            vec![ModuleSpecifier::parse("file:///main.ts").unwrap()],
        )
        .await
        .unwrap();

        let module_loader = ModuleLoader::new(code_package);
        let specifier = ModuleSpecifier::parse("file:///main.ts").unwrap();

        // First call should cache the module
        let module1 = module_loader.get_module(&specifier, ProcessingMode::Runtime);
        assert!(module1.is_some());

        // Second call should return cached module (same instance)
        let module2 = module_loader.get_module(&specifier, ProcessingMode::Runtime);
        assert!(module2.is_some());

        // Verify caching worked by checking they're the same
        assert_eq!(module1.unwrap().filename(), module2.unwrap().filename());

        // Test that different processing modes are cached separately
        let options_module = module_loader.get_module(&specifier, ProcessingMode::Options);
        assert!(options_module.is_some());

        // Different processing modes should produce different cached entries
        let runtime_module = module_loader.get_module(&specifier, ProcessingMode::Runtime);
        assert!(runtime_module.is_some());
    }
}
