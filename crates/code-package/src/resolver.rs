use async_trait::async_trait;
use deno_graph::source::{ResolutionKind, ResolveError, Resolver};
use deno_graph::{ModuleSpecifier, Range, resolve_import};
use std::collections::HashMap;
use std::path::Path;

const DEFAULT_JSX_IMPORT_SOURCE_MODULE: &str = "jsx-runtime";

#[derive(Debug)]
pub struct CodePackageResolver;

#[async_trait(?Send)]
impl Resolver for CodePackageResolver {
    /// An optional method that returns the default JSX import source if one is
    /// configured. If this method returns `Some` and a JSX file is encountered
    /// that does not have an import source specified as a pragma, this import
    /// source will be used instead.
    fn default_jsx_import_source(&self, _referrer: &ModuleSpecifier) -> Option<String> {
        None
    }

    /// An optional method that returns the default JSX types import source if one
    /// is configured. If this method returns `Some` and a JSX file is encountered
    /// that does not have an types import source specified as a pragma, this
    /// types import source will be used instead.
    fn default_jsx_import_source_types(&self, _referrer: &ModuleSpecifier) -> Option<String> {
        None
    }

    /// An optional method which returns the JSX import source module which will
    /// be appended to any JSX import source pragmas identified.
    fn jsx_import_source_module(&self, _referrer: &ModuleSpecifier) -> &str {
        DEFAULT_JSX_IMPORT_SOURCE_MODULE
    }

    /// Given a string specifier and a referring module specifier, return a
    /// resolved module specifier.
    fn resolve(
        &self,
        specifier_text: &str,
        referrer_range: &Range,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, ResolveError> {
        match specifier_text {
            // First-party packages
            "@proven-network/crypto" => Ok(ModuleSpecifier::parse("proven:crypto").unwrap()),
            "@proven-network/handler" => Ok(ModuleSpecifier::parse("proven:handler").unwrap()),
            "@proven-network/kv" => Ok(ModuleSpecifier::parse("proven:kv").unwrap()),
            "@proven-network/rpc" => Ok(ModuleSpecifier::parse("proven:rpc").unwrap()),
            "@proven-network/session" => Ok(ModuleSpecifier::parse("proven:session").unwrap()),
            "@proven-network/sql" => Ok(ModuleSpecifier::parse("proven:sql").unwrap()),

            _ => Ok(resolve_import(specifier_text, &referrer_range.specifier)?),
        }
    }

    /// Given a module specifier, return an optional tuple which provides a module
    /// specifier that contains the types for the module and an optional range
    /// which contains information about the source of the dependency. This will
    /// only be called for module specifiers are resolved to a non-typed input
    /// (e.g. JavaScript and JSX) and there is not yet types resolved for this
    /// module. Any result will be set on the modules `maybe_types_dependency`
    /// property.
    fn resolve_types(
        &self,
        _specifier: &ModuleSpecifier,
    ) -> Result<Option<(ModuleSpecifier, Option<Range>)>, ResolveError> {
        Ok(None)
    }
}

/// A resolver that is aware of the available modules and can properly resolve
/// extension-less imports to their full specifiers.
#[derive(Debug)]
pub struct ManifestAwareResolver {
    available_modules: HashMap<String, ModuleSpecifier>,
}

impl ManifestAwareResolver {
    pub fn new(module_sources: &HashMap<ModuleSpecifier, String>) -> Self {
        let mut available_modules = HashMap::new();

        // Build a map of extension-less paths to their full specifiers
        for specifier in module_sources.keys() {
            let specifier_str = specifier.as_str();

            // Extract the extension-less version for lookups
            let path = Path::new(specifier_str);
            if let Some(extension) = path.extension() {
                if extension.eq_ignore_ascii_case("tsx") || extension.eq_ignore_ascii_case("jsx") {
                    let extension_less = &specifier_str[..specifier_str.len() - 4];
                    available_modules.insert(extension_less.to_string(), specifier.clone());
                } else if extension.eq_ignore_ascii_case("ts")
                    || extension.eq_ignore_ascii_case("js")
                {
                    let extension_less = &specifier_str[..specifier_str.len() - 3];
                    available_modules.insert(extension_less.to_string(), specifier.clone());
                }
            }
        }

        Self { available_modules }
    }
}

#[async_trait(?Send)]
impl Resolver for ManifestAwareResolver {
    fn default_jsx_import_source(&self, _referrer: &ModuleSpecifier) -> Option<String> {
        None
    }

    fn default_jsx_import_source_types(&self, _referrer: &ModuleSpecifier) -> Option<String> {
        None
    }

    fn jsx_import_source_module(&self, _referrer: &ModuleSpecifier) -> &str {
        DEFAULT_JSX_IMPORT_SOURCE_MODULE
    }

    fn resolve(
        &self,
        specifier_text: &str,
        referrer_range: &Range,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, ResolveError> {
        match specifier_text {
            // First-party packages
            "@proven-network/crypto" => Ok(ModuleSpecifier::parse("proven:crypto").unwrap()),
            "@proven-network/handler" => Ok(ModuleSpecifier::parse("proven:handler").unwrap()),
            "@proven-network/kv" => Ok(ModuleSpecifier::parse("proven:kv").unwrap()),
            "@proven-network/rpc" => Ok(ModuleSpecifier::parse("proven:rpc").unwrap()),
            "@proven-network/session" => Ok(ModuleSpecifier::parse("proven:session").unwrap()),
            "@proven-network/sql" => Ok(ModuleSpecifier::parse("proven:sql").unwrap()),

            _ => {
                // First try the standard resolution
                let resolved = resolve_import(specifier_text, &referrer_range.specifier)?;

                // If the resolved specifier is extension-less, try to find the actual module
                let resolved_str = resolved.as_str();
                if let Some(full_specifier) = self.available_modules.get(resolved_str) {
                    return Ok(full_specifier.clone());
                }

                Ok(resolved)
            }
        }
    }

    fn resolve_types(
        &self,
        _specifier: &ModuleSpecifier,
    ) -> Result<Option<(ModuleSpecifier, Option<Range>)>, ResolveError> {
        Ok(None)
    }
}
