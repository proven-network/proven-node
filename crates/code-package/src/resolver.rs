use async_trait::async_trait;
use deno_graph::source::{ResolutionKind, ResolveError, Resolver};
use deno_graph::{resolve_import, ModuleSpecifier, Range};

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
            "@proven-network/session" => Ok(ModuleSpecifier::parse("proven:session").unwrap()),
            "@proven-network/sql" => Ok(ModuleSpecifier::parse("proven:sql").unwrap()),

            // Vendor packages
            // TODO: Load dynamically from registry
            "@radixdlt/babylon-gateway-api-sdk" => {
                Ok(ModuleSpecifier::parse("proven:babylon_gateway_api").unwrap())
            }
            "@radixdlt/radix-engine-toolkit" => {
                Ok(ModuleSpecifier::parse("proven:radix_engine_toolkit").unwrap())
            }
            "openai" => Ok(ModuleSpecifier::parse("proven:openai").unwrap()),
            "uuid" => Ok(ModuleSpecifier::parse("proven:uuid").unwrap()),
            "zod" => Ok(ModuleSpecifier::parse("proven:zod").unwrap()),
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