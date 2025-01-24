use async_trait::async_trait;
use deno_graph::source::{NpmResolver, UnknownBuiltInNodeModuleError};
use deno_graph::{ModuleSpecifier, NpmResolvePkgReqsResult, Range};
use deno_semver::package::PackageReq;

#[derive(Debug)]
#[allow(dead_code)]
struct CodePackageNpmResolver;

#[async_trait(?Send)]
impl NpmResolver for CodePackageNpmResolver {
    /// Gets the builtin node module name from the specifier (ex. "node:fs" -> "fs").
    fn resolve_builtin_node_module(
        &self,
        _specifier: &ModuleSpecifier,
    ) -> Result<Option<String>, UnknownBuiltInNodeModuleError> {
        unimplemented!()
    }

    /// The callback when a bare specifier is resolved to a builtin node module.
    /// (Note: used for printing warnings to discourage that usage of bare specifiers)
    fn on_resolve_bare_builtin_node_module(&self, _module_name: &str, _range: &Range) {
        unimplemented!()
    }

    /// This is an optimization for the implementation to start loading and caching
    /// the npm registry package information ahead of time.
    fn load_and_cache_npm_package_info(&self, _package_name: &str) {
        unimplemented!()
    }

    /// Resolves a the package version requirements.
    ///
    /// The implementation MUST return the same amount of resolutions back as
    /// version reqs provided or else a panic will occur.
    async fn resolve_pkg_reqs(&self, _package_req: &[PackageReq]) -> NpmResolvePkgReqsResult {
        unimplemented!()
    }

    /// Returns true when bare node specifier resoluion is enabled
    fn enables_bare_builtin_node_module(&self) -> bool {
        true
    }
}
