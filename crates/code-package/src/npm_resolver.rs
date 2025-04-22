use async_trait::async_trait;
use deno_graph::NpmResolvePkgReqsResult;
use deno_graph::source::NpmResolver;
use deno_semver::package::PackageReq;

#[derive(Debug)]
pub struct CodePackageNpmResolver;

#[async_trait(?Send)]
impl NpmResolver for CodePackageNpmResolver {
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
}
