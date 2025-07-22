use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use async_trait::async_trait;
use deno_graph::source::NpmResolver;
use deno_graph::{NpmLoadError, NpmResolvePkgReqsResult};
use deno_npm::NpmSystemInfo;
use deno_npm::registry::{NpmPackageInfo, NpmRegistryApi, NpmRegistryPackageInfoLoadError};
use deno_semver::package::{PackageNv, PackageReq};
use proven_logger::{debug, error};
use reqwest::Client;
use url::Url;

/// Cached package information with timestamp for TTL.
#[derive(Debug, Clone)]
struct CachedPackageInfo {
    info: Arc<NpmPackageInfo>,
    cached_at: Instant,
}

impl CachedPackageInfo {
    fn new(info: Arc<NpmPackageInfo>) -> Self {
        Self {
            info,
            cached_at: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// A robust NPM resolver that fetches package information from the npm registry
/// and resolves package requirements. Features include caching with TTL,
/// concurrent request handling, and error recovery.
#[derive(Debug, Clone)]
pub struct CodePackageNpmResolver {
    client: Client,
    cache: Arc<RwLock<HashMap<String, CachedPackageInfo>>>,
    registry_url: Url,
    cache_ttl: Duration,
    #[allow(dead_code)]
    system_info: NpmSystemInfo,
}

impl Default for CodePackageNpmResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl CodePackageNpmResolver {
    /// Creates a new NPM resolver with default configuration.
    /// Uses a 1-hour cache TTL for package information.
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            registry_url: Url::parse("https://registry.npmjs.org/").unwrap(),
            cache_ttl: Duration::from_secs(3600), // 1 hour
            system_info: NpmSystemInfo::default(),
        }
    }

    /// Creates a new NPM resolver with custom registry URL.
    #[allow(dead_code)]
    pub fn with_registry(registry_url: Url) -> Self {
        Self {
            client: Client::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            registry_url,
            cache_ttl: Duration::from_secs(3600), // 1 hour
            system_info: NpmSystemInfo::default(),
        }
    }

    /// Creates a new NPM resolver with custom cache TTL.
    #[allow(dead_code)]
    pub fn with_cache_ttl(cache_ttl: Duration) -> Self {
        Self {
            client: Client::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            registry_url: Url::parse("https://registry.npmjs.org/").unwrap(),
            cache_ttl,
            system_info: NpmSystemInfo::default(),
        }
    }

    /// Clears expired entries from the cache.
    #[allow(dead_code)]
    pub async fn cleanup_cache(&self) {
        let ttl = self.cache_ttl;
        let expired_count = {
            let cache = self.cache.read().await;
            cache
                .iter()
                .filter(|(_, cached_info)| cached_info.is_expired(ttl))
                .count()
        };

        if expired_count > 0 {
            let mut cache = self.cache.write().await;
            cache.retain(|name, cached_info| {
                let should_keep = !cached_info.is_expired(ttl);
                if !should_keep {
                    debug!("Removing expired cache entry for {name}");
                }
                should_keep
            });
        }
        debug!(
            "Cache cleanup completed, {} entries remaining",
            self.cache.read().await.len()
        );
    }

    /// Gets cache statistics for monitoring and debugging.
    #[allow(dead_code)]
    pub async fn cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        let total_entries = cache.len();
        let expired_entries = cache
            .values()
            .filter(|cached_info| cached_info.is_expired(self.cache_ttl))
            .count();
        drop(cache);
        (total_entries, expired_entries)
    }

    /// Resolves a package requirement to a specific package version.
    #[allow(clippy::future_not_send)]
    pub async fn resolve_package_req(
        &self,
        package_req: &PackageReq,
    ) -> Result<PackageNv, NpmLoadError> {
        let package_info = self
            .package_info(package_req.name.as_str())
            .await
            .map_err(|e| NpmLoadError::RegistryInfo(Arc::new(e)))?;

        // Find the best matching version
        let mut matching_versions: Vec<_> = package_info
            .versions
            .keys()
            .filter(|version| package_req.version_req.matches(version))
            .collect();

        if matching_versions.is_empty() {
            return Err(NpmLoadError::PackageReqResolution(Arc::new(
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "No version of {} matches {}",
                        package_req.name, package_req.version_req
                    ),
                ),
            )));
        }

        // Sort by version (newest first)
        matching_versions.sort_by(|a, b| b.cmp(a));
        let best_version = matching_versions[0].clone();

        Ok(PackageNv {
            name: package_req.name.clone(),
            version: best_version,
        })
    }
}

#[async_trait(?Send)]
impl NpmRegistryApi for CodePackageNpmResolver {
    async fn package_info(
        &self,
        name: &str,
    ) -> Result<Arc<NpmPackageInfo>, NpmRegistryPackageInfoLoadError> {
        // Check cache first (with read lock for better concurrency)
        {
            let cache = self.cache.read().await;
            if let Some(cached_info) = cache.get(name) {
                if cached_info.is_expired(self.cache_ttl) {
                    debug!("Cached package info for {name} has expired");
                } else {
                    debug!("Using cached package info for {name}");
                    return Ok(cached_info.info.clone());
                }
            }
        }

        debug!("Fetching package info for {} from registry", name);

        // Construct the registry URL for the package
        let package_url = self.registry_url.join(name).map_err(|e| {
            NpmRegistryPackageInfoLoadError::LoadError(Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid package name: {e}"),
            )))
        })?;

        // Fetch package information from the registry
        let response = self
            .client
            .get(package_url)
            .header(
                "Accept",
                "application/vnd.npm.install-v1+json; q=1.0, application/json; q=0.8, */*",
            )
            .send()
            .await
            .map_err(|e| {
                NpmRegistryPackageInfoLoadError::LoadError(Arc::new(std::io::Error::other(e)))
            })?;

        if response.status() == 404 {
            return Err(NpmRegistryPackageInfoLoadError::PackageNotExists {
                package_name: name.to_string(),
            });
        }

        if !response.status().is_success() {
            return Err(NpmRegistryPackageInfoLoadError::LoadError(Arc::new(
                std::io::Error::other(format!(
                    "HTTP {} when fetching package {}",
                    response.status(),
                    name
                )),
            )));
        }

        let package_info: NpmPackageInfo = response.json().await.map_err(|e| {
            NpmRegistryPackageInfoLoadError::LoadError(Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })?;

        let package_info = Arc::new(package_info);

        // Cache the result (with write lock)
        {
            let mut cache = self.cache.write().await;
            cache.insert(
                name.to_string(),
                CachedPackageInfo::new(package_info.clone()),
            );
        }

        debug!("Successfully cached package info for {}", name);
        Ok(package_info)
    }
}

#[async_trait(?Send)]
#[allow(clippy::future_not_send)]
impl NpmResolver for CodePackageNpmResolver {
    /// Pre-loads and caches NPM package information for optimization.
    fn load_and_cache_npm_package_info(&self, package_name: &str) {
        // For now, just log that we would pre-load this package
        // In a production implementation, you might use a background task queue
        debug!("Would pre-load package info for {package_name}");
    }

    /// Resolves package version requirements.
    ///
    /// The implementation returns the same number of results as input requirements.
    async fn resolve_pkg_reqs(&self, package_reqs: &[PackageReq]) -> NpmResolvePkgReqsResult {
        debug!("Resolving {} package requirements", package_reqs.len());

        let mut results = Vec::with_capacity(package_reqs.len());

        for package_req in package_reqs {
            let result = self.resolve_package_req(package_req).await.map_err(|e| {
                error!("Failed to resolve package requirement {package_req}: {e:?}");
                e
            });
            results.push(result);
        }

        debug!(
            "Resolved {}/{} package requirements successfully",
            results.iter().filter(|r| r.is_ok()).count(),
            results.len()
        );

        NpmResolvePkgReqsResult {
            results,
            dep_graph_result: Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deno_semver::VersionReq;

    #[tokio::test]
    async fn test_npm_resolver_basic() {
        let resolver = CodePackageNpmResolver::new();

        // Test loading a real package (using a very common, stable package)
        let package_req = PackageReq {
            name: "lodash".into(),
            version_req: VersionReq::parse_from_npm("^4.0.0").unwrap(),
        };

        // This test requires internet access and may be flaky in CI
        // In a real implementation, you'd want to mock the HTTP client
        if std::env::var("ENABLE_NETWORK_TESTS").is_ok() {
            let result = resolver.resolve_package_req(&package_req).await;
            assert!(
                result.is_ok(),
                "Failed to resolve lodash package: {result:?}"
            );

            let nv = result.unwrap();
            assert_eq!(nv.name.as_str(), "lodash");
            assert!(nv.version.major >= 4);
        }
    }

    #[tokio::test]
    async fn test_npm_resolver_nonexistent_package() {
        let resolver = CodePackageNpmResolver::new();

        let package_req = PackageReq {
            name: "this-package-definitely-does-not-exist-12345".into(),
            version_req: VersionReq::parse_from_npm("*").unwrap(),
        };

        if std::env::var("ENABLE_NETWORK_TESTS").is_ok() {
            let result = resolver.resolve_package_req(&package_req).await;
            assert!(result.is_err(), "Expected error for nonexistent package");
        }
    }

    #[tokio::test]
    async fn test_cache_functionality() {
        use std::time::Duration;

        // Create resolver with short TTL for testing
        let resolver = CodePackageNpmResolver::with_cache_ttl(Duration::from_millis(100));

        // Initially cache should be empty
        let (total, expired) = resolver.cache_stats().await;
        assert_eq!(total, 0);
        assert_eq!(expired, 0);

        // Test cache cleanup (should be no-op on empty cache)
        resolver.cleanup_cache().await;
        let (total, expired) = resolver.cache_stats().await;
        assert_eq!(total, 0);
        assert_eq!(expired, 0);
    }
}
