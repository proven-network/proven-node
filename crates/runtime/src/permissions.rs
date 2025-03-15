use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

use rustyscript::{PermissionDenied, SystemsPermissionKind, WebPermissions};

// Inner container for the allowlist permission set
#[derive(Clone, Default, Debug)]
#[allow(clippy::struct_excessive_bools)]
struct OriginAllowlistWebPermissionsSet {
    pub origins: HashSet<String>,
    default_origins: HashSet<String>,
}

/// Permissions manager for the web related extensions
/// Allows only operations that are explicitly enabled
/// Uses interior mutability to allow changing the permissions at runtime
#[derive(Clone, Default, Debug)]
pub struct OriginAllowlistWebPermissions(Arc<RwLock<OriginAllowlistWebPermissionsSet>>);
impl OriginAllowlistWebPermissions {
    /// Create a new instance with the specified default origins
    #[must_use]
    pub fn new(default_origins: impl IntoIterator<Item = String>) -> Self {
        let default_set: HashSet<String> = default_origins.into_iter().collect();
        Self(Arc::new(RwLock::new(OriginAllowlistWebPermissionsSet {
            origins: default_set.clone(),
            default_origins: default_set,
        })))
    }

    fn borrow(&self) -> std::sync::RwLockReadGuard<OriginAllowlistWebPermissionsSet> {
        self.0.read().expect("Could not lock permissions")
    }

    fn borrow_mut(&self) -> std::sync::RwLockWriteGuard<OriginAllowlistWebPermissionsSet> {
        self.0.write().expect("Could not lock permissions")
    }

    /// Add an origin to the allowlist
    pub fn allow_origin(&self, origin: &str) {
        self.borrow_mut().origins.insert(origin.to_string());
    }

    /// Reset the origins list back to the default set
    #[allow(clippy::assigning_clones)]
    pub fn reset_to_default(&self) {
        let mut guard = self.borrow_mut();
        guard.origins = guard.default_origins.clone();
    }
}
impl WebPermissions for OriginAllowlistWebPermissions {
    fn allow_hrtime(&self) -> bool {
        false
    }

    fn check_host(
        &self,
        host: &str,
        _port: Option<u16>,
        _api_name: &str,
    ) -> Result<(), PermissionDenied> {
        PermissionDenied::oops(host)?
    }

    fn check_url(
        &self,
        url: &deno_core::url::Url,
        _api_name: &str,
    ) -> Result<(), PermissionDenied> {
        let url_str = url.as_str();
        if self
            .borrow()
            .origins
            .iter()
            .any(|origin| url_str.starts_with(origin))
        {
            Ok(())
        } else {
            PermissionDenied::oops(url)?
        }
    }

    fn check_read<'a>(
        &self,
        _p: &'a Path,
        _api_name: Option<&str>,
    ) -> Result<Cow<'a, Path>, deno_io::fs::FsError> {
        Err(deno_io::fs::FsError::NotCapable(
            "Permission denied for reading",
        ))
    }

    fn check_write<'a>(
        &self,
        p: &'a Path,
        _api_name: Option<&str>,
    ) -> Result<Cow<'a, Path>, PermissionDenied> {
        PermissionDenied::oops(p.display())?
    }

    fn check_open<'a>(
        &self,
        _resolved: bool,
        _read: bool,
        _write: bool,
        _path: &'a Path,
        _api_name: &str,
    ) -> Option<std::borrow::Cow<'a, Path>> {
        None
    }

    fn check_read_all(&self, _api_name: Option<&str>) -> Result<(), PermissionDenied> {
        PermissionDenied::oops("read_all")?
    }

    fn check_read_blind(
        &self,
        _p: &Path,
        _display: &str,
        _api_name: &str,
    ) -> Result<(), PermissionDenied> {
        PermissionDenied::oops("read_all")?
    }

    fn check_write_all(&self, _api_name: &str) -> Result<(), PermissionDenied> {
        PermissionDenied::oops("write_all")?
    }

    fn check_write_blind(
        &self,
        _path: &Path,
        _display: &str,
        _api_name: &str,
    ) -> Result<(), PermissionDenied> {
        PermissionDenied::oops("write_blind")?
    }

    fn check_write_partial(
        &self,
        _path: &str,
        _api_name: &str,
    ) -> Result<std::path::PathBuf, PermissionDenied> {
        PermissionDenied::oops("write_partial")?
    }

    fn check_sys(
        &self,
        kind: SystemsPermissionKind,
        _api_name: &str,
    ) -> Result<(), PermissionDenied> {
        PermissionDenied::oops(kind.as_str())?
    }

    fn check_env(&self, var: &str) -> Result<(), PermissionDenied> {
        PermissionDenied::oops(var)?
    }

    fn check_exec(&self) -> Result<(), PermissionDenied> {
        PermissionDenied::oops("ffi")?
    }
}
