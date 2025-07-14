use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

use deno_core::url::Url;
use deno_fs::{CheckedPath, FsError, GetPath};
use rustyscript::{PermissionDeniedError, SystemsPermissionKind, WebPermissions};

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

    fn borrow(&'_ self) -> std::sync::RwLockReadGuard<'_, OriginAllowlistWebPermissionsSet> {
        self.0.read().expect("Could not lock permissions")
    }

    fn borrow_mut(&'_ self) -> std::sync::RwLockWriteGuard<'_, OriginAllowlistWebPermissionsSet> {
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
    ) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: host.to_string(),
        })
    }

    fn check_url(&self, url: &Url, _api_name: &str) -> Result<(), PermissionDeniedError> {
        let url_str = url.as_str();
        if self
            .borrow()
            .origins
            .iter()
            .any(|origin| url_str.starts_with(origin))
        {
            Ok(())
        } else {
            Err(PermissionDeniedError::Fatal {
                access: url_str.to_string(),
            })
        }
    }

    fn check_read<'a>(
        &self,
        _p: &'a Path,
        _api_name: Option<&str>,
    ) -> Result<Cow<'a, Path>, PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "read".to_string(),
        })
    }

    fn check_write<'a>(
        &self,
        p: &'a Path,
        _api_name: Option<&str>,
    ) -> Result<Cow<'a, Path>, PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: p.display().to_string(),
        })
    }

    fn check_open<'a>(
        &self,
        _read: bool,
        _write: bool,
        _path: Cow<'a, Path>,
        _api_name: &str,
        _get_path: &'a dyn GetPath,
    ) -> Result<CheckedPath<'a>, FsError> {
        Err(FsError::NotCapable("open"))
    }

    fn check_read_path<'a>(
        &self,
        _p: Cow<'a, Path>,
        _api_name: Option<&str>,
        _get_path: &'a dyn GetPath,
    ) -> Result<CheckedPath<'a>, FsError> {
        Err(FsError::NotCapable("read"))
    }

    fn check_read_all(&self, _api_name: Option<&str>) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "read_all".to_string(),
        })
    }

    fn check_read_blind(
        &self,
        _p: &Path,
        _display: &str,
        _api_name: &str,
    ) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "read_all".to_string(),
        })
    }

    fn check_write_all(&self, _api_name: &str) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "write_all".to_string(),
        })
    }

    fn check_write_blind(
        &self,
        _path: &Path,
        _display: &str,
        _api_name: &str,
    ) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "write_blind".to_string(),
        })
    }

    fn check_write_partial(
        &self,
        _path: &str,
        _api_name: &str,
    ) -> Result<std::path::PathBuf, PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "write_partial".to_string(),
        })
    }

    fn check_sys(
        &self,
        kind: SystemsPermissionKind,
        _api_name: &str,
    ) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: kind.as_str().to_string(),
        })
    }

    fn check_env(&self, var: &str) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: var.to_string(),
        })
    }

    fn check_exec(&self) -> Result<(), PermissionDeniedError> {
        Err(PermissionDeniedError::Fatal {
            access: "ffi".to_string(),
        })
    }
}
