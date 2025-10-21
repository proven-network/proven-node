use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

use deno_core::url::Url;
use deno_permissions::PermissionCheckError;
use rustyscript::{SystemsPermissionKind, WebPermissions};

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
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: host.to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_url(&self, url: &Url, _api_name: &str) -> Result<(), PermissionCheckError> {
        let url_str = url.as_str();
        if self
            .borrow()
            .origins
            .iter()
            .any(|origin| url_str.starts_with(origin))
        {
            Ok(())
        } else {
            Err(PermissionCheckError::PermissionDenied(
                rustyscript::PermissionDeniedError {
                    access: url_str.to_string(),
                    name: "permission",
                    custom_message: None,
                },
            ))
        }
    }

    fn check_read<'a>(
        &self,
        _p: Cow<'a, Path>,
        _api_name: Option<&str>,
    ) -> Result<Cow<'a, Path>, PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "read".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_write<'a>(
        &self,
        p: Cow<'a, Path>,
        _api_name: Option<&str>,
    ) -> Result<Cow<'a, Path>, PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: p.display().to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_open<'a>(
        &self,
        _resolved: bool,
        _read: bool,
        _write: bool,
        _path: Cow<'a, Path>,
        _api_name: &str,
    ) -> Option<Cow<'a, Path>> {
        None
    }

    fn check_read_all(&self, _api_name: Option<&str>) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "read_all".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_read_blind(
        &self,
        _p: &Path,
        _display: &str,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "read_all".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_write_all(&self, _api_name: &str) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "write_all".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_write_blind(
        &self,
        _path: &Path,
        _display: &str,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "write_blind".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_write_partial<'a>(
        &self,
        _path: Cow<'a, Path>,
        _api_name: &str,
    ) -> Result<Cow<'a, Path>, PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "write_partial".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_vsock(
        &self,
        cid: u32,
        port: u32,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: format!("vsock: {cid}:{port}"),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_sys(
        &self,
        kind: SystemsPermissionKind,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: kind.as_str().to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_env(&self, var: &str) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: var.to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }

    fn check_exec(&self) -> Result<(), PermissionCheckError> {
        Err(PermissionCheckError::PermissionDenied(
            rustyscript::PermissionDeniedError {
                access: "ffi".to_string(),
                name: "permission",
                custom_message: None,
            },
        ))
    }
}
