use deno_permissions::{PermissionCheckError, PermissionDeniedError};
use rustyscript::WebPermissions;

pub struct NoWebPermissions;

impl NoWebPermissions {
    pub fn new() -> Self {
        Self
    }
}

impl WebPermissions for NoWebPermissions {
    fn check_host(
        &self,
        host: &str,
        _port: Option<u16>,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionDeniedError {
            access: host.to_string(),
            name: "check_host",
        }
        .into())
    }

    fn check_url(
        &self,
        url: &deno_core::url::Url,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        Err(PermissionDeniedError {
            access: url.to_string(),
            name: "check_url",
        }
        .into())
    }

    fn check_read<'a>(
        &self,
        path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, PermissionCheckError> {
        Err(PermissionDeniedError {
            access: path.display().to_string(),
            name: "check_read",
        }
        .into())
    }

    fn check_write<'a>(
        &self,
        path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, PermissionCheckError> {
        Err(PermissionDeniedError {
            access: path.display().to_string(),
            name: "check_write",
        }
        .into())
    }
}
