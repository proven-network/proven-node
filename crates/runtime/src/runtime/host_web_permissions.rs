use std::collections::HashSet;

use deno_permissions::{PermissionCheckError, PermissionDeniedError};
use rustyscript::WebPermissions;

pub struct HostWebPermissions {
    allowed_hosts: HashSet<String>,
}

impl HostWebPermissions {
    pub fn new(allowed_hosts: HashSet<String>) -> Self {
        Self { allowed_hosts }
    }
}

impl WebPermissions for HostWebPermissions {
    fn check_host(
        &self,
        host: &str,
        _port: Option<u16>,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        if self.allowed_hosts.contains(host) {
            Ok(())
        } else {
            Err(PermissionDeniedError {
                access: host.to_string(),
                name: "check_host",
            }
            .into())
        }
    }

    fn check_url(
        &self,
        url: &deno_core::url::Url,
        _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
        if url.scheme() != "https" {
            return Err(PermissionDeniedError {
                access: url.to_string(),
                name: "check_url - not https",
            }
            .into());
        }

        if url.host_str().unwrap() == "localhost" {
            return Err(PermissionDeniedError {
                access: url.to_string(),
                name: "check_url - localhost",
            }
            .into());
        }

        if self.allowed_hosts.contains(url.host_str().unwrap()) {
            Ok(())
        } else {
            Err(PermissionDeniedError {
                access: url.to_string(),
                name: "check_url",
            }
            .into())
        }
    }

    fn check_read<'a>(
        &self,
        path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, PermissionCheckError> {
        // Disallow all file reads
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
        // Disallow all file writes
        Err(PermissionDeniedError {
            access: path.display().to_string(),
            name: "check_write",
        }
        .into())
    }
}
