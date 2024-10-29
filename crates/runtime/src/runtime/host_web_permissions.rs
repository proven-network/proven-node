use std::collections::HashSet;

use deno_core::anyhow::anyhow;
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
    ) -> Result<(), deno_core::error::AnyError> {
        if self.allowed_hosts.contains(host) {
            Ok(())
        } else {
            Err(anyhow!("Host '{}' is not allowed", host))
        }
    }

    fn check_url(
        &self,
        url: &deno_core::url::Url,
        _api_name: &str,
    ) -> Result<(), deno_core::error::AnyError> {
        if self.allowed_hosts.contains(url.host_str().unwrap()) {
            Ok(())
        } else {
            Err(anyhow!("URL '{}' is not allowed", url))
        }
    }

    fn check_read<'a>(
        &self,
        _path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, deno_core::error::AnyError> {
        Err(anyhow!("Read access is not allowed")) // Disallow all file reads
    }

    fn check_write<'a>(
        &self,
        _path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, deno_core::error::AnyError> {
        Err(anyhow!("Write access is not allowed")) // Disallow all file writes
    }
}
