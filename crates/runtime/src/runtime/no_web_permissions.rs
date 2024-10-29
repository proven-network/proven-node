use deno_core::anyhow::anyhow;
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
        _host: &str,
        _port: Option<u16>,
        _api_name: &str,
    ) -> Result<(), deno_core::error::AnyError> {
        Err(anyhow!("Web access is not allowed"))
    }

    fn check_url(
        &self,
        _url: &deno_core::url::Url,
        _api_name: &str,
    ) -> Result<(), deno_core::error::AnyError> {
        Err(anyhow!("URL access is not allowed"))
    }

    fn check_read<'a>(
        &self,
        _path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, deno_core::error::AnyError> {
        Err(anyhow!("Read access is not allowed"))
    }

    fn check_write<'a>(
        &self,
        _path: &'a std::path::Path,
        _api_name: &str,
    ) -> Result<std::borrow::Cow<'a, std::path::Path>, deno_core::error::AnyError> {
        Err(anyhow!("Write access is not allowed"))
    }
}
