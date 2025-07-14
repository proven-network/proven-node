use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use url::Url;

/// Represents a web origin (scheme + host + optional port)
///
/// An origin consists of a scheme (http/https), host, and optional port.
/// This type ensures that origins are properly formatted and validated.
///
/// # Examples
///
/// ```
/// use proven_util::Origin;
/// use std::str::FromStr;
///
/// let origin = Origin::from_str("https://example.com").unwrap();
/// assert_eq!(origin.to_string(), "https://example.com");
///
/// let origin_with_port = Origin::from_str("https://localhost:3000").unwrap();
/// assert_eq!(origin_with_port.to_string(), "https://localhost:3000");
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Origin {
    scheme: String,
    host: String,
    port: Option<u16>,
}

#[derive(Error, Debug)]
pub enum OriginError {
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("Invalid scheme: {scheme}. Only 'http' and 'https' are supported")]
    InvalidScheme { scheme: String },

    #[error("Missing host in URL")]
    MissingHost,

    #[error("URL cannot have a path, query, or fragment")]
    HasPathQueryOrFragment,
}

impl Origin {
    /// Creates a new Origin from components
    ///
    /// # Arguments
    ///
    /// * `scheme` - The scheme (http or https)
    /// * `host` - The host (domain or IP address)
    /// * `port` - Optional port number
    ///
    /// # Errors
    ///
    /// Returns an error if the scheme is not http or https
    pub fn new(scheme: &str, host: &str, port: Option<u16>) -> Result<Self, OriginError> {
        match scheme {
            "http" | "https" => {}
            _ => {
                return Err(OriginError::InvalidScheme {
                    scheme: scheme.to_string(),
                });
            }
        }

        Ok(Origin {
            scheme: scheme.to_string(),
            host: host.to_string(),
            port,
        })
    }

    /// Gets the scheme (http or https)
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Gets the host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Gets the port, if specified
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Returns true if this is an HTTPS origin
    pub fn is_secure(&self) -> bool {
        self.scheme == "https"
    }

    /// Returns true if this is a localhost origin
    pub fn is_localhost(&self) -> bool {
        self.host == "localhost"
            || self.host == "127.0.0.1"
            || self.host == "::1"
            || self.host == "[::1]"
    }

    /// Gets the effective port (actual port or default for scheme)
    pub fn effective_port(&self) -> u16 {
        self.port.unwrap_or_else(|| match self.scheme.as_str() {
            "https" => 443,
            "http" => 80,
            _ => unreachable!("Invalid scheme should have been caught during construction"),
        })
    }
}

impl FromStr for Origin {
    type Err = OriginError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(s)?;

        // Validate scheme
        match url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(OriginError::InvalidScheme {
                    scheme: scheme.to_string(),
                });
            }
        }

        // Ensure we have a host
        let host = url.host_str().ok_or(OriginError::MissingHost)?;

        // Ensure no path, query, or fragment
        if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
            return Err(OriginError::HasPathQueryOrFragment);
        }

        Ok(Origin {
            scheme: url.scheme().to_string(),
            host: host.to_string(),
            port: url.port(),
        })
    }
}

impl PartialEq for Origin {
    fn eq(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.host == other.host
            && self.effective_port() == other.effective_port()
    }
}

impl Eq for Origin {}

impl std::hash::Hash for Origin {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.scheme.hash(state);
        self.host.hash(state);
        self.effective_port().hash(state);
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.host)?;
        if let Some(port) = self.port {
            // Only show port if it's not the default for the scheme
            let default_port = match self.scheme.as_str() {
                "https" => 443,
                "http" => 80,
                _ => unreachable!(),
            };
            if port != default_port {
                write!(f, ":{port}")?;
            }
        }
        Ok(())
    }
}

impl TryFrom<String> for Origin {
    type Error = OriginError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl TryFrom<&str> for Origin {
    type Error = OriginError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl From<Origin> for String {
    fn from(origin: Origin) -> Self {
        origin.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_origin_from_str_https() {
        let origin = Origin::from_str("https://example.com").unwrap();
        assert_eq!(origin.scheme(), "https");
        assert_eq!(origin.host(), "example.com");
        assert_eq!(origin.port(), None);
        assert_eq!(origin.effective_port(), 443);
        assert!(origin.is_secure());
        assert!(!origin.is_localhost());
    }

    #[test]
    fn test_origin_from_str_http_with_port() {
        let origin = Origin::from_str("http://localhost:3000").unwrap();
        assert_eq!(origin.scheme(), "http");
        assert_eq!(origin.host(), "localhost");
        assert_eq!(origin.port(), Some(3000));
        assert_eq!(origin.effective_port(), 3000);
        assert!(!origin.is_secure());
        assert!(origin.is_localhost());
    }

    #[test]
    fn test_origin_display() {
        let origin1 = Origin::from_str("https://example.com").unwrap();
        assert_eq!(origin1.to_string(), "https://example.com");

        let origin2 = Origin::from_str("https://example.com:443").unwrap();
        assert_eq!(origin2.to_string(), "https://example.com"); // Default port omitted

        let origin3 = Origin::from_str("https://example.com:8443").unwrap();
        assert_eq!(origin3.to_string(), "https://example.com:8443"); // Non-default port shown

        let origin4 = Origin::from_str("http://localhost:80").unwrap();
        assert_eq!(origin4.to_string(), "http://localhost"); // Default port omitted
    }

    #[test]
    fn test_origin_new() {
        let origin = Origin::new("https", "example.com", Some(8080)).unwrap();
        assert_eq!(origin.scheme(), "https");
        assert_eq!(origin.host(), "example.com");
        assert_eq!(origin.port(), Some(8080));
    }

    #[test]
    fn test_origin_invalid_scheme() {
        let result = Origin::from_str("ftp://example.com");
        assert!(matches!(result, Err(OriginError::InvalidScheme { .. })));

        let result = Origin::new("ftp", "example.com", None);
        assert!(matches!(result, Err(OriginError::InvalidScheme { .. })));
    }

    #[test]
    fn test_origin_with_path_fails() {
        let result = Origin::from_str("https://example.com/path");
        assert!(matches!(result, Err(OriginError::HasPathQueryOrFragment)));
    }

    #[test]
    fn test_origin_with_query_fails() {
        let result = Origin::from_str("https://example.com?query=1");
        assert!(matches!(result, Err(OriginError::HasPathQueryOrFragment)));
    }

    #[test]
    fn test_origin_with_fragment_fails() {
        let result = Origin::from_str("https://example.com#fragment");
        assert!(matches!(result, Err(OriginError::HasPathQueryOrFragment)));
    }

    #[test]
    fn test_origin_localhost_detection() {
        assert!(Origin::from_str("http://localhost").unwrap().is_localhost());
        assert!(Origin::from_str("http://127.0.0.1").unwrap().is_localhost());
        assert!(Origin::from_str("http://[::1]").unwrap().is_localhost());
        assert!(
            !Origin::from_str("http://example.com")
                .unwrap()
                .is_localhost()
        );
    }

    #[test]
    fn test_origin_serialization() {
        let origin = Origin::from_str("https://example.com:8080").unwrap();
        let json = serde_json::to_string(&origin).unwrap();
        let deserialized: Origin = serde_json::from_str(&json).unwrap();
        assert_eq!(origin, deserialized);
    }

    #[test]
    fn test_origin_equality() {
        let origin1 = Origin::from_str("https://example.com").unwrap();
        let origin2 = Origin::from_str("https://example.com:443").unwrap();
        let origin3 = Origin::new("https", "example.com", None).unwrap();
        let origin4 = Origin::new("https", "example.com", Some(443)).unwrap();

        // These should all be equal since 443 is the default HTTPS port
        assert_eq!(origin1, origin2);
        assert_eq!(origin1, origin3);
        assert_eq!(origin1, origin4);
    }

    #[test]
    fn test_origin_try_from_string() {
        let origin_string = "https://example.com".to_string();
        let origin: Origin = origin_string.try_into().unwrap();
        assert_eq!(origin.scheme(), "https");
        assert_eq!(origin.host(), "example.com");

        let invalid_string = "ftp://example.com".to_string();
        let result: Result<Origin, _> = invalid_string.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_origin_try_from_str() {
        let origin: Origin = "http://localhost:3000".try_into().unwrap();
        assert_eq!(origin.scheme(), "http");
        assert_eq!(origin.host(), "localhost");
        assert_eq!(origin.port(), Some(3000));

        let result: Result<Origin, _> = "https://example.com/path".try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_origin_into_string() {
        let origin = Origin::from_str("https://example.com:8080").unwrap();
        let origin_string: String = origin.into();
        assert_eq!(origin_string, "https://example.com:8080");
    }
}
