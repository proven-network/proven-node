use public_suffix::{DEFAULT_PROVIDER, EffectiveTLDProvider};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use url::Host;

/// Represents a valid domain name
///
/// A domain consists of a valid hostname with a valid top-level domain (TLD).
/// This type ensures that domains are properly formatted and have valid TLDs.
///
/// # Examples
///
/// ```
/// use proven_util::Domain;
/// use std::str::FromStr;
///
/// let domain = Domain::from_str("example.com").unwrap();
/// assert_eq!(domain.to_string(), "example.com");
///
/// let subdomain = Domain::from_str("www.example.com").unwrap();
/// assert_eq!(subdomain.to_string(), "www.example.com");
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Domain {
    domain: String,
    tld: String,
    subdomain: Option<String>,
    root_domain: String,
}

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Invalid domain format: {0}")]
    InvalidFormat(String),

    #[error("Invalid or unknown TLD: {tld}")]
    InvalidTld { tld: String },

    #[error("Domain cannot be empty")]
    Empty,

    #[error("Domain cannot contain scheme or port")]
    ContainsSchemeOrPort,

    #[error("Invalid hostname: {0}")]
    InvalidHostname(String),

    #[error("Cannot determine effective TLD: {0}")]
    CannotDetermineEffectiveTld(String),
}

impl Domain {
    /// Creates a new Domain from a string
    ///
    /// # Arguments
    ///
    /// * `domain` - The domain string to validate
    ///
    /// # Errors
    ///
    /// Returns an error if the domain is invalid or has an unknown TLD
    pub fn new(domain: &str) -> Result<Self, DomainError> {
        Self::from_str(domain)
    }

    /// Gets the full domain string
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Gets the top-level domain (TLD)
    pub fn tld(&self) -> &str {
        &self.tld
    }

    /// Gets the subdomain if present
    pub fn subdomain(&self) -> Option<&str> {
        self.subdomain.as_deref()
    }

    /// Gets the root domain (domain without subdomain)
    pub fn root_domain(&self) -> &str {
        &self.root_domain
    }

    /// Returns true if this is a subdomain (has a subdomain part)
    pub fn is_subdomain(&self) -> bool {
        self.subdomain.is_some()
    }

    /// Returns true if this is a localhost domain
    pub fn is_localhost(&self) -> bool {
        self.domain == "localhost"
    }

    /// Returns the parent domain if this is a subdomain
    ///
    /// # Examples
    ///
    /// ```
    /// use proven_util::Domain;
    /// use std::str::FromStr;
    ///
    /// let domain = Domain::from_str("www.example.com").unwrap();
    /// let parent = domain.parent_domain().unwrap();
    /// assert_eq!(parent.to_string(), "example.com");
    /// ```
    pub fn parent_domain(&self) -> Option<Domain> {
        if let Some(subdomain) = &self.subdomain {
            // Remove the first subdomain part
            let parts: Vec<&str> = subdomain.split('.').collect();
            if parts.len() > 1 {
                let remaining_subdomain = parts[1..].join(".");
                Some(Domain {
                    domain: format!("{}.{}", remaining_subdomain, self.root_domain),
                    tld: self.tld.clone(),
                    subdomain: Some(remaining_subdomain),
                    root_domain: self.root_domain.clone(),
                })
            } else {
                // Return the root domain
                Some(Domain {
                    domain: self.root_domain.clone(),
                    tld: self.tld.clone(),
                    subdomain: None,
                    root_domain: self.root_domain.clone(),
                })
            }
        } else {
            None
        }
    }
}

impl FromStr for Domain {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(DomainError::Empty);
        }

        // Check for scheme
        if s.contains("://") {
            return Err(DomainError::ContainsSchemeOrPort);
        }

        let domain = s.trim().to_lowercase();

        // Special case for localhost
        if domain == "localhost" {
            return Ok(Domain {
                domain: domain.clone(),
                tld: "localhost".to_string(),
                subdomain: None,
                root_domain: domain.clone(),
            });
        }

        // Use url::Host to validate and categorize the input
        match Host::parse(&domain) {
            Ok(Host::Ipv4(_)) | Ok(Host::Ipv6(_)) => {
                // IP addresses are not valid domains
                return Err(DomainError::InvalidFormat(
                    "IP addresses are not valid domains".to_string(),
                ));
            }
            Ok(Host::Domain(_)) => {
                // Valid domain format, continue with public suffix validation
            }
            Err(_) => {
                // Invalid hostname format (could be port, invalid characters, etc.)
                if domain.contains(':') {
                    return Err(DomainError::ContainsSchemeOrPort);
                } else {
                    return Err(DomainError::InvalidHostname(domain));
                }
            }
        }

        // Use public_suffix to get the effective TLD and eTLD+1
        // No fallback - domain must be on the public suffix list
        match DEFAULT_PROVIDER.effective_tld_plus_one(&domain) {
            Ok(etld_plus_one) => {
                // Get the effective TLD by removing the eTLD+1 from the full domain
                let etld = domain
                    .strip_suffix(&format!(".{etld_plus_one}"))
                    .or_else(|| {
                        if domain == etld_plus_one {
                            Some("")
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| DomainError::CannotDetermineEffectiveTld(domain.clone()))?;

                // Parse the eTLD+1 to get domain and TLD parts
                let etld_parts: Vec<&str> = etld_plus_one.split('.').collect();
                if etld_parts.len() < 2 {
                    return Err(DomainError::InvalidFormat(domain));
                }

                // The effective TLD is everything after the first part of eTLD+1
                let effective_tld = etld_parts[1..].join(".");
                let root_domain = etld_plus_one.to_string();

                let subdomain = if etld.is_empty() {
                    None
                } else {
                    Some(etld.to_string())
                };

                Ok(Domain {
                    domain,
                    tld: effective_tld,
                    subdomain,
                    root_domain,
                })
            }
            Err(_) => {
                // Domain is not on the public suffix list - reject it
                Err(DomainError::InvalidTld {
                    tld: "not on public suffix list".to_string(),
                })
            }
        }
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.domain)
    }
}

impl PartialEq for Domain {
    fn eq(&self, other: &Self) -> bool {
        self.domain == other.domain
    }
}

impl Eq for Domain {}

impl std::hash::Hash for Domain {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.domain.hash(state);
    }
}

impl TryFrom<String> for Domain {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl TryFrom<&str> for Domain {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl From<Domain> for String {
    fn from(domain: Domain) -> Self {
        domain.domain
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_from_str_simple() {
        let domain = Domain::from_str("example.com").unwrap();
        assert_eq!(domain.domain(), "example.com");
        assert_eq!(domain.tld(), "com");
        assert_eq!(domain.subdomain(), None);
        assert_eq!(domain.root_domain(), "example.com");
        assert!(!domain.is_subdomain());
        assert!(!domain.is_localhost());
    }

    #[test]
    fn test_domain_from_str_subdomain() {
        let domain = Domain::from_str("www.example.com").unwrap();
        assert_eq!(domain.domain(), "www.example.com");
        assert_eq!(domain.tld(), "com");
        assert_eq!(domain.subdomain(), Some("www"));
        assert_eq!(domain.root_domain(), "example.com");
        assert!(domain.is_subdomain());
        assert!(!domain.is_localhost());
    }

    #[test]
    fn test_domain_from_str_deep_subdomain() {
        let domain = Domain::from_str("api.v1.example.com").unwrap();
        assert_eq!(domain.domain(), "api.v1.example.com");
        assert_eq!(domain.tld(), "com");
        assert_eq!(domain.subdomain(), Some("api.v1"));
        assert_eq!(domain.root_domain(), "example.com");
        assert!(domain.is_subdomain());
    }

    #[test]
    fn test_domain_multi_part_tld() {
        let domain = Domain::from_str("example.co.uk").unwrap();
        assert_eq!(domain.domain(), "example.co.uk");
        assert_eq!(domain.tld(), "co.uk");
        assert_eq!(domain.subdomain(), None);
        assert_eq!(domain.root_domain(), "example.co.uk");
        assert!(!domain.is_subdomain());
    }

    #[test]
    fn test_domain_subdomain_with_multi_part_tld() {
        let domain = Domain::from_str("www.example.co.uk").unwrap();
        assert_eq!(domain.domain(), "www.example.co.uk");
        assert_eq!(domain.tld(), "co.uk");
        assert_eq!(domain.subdomain(), Some("www"));
        assert_eq!(domain.root_domain(), "example.co.uk");
        assert!(domain.is_subdomain());
    }

    #[test]
    fn test_domain_localhost() {
        let domain = Domain::from_str("localhost").unwrap();
        assert_eq!(domain.domain(), "localhost");
        assert_eq!(domain.tld(), "localhost");
        assert_eq!(domain.subdomain(), None);
        assert_eq!(domain.root_domain(), "localhost");
        assert!(!domain.is_subdomain());
        assert!(domain.is_localhost());
    }

    #[test]
    fn test_domain_ipv4_invalid() {
        let result = Domain::from_str("192.168.1.1");
        assert!(matches!(result, Err(DomainError::InvalidFormat(_))));
    }

    #[test]
    fn test_domain_ipv6_invalid() {
        let result = Domain::from_str("2001:db8::1");
        assert!(matches!(result, Err(DomainError::ContainsSchemeOrPort)));
    }

    #[test]
    fn test_domain_ipv6_bracketed_invalid() {
        let result = Domain::from_str("[2001:db8::1]");
        assert!(matches!(result, Err(DomainError::InvalidFormat(_))));
    }

    #[test]
    fn test_domain_invalid_empty() {
        let result = Domain::from_str("");
        assert!(matches!(result, Err(DomainError::Empty)));
    }

    #[test]
    fn test_domain_invalid_with_scheme() {
        let result = Domain::from_str("https://example.com");
        assert!(matches!(result, Err(DomainError::ContainsSchemeOrPort)));
    }

    #[test]
    fn test_domain_invalid_with_port() {
        let result = Domain::from_str("example.com:8080");
        assert!(matches!(result, Err(DomainError::ContainsSchemeOrPort)));
    }

    #[test]
    fn test_domain_display() {
        let domain = Domain::from_str("www.example.com").unwrap();
        assert_eq!(domain.to_string(), "www.example.com");
    }

    #[test]
    fn test_domain_equality() {
        let domain1 = Domain::from_str("example.com").unwrap();
        let domain2 = Domain::from_str("example.com").unwrap();
        let domain3 = Domain::from_str("www.example.com").unwrap();

        assert_eq!(domain1, domain2);
        assert_ne!(domain1, domain3);
    }

    #[test]
    fn test_domain_parent_domain() {
        let domain = Domain::from_str("api.v1.example.com").unwrap();
        let parent = domain.parent_domain().unwrap();
        assert_eq!(parent.to_string(), "v1.example.com");

        let parent2 = parent.parent_domain().unwrap();
        assert_eq!(parent2.to_string(), "example.com");

        let parent3 = parent2.parent_domain();
        assert!(parent3.is_none());
    }

    #[test]
    fn test_domain_serialization() {
        let domain = Domain::from_str("www.example.com").unwrap();
        let json = serde_json::to_string(&domain).unwrap();
        let deserialized: Domain = serde_json::from_str(&json).unwrap();
        assert_eq!(domain, deserialized);
    }

    #[test]
    fn test_domain_case_insensitive() {
        let domain1 = Domain::from_str("Example.COM").unwrap();
        let domain2 = Domain::from_str("example.com").unwrap();
        assert_eq!(domain1, domain2);
        assert_eq!(domain1.to_string(), "example.com");
    }

    #[test]
    fn test_domain_try_from_string() {
        let domain_string = "example.com".to_string();
        let domain: Domain = domain_string.try_into().unwrap();
        assert_eq!(domain.domain(), "example.com");

        let invalid_string = "192.168.1.1".to_string();
        let result: Result<Domain, _> = invalid_string.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_domain_try_from_str() {
        let domain: Domain = "www.example.com".try_into().unwrap();
        assert_eq!(domain.domain(), "www.example.com");
        assert_eq!(domain.subdomain(), Some("www"));

        let result: Result<Domain, _> = "192.168.1.1".try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_domain_into_string() {
        let domain = Domain::from_str("example.com").unwrap();
        let domain_string: String = domain.into();
        assert_eq!(domain_string, "example.com");
    }
}
