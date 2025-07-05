use std::collections::HashMap;
use std::path::Path;

use deno_semver::package::PackageReq;
use serde::{Deserialize, Serialize};

/// Represents a package.json file structure with the fields relevant to dependency resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageJson {
    /// The name of the package.
    #[serde(default)]
    pub name: Option<String>,
    
    /// The version of the package.
    #[serde(default)]
    pub version: Option<String>,
    
    /// Production dependencies.
    #[serde(default)]
    pub dependencies: HashMap<String, String>,
    
    /// Development dependencies.
    #[serde(default, rename = "devDependencies")]
    pub dev_dependencies: HashMap<String, String>,
    
    /// Peer dependencies.
    #[serde(default, rename = "peerDependencies")]
    pub peer_dependencies: HashMap<String, String>,
    
    /// Optional dependencies.
    #[serde(default, rename = "optionalDependencies")]
    pub optional_dependencies: HashMap<String, String>,
}

impl PackageJson {
    /// Parses a package.json from a JSON string.
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON is malformed or doesn't match the expected schema.
    pub fn from_str(content: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(content)
    }

    /// Reads and parses a package.json file from the filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the JSON is malformed.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        Ok(Self::from_str(&content)?)
    }

    /// Gets all dependencies (combining dependencies and dev dependencies).
    /// Returns tuples of (`package_name`, `version_spec`).
    #[must_use]
    pub fn all_dependencies(&self) -> Vec<(String, String)> {
        let mut deps = Vec::new();
        
        for (name, version) in &self.dependencies {
            deps.push((name.clone(), version.clone()));
        }
        
        for (name, version) in &self.dev_dependencies {
            if !self.dependencies.contains_key(name) {
                deps.push((name.clone(), version.clone()));
            }
        }
        
        deps
    }

    /// Gets only production dependencies (excluding dev dependencies).
    #[must_use]
    pub fn production_dependencies(&self) -> Vec<(String, String)> {
        self.dependencies.iter()
            .map(|(name, version)| (name.clone(), version.clone()))
            .collect()
    }

    /// Converts package.json dependencies to `PackageReq` objects that can be resolved.
    ///
    /// # Errors
    ///
    /// Returns an error if any package name is invalid or cannot be parsed.
    pub fn to_package_reqs(&self, include_dev: bool) -> Result<Vec<PackageReq>, String> {
        let dependencies = if include_dev {
            self.all_dependencies()
        } else {
            self.production_dependencies()
        };

        let mut package_reqs = Vec::new();
        
        for (name, version_spec) in dependencies {
            // Skip non-npm dependencies (file:, git:, http:, etc.)
            if is_npm_dependency(&version_spec) {
                // Try to parse the package requirement, but handle complex version ranges gracefully
                match PackageReq::from_str(&format!("{name}@{version_spec}")) {
                    Ok(package_req) => package_reqs.push(package_req),
                    Err(_) => {
                        // If the complex version range fails, try with "*" as a fallback
                        // This allows the dependency to be included even if the exact version range isn't supported
                        match PackageReq::from_str(&format!("{name}@*")) {
                            Ok(package_req) => package_reqs.push(package_req),
                            Err(e) => return Err(format!("Invalid package name '{name}': {e}")),
                        }
                    }
                }
            }
        }
        
        Ok(package_reqs)
    }
}

/// Checks if a version specification refers to an NPM package (not a file, git, or other external dependency).
fn is_npm_dependency(version_spec: &str) -> bool {
    !version_spec.starts_with("file:")
        && !version_spec.starts_with("git:")
        && !version_spec.starts_with("git+")
        && !version_spec.starts_with("github:")
        && !version_spec.starts_with("http:")
        && !version_spec.starts_with("https:")
        && !version_spec.starts_with("./")
        && !version_spec.starts_with("../")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_package_json_parsing() {
        let content = r#"
        {
            "name": "my-package",
            "version": "1.0.0",
            "dependencies": {
                "lodash": "^4.17.21",
                "axios": "~0.21.0"
            },
            "devDependencies": {
                "typescript": "^4.5.0",
                "jest": "*"
            },
            "peerDependencies": {
                "react": ">=16.0.0"
            }
        }
        "#;

        let package_json = PackageJson::from_str(content).unwrap();
        
        assert_eq!(package_json.name, Some("my-package".to_string()));
        assert_eq!(package_json.version, Some("1.0.0".to_string()));
        assert_eq!(package_json.dependencies.len(), 2);
        assert_eq!(package_json.dev_dependencies.len(), 2);
        assert_eq!(package_json.peer_dependencies.len(), 1);
        
        let all_deps = package_json.all_dependencies();
        assert_eq!(all_deps.len(), 4); // 2 dependencies + 2 dev dependencies
        
        let prod_deps = package_json.production_dependencies();
        assert_eq!(prod_deps.len(), 2); // Only production dependencies
    }

    #[test]
    fn test_package_reqs_conversion() {
        let content = r#"
        {
            "dependencies": {
                "lodash": "^4.17.21",
                "my-local-package": "file:../local-package",
                "some-git-package": "git://github.com/user/repo.git"
            }
        }
        "#;

        let package_json = PackageJson::from_str(content).unwrap();
        let package_reqs = package_json.to_package_reqs(false).unwrap();
        
        // Should only include NPM dependencies, not file: or git: dependencies
        assert_eq!(package_reqs.len(), 1);
        assert_eq!(package_reqs[0].name.as_str(), "lodash");
    }

    #[test]
    fn test_is_npm_dependency() {
        assert!(is_npm_dependency("^4.17.21"));
        assert!(is_npm_dependency("~1.0.0"));
        assert!(is_npm_dependency("*"));
        assert!(is_npm_dependency("1.2.3"));
        
        assert!(!is_npm_dependency("file:../local-package"));
        assert!(!is_npm_dependency("git://github.com/user/repo.git"));
        assert!(!is_npm_dependency("git+https://github.com/user/repo.git"));
        assert!(!is_npm_dependency("github:user/repo"));
        assert!(!is_npm_dependency("http://example.com/package.tgz"));
        assert!(!is_npm_dependency("https://example.com/package.tgz"));
        assert!(!is_npm_dependency("./local-path"));
        assert!(!is_npm_dependency("../relative-path"));
    }

    #[test]
    fn test_complex_package_json_parsing() {
        let content = r#"
        {
            "name": "@my-org/complex-package",
            "version": "2.1.0-beta.1",
            "dependencies": {
                "lodash": "^4.17.21",
                "express": "~4.18.0",
                "react": ">=16.0.0 <19.0.0",
                "local-dep": "file:../local-package",
                "git-dep": "git+https://github.com/user/repo.git#v1.0.0"
            },
            "devDependencies": {
                "typescript": "^4.5.0",
                "jest": "*",
                "webpack": "latest"
            },
            "peerDependencies": {
                "react-dom": ">=16.0.0"
            },
            "optionalDependencies": {
                "fsevents": "^2.3.0"
            }
        }
        "#;

        let package_json = PackageJson::from_str(content).unwrap();
        
        assert_eq!(package_json.name, Some("@my-org/complex-package".to_string()));
        assert_eq!(package_json.version, Some("2.1.0-beta.1".to_string()));
        
        // Test dependency counts
        assert_eq!(package_json.dependencies.len(), 5);
        assert_eq!(package_json.dev_dependencies.len(), 3);
        assert_eq!(package_json.peer_dependencies.len(), 1);
        assert_eq!(package_json.optional_dependencies.len(), 1);

        // Test package requirements conversion (should filter out non-npm deps)
        let package_reqs = package_json.to_package_reqs(false).unwrap();
        assert_eq!(package_reqs.len(), 3); // Only lodash, express, and react (no file: or git: deps)

        let package_reqs_with_dev = package_json.to_package_reqs(true).unwrap();
        assert_eq!(package_reqs_with_dev.len(), 6); // 3 prod + 3 dev dependencies

        // Verify specific package requirements
        assert!(package_reqs.iter().any(|req| req.name.as_str() == "lodash"));
        assert!(package_reqs.iter().any(|req| req.name.as_str() == "express"));
        assert!(package_reqs.iter().any(|req| req.name.as_str() == "react"));
    }

    #[test]
    fn test_empty_package_json() {
        let content = r#"
        {
            "name": "minimal-package"
        }
        "#;

        let package_json = PackageJson::from_str(content).unwrap();
        
        assert_eq!(package_json.name, Some("minimal-package".to_string()));
        assert_eq!(package_json.version, None);
        assert!(package_json.dependencies.is_empty());
        assert!(package_json.dev_dependencies.is_empty());
        
        let package_reqs = package_json.to_package_reqs(true).unwrap();
        assert!(package_reqs.is_empty());
    }
}