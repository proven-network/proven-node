//! Path utilities and helper functions
//!
//! This module contains utility functions for path manipulation and route validation.

use crate::Error;

use std::collections::HashSet;

use proven_runtime::HttpEndpoint;

/// Path utilities
pub struct PathUtils;

impl PathUtils {
    /// Switches the path parameters from the colon-prefixed style, used in `Runtime`, to Axum capture groups.
    pub fn convert_path_use_axum_capture_groups(path: &str) -> String {
        path.split('/')
            .map(|segment| {
                if let Some(without_colon) = segment.strip_prefix(':') {
                    format!("{{{without_colon}}}")
                } else {
                    segment.to_string()
                }
            })
            .collect::<Vec<String>>()
            .join("/")
    }

    /// Normalizes path parameters for comparison
    pub fn normalize_path_parameters(path: &str) -> String {
        path.split('/')
            .map(|segment| {
                if segment.starts_with(':') {
                    ":param".to_string()
                } else {
                    segment.to_string()
                }
            })
            .collect::<Vec<String>>()
            .join("/")
    }
}

/// Route validation utilities
pub struct RouteValidator;

impl RouteValidator {
    /// Validates that there are no overlapping routes in the endpoint set.
    pub fn ensure_no_overlapping_routes(endpoints: &HashSet<HttpEndpoint>) -> Result<(), Error> {
        let mut routes: Vec<(&str, &str)> = Vec::new();

        for endpoint in endpoints {
            let method = endpoint.method.as_deref().unwrap_or("*");
            let path = endpoint.path.as_str();

            for (existing_method, existing_path) in &routes {
                if (method == *existing_method || method == "*" || *existing_method == "*")
                    && PathUtils::normalize_path_parameters(path)
                        == PathUtils::normalize_path_parameters(existing_path)
                {
                    return Err(Error::OverlappingRoutes(
                        format!("{existing_method} {existing_path}"),
                        format!("{method} {path}"),
                    ));
                }
            }

            routes.push((method, path));
        }

        Ok(())
    }
}
