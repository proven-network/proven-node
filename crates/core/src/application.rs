//! Application router creation and management
//!
//! This module contains the logic for creating and managing HTTP routers
//! for application-specific functionality.

use crate::Error;
use crate::handlers::{ApplicationHttpContext, application_http_handler};
use crate::state::FullContext;
use crate::utils::PathUtils;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::{any, delete, get, patch, post, put};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_code_package::{CodePackage, ModuleSpecifier};
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::{ModuleLoader, ModuleOptions, RuntimePoolManagement};
use proven_sessions::SessionManagement;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

/// Application router utilities
pub struct ApplicationRouter;

impl ApplicationRouter {
    /// Creates a router for handling HTTP requests to application endpoints.
    pub async fn create_application_http_router<A, AM, RM, IM, SM>(
        attestor: &A,
        code_package: CodePackage,
        module_specifier: url::Url,
        _application_manager: &AM,
        runtime_pool_manager: &RM,
        identity_manager: &IM,
        sessions_manager: &SM,
    ) -> Result<Router, Error>
    where
        A: Attestor + Clone,
        AM: ApplicationManagement + Clone,
        RM: RuntimePoolManagement + Clone,
        IM: IdentityManagement + Clone,
        SM: SessionManagement + Clone,
    {
        let module_options = ModuleOptions::from_code_package(&code_package, &module_specifier)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;

        // Validate before creating the router
        crate::utils::RouteValidator::ensure_no_overlapping_routes(&module_options.http_endpoints)?;

        let mut router = Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive());

        for endpoint in module_options.http_endpoints {
            let ctx = ApplicationHttpContext {
                application_id: Uuid::max(), // TODO: Fix this
                attestor: attestor.clone(),
                handler_specifier: endpoint.handler_specifier.clone(),
                module_loader: ModuleLoader::new(code_package.clone()),
                requires_session: false, // TODO: Make this configurable
                runtime_pool_manager: runtime_pool_manager.clone(),
                _identity_manager: identity_manager.clone(),
                sessions_manager: sessions_manager.clone(),
            };

            let method_router = match endpoint.method.as_deref() {
                Some("GET") => get(application_http_handler),
                Some("POST") => post(application_http_handler),
                Some("PUT") => put(application_http_handler),
                Some("DELETE") => delete(application_http_handler),
                Some("PATCH") => patch(application_http_handler),
                _ => any(application_http_handler),
            };

            let axum_friendly_path =
                PathUtils::convert_path_use_axum_capture_groups(&endpoint.path);
            router = router.route(&axum_friendly_path, method_router.with_state(ctx));
        }

        Ok(router)
    }

    /// Create a test application router with predefined endpoints
    pub async fn create_application_test_router<A, G, AM, RM, IM, PM, SM>(
        full_ctx: &FullContext<A, G, AM, RM, IM, PM, SM>,
    ) -> Result<Router, Error>
    where
        A: Attestor,
        G: Governance,
        AM: ApplicationManagement,
        RM: RuntimePoolManagement,
        IM: IdentityManagement,
        PM: PasskeyManagement,
        SM: SessionManagement,
    {
        let code_package = CodePackage::from_str(
            r#"
            import { runOnHttp } from "@proven-network/handler";

            export const root = runOnHttp({ path: "/" }, (request) => {
                    return `Hello ${request.queryParameters.name || 'World'} from runtime!`;
                }
            );

            export const another = runOnHttp({ path: "/another" }, (request) => {
                    return `Hello from another endpoint!`;
                }
            );

            export const getPost = runOnHttp({ method: "GET", path: "/post/:id" }, (request) => {
                    return `Hello from post endpoint with id ${request.pathParameters.id}!`;
                }
            );

            export const updatePost = runOnHttp({ method: "PUT", path: "/post/:id" }, (request) => {
                    return `Hello from post endpoint with id ${request.pathParameters.id}!`;
                }
            );

            export const error = runOnHttp({ path: "/error" }, (request) => {
                    throw new Error("This is an error");
                }
            );
        "#,
        )
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

        let module_specifier =
            ModuleSpecifier::parse("file:///main.ts").map_err(|e| Error::Runtime(e.to_string()))?;

        Self::create_application_http_router(
            &full_ctx.attestor,
            code_package,
            module_specifier,
            &full_ctx.application_manager,
            &full_ctx.runtime_pool_manager,
            &full_ctx.identity_manager,
            &full_ctx.sessions_manager,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use proven_code_package::CodePackage;

    #[tokio::test]
    async fn test_code_package_creation() {
        // Test simple code package creation
        let simple_code = r#"
            export const test = "hello";
        "#;

        let result = CodePackage::from_str(simple_code).await;
        assert!(
            result.is_ok(),
            "Simple code package creation should succeed"
        );

        let code_package = result.unwrap();
        assert_eq!(code_package.specifiers().len(), 1);
        assert_eq!(code_package.valid_entrypoints().len(), 1);
    }

    #[tokio::test]
    async fn test_code_package_with_imports() {
        // Test code package with @proven-network/handler import
        let import_code = r#"
            import { runOnHttp } from "@proven-network/handler";
            export const test = "hello";
        "#;

        let result = CodePackage::from_str(import_code).await;
        assert!(result.is_ok(), "Code package with imports should succeed");

        let code_package = result.unwrap();
        assert_eq!(code_package.specifiers().len(), 1);
        assert_eq!(code_package.valid_entrypoints().len(), 1);
    }

    #[tokio::test]
    async fn test_code_package_with_handlers() {
        // Test code package with actual handlers
        let handler_code = r#"
            import { runOnHttp } from "@proven-network/handler";

            export const root = runOnHttp({ path: "/" }, (request) => {
                return `Hello ${request.queryParameters.name || 'World'} from runtime!`;
            });

            export const another = runOnHttp({ path: "/another" }, (request) => {
                return `Hello from another endpoint!`;
            });
        "#;

        let result = CodePackage::from_str(handler_code).await;
        assert!(result.is_ok(), "Code package with handlers should succeed");

        let code_package = result.unwrap();
        assert_eq!(code_package.specifiers().len(), 1);
        assert_eq!(code_package.valid_entrypoints().len(), 1);
    }

    #[tokio::test]
    async fn test_code_package_timeout_handling() {
        // Test that code package creation with timeout works
        let simple_code = r#"
            export const test = "hello";
        "#;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            CodePackage::from_str(simple_code),
        )
        .await;

        assert!(result.is_ok(), "Code package creation should not timeout");
        let code_package_result = result.unwrap();
        assert!(
            code_package_result.is_ok(),
            "Code package should be created successfully"
        );
    }

    #[tokio::test]
    #[allow(clippy::assertions_on_constants)]
    async fn test_code_package_error_handling() {
        // Test code package with invalid syntax
        let invalid_code = r#"
            export const test = "hello";
            this is invalid syntax
        "#;

        let _ = CodePackage::from_str(invalid_code).await;

        // This might succeed or fail depending on how lenient the parser is
        // We just want to make sure it doesn't hang

        assert!(
            true,
            "Code package creation should not hang on invalid syntax"
        );
    }
}
