use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_code_package::CodePackage;
use proven_runtime::{ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader};
use proven_sessions::{ApplicationSession, Session};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type Args = Vec<serde_json::Value>;
type HandlerSpecifierString = String;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ParameterInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    pub optional: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HandlerInfo {
    pub name: String,
    pub r#type: String, // "http", "schedule", "event", "rpc"
    pub parameters: Vec<ParameterInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ManifestModule {
    pub path: String,
    pub content: String,
    pub handlers: Vec<HandlerInfo>,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BundleMetadata {
    pub created_at: String,
    pub mode: String,
    pub plugin_version: String,
    pub file_count: u32,
    pub bundle_size: u32,
    pub source_maps: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BundleManifest {
    pub id: String,
    pub version: String,
    pub modules: Vec<ManifestModule>,
    pub dependencies: HashMap<String, String>,
    pub metadata: BundleMetadata,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecuteCommand {
    pub args: Args,
    pub handler_specifier: HandlerSpecifierString,
    pub manifest: BundleManifest,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecuteSuccessResponse {
    pub execution_result: ExecutionResult,
    pub code_package_hash: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum ExecuteResponse {
    #[serde(rename = "failure")]
    Failure(String),

    #[serde(rename = "success")]
    Success(ExecuteSuccessResponse),
}

#[async_trait]
impl RpcCommand for ExecuteCommand {
    type Response = ExecuteResponse;

    async fn execute<AM, IM, SM, RM>(
        &self,
        context: &mut RpcContext<AM, IM, SM, RM>,
    ) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        SM: proven_sessions::SessionManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        let Ok(handler_specifier) = HandlerSpecifier::parse(&self.handler_specifier) else {
            return ExecuteResponse::Failure("Invalid handler specifier".to_string());
        };

        let execution_request = match &context.session {
            Session::Application(app_session) => match app_session {
                ApplicationSession::Anonymous { application_id, .. } => ExecutionRequest::Rpc {
                    application_id: *application_id,
                    args: self.args.clone(),
                    handler_specifier,
                },
                ApplicationSession::Identified {
                    application_id,
                    identity_id,
                    ..
                } => match context.identity_manager.get_identity(identity_id).await {
                    Ok(Some(identity)) => ExecutionRequest::RpcWithIdentity {
                        application_id: *application_id,
                        args: self.args.clone(),
                        handler_specifier,
                        identity,
                    },
                    Ok(None) => {
                        return ExecuteResponse::Failure("Identity not found".to_string());
                    }
                    Err(e) => return ExecuteResponse::Failure(format!("{e:?}")),
                },
            },
            Session::Management(_) => {
                return ExecuteResponse::Failure(
                    "Execute command not available in management context".to_string(),
                );
            }
        };

        // Create CodePackage from manifest
        let code_package = match Self::create_code_package_from_manifest(&self.manifest).await {
            Ok(package) => package,
            Err(e) => return ExecuteResponse::Failure(e),
        };

        // Get the hash before moving the code_package
        let code_package_hash = code_package.hash().to_string();

        match context
            .runtime_pool_manager
            .execute(ModuleLoader::new(code_package), execution_request)
            .await
        {
            Ok(execution_result) => ExecuteResponse::Success(ExecuteSuccessResponse {
                execution_result,
                code_package_hash,
            }),
            Err(e) => ExecuteResponse::Failure(format!("{e:?}")),
        }
    }
}

impl ExecuteCommand {
    /// Create a `CodePackage` from a `BundleManifest` with proper module resolution and NPM support
    async fn create_code_package_from_manifest(
        manifest: &BundleManifest,
    ) -> Result<CodePackage, String> {
        use proven_code_package::ModuleSpecifier;
        use proven_code_package::PackageJson;
        use std::collections::HashMap;

        // Convert manifest modules to the format expected by CodePackage
        let mut module_sources = HashMap::new();
        let mut module_roots = Vec::new();

        for module in &manifest.modules {
            // Convert relative paths to proper file:// URLs
            let module_url = if module.path.starts_with("file://") {
                module.path.clone()
            } else {
                format!("file:///{}", module.path.trim_start_matches('/'))
            };

            let module_specifier = ModuleSpecifier::parse(&module_url)
                .map_err(|e| format!("Invalid module path '{}': {e}", module.path))?;

            module_sources.insert(module_specifier.clone(), module.content.clone());

            // If this module has handlers, it's a potential root
            if !module.handlers.is_empty() {
                module_roots.push(module_specifier);
            }
        }

        // If no modules with handlers, use the first module as root
        if module_roots.is_empty() && !manifest.modules.is_empty() {
            let first_module_url = if manifest.modules[0].path.starts_with("file://") {
                manifest.modules[0].path.clone()
            } else {
                format!(
                    "file:///{}",
                    manifest.modules[0].path.trim_start_matches('/')
                )
            };

            let first_specifier = ModuleSpecifier::parse(&first_module_url)
                .map_err(|e| format!("Invalid module path '{}': {e}", manifest.modules[0].path))?;
            module_roots.push(first_specifier);
        }

        // Create a minimal package.json from manifest dependencies
        let package_json = if manifest.dependencies.is_empty() {
            None
        } else {
            let package_json_content = serde_json::json!({
                "name": manifest.id,
                "version": manifest.version,
                "dependencies": manifest.dependencies
            });

            let package_json_str = serde_json::to_string(&package_json_content)
                .map_err(|e| format!("Failed to serialize package.json: {e}"))?;

            Some(
                package_json_str
                    .parse::<PackageJson>()
                    .map_err(|e| format!("Failed to parse generated package.json: {e}"))?,
            )
        };

        // Create CodePackage with NPM dependency support
        CodePackage::from_map_with_npm_deps(
            &module_sources,
            module_roots,
            package_json.as_ref(),
            false, // Don't include dev dependencies in execution
        )
        .await
        .map_err(|e| format!("Failed to create CodePackage from manifest: {e:?}"))
    }
}
