use crate::extensions::*;
use crate::options::HandlerOptions;
use crate::options_parser::OptionsParser;
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::vendor_replacements::replace_vendor_imports;
use crate::{Error, ExecutionLogs, ExecutionRequest, ExecutionResult};

use std::sync::Arc;
use std::time::Duration;

use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use regex::Regex;
use rustyscript::js_value::Value;
use rustyscript::{ExtensionOptions, Module, ModuleHandle, WebOptions};
use tokio::time::Instant;

#[derive(Clone)]
pub struct RuntimeOptions<
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
> {
    pub application_sql_store: ASS,
    pub application_store: AS,
    pub gateway_origin: String,
    pub handler_name: Option<String>,
    pub module: String,
    pub nft_sql_store: NSS,
    pub nft_store: NS,
    pub personal_sql_store: PSS,
    pub personal_store: PS,
}

pub struct Runtime<
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
> {
    application_sql_store: ASS,
    application_store: AS,
    handler_name: Option<String>,
    module_handle: ModuleHandle,
    nft_sql_store: NSS,
    nft_store: NS,
    personal_sql_store: PSS,
    personal_store: PS,
    runtime: rustyscript::Runtime,
}

/// Executes ESM modules in a single-threaded environment. Cannot use in tokio without spawning in dedicated thread.
///
/// # Type Parameters
/// - `AS`: Application Store type implementing `Store2`.
/// - `NS`: NFT Store type implementing `Store3`.
/// - `PS`: Personal Store type implementing `Store3`.
/// - `ASS`: Application SQL Store type implementing `SqlStore2`.
/// - `NSS`: NFT SQL Store type implementing `SqlStore3`.
/// - `PSS`: Personal SQL Store type implementing `SqlStore3`.
///
/// # Example
/// ```rust
/// use proven_runtime::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};
/// use proven_store_memory::MemoryStore;
/// use serde_json::json;
///
/// let mut runtime = Runtime::new(RuntimeOptions {
///     application_store: MemoryStore::new(),
///     gateway_origin: "https://stokenet.radixdlt.com".to_string(),
///     handler_name: Some("handler".to_string()),
///     module: "export const handler = (a, b) => a + b;".to_string(),
///     nft_store: MemoryStore::new(),
///     personal_store: MemoryStore::new(),
/// })
/// .expect("Failed to create runtime");
///
/// runtime.execute(ExecutionRequest {
///     accounts: None,
///     args: vec![json!(10), json!(20)],
///     dapp_definition_address: "dapp_definition_address".to_string(),
///     identity: None,
/// });
/// ```
impl<AS: Store2, PS: Store3, NS: Store3, ASS: SqlStore2, PSS: SqlStore3, NSS: SqlStore3>
    Runtime<AS, PS, NS, ASS, PSS, NSS>
{
    /// Creates a new runtime with the given runtime options and stores.
    ///
    /// # Parameters
    /// - `options`: The runtime options to use.
    ///
    /// # Returns
    /// The created runtime.
    pub fn new(
        RuntimeOptions {
            application_sql_store,
            application_store,
            gateway_origin,
            handler_name,
            module,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
        }: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS>,
    ) -> Result<Self, Error> {
        let module_options = OptionsParser::new()?.parse(module.as_str())?;
        let handler_options = module_options
            .handler_options
            .get(handler_name.as_ref().unwrap_or(&"__default__".to_string()));

        let timeout_millis = handler_options
            .map(|handler_options| match handler_options {
                HandlerOptions::Http(http_handler_options) => {
                    http_handler_options.timeout_millis.unwrap_or(1000)
                }
                HandlerOptions::Rpc(rpc_handler_options) => {
                    rpc_handler_options.timeout_millis.unwrap_or(1000)
                }
            })
            .unwrap_or(1000);

        let max_heap_mbs = handler_options
            .map(|handler_options| match handler_options {
                HandlerOptions::Http(http_handler_options) => {
                    http_handler_options.max_heap_mbs.unwrap_or(32)
                }
                HandlerOptions::Rpc(rpc_handler_options) => {
                    rpc_handler_options.max_heap_mbs.unwrap_or(32)
                }
            })
            .unwrap_or(32);

        let allowed_web_origins = handler_options
            .map(|handler_options| match handler_options {
                HandlerOptions::Http(http_handler_options) => {
                    http_handler_options.allowed_web_origins.clone()
                }
                HandlerOptions::Rpc(rpc_handler_options) => {
                    rpc_handler_options.allowed_web_origins.clone()
                }
            })
            .unwrap_or_default();

        let allowlist_web_permissions = OriginAllowlistWebPermissions::new();
        allowlist_web_permissions.allow_origin(&gateway_origin); // Always allow Radix gateway origin
        allowed_web_origins.iter().for_each(|origin| {
            allowlist_web_permissions.allow_origin(origin);
        });

        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            timeout: Duration::from_millis(timeout_millis as u64),
            max_heap_size: Some(max_heap_mbs as usize * 1024 * 1024),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                run_mock_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                sessions_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                kv_application_ext::init_ops::<AS::Scoped>(),
                kv_personal_ext::init_ops::<<<PS as Store3>::Scoped as Store2>::Scoped>(),
                kv_nft_ext::init_ops_and_esm::<NS::Scoped>(),
                kv_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                sql_ext::init_ops_and_esm(),
                sql_application_ext::init_ops::<ASS::Scoped>(),
                // Vendered modules
                openai_ext::init_ops_and_esm(),
                radixdlt_babylon_gateway_api_ext::init_ops_and_esm(),
                radixdlt_radix_engine_toolkit_ext::init_ops_and_esm(),
                uuid_ext::init_ops_and_esm(),
                zod_ext::init_ops_and_esm(),
            ],
            extension_options: ExtensionOptions {
                web: WebOptions {
                    permissions: Arc::new(allowlist_web_permissions),
                    user_agent: format!(
                        "Proven Network {} (https://proven.network)",
                        env!("CARGO_PKG_VERSION")
                    )
                    .to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        })?;

        // Set the gateway origin and id for the gateway API SDK extension
        runtime.put(GatewayDetailsState {
            gateway_origin: gateway_origin.clone(),
            network_id: 2, // TODO: Add to options
        })?;

        // In case there are any top-level console.* calls in the module
        runtime.put(ConsoleState::default())?;

        let async_module = Self::ensure_exported_functions_are_async(module.as_str());
        let async_module = replace_vendor_imports(async_module);

        let module = Module::new("module.ts", async_module.as_str());
        let module_handle = runtime.load_module(&module)?;

        Ok(Self {
            application_sql_store,
            application_store,
            handler_name,
            module_handle,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            runtime,
        })
    }

    /// Executes the given execution request.
    ///
    /// # Parameters
    /// - `request`: The execution request to execute.
    ///
    /// # Returns
    /// An a result containing the execution result.
    pub fn execute(
        &mut self,
        ExecutionRequest {
            accounts,
            args,
            dapp_definition_address,
            identity,
        }: ExecutionRequest,
    ) -> Result<ExecutionResult, Error> {
        let start = Instant::now();

        // Reset the console state before each execution
        self.runtime.put(ConsoleState::default())?;

        // Set the stores for the storage extension
        let personal_store = match identity.as_ref() {
            Some(current_identity) => Some(
                self.personal_store
                    .clone()
                    .scope(dapp_definition_address.clone())
                    .scope(current_identity.clone()),
            ),
            None => None,
        };
        self.runtime.put(personal_store)?;

        self.runtime.put(
            self.application_store
                .clone()
                .scope(dapp_definition_address.clone()),
        )?;

        self.runtime.put(
            self.nft_store
                .clone()
                .scope(dapp_definition_address.clone()),
        )?;

        // Set the sql stores for the storage extension
        let personal_sql_store = match identity.as_ref() {
            Some(current_identity) => Some(
                self.personal_sql_store
                    .clone()
                    .scope(dapp_definition_address.clone())
                    .scope(current_identity.clone()),
            ),
            None => None,
        };
        self.runtime.put(personal_sql_store)?;

        self.runtime.put(
            self.application_sql_store
                .clone()
                .scope(dapp_definition_address.clone()),
        )?;

        self.runtime
            .put(self.nft_sql_store.clone().scope(dapp_definition_address))?;

        // Set the context for the session extension
        self.runtime.put(SessionsState { identity, accounts })?;

        let output: Value = match self.handler_name {
            Some(ref handler_name) => {
                self.runtime
                    .call_function(Some(&self.module_handle), handler_name, &args)?
            }
            None => self.runtime.call_entrypoint(&self.module_handle, &args)?,
        };

        let output: rustyscript::serde_json::Value = output.try_into(&mut self.runtime)?;

        let console_state: ConsoleState = self.runtime.take().unwrap_or_default();
        let duration = start.elapsed();

        let logs = console_state
            .messages
            .into_iter()
            .map(|message| {
                let args: rustyscript::serde_json::Value = Value::from_v8(message.args)
                    .try_into(&mut self.runtime)
                    .unwrap();

                ExecutionLogs {
                    level: message.level,
                    args,
                }
            })
            .collect();

        Ok(ExecutionResult {
            output,
            duration,
            logs,
        })
    }

    fn ensure_exported_functions_are_async(module: &str) -> String {
        // Find matches like `export const test = function () { console.log('Hello, world!'); }`
        let re_fn =
            Regex::new(r"(?m)^\s*export\s+(const|let)\s+(\w+)\s*=\s*function\s*\(").unwrap();

        let result = re_fn.replace_all(module, |caps: &regex::Captures| {
            format!("export {} {} = async function (", &caps[1], &caps[2])
        });

        // Find matches like `export const test = () => { console.log('Hello, world!'); }`
        let re_arrow = Regex::new(r"(?m)^\s*export\s+(const|let)\s+(\w+)\s*=\s*\(").unwrap();

        let result = re_arrow.replace_all(result.as_ref(), |caps: &regex::Captures| {
            format!("export {} {} = async (", &caps[1], &caps[2])
        });

        // Find matches like `export const test = runWithOptions(() => { console.log('Hello, world!'); }, {})`
        let re_run =
            Regex::new(r"(?m)^\s*export\s+(const|let)\s+(\w+)\s*=\s*(runWithOptions|runOnSchedule|runOnRadixEvent|runOnProvenEvent)\s*\(").unwrap();

        let result = re_run.replace_all(result.as_ref(), |caps: &regex::Captures| {
            format!("export {} {} = {}(async ", &caps[1], &caps[2], &caps[3])
        });

        let re_duplicate_async = Regex::new(r"async\s*\r?\n?\s*async").unwrap();
        let result = re_duplicate_async.replace_all(&result, "async");

        result.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proven_sql_direct::DirectSqlStore;
    use proven_store_memory::MemoryStore;
    use serde_json::json;
    use tempfile::tempdir;

    // spawn in std::thread to avoid rustyscript panic
    fn run_in_thread<F: FnOnce() + Send + 'static>(f: F) {
        std::thread::spawn(f).join().unwrap();
    }

    fn create_runtime_options(
        script: &str,
        handler_name: Option<String>,
    ) -> RuntimeOptions<
        MemoryStore,
        MemoryStore,
        MemoryStore,
        DirectSqlStore,
        DirectSqlStore,
        DirectSqlStore,
    > {
        let mut temp_application_sql = tempdir().unwrap().into_path();
        temp_application_sql.push("application.db");
        let mut temp_nft_sql = tempdir().unwrap().into_path();
        temp_nft_sql.push("nft.db");
        let mut temp_personal_sql = tempdir().unwrap().into_path();
        temp_personal_sql.push("personal.db");

        RuntimeOptions {
            application_sql_store: DirectSqlStore::new(temp_application_sql),
            application_store: MemoryStore::new(),
            gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            handler_name,
            module: script.to_string(),
            nft_sql_store: DirectSqlStore::new(temp_nft_sql),
            nft_store: MemoryStore::new(),
            personal_sql_store: DirectSqlStore::new(temp_personal_sql),
            personal_store: MemoryStore::new(),
        }
    }

    fn create_execution_request() -> ExecutionRequest {
        ExecutionRequest {
            accounts: None,
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        }
    }

    #[tokio::test]
    async fn test_runtime_new() {
        run_in_thread(|| {
            let options = create_runtime_options(
                "export const test = () => { console.log('Hello, world!'); }",
                Some("test".to_string()),
            );

            let runtime = Runtime::new(options);
            assert!(runtime.is_ok());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute() {
        run_in_thread(|| {
            let options = create_runtime_options(
                "export const test = () => { console.log('Hello, world!'); }",
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_null());
            assert!(execution_result.duration.as_millis() < 1000);
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_default_export() {
        run_in_thread(|| {
            let options = create_runtime_options(
                "export default () => { console.log('Hello, world!'); }",
                None,
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_gateway_api_sdk() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { RadixNetwork } from "@radixdlt/babylon-gateway-api-sdk";

                export const test = () => {
                    return RadixNetwork.Mainnet;
                }
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();

            // RadixNetwork.Mainnet should be 1
            assert_eq!(execution_result.output, 1);
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_radix_engine_toolkit() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { RadixEngineToolkit } from "@radixdlt/radix-engine-toolkit";

                export const test = async () => {
                    return JSON.stringify(await RadixEngineToolkit.Build.information());
                }
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_string());
            assert_eq!(
                execution_result.output,
                r#"{"version":"2.1.0-dev1","scryptoDependency":{"kind":"Version","value":"1.2.0"}}"#
            );
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_uuid() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { v4 as uuidv4 } from "uuid";

                export const test = () => {
                    return uuidv4();
                }
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_string());
            assert_eq!(execution_result.output.as_str().unwrap().len(), 36);
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_zod() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { z } from "zod";

                export const test = () => {
                    const schema = z.object({
                        name: z.string(),
                        age: z.number(),
                    });

                    return schema.parse({ name: "Alice", age: 30 });
                }
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_object());
            assert_eq!(execution_result.output.as_object().unwrap().len(), 2);
            assert_eq!(
                execution_result
                    .output
                    .as_object()
                    .unwrap()
                    .get("name")
                    .unwrap(),
                "Alice"
            );
            assert_eq!(
                execution_result
                    .output
                    .as_object()
                    .unwrap()
                    .get("age")
                    .unwrap(),
                30
            );
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_identity() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { getCurrentIdentity } from "proven:sessions";

                export const test = () => {
                    const identity = getCurrentIdentity();
                    return identity;
                }
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let mut request = create_execution_request();
            request.identity = Some("test_identity".to_string());

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_string());
            assert_eq!(execution_result.output.as_str().unwrap(), "test_identity");
            assert!(execution_result.duration.as_millis() < 1000);
        })
    }

    #[tokio::test]
    async fn test_runtime_execute_with_accounts() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { getCurrentAccounts } from "proven:sessions";

                export const test = () => {
                    const accounts = getCurrentAccounts();
                    return accounts;
                }
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let mut request = create_execution_request();
            request.accounts = Some(vec!["account1".to_string(), "account2".to_string()]);

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_array());
            assert_eq!(execution_result.output.as_array().unwrap().len(), 2);
            assert_eq!(
                execution_result.output.as_array().unwrap()[0]
                    .as_str()
                    .unwrap(),
                "account1"
            );
            assert!(execution_result.duration.as_millis() < 1000);
        });
    }

    #[tokio::test]
    async fn test_runtime_sets_timeout() {
        run_in_thread(|| {
            // The script will sleep for 1.5 seconds, but the timeout is set to 2 seconds
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "proven:run";

                export const test = runWithOptions(async () => {
                    return new Promise((resolve) => {
                        setTimeout(() => resolve(), 1500);
                    });
                }, { timeout: 2000 });
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());
        });
    }

    #[tokio::test]
    async fn test_runtime_default_max_heap_size() {
        run_in_thread(|| {
            // The script will allocate 40MB of memory, but the default max heap size is set to 10MB
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "proven:run";

                export const test = runWithOptions(() => {
                    const largeArray = new Array(40 * 1024 * 1024).fill('a');
                    return largeArray;
                }, { timeout: 30000 });
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_err());
        });
    }

    // Slow test - disabled for now
    // #[tokio::test]
    // async fn test_runtime_custom_max_heap_size() {
    //     run_in_thread(|| {
    //         // The script will allocate 40MB of memory, and the max heap size is set to 2048MB
    //         let options = create_runtime_options(
    //             r#"
    //             import { runWithOptions } from "proven:run";

    //             export const test = runWithOptions(() => {
    //                 const largeArray = new Array(40 * 1024 * 1024).fill('a');
    //                 return largeArray;
    //             }, { memory: 2048, timeout: 30000 });
    //         "#,
    //             Some("test".to_string()),
    //         );

    //         let mut runtime = Runtime::new(options).unwrap();

    //         let request = create_execution_request();

    //         let result = runtime.execute(request);
    //         println!("{:?}", result);
    //         assert!(result.is_ok());
    //     });
    // }

    #[tokio::test]
    async fn test_runtime_execute_with_disallowed_origins() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "proven:run";

                export const test = runWithOptions(async () => {
                    const response = await fetch("https://example.com/");
                    return response;
                }, { timeout: 10000 });
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_allowed_origins() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "proven:run";

                export const test = runWithOptions(async () => {
                    const response = await fetch("https://example.com/");
                    return response.status;
                }, { allowedOrigins: ["https://example.com"], timeout: 10000 });
            "#,
                Some("test".to_string()),
            );

            let mut runtime = Runtime::new(options).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            println!("{:?}", result);
            assert!(result.is_ok());
            let execution_result = result.unwrap();
            assert_eq!(execution_result.output, 200);
        });
    }
}
