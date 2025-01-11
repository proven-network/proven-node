use crate::extensions::{
    console_ext, crypto_ext, handler_runtime_ext, kv_application_ext, kv_ext, kv_nft_ext,
    kv_personal_ext, openai_ext, radixdlt_babylon_gateway_api_ext,
    radixdlt_radix_engine_toolkit_ext, session_ext, sql_application_ext, sql_personal_ext,
    sql_runtime_ext, uuid_ext, zod_ext, ApplicationSqlConnectionManager,
    ApplicationSqlParamListManager, ConsoleState, CryptoState, GatewayDetailsState,
    NftSqlConnectionManager, PersonalSqlConnectionManager, PersonalSqlParamListManager,
    SessionState,
};
use crate::import_replacements::replace_esm_imports;
use crate::options::{HandlerOptions, SqlMigrations};
use crate::options_parser::OptionsParser;
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::{ExecutionLogs, ExecutionRequest, ExecutionResult, Result};

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use radix_common::network::NetworkDefinition;
use regex::Regex;
use rustyscript::js_value::Value;
use rustyscript::{ExtensionOptions, Module, ModuleHandle, WebOptions};
use tokio::time::Instant;

static DEFAULT_HEAP_SIZE: u16 = 32;
static DEFAULT_TIMEOUT_MILLIS: u32 = 5000;

/// Options for creating a new `Runtime`.
#[derive(Clone)]
pub struct RuntimeOptions<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

    /// Name of the handler function.
    pub handler_name: Option<String>,

    /// NFT-scoped SQL store.
    pub nft_sql_store: NSS,

    /// NFT-scoped KV store.
    pub nft_store: NS,

    /// Source code of the module.
    pub module: String,

    /// Persona-scoped SQL store.
    pub personal_sql_store: PSS,

    /// Persona-scoped KV store.
    pub personal_store: PS,

    /// Origin for Radix Network gateway.
    pub radix_gateway_origin: String,

    /// Network definition for Radix Network.
    pub radix_network_definition: NetworkDefinition,
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
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore2, MemoryStore3};
/// use radix_common::network::NetworkDefinition;
/// use serde_json::json;
/// use tempfile::tempdir;
///
/// let mut runtime = Runtime::new(RuntimeOptions {
///     application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
///     application_store: MemoryStore2::new(),
///     handler_name: Some("handler".to_string()),
///     module: "export const handler = (a, b) => a + b;".to_string(),
///     nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///     nft_store: MemoryStore3::new(),
///     personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///     personal_store: MemoryStore3::new(),
///     radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
///     radix_network_definition: NetworkDefinition::stokenet(),
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
pub struct Runtime<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2<Bytes, Infallible, Infallible>,
    PS: Store3<Bytes, Infallible, Infallible>,
    NS: Store3<Bytes, Infallible, Infallible>,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    application_sql_store: ASS,
    application_store: AS,
    handler_name: Option<String>,
    module_handle: ModuleHandle,
    nft_sql_store: NSS,
    nft_store: NS,
    personal_sql_store: PSS,
    personal_store: PS,
    runtime: rustyscript::Runtime,
    sql_migrations: SqlMigrations,
}

impl<AS, PS, NS, ASS, PSS, NSS> Runtime<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    /// Creates a new runtime with the given runtime options and stores.
    ///
    /// # Parameters
    /// - `options`: The runtime options to use.
    ///
    /// # Returns
    /// The created runtime.
    ///
    /// # Errors
    /// This function will return an error if the options parsing or runtime creation fails.
    pub fn new(
        RuntimeOptions {
            application_sql_store,
            application_store,
            handler_name,
            module,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin: gateway_origin,
            radix_network_definition,
        }: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS>,
    ) -> Result<Self> {
        let module_options = OptionsParser::new()?.parse(module.as_str())?;
        #[allow(clippy::or_fun_call)]
        let handler_options = module_options
            .handler_options
            .get(handler_name.as_ref().unwrap_or(&"__default__".to_string()));

        let timeout_millis =
            handler_options.map_or(
                DEFAULT_TIMEOUT_MILLIS,
                |handler_options| match handler_options {
                    HandlerOptions::Http(http_handler_options) => http_handler_options
                        .timeout_millis
                        .unwrap_or(DEFAULT_TIMEOUT_MILLIS),
                    HandlerOptions::Rpc(rpc_handler_options) => rpc_handler_options
                        .timeout_millis
                        .unwrap_or(DEFAULT_TIMEOUT_MILLIS),
                },
            );

        let max_heap_mbs =
            handler_options.map_or(DEFAULT_HEAP_SIZE, |handler_options| match handler_options {
                HandlerOptions::Http(http_handler_options) => http_handler_options
                    .max_heap_mbs
                    .unwrap_or(DEFAULT_HEAP_SIZE),
                HandlerOptions::Rpc(rpc_handler_options) => rpc_handler_options
                    .max_heap_mbs
                    .unwrap_or(DEFAULT_HEAP_SIZE),
            });

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
        for origin in &allowed_web_origins {
            allowlist_web_permissions.allow_origin(origin);
        }

        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            timeout: Duration::from_millis(timeout_millis.into()),
            max_heap_size: Some(max_heap_mbs as usize * 1024 * 1024),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                handler_runtime_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                crypto_ext::init_ops_and_esm(),
                session_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                kv_application_ext::init_ops::<AS::Scoped>(),
                kv_personal_ext::init_ops::<<<PS as Store3>::Scoped as Store2>::Scoped>(),
                kv_nft_ext::init_ops_and_esm::<NS::Scoped>(),
                kv_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                sql_runtime_ext::init_ops_and_esm(),
                sql_application_ext::init_ops::<ASS::Scoped>(),
                sql_personal_ext::init_ops::<<<PSS as SqlStore3>::Scoped as SqlStore2>::Scoped>(),
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
                    ),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        })?;

        // Set the gateway origin and id for the gateway API SDK extension
        runtime.put(GatewayDetailsState {
            gateway_origin,
            network_id: radix_network_definition.id,
        })?;

        // In case there are any top-level console.* calls in the module
        runtime.put(ConsoleState::default())?;

        let async_module = Self::ensure_exported_functions_are_async(module.as_str())?;
        let async_module = replace_esm_imports(&async_module);

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
            sql_migrations: module_options.sql_migrations,
        })
    }

    /// Executes the given execution request.
    ///
    /// # Parameters
    /// - `request`: The execution request to execute.
    ///
    /// # Returns
    /// A result containing the execution result.
    ///
    /// # Errors
    /// This function will return an error if the execution fails.
    ///
    /// # Panics
    /// This function may panic if the runtime encounters an unrecoverable error.
    pub fn execute(
        &mut self,
        ExecutionRequest {
            accounts,
            args,
            dapp_definition_address,
            identity,
        }: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        let start = Instant::now();

        // Reset the console state before each execution
        self.runtime.put(ConsoleState::default())?;

        // Reset the crypto key cache before each execution
        self.runtime.put(CryptoState::default())?;

        // Set the kv stores for the storage extension
        self.runtime.put(
            self.application_store
                .clone()
                .scope(dapp_definition_address.clone()),
        )?;

        self.runtime.put(match identity.as_ref() {
            Some(current_identity) => Some(
                self.personal_store
                    .clone()
                    .scope(dapp_definition_address.clone())
                    .scope(current_identity.clone()),
            ),
            None => None,
        })?;

        self.runtime.put(
            self.nft_store
                .clone()
                .scope(dapp_definition_address.clone()),
        )?;

        // Set the sql stores for the storage extension
        self.runtime.put(ApplicationSqlParamListManager::new())?;
        self.runtime.put(ApplicationSqlConnectionManager::new(
            self.application_sql_store
                .clone()
                .scope(dapp_definition_address.clone()),
            self.sql_migrations.application.clone(),
        ))?;

        self.runtime.put(PersonalSqlParamListManager::new())?;
        self.runtime.put(match identity.as_ref() {
            Some(current_identity) => Some(PersonalSqlConnectionManager::new(
                self.personal_sql_store
                    .clone()
                    .scope(dapp_definition_address.clone())
                    .scope(current_identity.clone()),
                self.sql_migrations.personal.clone(),
            )),
            None => None,
        })?;

        self.runtime.put(NftSqlConnectionManager::new(
            self.nft_sql_store.clone().scope(dapp_definition_address),
            self.sql_migrations.nft.clone(),
        ))?;

        // Set the context for the session extension
        self.runtime.put(SessionState { identity, accounts })?;

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

        let logs: Result<Vec<ExecutionLogs>> = console_state
            .messages
            .into_iter()
            .map(|message| {
                let args: rustyscript::serde_json::Value =
                    Value::from_v8(message.args).try_into(&mut self.runtime)?;

                Ok(ExecutionLogs {
                    level: message.level,
                    args,
                })
            })
            .collect();

        let logs = logs?;

        Ok(ExecutionResult {
            duration,
            logs,
            output,
        })
    }

    fn ensure_exported_functions_are_async(module: &str) -> Result<String> {
        // Find matches like `export const test = function () { console.log('Hello, world!'); }`
        let re_fn = Regex::new(r"(?m)^\s*export\s+(const|let)\s+(\w+)\s*=\s*function\s*\(")?;

        let result = re_fn.replace_all(module, |caps: &regex::Captures| {
            format!("export {} {} = async function (", &caps[1], &caps[2])
        });

        // Find matches like `export const test = () => { console.log('Hello, world!'); }`
        let re_arrow = Regex::new(r"(?m)^\s*export\s+(const|let)\s+(\w+)\s*=\s*\(")?;

        let result = re_arrow.replace_all(result.as_ref(), |caps: &regex::Captures| {
            format!("export {} {} = async (", &caps[1], &caps[2])
        });

        // Find matches like `export const test = runWithOptions(() => { console.log('Hello, world!'); }, {})`
        let re_run = Regex::new(
            r"(?m)^\s*export\s+(const|let)\s+(\w+)\s*=\s*(runWithOptions|runOnSchedule|runOnRadixEvent|runOnProvenEvent)\s*\(",
        )?;

        let result = re_run.replace_all(result.as_ref(), |caps: &regex::Captures| {
            format!("export {} {} = {}(async ", &caps[1], &caps[2], &caps[3])
        });

        let re_duplicate_async = Regex::new(r"async\s*\r?\n?\s*async")?;
        let result = re_duplicate_async.replace_all(&result, "async");

        Ok(result.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ed25519_dalek::Verifier;
    use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
    use proven_store_memory::{MemoryStore2, MemoryStore3};
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
        MemoryStore2,
        MemoryStore3,
        MemoryStore3,
        DirectSqlStore2,
        DirectSqlStore3,
        DirectSqlStore3,
    > {
        RuntimeOptions {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            handler_name,
            module: script.to_string(),
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            personal_store: MemoryStore3::new(),
            radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            radix_network_definition: NetworkDefinition::stokenet(),
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

            assert!(Runtime::new(options).is_ok());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute() {
        run_in_thread(|| {
            let options = create_runtime_options(
                "export const test = () => { console.log('Hello, world!'); }",
                Some("test".to_string()),
            );

            let request = create_execution_request();
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

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

            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

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

            let request = create_execution_request();
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

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

            let request = create_execution_request();
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

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

            let request = create_execution_request();
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

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

            let request = create_execution_request();
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

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
                import { getCurrentIdentity } from "@proven-network/session";

                export const test = () => {
                    const identity = getCurrentIdentity();
                    return identity;
                }
            "#,
                Some("test".to_string()),
            );

            let mut request = create_execution_request();
            request.identity = Some("test_identity".to_string());
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

            assert!(execution_result.output.is_string());
            assert_eq!(execution_result.output.as_str().unwrap(), "test_identity");
            assert!(execution_result.duration.as_millis() < 1000);
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_accounts() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { getCurrentAccounts } from "@proven-network/session";

                export const test = () => {
                    const accounts = getCurrentAccounts();
                    return accounts;
                }
            "#,
                Some("test".to_string()),
            );

            let mut request = create_execution_request();
            request.accounts = Some(vec!["account1".to_string(), "account2".to_string()]);
            let result = Runtime::new(options).unwrap().execute(request);

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
                import { runWithOptions } from "@proven-network/handler";

                export const test = runWithOptions(async () => {
                    return new Promise((resolve) => {
                        setTimeout(() => resolve(), 1500);
                    });
                }, { timeout: 2000 });
            "#,
                Some("test".to_string()),
            );

            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_ok());
        });
    }

    #[tokio::test]
    async fn test_runtime_default_max_heap_size() {
        run_in_thread(|| {
            // The script will allocate 40MB of memory, but the default max heap size is set to 10MB
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "@proven-network/handler";

                export const test = runWithOptions(() => {
                    const largeArray = new Array(40 * 1024 * 1024).fill('a');
                    return largeArray;
                }, { timeout: 30000 });
            "#,
                Some("test".to_string()),
            );

            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_disallowed_origins() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "@proven-network/handler";

                export const test = runWithOptions(async () => {
                    const response = await fetch("https://example.com/");
                    return response;
                }, { timeout: 10000 });
            "#,
                Some("test".to_string()),
            );

            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_allowed_origins() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { runWithOptions } from "@proven-network/handler";

                export const test = runWithOptions(async () => {
                    const response = await fetch("https://example.com/");
                    return response.status;
                }, { allowedOrigins: ["https://example.com"], timeout: 10000 });
            "#,
                Some("test".to_string()),
            );

            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_ok());
            assert_eq!(result.unwrap().output, 200);
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_sql() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { getApplicationDb, sql } from "@proven-network/sql";

                const DB = getApplicationDb("main");

                DB.migrate(sql`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`);

                export const test = async () => {
                    const email = "alice@example.com";

                    await DB.execute(sql`INSERT INTO users (email) VALUES (${email})`);

                    const results = await DB.query(sql`SELECT id, email FROM users`);

                    return results[0][1];
                };
            "#,
                Some("test".to_string()),
            );

            let mut request = create_execution_request();
            request.identity = Some("test_identity".to_string());
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_ok());
            assert_eq!(result.unwrap().output, "alice@example.com");
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_basic_ed25519_signing() {
        run_in_thread(|| {
            let options = create_runtime_options(
                r#"
                import { generateEd25519Key } from "@proven-network/crypto";

                export const test = async () => {
                    const key = generateEd25519Key();

                    return [
                        key.publicKey().toString(),
                        key.sign("Hello, world!").toString()
                    ];
                }
            "#,
                Some("test".to_string()),
            );

            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_ok());

            // Check that the signature is valid
            let result = result.unwrap();
            let output = result.output.as_array().unwrap();
            let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(
                &hex::decode(output[0].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .expect("slice with incorrect length"),
            )
            .unwrap();
            let signature = ed25519_dalek::Signature::from_bytes(
                &hex::decode(output[1].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .expect("slice with incorrect length"),
            );

            let message = "Hello, world!";
            assert!(verifying_key.verify(message.as_bytes(), &signature).is_ok());
        });
    }
}
