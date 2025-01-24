use crate::extensions::{
    babylon_gateway_api_ext, console_ext, crypto_ext, handler_runtime_ext, kv_runtime_ext,
    openai_ext, radix_engine_toolkit_ext, session_ext, sql_application_ext, sql_nft_ext,
    sql_personal_ext, sql_runtime_ext, uuid_ext, zod_ext, ApplicationSqlConnectionManager,
    ConsoleState, CryptoState, GatewayDetailsState, HandlerOutput, NftSqlConnectionManager,
    PersonalSqlConnectionManager, SessionState, SqlParamListManager, SqlQueryResultsManager,
};
use crate::module_loader::{ModuleLoader, ProcessingMode};
use crate::options::{HandlerOptions, SqlMigrations};
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::{Error, ExecutionLogs, ExecutionRequest, ExecutionResult, HandlerSpecifier, Result};

use std::collections::HashSet;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use bytes::Bytes;
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use radix_common::network::NetworkDefinition;
use rustyscript::js_value::Value;
use rustyscript::{ExtensionOptions, ModuleHandle, WebOptions};
use serde_json::json;
use tokio::time::Instant;

static DEFAULT_HEAP_SIZE: u16 = 32;
static DEFAULT_TIMEOUT_MILLIS: u32 = 5000;

/// The type of request this `Runtime` should be handling.
#[derive(Debug)]
enum HandlerType {
    Http,
    RadixEvent,
    Rpc,
}

/// Options for creating a new `Runtime`.
#[derive(Clone)]
pub struct RuntimeOptions<AS, PS, NS, ASS, PSS, NSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    RNV: RadixNftVerifier,
{
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

    /// The exported handler to run.
    pub handler_specifier: HandlerSpecifier,

    /// The module loader to use during execution.
    pub module_loader: ModuleLoader,

    /// NFT-scoped SQL store.
    pub nft_sql_store: NSS,

    /// NFT-scoped KV store.
    pub nft_store: NS,

    /// Persona-scoped SQL store.
    pub personal_sql_store: PSS,

    /// Persona-scoped KV store.
    pub personal_store: PS,

    /// Origin for Radix Network gateway.
    pub radix_gateway_origin: String,

    /// Network definition for Radix Network.
    pub radix_network_definition: NetworkDefinition,

    /// Verifier for checking NFT ownership on the Radix Network.
    pub radix_nft_verifier: RNV,
}

#[cfg(test)]
impl
    RuntimeOptions<
        proven_store_memory::MemoryStore2,
        proven_store_memory::MemoryStore3,
        proven_store_memory::MemoryStore3,
        proven_sql_direct::DirectSqlStore2,
        proven_sql_direct::DirectSqlStore3,
        proven_sql_direct::DirectSqlStore3,
        proven_radix_nft_verifier_mock::MockRadixNftVerifier,
    >
{
    #[must_use]
    /// Creates a new `RuntimeOptions` instance for testing purposes.
    ///
    /// # Parameters
    /// - `script_name`: The name of the script to use.
    /// - `handler_name`: The name of the handler to use.
    ///
    /// # Returns
    /// A new `RuntimeOptions` instance.
    ///
    /// # Panics
    /// This function will panic if creating a temporary directory fails.
    pub fn for_test_code(script_name: &str, handler_name: &str) -> Self {
        use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
        use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
        use proven_store_memory::{MemoryStore2, MemoryStore3};
        use radix_common::network::NetworkDefinition;
        use tempfile::tempdir;

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            handler_specifier: HandlerSpecifier::parse(
                format!("file:///main.ts#{handler_name}").as_str(),
            )
            .unwrap(),
            module_loader: ModuleLoader::from_test_code(script_name),
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            personal_store: MemoryStore3::new(),
            radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            radix_network_definition: NetworkDefinition::stokenet(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
        }
    }
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
/// use proven_code_package::CodePackage;
/// use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
/// use proven_runtime::{
///     Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, Runtime,
///     RuntimeOptions,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore2, MemoryStore3};
/// use radix_common::network::NetworkDefinition;
/// use serde_json::json;
/// use tempfile::tempdir;
///
/// let code_package = CodePackage::from_str("export const handler = (a, b) => a + b;").unwrap();
///
/// let mut runtime = Runtime::new(RuntimeOptions {
///     application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
///     application_store: MemoryStore2::new(),
///     handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
///     module_loader: ModuleLoader::new(code_package),
///     nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///     nft_store: MemoryStore3::new(),
///     personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
///     personal_store: MemoryStore3::new(),
///     radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
///     radix_network_definition: NetworkDefinition::stokenet(),
///     radix_nft_verifier: MockRadixNftVerifier::new(),
/// })
/// .expect("Failed to create runtime");
///
/// runtime.execute(ExecutionRequest::Rpc {
///     accounts: vec![],
///     args: vec![json!(10), json!(20)],
///     dapp_definition_address: "dapp_definition_address".to_string(),
///     identity: "my_identity".to_string(),
/// });
/// ```
pub struct Runtime<AS, PS, NS, ASS, PSS, NSS, RNV>
where
    AS: Store2<Bytes, Infallible, Infallible>,
    PS: Store3<Bytes, Infallible, Infallible>,
    NS: Store3<Bytes, Infallible, Infallible>,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    RNV: RadixNftVerifier,
{
    application_sql_store: ASS,
    application_store: AS,
    handler_specifier: HandlerSpecifier,
    handler_type: HandlerType,
    module_handle: ModuleHandle,
    nft_sql_store: NSS,
    nft_store: NS,
    personal_sql_store: PSS,
    personal_store: PS,
    runtime: rustyscript::Runtime,
    sql_migrations: SqlMigrations,
    _marker: PhantomData<RNV>,
}

impl<AS, PS, NS, ASS, PSS, NSS, RNV> Runtime<AS, PS, NS, ASS, PSS, NSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    RNV: RadixNftVerifier,
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
    #[allow(clippy::too_many_lines)]
    pub fn new(
        RuntimeOptions {
            application_sql_store,
            application_store,
            handler_specifier,
            module_loader,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin: gateway_origin,
            radix_network_definition,
            radix_nft_verifier,
        }: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS, RNV>,
    ) -> Result<Self> {
        let module_specifier = handler_specifier.module_specifier();
        let Some(module) = module_loader.get_module(&module_specifier, &ProcessingMode::Runtime)
        else {
            return Err(Error::SpecifierNotFoundInCodePackage);
        };

        if let Err(err) = module_loader.get_module_options(&module_specifier) {
            println!("{err:?}");
        };

        let Ok(module_options) = module_loader.get_module_options(&module_specifier) else {
            return Err(Error::SpecifierNotFoundInCodePackage);
        };

        let handler_options = module_options
            .handler_options
            .get(handler_specifier.as_str());

        let (allowed_web_origins, handler_type, max_heap_mbs, timeout_millis) =
            match handler_options {
                Some(HandlerOptions::Http {
                    allowed_web_origins,
                    max_heap_mbs,
                    timeout_millis,
                    ..
                }) => (
                    allowed_web_origins.clone(),
                    HandlerType::Http,
                    max_heap_mbs.unwrap_or(DEFAULT_HEAP_SIZE),
                    timeout_millis.unwrap_or(DEFAULT_TIMEOUT_MILLIS),
                ),
                Some(HandlerOptions::RadixEvent {
                    allowed_web_origins,
                    max_heap_mbs,
                    timeout_millis,
                    ..
                }) => (
                    allowed_web_origins.clone(),
                    HandlerType::RadixEvent,
                    max_heap_mbs.unwrap_or(DEFAULT_HEAP_SIZE),
                    timeout_millis.unwrap_or(DEFAULT_TIMEOUT_MILLIS),
                ),
                Some(HandlerOptions::Rpc {
                    allowed_web_origins,
                    max_heap_mbs,
                    timeout_millis,
                    ..
                }) => (
                    allowed_web_origins.clone(),
                    HandlerType::Rpc,
                    max_heap_mbs.unwrap_or(DEFAULT_HEAP_SIZE),
                    timeout_millis.unwrap_or(DEFAULT_TIMEOUT_MILLIS),
                ),
                None => (
                    HashSet::new(),
                    HandlerType::Rpc,
                    DEFAULT_HEAP_SIZE,
                    DEFAULT_TIMEOUT_MILLIS,
                ),
            };

        let allowlist_web_permissions = OriginAllowlistWebPermissions::new();
        allowlist_web_permissions.allow_origin(&gateway_origin); // Always allow Radix gateway origin
        for origin in &allowed_web_origins {
            allowlist_web_permissions.allow_origin(origin);
        }

        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            import_provider: Some(Box::new(
                module_loader.import_provider(ProcessingMode::Runtime),
            )),
            timeout: Duration::from_millis(timeout_millis.into()),
            max_heap_size: Some(max_heap_mbs as usize * 1024 * 1024),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                handler_runtime_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                crypto_ext::init_ops_and_esm(),
                session_ext::init_ops_and_esm(),
                kv_runtime_ext::init_ops_and_esm::<
                    AS::Scoped,
                    <<PS as Store3>::Scoped as Store2>::Scoped,
                    NS::Scoped,
                    RNV,
                >(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                // TODO: The above is no longer the case - combine these again at some point
                sql_runtime_ext::init_ops_and_esm(),
                sql_application_ext::init_ops::<ASS::Scoped>(),
                sql_personal_ext::init_ops::<<<PSS as SqlStore3>::Scoped as SqlStore2>::Scoped>(),
                sql_nft_ext::init_ops::<NSS::Scoped, RNV>(),
                // Vendered modules
                openai_ext::init_ops_and_esm(),
                babylon_gateway_api_ext::init_ops_and_esm(),
                radix_engine_toolkit_ext::init_ops_and_esm(),
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

        // Set the Radix NFT verifier for storage extensions
        runtime.put(radix_nft_verifier)?;

        // In case there are any top-level console.* calls in the module
        runtime.put(ConsoleState::default())?;

        let module_handle = runtime.load_module(&module)?;

        Ok(Self {
            application_sql_store,
            application_store,
            handler_specifier,
            handler_type,
            module_handle,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            runtime,
            sql_migrations: module_options.sql_migrations,
            _marker: PhantomData,
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
    #[allow(clippy::too_many_lines)]
    pub fn execute(&mut self, execution_request: ExecutionRequest) -> Result<ExecutionResult> {
        let start = Instant::now();

        #[allow(clippy::match_same_arms)]
        let (accounts, args, dapp_definition_address, identity) =
            match (&self.handler_type, execution_request) {
                (
                    HandlerType::Http,
                    ExecutionRequest::Http {
                        body,
                        dapp_definition_address,
                        method,
                        path,
                        query,
                    },
                ) => {
                    let mut args = vec![];

                    args.push(json!(method.as_str()));
                    args.push(json!(path));

                    if let Some(query) = query {
                        args.push(json!(query));
                    } else {
                        args.push(json!(null));
                    }

                    if let Some(body) = body {
                        args.push(json!(body));
                    }

                    (None, args, dapp_definition_address, None)
                }
                (
                    HandlerType::Http,
                    ExecutionRequest::HttpWithUserContext {
                        accounts,
                        body,
                        dapp_definition_address,
                        identity,
                        method,
                        path,
                        query,
                    },
                ) => {
                    let mut args = vec![];

                    args.push(json!(method.as_str()));
                    args.push(json!(path));

                    if let Some(query) = query {
                        args.push(json!(query));
                    } else {
                        args.push(json!(null));
                    }

                    if let Some(body) = body {
                        args.push(json!(body));
                    }

                    (
                        Some(accounts),
                        args,
                        dapp_definition_address,
                        Some(identity),
                    )
                }
                (
                    HandlerType::RadixEvent,
                    ExecutionRequest::RadixEvent {
                        dapp_definition_address,
                    },
                ) => (
                    None,
                    vec![], // TODO: Should use transaction data
                    dapp_definition_address,
                    None,
                ),
                (
                    HandlerType::Rpc,
                    ExecutionRequest::Rpc {
                        accounts,
                        args,
                        dapp_definition_address,
                        identity,
                    },
                ) => (
                    Some(accounts),
                    args,
                    dapp_definition_address,
                    Some(identity),
                ),
                _ => return Err(Error::MismatchedExecutionRequest),
            };

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

        self.runtime.put(match accounts.as_ref() {
            Some(accounts) if !accounts.is_empty() => Some(
                self.nft_store
                    .clone()
                    .scope(dapp_definition_address.clone()),
            ),
            Some(_) | None => None,
        })?;

        // Set the sql stores for the storage extension
        self.runtime.put(SqlParamListManager::new())?;
        self.runtime.put(SqlQueryResultsManager::new())?;

        self.runtime.put(ApplicationSqlConnectionManager::new(
            self.application_sql_store
                .clone()
                .scope(dapp_definition_address.clone()),
            self.sql_migrations.application.clone(),
        ))?;

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

        self.runtime.put(match accounts.as_ref() {
            Some(_) => Some(NftSqlConnectionManager::new(
                self.nft_sql_store.clone().scope(dapp_definition_address),
                self.sql_migrations.nft.clone(),
            )),
            None => None,
        })?;

        // Set the context for the session extension
        self.runtime.put(SessionState { identity, accounts })?;

        let output: Value = match self.handler_specifier.handler_name() {
            Some(ref handler_name) => {
                self.runtime
                    .call_function(Some(&self.module_handle), handler_name, &args)?
            }
            None => self.runtime.call_entrypoint(&self.module_handle, &args)?,
        };

        let handler_output: HandlerOutput = output.try_into(&mut self.runtime)?;

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
            output: handler_output.output.unwrap_or_default(),
            paths_to_uint8_arrays: handler_output.uint8_array_json_paths,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    // spawn in std::thread to avoid rustyscript panic
    fn run_in_thread<F: FnOnce() + Send + 'static>(f: F) {
        std::thread::spawn(f).join().unwrap();
    }

    fn create_execution_request() -> ExecutionRequest {
        ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![json!(10), json!(20)],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        }
    }

    #[tokio::test]
    async fn test_runtime_execute() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute", "test");

        run_in_thread(|| {
            let request = create_execution_request();
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

            assert!(execution_result.output.is_null());
            assert!(execution_result.duration.as_millis() < 1000);
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_default_export() {
        let mut options =
            RuntimeOptions::for_test_code("test_runtime_execute_with_default_export", "remove");
        // Remove fragment to use default export
        options.handler_specifier = HandlerSpecifier::parse("file:///main.ts").unwrap();

        run_in_thread(|| {
            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            if let Err(err) = result {
                panic!("Error: {err:?}");
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_sets_timeout() {
        // The script will sleep for 1.5 seconds, but the timeout is set to 2 seconds
        let options = RuntimeOptions::for_test_code("test_runtime_execute_sets_timeout", "test");

        run_in_thread(|| {
            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            if let Err(err) = result {
                panic!("Error: {err:?}");
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_default_max_heap_size() {
        // The script will allocate 40MB of memory, but the default max heap size is set to 10MB
        let options =
            RuntimeOptions::for_test_code("test_runtime_execute_default_max_heap_size", "test");

        run_in_thread(|| {
            let request = create_execution_request();
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_err());
        });
    }
}
