use crate::extensions::{
    babylon_gateway_api_ext, console_ext, crypto_ext, handler_runtime_ext, kv_runtime_ext,
    openai_ext, radix_engine_toolkit_ext, session_ext, sql_runtime_ext, uuid_ext, zod_ext,
    ApplicationSqlConnectionManager, ConsoleState, CryptoState, GatewayDetailsState, HandlerOutput,
    NftSqlConnectionManager, PersonalSqlConnectionManager, SessionState, SqlParamListManager,
    SqlQueryResultsManager,
};
use crate::file_system::{Entry, FileSystem};
use crate::module_loader::{ModuleLoader, ProcessingMode};
use crate::options::HandlerOptions;
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::{Error, ExecutionLogs, ExecutionRequest, ExecutionResult, HandlerSpecifier, Result};

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proven_code_package::ModuleSpecifier;
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store, Store2, Store3};
use radix_common::network::NetworkDefinition;
use rustyscript::js_value::Value;
use rustyscript::{ExtensionOptions, ModuleHandle, WebOptions};
use serde_json::json;
use tokio::time::Instant;

static MAX_HEAP_SIZE_MBS_HARD_LIMIT: usize = 32;
static MAX_TIMEOUT_SECONDS_HARD_LIMIT: u64 = 60;

/// Options for creating a new `Runtime`.
#[derive(Clone)]
pub struct RuntimeOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<Entry, serde_json::Error, serde_json::Error>,
    RNV: RadixNftVerifier,
{
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

    /// Store used for file-system virtualisation.
    pub file_system_store: FSS,

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
        proven_store_memory::MemoryStore<Entry, serde_json::Error, serde_json::Error>,
        proven_radix_nft_verifier_mock::MockRadixNftVerifier,
    >
{
    /// Creates a new `RuntimeOptions` instance for testing purposes.
    ///
    /// # Parameters
    /// - `script_name`: The name of the script to use.
    ///
    /// # Returns
    /// A new `RuntimeOptions` instance.
    ///
    /// # Panics
    /// This function will panic if creating a temporary directory fails.
    #[must_use]
    pub fn for_test_code(script_name: &str) -> Self {
        use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
        use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
        use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
        use radix_common::network::NetworkDefinition;
        use tempfile::tempdir;

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::new(),
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

    /// Creates a new `RuntimeOptions` instance for testing purposes.
    ///
    /// # Parameters
    ///
    /// * `module_sources` - A map of module specifiers to their source code.
    /// * `module_roots` - An iterator over the module specifiers that are considered roots.
    ///
    /// # Returns
    /// A new `RuntimeOptions` instance.
    ///
    /// # Panics
    /// This function will panic if creating a temporary directory fails.
    #[must_use]
    pub fn for_test_code_map(
        module_sources: &HashMap<ModuleSpecifier, &str>,
        module_roots: impl IntoIterator<Item = ModuleSpecifier>,
    ) -> Self {
        use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
        use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
        use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
        use radix_common::network::NetworkDefinition;
        use tempfile::tempdir;

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::<Entry, serde_json::Error, serde_json::Error>::new(),
            module_loader: ModuleLoader::from_test_code_map(module_sources, module_roots),
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
///     handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
///     identity: "my_identity".to_string(),
/// });
/// ```
pub struct Runtime<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2<Bytes, Infallible, Infallible>,
    PS: Store3<Bytes, Infallible, Infallible>,
    NS: Store3<Bytes, Infallible, Infallible>,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<Entry, serde_json::Error, serde_json::Error>,
    RNV: RadixNftVerifier,
{
    application_sql_store: ASS,
    application_store: AS,
    module_handle_cache: HashMap<ModuleSpecifier, ModuleHandle>,
    module_loader: ModuleLoader,
    nft_sql_store: NSS,
    nft_store: NS,
    origin_allowlist_web_permissions: Arc<OriginAllowlistWebPermissions>,
    personal_sql_store: PSS,
    personal_store: PS,
    runtime: rustyscript::Runtime,
    _marker: PhantomData<(FSS, RNV)>,
}

impl<AS, PS, NS, ASS, PSS, NSS, FSS, RNV> Runtime<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<Entry, serde_json::Error, serde_json::Error>,
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
            file_system_store,
            module_loader,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin,
            radix_network_definition,
            radix_nft_verifier,
        }: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>,
    ) -> Result<Self> {
        let origin_allowlist_web_permissions = Arc::new(OriginAllowlistWebPermissions::new(vec![
            // Always allow Radix gateway origin
            radix_gateway_origin.clone(),
        ]));

        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            import_provider: Some(Box::new(
                module_loader.import_provider(ProcessingMode::Runtime),
            )),
            timeout: Duration::from_secs(MAX_TIMEOUT_SECONDS_HARD_LIMIT),
            max_heap_size: Some(MAX_HEAP_SIZE_MBS_HARD_LIMIT * 1024 * 1024),
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
                sql_runtime_ext::init_ops_and_esm::<
                    ASS::Scoped,
                    <<PSS as SqlStore3>::Scoped as SqlStore2>::Scoped,
                    NSS::Scoped,
                    RNV,
                >(),
                // Vendered modules
                openai_ext::init_ops_and_esm(),
                babylon_gateway_api_ext::init_ops_and_esm(),
                radix_engine_toolkit_ext::init_ops_and_esm(),
                uuid_ext::init_ops_and_esm(),
                zod_ext::init_ops_and_esm(),
            ],
            extension_options: ExtensionOptions {
                filesystem: Arc::new(FileSystem::new(file_system_store)),
                web: WebOptions {
                    permissions: origin_allowlist_web_permissions.clone(),
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
        rustyscript::Runtime::put(
            &mut runtime,
            GatewayDetailsState {
                gateway_origin: radix_gateway_origin,
                network_id: radix_network_definition.id,
            },
        )?;

        // Set the Radix NFT verifier for storage extensions
        rustyscript::Runtime::put(&mut runtime, radix_nft_verifier)?;

        Ok(Self {
            application_sql_store,
            application_store,
            module_handle_cache: HashMap::new(),
            module_loader,
            nft_sql_store,
            nft_store,
            origin_allowlist_web_permissions,
            personal_sql_store,
            personal_store,
            runtime,
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

        let (accounts, args, dapp_definition_address, handler_specifier, identity) =
            match execution_request {
                ExecutionRequest::Http {
                    body,
                    dapp_definition_address,
                    handler_specifier,
                    method,
                    path,
                    query,
                } => {
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

                    (None, args, dapp_definition_address, handler_specifier, None)
                }

                ExecutionRequest::HttpWithUserContext {
                    accounts,
                    body,
                    dapp_definition_address,
                    handler_specifier,
                    identity,
                    method,
                    path,
                    query,
                } => {
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
                        handler_specifier,
                        Some(identity),
                    )
                }

                ExecutionRequest::RadixEvent {
                    dapp_definition_address,
                    handler_specifier,
                } => (
                    None,
                    vec![], // TODO: Should use transaction data
                    dapp_definition_address,
                    handler_specifier,
                    None,
                ),

                ExecutionRequest::Rpc {
                    accounts,
                    args,
                    dapp_definition_address,
                    handler_specifier,
                    identity,
                } => (
                    Some(accounts),
                    args,
                    dapp_definition_address,
                    handler_specifier,
                    Some(identity),
                ),
            };

        let module_specifier = handler_specifier.module_specifier();

        let Ok(module_options) = self.module_loader.get_module_options(&module_specifier) else {
            return Err(Error::SpecifierNotFoundInCodePackage);
        };

        let handler_options = module_options
            .handler_options
            .get(handler_specifier.as_str());

        let allowed_web_origins = match handler_options {
            Some(
                HandlerOptions::Http {
                    allowed_web_origins,
                    max_heap_mbs: _,
                    timeout_millis: _,
                    ..
                }
                | HandlerOptions::RadixEvent {
                    allowed_web_origins,
                    ..
                }
                | HandlerOptions::Rpc {
                    allowed_web_origins,
                    max_heap_mbs: _,
                    timeout_millis: _,
                    ..
                },
            ) => allowed_web_origins.clone(),
            None => HashSet::new(),
        };

        // Reset the origin allowlist before each execution
        self.origin_allowlist_web_permissions.reset_to_default();
        for origin in &allowed_web_origins {
            self.origin_allowlist_web_permissions.allow_origin(origin);
        }

        // Reset the console state before each execution
        rustyscript::Runtime::put(&mut self.runtime, ConsoleState::default())?;

        // Reset the crypto key cache before each execution
        rustyscript::Runtime::put(&mut self.runtime, CryptoState::default())?;

        // Set the kv stores for the storage extension
        rustyscript::Runtime::put(
            &mut self.runtime,
            self.application_store
                .clone()
                .scope(dapp_definition_address.clone()),
        )?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            match identity.as_ref() {
                Some(current_identity) => Some(
                    self.personal_store
                        .clone()
                        .scope(dapp_definition_address.clone())
                        .scope(current_identity.clone()),
                ),
                None => None,
            },
        )?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            match accounts.as_ref() {
                Some(accounts) if !accounts.is_empty() => Some(
                    self.nft_store
                        .clone()
                        .scope(dapp_definition_address.clone()),
                ),
                Some(_) | None => None,
            },
        )?;

        // Set the sql stores for the storage extension
        rustyscript::Runtime::put(&mut self.runtime, SqlParamListManager::new())?;
        rustyscript::Runtime::put(&mut self.runtime, SqlQueryResultsManager::new())?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            ApplicationSqlConnectionManager::new(
                self.application_sql_store
                    .clone()
                    .scope(dapp_definition_address.clone()),
                module_options.sql_migrations.application.clone(),
            ),
        )?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            match identity.as_ref() {
                Some(current_identity) => Some(PersonalSqlConnectionManager::new(
                    self.personal_sql_store
                        .clone()
                        .scope(dapp_definition_address.clone())
                        .scope(current_identity.clone()),
                    module_options.sql_migrations.personal.clone(),
                )),
                None => None,
            },
        )?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            match accounts.as_ref() {
                Some(_) => Some(NftSqlConnectionManager::new(
                    self.nft_sql_store.clone().scope(dapp_definition_address),
                    module_options.sql_migrations.nft.clone(),
                )),
                None => None,
            },
        )?;

        // Set the context for the session extension
        rustyscript::Runtime::put(&mut self.runtime, SessionState { identity, accounts })?;

        let module_handle = self.get_module_for_handler_specifier(&handler_specifier)?;

        let output: Value = match handler_specifier.handler_name() {
            Some(ref handler_name) => {
                self.runtime
                    .call_function(Some(&module_handle), handler_name, &args)?
            }
            None => self.runtime.call_entrypoint(&module_handle, &args)?,
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

    fn get_module_for_handler_specifier(
        &mut self,
        handler_specifier: &HandlerSpecifier,
    ) -> Result<ModuleHandle> {
        let module_specifier = handler_specifier.module_specifier();

        if let Some(module_handle) = self.module_handle_cache.get(&module_specifier) {
            return Ok(module_handle.clone());
        }

        let Some(module) = self
            .module_loader
            .get_module(&module_specifier, ProcessingMode::Runtime)
        else {
            return Err(Error::SpecifierNotFoundInCodePackage);
        };

        let module_handle = self.runtime.load_module(&module)?;

        self.module_handle_cache
            .insert(module_specifier, module_handle.clone());

        Ok(module_handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::run_in_thread;

    #[tokio::test]
    async fn test_runtime_execute() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute");

        run_in_thread(|| {
            let request = ExecutionRequest::Rpc {
                accounts: vec![],
                args: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
                identity: "my_identity".to_string(),
            };
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

            assert!(execution_result.output.is_null());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_nested() {
        let module_sources = HashMap::from([
            (
                ModuleSpecifier::parse("file:///main.ts").unwrap(),
                "test_runtime_nested_base",
            ),
            (
                ModuleSpecifier::parse("file:///test_runtime_nested_loaded.ts").unwrap(),
                "test_runtime_nested_loaded",
            ),
        ]);
        let module_roots = vec![ModuleSpecifier::parse("file:///main.ts").unwrap()];

        let options = RuntimeOptions::for_test_code_map(&module_sources, module_roots);

        run_in_thread(|| {
            let request = ExecutionRequest::Rpc {
                accounts: vec![],
                args: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
                identity: "my_identity".to_string(),
            };
            let execution_result = Runtime::new(options).unwrap().execute(request).unwrap();

            assert!(execution_result.output.is_string());

            assert_eq!(execution_result.output.as_str().unwrap(), "Hello, world!");
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_default_export() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute_with_default_export");

        run_in_thread(|| {
            let request = ExecutionRequest::Rpc {
                accounts: vec![],
                args: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts").unwrap(),
                identity: "my_identity".to_string(),
            };
            let result = Runtime::new(options).unwrap().execute(request);

            if let Err(err) = result {
                panic!("Error: {err:?}");
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_sets_timeout() {
        // The script will sleep for 1.5 seconds, but the timeout is set to 2 seconds
        let options = RuntimeOptions::for_test_code("test_runtime_execute_sets_timeout");

        run_in_thread(|| {
            let request = ExecutionRequest::Rpc {
                accounts: vec![],
                args: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
                identity: "my_identity".to_string(),
            };
            let result = Runtime::new(options).unwrap().execute(request);

            if let Err(err) = result {
                panic!("Error: {err:?}");
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_exhausts_timeout() {
        // The script will sleep for 5 seconds, but the timeout is set to 2 seconds
        let options = RuntimeOptions::for_test_code("test_runtime_execute_exhausts_timeout");

        run_in_thread(|| {
            let request = ExecutionRequest::Rpc {
                accounts: vec![],
                args: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
                identity: "my_identity".to_string(),
            };
            let result = Runtime::new(options).unwrap().execute(request);

            if let Ok(execution_result) = result {
                panic!("Ok: {execution_result:?}");
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_default_max_heap_size() {
        // The script will allocate 40MB of memory, but the default max heap size is set to 10MB
        let options = RuntimeOptions::for_test_code("test_runtime_execute_default_max_heap_size");

        run_in_thread(|| {
            let request = ExecutionRequest::Rpc {
                accounts: vec![],
                args: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
                identity: "my_identity".to_string(),
            };
            let result = Runtime::new(options).unwrap().execute(request);

            assert!(result.is_err());
        });
    }
}
