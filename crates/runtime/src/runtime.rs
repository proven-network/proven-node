use crate::extensions::{
    ApplicationSqlConnectionManager, ConsoleState, CryptoState, HandlerOutput, IdentityState,
    PersonalSqlConnectionManager, SqlParamListManager, SqlQueryResultsManager, console_ext,
    crypto_ext, handler_runtime_ext, kv_runtime_ext, openai_ext, radix_engine_toolkit_ext,
    session_ext, sql_runtime_ext, uuid_ext, zod_ext,
};
use crate::file_system::{FileSystem, StoredEntry};
use crate::module_loader::{ModuleLoader, ProcessingMode};
use crate::options::HandlerOptions;
use crate::permissions::OriginAllowlistWebPermissions;
use crate::rpc_endpoints::RpcEndpoints;
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
    FSS: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
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

    /// Verifier for checking NFT ownership on the Radix Network.
    pub radix_nft_verifier: RNV,

    /// RPC endpoints for the runtime.
    pub rpc_endpoints: RpcEndpoints,
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
        proven_store_memory::MemoryStore<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
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
        use tempfile::tempdir;

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::new(),
            module_loader: ModuleLoader::from_test_code(script_name),
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
            personal_store: MemoryStore3::new(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
            rpc_endpoints: RpcEndpoints::external(),
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
        module_roots: impl IntoIterator<Item = ModuleSpecifier> + Clone,
    ) -> Self {
        use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
        use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
        use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
        use tempfile::tempdir;

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::<
                StoredEntry,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >::new(),
            module_loader: ModuleLoader::from_test_code_map(module_sources, module_roots),
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
            personal_store: MemoryStore3::new(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
            rpc_endpoints: RpcEndpoints::external(),
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
///     Error, ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RpcEndpoints,
///     Runtime, RuntimeOptions,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
/// use serde_json::json;
/// use tempfile::tempdir;
/// use uuid::Uuid;
///
/// let code_package = CodePackage::from_str("export const handler = (a, b) => a + b;").unwrap();
///
/// let mut runtime = Runtime::new(RuntimeOptions {
///     application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
///     application_store: MemoryStore2::new(),
///     file_system_store: MemoryStore::new(),
///     module_loader: ModuleLoader::new(code_package),
///     nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
///     nft_store: MemoryStore3::new(),
///     personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
///     personal_store: MemoryStore3::new(),
///     radix_nft_verifier: MockRadixNftVerifier::new(),
///     rpc_endpoints: RpcEndpoints::external(),
/// })
/// .expect("Failed to create runtime");
///
/// runtime.execute(ExecutionRequest::Rpc {
///     application_id: Uuid::new_v4(),
///     args: vec![json!(10), json!(20)],
///     handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
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
    FSS: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    RNV: RadixNftVerifier,
{
    application_sql_store: ASS,
    application_store: AS,
    module_handle_cache: HashMap<ModuleSpecifier, ModuleHandle>,
    module_loader: ModuleLoader,
    _nft_sql_store: NSS,
    _nft_store: NS,
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
    FSS: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
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
            rpc_endpoints,
            radix_nft_verifier,
        }: RuntimeOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>,
    ) -> Result<Self> {
        // TODO: Remove this after tracking down and removing all instances of ring.
        // Install the default crypto provider for rustls.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Allow outbound requests to all endpoints
        let origin_allowlist_web_permissions = Arc::new(OriginAllowlistWebPermissions::new(
            rpc_endpoints
                .clone()
                .into_vec()
                .into_iter()
                .map(|url| url.to_string()),
        ));

        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            import_provider: Some(Box::new(
                module_loader.import_provider(ProcessingMode::Runtime),
            )),
            timeout: Duration::from_secs(MAX_TIMEOUT_SECONDS_HARD_LIMIT),
            max_heap_size: Some(MAX_HEAP_SIZE_MBS_HARD_LIMIT * 1024 * 1024),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                rpc_endpoints.into_extension(),
                handler_runtime_ext::init(),
                console_ext::init(),
                crypto_ext::init(),
                session_ext::init(),
                kv_runtime_ext::init::<
                    AS::Scoped,
                    <<PS as Store3>::Scoped as Store2>::Scoped,
                    NS::Scoped,
                    RNV,
                >(),
                sql_runtime_ext::init::<
                    ASS::Scoped,
                    <<PSS as SqlStore3>::Scoped as SqlStore2>::Scoped,
                    NSS::Scoped,
                    RNV,
                >(),
                // Vendered modules
                openai_ext::init(),
                radix_engine_toolkit_ext::init(),
                uuid_ext::init(),
                zod_ext::init(),
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

        // Set the Radix NFT verifier for storage extensions
        rustyscript::Runtime::put(&mut runtime, radix_nft_verifier)?;

        Ok(Self {
            application_sql_store,
            application_store,
            module_handle_cache: HashMap::new(),
            module_loader,
            _nft_sql_store: nft_sql_store,
            _nft_store: nft_store,
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

        let (application_id, args, handler_specifier, session_state) = match execution_request {
            ExecutionRequest::Http {
                application_id,
                body,
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

                (
                    application_id,
                    args,
                    handler_specifier,
                    IdentityState::NoIdentity,
                )
            }

            ExecutionRequest::HttpWithIdentity {
                application_id,
                body,
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
                    application_id,
                    args,
                    handler_specifier,
                    IdentityState::Identity(identity.id),
                )
            }

            ExecutionRequest::RadixEvent {
                application_id,
                handler_specifier,
            } => (
                application_id,
                vec![], // TODO: Should use transaction data for args
                handler_specifier,
                IdentityState::NoIdentity,
            ),

            ExecutionRequest::Rpc {
                application_id,
                args,
                handler_specifier,
            } => (
                application_id,
                args,
                handler_specifier,
                IdentityState::NoIdentity,
            ),

            ExecutionRequest::RpcWithIdentity {
                application_id,
                args,
                handler_specifier,
                identity,
            } => (
                application_id,
                args,
                handler_specifier,
                IdentityState::Identity(identity.id),
            ),
        };

        let module_specifier = handler_specifier.module_specifier();

        let Ok(module_options) = self.module_loader.get_module_options(&module_specifier) else {
            return Err(Error::SpecifierNotFoundInCodePackage(module_specifier));
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
                .scope(application_id.to_string()),
        )?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            match session_state {
                IdentityState::Identity(ref identity_id) => Some(
                    self.personal_store
                        .clone()
                        .scope(application_id.to_string())
                        .scope(identity_id.to_string()),
                ),
                IdentityState::NoIdentity => None,
            },
        )?;

        // rustyscript::Runtime::put(
        //     &mut self.runtime,
        //     match session_state {
        //         IdentityState::Identity(identity) => {
        //             Some(self.nft_store.clone().scope(application_id.clone()))
        //         }
        //         _ => None,
        //     },
        // )?;

        // Set the sql stores for the storage extension
        rustyscript::Runtime::put(&mut self.runtime, SqlParamListManager::new())?;
        rustyscript::Runtime::put(&mut self.runtime, SqlQueryResultsManager::new())?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            ApplicationSqlConnectionManager::new(
                self.application_sql_store
                    .clone()
                    .scope(&application_id.to_string()),
                module_options.sql_migrations.application.clone(),
            ),
        )?;

        rustyscript::Runtime::put(
            &mut self.runtime,
            match session_state {
                IdentityState::Identity(ref identity_id) => {
                    Some(PersonalSqlConnectionManager::new(
                        self.personal_sql_store
                            .clone()
                            .scope(&application_id.to_string())
                            .scope(&identity_id.to_string()),
                        module_options.sql_migrations.personal.clone(),
                    ))
                }
                IdentityState::NoIdentity => None,
            },
        )?;

        // rustyscript::Runtime::put(
        //     &mut self.runtime,
        //     match session_state {
        //         IdentityState::Identity(identity) => Some(NftSqlConnectionManager::new(
        //             self.nft_sql_store.clone().scope(&application_id),
        //             module_options.sql_migrations.nft.clone(),
        //         )),
        //         _ => None,
        //     },
        // )?;

        // Set the context for the session extension
        rustyscript::Runtime::put(&mut self.runtime, session_state)?;

        let module_handle = self.get_module_for_handler_specifier(&handler_specifier)?;

        let result: std::result::Result<Value, rustyscript::Error> =
            match handler_specifier.handler_name() {
                Some(ref handler_name) => {
                    self.runtime
                        .call_function(Some(&module_handle), handler_name, &args)
                }
                None => self.runtime.call_entrypoint(&module_handle, &args),
            };

        let console_state: ConsoleState = self.runtime.take().unwrap_or_default();
        let duration = start.elapsed();

        let logs: Vec<ExecutionLogs> = console_state
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
            .collect::<Result<Vec<ExecutionLogs>>>()?;

        match result {
            Ok(output) => {
                let handler_output: HandlerOutput = output.try_into(&mut self.runtime)?;

                Ok(ExecutionResult::Ok {
                    duration,
                    logs,
                    output: handler_output.output.unwrap_or_default(),
                    paths_to_uint8_arrays: handler_output.uint8_array_json_paths,
                })
            }
            Err(rustyscript::Error::JsError(js_error)) => Ok(ExecutionResult::Error {
                duration,
                logs,
                error: js_error,
            }),
            Err(err) => Err(Error::RuntimeError(err)),
        }
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
            return Err(Error::SpecifierNotFoundInCodePackage(module_specifier));
        };

        let module_handle = self.runtime.load_module(&module)?;

        self.module_handle_cache
            .insert(module_specifier, module_handle.clone());

        Ok(module_handle)
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;
    use crate::util::run_in_thread;

    #[tokio::test]
    async fn test_runtime_execute() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute");

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { output, .. }) => {
                    assert!(output.is_null());
                }
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }
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
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { output, .. }) => {
                    assert!(output.is_string());
                    assert_eq!(output.as_str().unwrap(), "Hello, world!");
                }
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_with_default_export() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute_with_default_export");

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { .. }) => {}
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_sets_timeout() {
        // The script will sleep for 1.5 seconds, but the timeout is set to 2 seconds
        let options = RuntimeOptions::for_test_code("test_runtime_execute_sets_timeout");

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { .. }) => {}
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_exhausts_timeout() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute_exhausts_timeout");

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { .. }) => {
                    panic!("Expected timeout error but got success");
                }
                Ok(ExecutionResult::Error { .. }) => {}
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_default_max_heap_size() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute_default_max_heap_size");

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { .. }) => {
                    panic!("Expected heap size error but got success");
                }
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(_) => {
                    // Heap size error is expected
                }
            }
        });
    }

    #[tokio::test]
    async fn test_runtime_execute_rpc_endpoints() {
        let options = RuntimeOptions::for_test_code("test_runtime_execute_rpc_endpoints");

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            let request =
                ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

            match runtime.execute(request) {
                Ok(ExecutionResult::Ok { output, .. }) => {
                    assert!(output.is_array());
                    assert_eq!(output.as_array().unwrap().len(), 7);
                }
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }
        });
    }
}
