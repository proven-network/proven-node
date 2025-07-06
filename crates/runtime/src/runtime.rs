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
    ///
    /// * `script_name` - The name of the script file to load from the `test_esm` directory.
    ///
    /// # Returns
    /// A new `RuntimeOptions` instance.
    ///
    /// # Panics
    /// This function will panic if creating a temporary directory fails.
    #[must_use]
    pub async fn for_test_code(script_name: &str) -> Self {
        use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
        use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
        use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
        use tempfile::tempdir;

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::new(),
            module_loader: ModuleLoader::from_test_code(script_name).await,
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
    pub async fn for_test_code_map(
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
            module_loader: ModuleLoader::from_test_code_map(module_sources, module_roots).await,
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
            personal_store: MemoryStore3::new(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
            rpc_endpoints: RpcEndpoints::external(),
        }
    }

    /// Creates a new [`RuntimeOptions`] instance with all testing stores and a module loader created from a manifest.
    /// This is useful for testing runtime execution with `CodePackages` created from manifests.
    ///
    /// # Panics
    /// This function will panic if creating a temporary directory fails or if the manifest is invalid.
    #[cfg(test)]
    #[must_use]
    pub async fn for_test_manifest(manifest: &proven_code_package::BundleManifest) -> Self {
        use proven_code_package::CodePackage;
        use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
        use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
        use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
        use tempfile::tempdir;

        let code_package = CodePackage::from_manifest(manifest).await.unwrap();

        Self {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::<
                StoredEntry,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >::new(),
            module_loader: ModuleLoader::new(code_package),
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
///     Runtime, RuntimeOptions, Worker,
/// };
/// use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
/// use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
/// use serde_json::json;
/// use tempfile::tempdir;
/// use uuid::Uuid;
///
/// #[tokio::main]
/// async fn main() {
///     let code_package = CodePackage::from_str("export const handler = (a, b) => a + b;")
///         .await
///         .unwrap();
///
///     let mut worker = Worker::new(RuntimeOptions {
///         application_sql_store: DirectSqlStore2::new(tempdir().unwrap().keep()),
///         application_store: MemoryStore2::new(),
///         file_system_store: MemoryStore::new(),
///         module_loader: ModuleLoader::new(code_package),
///         nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
///         nft_store: MemoryStore3::new(),
///         personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().keep()),
///         personal_store: MemoryStore3::new(),
///         radix_nft_verifier: MockRadixNftVerifier::new(),
///         rpc_endpoints: RpcEndpoints::external(),
///     })
///     .await
///     .expect("Failed to create worker");
///
///     worker
///         .execute(ExecutionRequest::Rpc {
///             application_id: Uuid::new_v4(),
///             args: vec![json!(10), json!(20)],
///             handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
///         })
///         .await;
/// }
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
        let options = RuntimeOptions::for_test_code("test_runtime_execute").await;

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
    async fn test_runtime_execute_with_default_export() {
        let options =
            RuntimeOptions::for_test_code("test_runtime_execute_with_default_export").await;

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
        let options = RuntimeOptions::for_test_code("test_runtime_execute_sets_timeout").await;

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
        let options = RuntimeOptions::for_test_code("test_runtime_execute_exhausts_timeout").await;

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
        let options =
            RuntimeOptions::for_test_code("test_runtime_execute_default_max_heap_size").await;

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
        let options = RuntimeOptions::for_test_code("test_runtime_execute_rpc_endpoints").await;

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

    #[tokio::test]
    async fn test_runtime_execute_with_manifest_codepackage() {
        // Test runtime execution with a CodePackage created from a manifest
        // This tests the same scenario as the test_todo_manifest_module_not_found_error
        // but actually executes one of the handlers to verify full end-to-end functionality

        let manifest_json = r#"{
            "id": "manifest-31f0c8950596982b",
            "version": "1.0.0",
            "modules": [
                {
                    "specifier": "file:///src/todo-handlers.ts",
                    "content": "import { run } from '@proven-network/handler';\n\n// In-memory storage for this example\nlet todos = [];\nlet nextId = 1;\n\nexport const createTodo = run((request) => {\n  const todo = {\n    id: `todo-${nextId++}`,\n    title: request.title,\n    description: request.description,\n    completed: false,\n    createdAt: new Date(),\n    updatedAt: new Date(),\n  };\n  todos.push(todo);\n  return todo;\n});\n\nexport const getTodos = run((filter) => {\n  let filteredTodos = [...todos];\n  if (filter?.completed !== undefined) {\n    filteredTodos = filteredTodos.filter((todo) => todo.completed === filter.completed);\n  }\n  return filteredTodos;\n});",
                    "handlers": [
                        {
                            "name": "createTodo",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "request",
                                    "type": "CreateTodoRequest",
                                    "optional": false
                                }
                            ]
                        },
                        {
                            "name": "getTodos",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "filter",
                                    "type": "TodoFilter",
                                    "optional": false
                                }
                            ]
                        }
                    ],
                    "imports": [
                        "@proven-network/handler"
                    ]
                }
            ],
            "dependencies": {},
            "metadata": {
                "createdAt": "2025-07-06T12:31:55.308Z",
                "mode": "development",
                "pluginVersion": "0.0.1"
            }
        }"#;

        let manifest: proven_code_package::BundleManifest =
            serde_json::from_str(manifest_json).unwrap();

        // Create RuntimeOptions from manifest
        let options = RuntimeOptions::for_test_manifest(&manifest).await;

        run_in_thread(|| {
            let mut runtime = Runtime::new(options).unwrap();

            // Test creating a todo
            let create_request = ExecutionRequest::for_identified_session_rpc_test(
                "file:///src/todo-handlers.ts#createTodo",
                vec![serde_json::json!({"title": "Test Todo", "description": "A test todo item"})],
            );

            match runtime.execute(create_request) {
                Ok(ExecutionResult::Ok { output, .. }) => {
                    // Verify the todo was created successfully
                    assert!(output.is_object(), "Should return a todo object");
                    let todo = output.as_object().unwrap();
                    assert!(todo.contains_key("id"), "Should have an id");
                    assert!(todo.contains_key("title"), "Should have a title");
                    assert_eq!(todo.get("title").unwrap(), "Test Todo");
                    assert_eq!(todo.get("description").unwrap(), "A test todo item");
                    assert_eq!(todo.get("completed").unwrap(), false);
                }
                Ok(ExecutionResult::Error { error, .. }) => {
                    panic!("Unexpected js error: {error:?}");
                }
                Err(error) => {
                    panic!("Unexpected execution error: {error:?}");
                }
            }

            // Test getting todos
            let get_request = ExecutionRequest::for_identified_session_rpc_test(
                "file:///src/todo-handlers.ts#getTodos",
                vec![serde_json::json!({})],
            );

            match runtime.execute(get_request) {
                Ok(ExecutionResult::Ok { output, .. }) => {
                    // Verify we get the todo we created
                    assert!(output.is_array(), "Should return an array of todos");
                    let todos = output.as_array().unwrap();
                    assert_eq!(todos.len(), 1, "Should have 1 todo");
                    let todo = &todos[0];
                    assert_eq!(todo.get("title").unwrap(), "Test Todo");
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
    #[allow(clippy::too_many_lines)]
    async fn test_debug_manifest_specifier_issue() {
        use proven_code_package::CodePackage;
        // Debug test to investigate the SpecifierNotFoundInCodePackage error
        // Using the exact manifest from the user's error

        let manifest_json = r#"{
            "id": "manifest-31f0c8950596982b",
            "version": "1.0.0",
            "modules": [
                {
                    "specifier": "file:///src/auth-handlers.ts",
                    "content": "import { run } from '@proven-network/handler';\nimport { getCurrentIdentity } from '@proven-network/session';\n\n/**\n * Get current user information\n */\nexport const getCurrentUser = run(() => {\n  console.log('Getting current user identity');\n\n  const identity = getCurrentIdentity();\n\n  if (!identity) {\n    console.log('No user currently authenticated');\n    return { authenticated: false, user: null };\n  }\n\n  console.log('User is authenticated:', identity);\n  return {\n    authenticated: true,\n    user: {\n      id: identity || 'unknown',\n      name: `User ${identity}`,\n    },\n  };\n});\n\n/**\n * Check if user is authenticated\n */\nexport const isAuthenticated = run((): boolean => {\n  const identity = getCurrentIdentity();\n  const isAuth = !!identity;\n\n  console.log('Authentication check:', isAuth);\n  return isAuth;\n});\n\n/**\n * Get user's todo permissions (example of authorization)\n */\nexport const getUserPermissions = run(() => {\n  const identity = getCurrentIdentity();\n\n  if (!identity) {\n    console.log('No identity, returning guest permissions');\n    return {\n      canRead: false,\n      canWrite: false,\n      canDelete: false,\n    };\n  }\n\n  // For this example, authenticated users have full permissions\n  const permissions = {\n    canRead: true,\n    canWrite: true,\n    canDelete: true,\n  };\n\n  console.log('User permissions:', permissions);\n  return permissions;\n});",
                    "handlers": [
                        {
                            "name": "getCurrentUser",
                            "type": "rpc",
                            "parameters": []
                        },
                        {
                            "name": "isAuthenticated",
                            "type": "rpc",
                            "parameters": []
                        },
                        {
                            "name": "getUserPermissions",
                            "type": "rpc",
                            "parameters": []
                        }
                    ],
                    "imports": [
                        "@proven-network/handler",
                        "@proven-network/session"
                    ]
                },
                {
                    "specifier": "file:///src/todo-handlers.ts",
                    "content": "import { run } from '@proven-network/handler';\nimport { Todo, CreateTodoRequest, UpdateTodoRequest, TodoFilter } from './types';\n\n// In-memory storage for this example (in a real app, this would be persistent storage)\nlet todos: Todo[] = [];\nlet nextId = 1;\n\n// Non-handler utility function that can be imported directly\nexport const formatTodoId = (id: number): string => `todo-${id}`;\n\n// Non-handler constant that can be imported directly\nexport const MAX_TODOS = 100;\n\n/**\n * Create a new todo item\n */\nexport const createTodo = run((request: CreateTodoRequest): Todo => {\n  console.log('Creating new todo:', request);\n\n  const now = new Date();\n  const todo: Todo = {\n    id: formatTodoId(nextId++),\n    title: request.title,\n    description: request.description,\n    completed: false,\n    createdAt: now,\n    updatedAt: now,\n  };\n\n  todos.push(todo);\n  console.log(`Created todo \"${todo.title}\" with ID: ${todo.id}`);\n\n  return todo;\n});\n\n/**\n * Get all todos with optional filtering\n */\nexport const getTodos = run((filter?: TodoFilter): Todo[] => {\n  console.log('Fetching todos with filter:', filter);\n\n  let filteredTodos = [...todos];\n\n  if (filter?.completed !== undefined) {\n    filteredTodos = filteredTodos.filter((todo) => todo.completed === filter.completed);\n  }\n\n  if (filter?.search) {\n    const searchLower = filter.search.toLowerCase();\n    filteredTodos = filteredTodos.filter(\n      (todo) =>\n        todo.title.toLowerCase().includes(searchLower) ||\n        todo.description?.toLowerCase().includes(searchLower)\n    );\n  }\n\n  console.log(`Returning ${filteredTodos.length} todos`);\n  return filteredTodos;\n});\n\n/**\n * Update an existing todo\n */\nexport const updateTodo = run((request: UpdateTodoRequest): Todo => {\n  console.log('Updating todo:', request);\n\n  const todoIndex = todos.findIndex((todo) => todo.id === request.id);\n  if (todoIndex === -1) {\n    throw new Error(`Todo with ID ${request.id} not found`);\n  }\n\n  const todo = todos[todoIndex];\n  const updatedTodo: Todo = {\n    ...todo,\n    ...request,\n    updatedAt: new Date(),\n  };\n\n  todos[todoIndex] = updatedTodo;\n  console.log(`Updated todo \"${updatedTodo.title}\"`);\n\n  return updatedTodo;\n});\n\n/**\n * Delete a todo by ID\n */\nexport const deleteTodo = run((todoId: string): boolean => {\n  console.log('Deleting todo:', todoId);\n\n  const initialLength = todos.length;\n  todos = todos.filter((todo) => todo.id !== todoId);\n\n  const deleted = todos.length < initialLength;\n  if (deleted) {\n    console.log(`Deleted todo with ID: ${todoId}`);\n  } else {\n    console.log(`Todo with ID ${todoId} not found`);\n  }\n\n  return deleted;\n});\n\n/**\n * Mark all todos as completed or uncompleted\n */\nexport const toggleAllTodos = run((completed: boolean): Todo[] => {\n  console.log(`Marking all todos as ${completed ? 'completed' : 'uncompleted'}`);\n\n  const now = new Date();\n  todos = todos.map((todo) => ({\n    ...todo,\n    completed,\n    updatedAt: now,\n  }));\n\n  console.log(`Updated ${todos.length} todos`);\n  return todos;\n});\n\n/**\n * Get todo statistics\n */\nexport const getTodoStats = run(() => {\n  const total = todos.length;\n  const completed = todos.filter((todo) => todo.completed).length;\n  const pending = total - completed;\n\n  const stats = {\n    total,\n    completed,\n    pending,\n    completionRate: total > 0 ? Math.round((completed / total) * 100) : 0,\n  };\n\n  console.log('Todo statistics:', stats);\n  return stats;\n});",
                    "handlers": [
                        {
                            "name": "createTodo",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "request",
                                    "type": "CreateTodoRequest",
                                    "optional": false
                                }
                            ]
                        },
                        {
                            "name": "getTodos",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "filter",
                                    "type": "TodoFilter",
                                    "optional": false
                                }
                            ]
                        },
                        {
                            "name": "updateTodo",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "request",
                                    "type": "UpdateTodoRequest",
                                    "optional": false
                                }
                            ]
                        },
                        {
                            "name": "deleteTodo",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "todoId",
                                    "type": "string",
                                    "optional": false
                                }
                            ]
                        },
                        {
                            "name": "toggleAllTodos",
                            "type": "rpc",
                            "parameters": [
                                {
                                    "name": "completed",
                                    "type": "boolean",
                                    "optional": false
                                }
                            ]
                        },
                        {
                            "name": "getTodoStats",
                            "type": "rpc",
                            "parameters": []
                        }
                    ],
                    "imports": [
                        "@proven-network/handler",
                        "file:///src/types.ts"
                    ]
                },
                {
                    "specifier": "file:///src/types.ts",
                    "content": "// Type definitions for the todo app\n\nexport interface Todo {\n  id: string;\n  title: string;\n  description?: string;\n  completed: boolean;\n  createdAt: Date;\n  updatedAt: Date;\n}\n\nexport interface CreateTodoRequest {\n  title: string;\n  description?: string;\n}\n\nexport interface UpdateTodoRequest {\n  id: string;\n  title?: string;\n  description?: string;\n  completed?: boolean;\n}\n\nexport interface TodoFilter {\n  completed?: boolean;\n  search?: string;\n}",
                    "handlers": [],
                    "imports": []
                }
            ],
            "dependencies": {},
            "metadata": {
                "createdAt": "2025-07-06T12:31:55.308Z",
                "mode": "development",
                "pluginVersion": "0.0.1"
            }
        }"#;

        let manifest: proven_code_package::BundleManifest =
            serde_json::from_str(manifest_json).unwrap();

        // Create CodePackage from manifest
        let code_package = CodePackage::from_manifest(&manifest).await.unwrap();

        // Debug: Check what specifiers are available
        println!("Available specifiers in CodePackage:");
        for spec in code_package.specifiers() {
            println!("  - {spec}");
        }

        // Debug: Check valid entrypoints
        println!("Valid entrypoints:");
        for entrypoint in code_package.valid_entrypoints() {
            println!("  - {entrypoint:?}");
        }

        // The problematic specifier from the error
        let problem_specifier = ModuleSpecifier::parse("file:///src/todo-handlers.ts").unwrap();
        println!("Looking for specifier: {problem_specifier:?}");

        // Check if the specifier exists in the CodePackage
        let source = code_package.get_module_source(&problem_specifier);
        println!("Module source found: {}", source.is_some());

        // Check if it's a valid entrypoint
        let is_entrypoint = code_package
            .valid_entrypoints()
            .contains(&problem_specifier);
        println!("Is valid entrypoint: {is_entrypoint}");

        // Create module loader and check
        let module_loader = ModuleLoader::new(code_package);
        let module = module_loader.get_module(&problem_specifier, ProcessingMode::Runtime);
        println!("Module loader can load module: {}", module.is_some());

        // Check the processed module source for Options mode
        let options_source =
            module_loader.get_module_source(&problem_specifier, ProcessingMode::Options);
        if let Some(ref source) = options_source {
            println!("Options mode source (first 1000 chars):");
            println!("{}", &source[..source.len().min(1000)]);

            // Look for createTodo specifically
            if let Some(start) = source.find("export const createTodo") {
                let end = source[start..].find('\n').unwrap_or(100) + start;
                println!("createTodo line: {}", &source[start..end]);
            }
        }

        // Test the specific handler from the error
        let handler_specifier =
            HandlerSpecifier::parse("file:///src/todo-handlers.ts#getTodoStats").unwrap();
        println!("Handler specifier: {}", handler_specifier.as_str());
        println!(
            "Module specifier from handler: {}",
            handler_specifier.module_specifier()
        );

        // Test module options parsing specifically
        let module_options_result = module_loader.get_module_options(&problem_specifier);
        println!("Module options result: {:?}", module_options_result.is_ok());
        if let Err(ref err) = module_options_result {
            println!("Module options error: {err:?}");
        }
        if let Ok(ref opts) = module_options_result {
            println!("Number of handlers found: {}", opts.handler_options.len());
            for key in opts.handler_options.keys() {
                println!("  Handler: {key}");
            }

            // Check if the specific handler exists
            let handler_exists = opts
                .handler_options
                .contains_key("file:///src/todo-handlers.ts#getTodoStats");
            println!("getTodoStats handler exists: {handler_exists}");
        }

        // The main issue was that JSDoc comments before export statements weren't being
        // handled correctly by the rewrite_run_functions regex. This has been fixed.
        assert!(
            is_entrypoint,
            "The todo-handlers module should be a valid entrypoint"
        );
        assert!(
            module_options_result.is_ok(),
            "Module options should parse successfully after the fix"
        );
    }
}
