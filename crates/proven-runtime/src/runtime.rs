use crate::extensions::*;
use crate::{Error, ExecutionRequest, ExecutionResult};

use std::collections::HashSet;
use std::time::Duration;

use proven_store::{Store1, Store2};
use rustyscript::js_value::Value;
use rustyscript::{Module, RuntimeOptions as RustyScriptOptions};
use tokio::time::Instant;

#[derive(Clone)]
pub struct RuntimeOptions {
    pub max_heap_mbs: u16,
    pub module: String,
    pub timeout_millis: u32,
}

pub struct Runtime<AS: Store1, PS: Store2, NS: Store2> {
    module_handle: rustyscript::ModuleHandle,
    runtime: rustyscript::Runtime,
    application_store: AS,
    personal_store: PS,
    nft_store: NS,
}

/// Executes ESM modules in a single-threaded environment. Cannot use in tokio without spawning in dedicated thread.
///
/// # Type Parameters
/// - `AS`: Application Store type implementing `Store1`.
/// - `PS`: Personal Store type implementing `Store2`.
/// - `NS`: NFT Store type implementing `Store2`.
///
/// # Methods
/// - `new`: Creates a new runtime with the given runtime options and stores.
/// - `execute`: Sends an execution request to the runtime and awaits the result.
///
/// # Example
/// ```rust
/// use proven_runtime::{
///     Context, Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions,
/// };
/// use proven_store_memory::MemoryStore;
/// use serde_json::json;
///
/// let runtime_options = RuntimeOptions {
///     max_heap_mbs: 10,
///     module: "export const test = (a, b) => a + b;".to_string(),
///     timeout_millis: 1000,
/// };
/// let application_store = MemoryStore::new();
/// let personal_store = MemoryStore::new();
/// let nft_store = MemoryStore::new();
/// let request = ExecutionRequest {
///     context: Context {
///         dapp_definition_address: "dapp_definition_address".to_string(),
///         identity: None,
///         accounts: None,
///     },
///     handler_name: "test".to_string(),
///     args: vec![json!(10), json!(20)],
/// };
/// let mut runtime = Runtime::new(
///     runtime_options,
///     application_store,
///     personal_store,
///     nft_store,
/// )
/// .expect("Failed to create runtime");
/// let result = runtime.execute(request);
/// ```
impl<AS: Store1, PS: Store2, NS: Store2> Runtime<AS, PS, NS> {
    pub fn new(
        options: RuntimeOptions,
        application_store: AS,
        personal_store: PS,
        nft_store: NS,
    ) -> Result<Self, Error> {
        let mut schema_whlist = HashSet::with_capacity(1);
        schema_whlist.insert("proven:".to_string());
        let mut runtime = rustyscript::Runtime::new(RustyScriptOptions {
            timeout: Duration::from_millis(options.timeout_millis as u64),
            max_heap_size: Some(options.max_heap_mbs as usize * 1024 * 1024),
            schema_whlist,
            extensions: vec![
                console_ext::init_ops_and_esm(),
                sessions_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                storage_application_ext::init_ops::<AS::Scoped>(),
                storage_personal_ext::init_ops::<<<PS as Store2>::Scoped as Store1>::Scoped>(),
                storage_nft_ext::init_ops_and_esm::<NS::Scoped>(),
                storage_ext::init_ops_and_esm(),
            ],
            ..Default::default()
        })?;

        // In case there are any to-level console.* calls in the module
        runtime.put(ConsoleState::default())?;

        let module = Module::new("module.ts", options.module.as_str());
        let module_handle = runtime.load_module(&module)?;

        Ok(Self {
            module_handle,
            runtime,
            application_store,
            personal_store,
            nft_store,
        })
    }

    pub fn execute(
        &mut self,
        ExecutionRequest {
            context,
            handler_name,
            args,
        }: ExecutionRequest,
    ) -> Result<ExecutionResult, Error> {
        let start = Instant::now();

        // Reset the console state before each execution
        self.runtime.put(ConsoleState::default())?;

        // Set the store for the storage extension
        let personal_store = match context.identity.as_ref() {
            Some(current_identity) => Some(
                self.personal_store
                    .clone()
                    .scope(context.dapp_definition_address.clone())
                    .scope(current_identity.clone()),
            ),
            None => None,
        };
        self.runtime.put(personal_store)?;

        self.runtime.put(
            self.application_store
                .clone()
                .scope(context.dapp_definition_address.clone()),
        )?;

        self.runtime.put(
            self.nft_store
                .clone()
                .scope(context.dapp_definition_address),
        )?;

        // Set the context for the session extension
        self.runtime.put(SessionsState {
            identity: context.identity,
            accounts: context.accounts,
        })?;

        let output: Value =
            self.runtime
                .call_function(Some(&self.module_handle), handler_name.as_str(), &args)?;
        let output: rustyscript::serde_json::Value = output.try_into(&mut self.runtime)?;

        let console_state: ConsoleState = self.runtime.take().unwrap_or_default();
        let duration = start.elapsed();

        Ok(ExecutionResult {
            output,
            duration,
            logs: console_state.messages,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;

    use proven_store_memory::MemoryStore;
    use serde_json::json;

    // spawn in std::thread to avoid rustyscript panic
    fn run_in_thread<F: FnOnce() + Send + 'static>(f: F) {
        std::thread::spawn(f).join().unwrap();
    }

    fn create_runtime_options(script: &str) -> RuntimeOptions {
        RuntimeOptions {
            max_heap_mbs: 10,
            module: script.to_string(),
            timeout_millis: 1000,
        }
    }

    fn create_execution_request() -> ExecutionRequest {
        ExecutionRequest {
            context: Context {
                dapp_definition_address: "dapp_definition_address".to_string(),
                identity: None,
                accounts: None,
            },
            handler_name: "test".to_string(),
            args: vec![json!(10), json!(20)],
        }
    }

    #[tokio::test]
    async fn test_runtime_new() {
        run_in_thread(|| {
            let options = create_runtime_options(
                "export const test = () => { console.log('Hello, world!'); }",
            );
            let application_store = MemoryStore::new();
            let personal_store = MemoryStore::new();
            let nft_store = MemoryStore::new();

            let runtime = Runtime::new(options, application_store, personal_store, nft_store);
            assert!(runtime.is_ok());
        });
    }

    #[tokio::test]
    async fn test_runtime_execute() {
        run_in_thread(|| {
            let options = create_runtime_options(
                "export const test = () => { console.log('Hello, world!'); }",
            );
            let application_store = MemoryStore::new();
            let personal_store = MemoryStore::new();
            let nft_store = MemoryStore::new();

            let mut runtime =
                Runtime::new(options, application_store, personal_store, nft_store).unwrap();
            let request = create_execution_request();

            let result = runtime.execute(request);
            assert!(result.is_ok());

            let execution_result = result.unwrap();
            assert!(execution_result.output.is_null());
            assert!(execution_result.duration.as_millis() < 1000);
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
            );
            let application_store = MemoryStore::new();
            let personal_store = MemoryStore::new();
            let nft_store = MemoryStore::new();

            let mut runtime =
                Runtime::new(options, application_store, personal_store, nft_store).unwrap();
            let mut request = create_execution_request();
            request.context.identity = Some("test_identity".to_string());

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
            );

            let application_store = MemoryStore::new();
            let personal_store = MemoryStore::new();
            let nft_store = MemoryStore::new();

            let mut runtime =
                Runtime::new(options, application_store, personal_store, nft_store).unwrap();
            let mut request = create_execution_request();
            request.context.accounts = Some(vec!["account1".to_string(), "account2".to_string()]);

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
}
