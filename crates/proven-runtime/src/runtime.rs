use crate::extensions::*;
use crate::{Error, ExecutionRequest, ExecutionResult};

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use proven_store::{Store2, Store3};
use regex::Regex;
use rustyscript::js_value::Value;
use rustyscript::{Module, ModuleHandle};
use tokio::time::Instant;

static SCHEMA_WHLIST: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut set = HashSet::with_capacity(1);
    set.insert("proven:".to_string());
    set
});

#[derive(Clone)]
pub struct RuntimeOptions<AS: Store2, PS: Store3, NS: Store3> {
    pub application_store: AS,
    pub handler_name: Option<String>,
    pub module: String,
    pub nft_store: NS,
    pub personal_store: PS,
}

pub struct Runtime<AS: Store2, PS: Store3, NS: Store3> {
    application_store: AS,
    handler_name: Option<String>,
    module_handle: ModuleHandle,
    nft_store: NS,
    personal_store: PS,
    runtime: rustyscript::Runtime,
}

/// Executes ESM modules in a single-threaded environment. Cannot use in tokio without spawning in dedicated thread.
///
/// # Type Parameters
/// - `AS`: Application Store type implementing `Store2`.
/// - `PS`: Personal Store type implementing `Store3`.
/// - `NS`: NFT Store type implementing `Store3`.
///
/// # Example
/// ```rust
/// use proven_runtime::{Error, ExecutionRequest, ExecutionResult, Runtime, RuntimeOptions};
/// use proven_store_memory::MemoryStore;
/// use serde_json::json;
///
/// let mut runtime = Runtime::new(RuntimeOptions {
///     application_store: MemoryStore::new(),
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
impl<AS: Store2, PS: Store3, NS: Store3> Runtime<AS, PS, NS> {
    /// Creates a new runtime with the given runtime options and stores.
    ///
    /// # Parameters
    /// - `options`: The runtime options to use.
    /// - `application_store`: The application store to use.
    /// - `personal_store`: The personal store to use.
    /// - `nft_store`: The NFT store to use.
    ///
    /// # Returns
    /// The created runtime.
    pub fn new(options: RuntimeOptions<AS, PS, NS>) -> Result<Self, Error> {
        let handler_options =
            Self::get_options_for_handler(options.module.clone(), options.handler_name.clone())?;

        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            timeout: Duration::from_millis(handler_options.timeout_millis.unwrap_or(1000) as u64),
            max_heap_size: Some(handler_options.max_heap_mbs.unwrap_or(10) as usize * 1024 * 1024),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                run_mock_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                sessions_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                storage_application_ext::init_ops::<AS::Scoped>(),
                storage_personal_ext::init_ops::<<<PS as Store3>::Scoped as Store2>::Scoped>(),
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
            application_store: options.application_store,
            handler_name: options.handler_name,
            module_handle,
            nft_store: options.nft_store,
            personal_store: options.personal_store,
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

        // Set the store for the storage extension
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

        self.runtime
            .put(self.nft_store.clone().scope(dapp_definition_address))?;

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

        Ok(ExecutionResult {
            output,
            duration,
            logs: console_state.messages,
        })
    }

    fn get_options_for_handler(
        module: String,
        handler_name: Option<String>,
    ) -> Result<HandlerOptions, Error> {
        // Prepare the module for option extraction using run extenstion
        let module = Self::strip_comments(module.as_str());
        let module = Self::name_default_export(module.as_str());
        let module = Self::rewrite_run_functions(module.as_str());

        let module = Module::new("tmp.ts", module.as_str());
        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            timeout: Duration::from_millis(1000),
            max_heap_size: Some(10 * 1024 * 1024),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                run_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                sessions_ext::init_ops_and_esm(),
                // Split into seperate extensions to avoid issue with macro supporting only 1 generic
                storage_application_ext::init_ops::<AS::Scoped>(),
                storage_personal_ext::init_ops::<<<PS as Store3>::Scoped as Store2>::Scoped>(),
                storage_nft_ext::init_ops_and_esm::<NS::Scoped>(),
                storage_ext::init_ops_and_esm(),
            ],
            ..Default::default()
        })?;

        runtime.put(ConsoleState::default())?;
        runtime.put(HandlerOptionsMap::default())?;

        runtime.load_module(&module)?;

        let mut options: HandlerOptionsMap = runtime.take().unwrap();

        let options = options
            .entry(
                handler_name
                    .as_ref()
                    .unwrap_or(&"__default__".to_string())
                    .clone(),
            )
            .or_default();

        Ok(options.clone())
    }

    fn strip_comments(module: &str) -> String {
        let comment_re = Regex::new(r"(?m)//.*|/\*[\s\S]*?\*/").unwrap();
        comment_re.replace_all(module, "").to_string()
    }

    fn name_default_export(module: &str) -> String {
        module.replace("export default ", "export const __default__ = ")
    }

    fn rewrite_run_functions(input: &str) -> String {
        // Define the regex to match `export const` declarations with the specified functions
        let re = Regex::new(r"(?m)^\s*export\s+const\s+(\w+)\s*=\s*(runWithOptions|runOnSchedule|runOnRadixEvent|runOnProvenEvent)\(").unwrap();

        // Replace the matched string with the modified version
        let result = re.replace_all(input, |caps: &regex::Captures| {
            format!("export const {} = {}('{}', ", &caps[1], &caps[2], &caps[1])
        });

        result.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proven_store_memory::MemoryStore;
    use serde_json::json;

    // spawn in std::thread to avoid rustyscript panic
    fn run_in_thread<F: FnOnce() + Send + 'static>(f: F) {
        std::thread::spawn(f).join().unwrap();
    }

    fn create_runtime_options(
        script: &str,
        handler_name: Option<String>,
    ) -> RuntimeOptions<MemoryStore, MemoryStore, MemoryStore> {
        RuntimeOptions {
            application_store: MemoryStore::new(),
            handler_name,
            module: script.to_string(),
            nft_store: MemoryStore::new(),
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
}
