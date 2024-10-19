use crate::extensions::{console_ext, ConsoleState};
use crate::extensions::{sessions_ext, SessionsState};
use crate::{Error, ExecutionRequest, ExecutionResult};

use std::collections::HashSet;
use std::time::Duration;

use rustyscript::{js_value::Value, Module, RuntimeOptions as RustyScriptOptions};
use tokio::time::Instant;

#[derive(Clone)]
pub struct RuntimeOptions {
    pub max_heap_size: Option<usize>,
    pub module: String,
    pub timeout: Duration,
}

pub struct Runtime {
    module_handle: rustyscript::ModuleHandle,
    runtime: rustyscript::Runtime,
}

impl Runtime {
    pub fn new(options: RuntimeOptions) -> Result<Self, Error> {
        let mut schema_whlist = HashSet::with_capacity(1);
        schema_whlist.insert("proven:".to_string());
        let mut runtime = rustyscript::Runtime::new(RustyScriptOptions {
            timeout: options.timeout,
            max_heap_size: options.max_heap_size,
            schema_whlist,
            extensions: vec![
                console_ext::init_ops_and_esm(),
                sessions_ext::init_ops_and_esm(),
            ],
            ..Default::default()
        })?;

        let module = Module::new("module.ts", options.module.as_str());
        let module_handle = runtime.load_module(&module)?;

        Ok(Self {
            module_handle,
            runtime,
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
