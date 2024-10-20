use crate::extensions::*;
use crate::{Error, ExecutionRequest, ExecutionResult};

use std::collections::HashSet;
use std::time::Duration;

use proven_store::{Store1, Store2};
use rustyscript::{js_value::Value, Module, RuntimeOptions as RustyScriptOptions};
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
