use std::collections::HashMap;

use deno_core::{extension, op2};

#[derive(Clone, Debug, Default)]
pub struct HandlerOptions {
    pub max_heap_mbs: Option<u16>,
    pub timeout_millis: Option<u32>,
}
pub type HandlerOptionsMap = HashMap<String, HandlerOptions>;

#[op2(fast)]
pub fn op_set_memory_option(
    #[state] state: &mut HandlerOptionsMap,
    #[string] handler_name: String,
    value: u16,
) {
    let options = state.entry(handler_name).or_default();
    options.max_heap_mbs.replace(value);
}

#[op2(fast)]
pub fn op_set_timeout_option(
    #[state] state: &mut HandlerOptionsMap,
    #[string] handler_name: String,
    value: u32,
) {
    println!("Setting timeout for {} to {}", handler_name, value);
    let options = state.entry(handler_name).or_default();
    options.timeout_millis.replace(value);
}

extension!(
    run_ext,
    ops = [op_set_memory_option, op_set_timeout_option],
    esm_entry_point = "proven:run",
    esm = [ dir "src/extensions/run", "proven:run" = "run.js" ],
    docs = "Functions for defining how exports should be run"
);

extension!(
    run_mock_ext,
    esm_entry_point = "proven:run",
    esm = [ dir "src/extensions/run", "proven:run" = "run-mock.js" ],
    docs = "Functions for defining how exports should be run"
);
