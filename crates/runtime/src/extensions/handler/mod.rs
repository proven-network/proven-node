#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use crate::options::{HandlerOptions, HttpHandlerOptions, ModuleHandlerOptions, RpcHandlerOptions};

use deno_core::{extension, op2};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct HandlerOutput {
    pub output: Option<serde_json::Value>,
    pub paths_to_uint8_arrays: Vec<String>,
}

#[op2(fast)]
pub fn op_add_allowed_origin(
    #[state] state: &mut ModuleHandlerOptions,
    #[string] handler_type: &str,
    #[string] handler_name: String,
    #[string] origin: &str,
) {
    let options = state.entry(handler_name).or_insert(match handler_type {
        "http" => HandlerOptions::Http(HttpHandlerOptions::default()),
        "rpc" => HandlerOptions::Rpc(RpcHandlerOptions::default()),
        _ => unreachable!(),
    });

    match options {
        HandlerOptions::Http(options) => options
            .allowed_web_origins
            .insert(origin.to_ascii_lowercase()),
        HandlerOptions::Rpc(options) => options
            .allowed_web_origins
            .insert(origin.to_ascii_lowercase()),
    };
}

#[op2(fast)]
pub fn op_set_memory_option(
    #[state] state: &mut ModuleHandlerOptions,
    #[string] handler_type: &str,
    #[string] handler_name: String,
    value: u16,
) {
    let value = std::cmp::max(value, 32); // 32 MB is the minimum heap size

    let options = state.entry(handler_name).or_insert(match handler_type {
        "http" => HandlerOptions::Http(HttpHandlerOptions::default()),
        "rpc" => HandlerOptions::Rpc(RpcHandlerOptions::default()),
        _ => unreachable!(),
    });

    match options {
        HandlerOptions::Http(options) => options.max_heap_mbs.replace(value),
        HandlerOptions::Rpc(options) => options.max_heap_mbs.replace(value),
    };
}

#[op2(fast)]
pub fn op_set_path_option(
    #[state] state: &mut ModuleHandlerOptions,
    #[string] handler_type: &str,
    #[string] handler_name: String,
    #[string] value: String,
) {
    assert_eq!(handler_type, "http");

    let options = state
        .entry(handler_name)
        .or_insert(HandlerOptions::Http(HttpHandlerOptions::default()));

    match options {
        HandlerOptions::Http(options) => options.path.replace(value),
        HandlerOptions::Rpc(_) => unreachable!(),
    };
}

#[op2(fast)]
pub fn op_set_timeout_option(
    #[state] state: &mut ModuleHandlerOptions,
    #[string] handler_type: &str,
    #[string] handler_name: String,
    value: u32,
) {
    let options = state.entry(handler_name).or_insert(match handler_type {
        "http" => HandlerOptions::Http(HttpHandlerOptions::default()),
        "rpc" => HandlerOptions::Rpc(RpcHandlerOptions::default()),
        _ => unreachable!(),
    });

    match options {
        HandlerOptions::Http(options) => options.timeout_millis.replace(value),
        HandlerOptions::Rpc(options) => options.timeout_millis.replace(value),
    };
}

extension!(
    handler_options_parser_ext,
    ops = [
        op_add_allowed_origin,
        op_set_memory_option,
        op_set_path_option,
        op_set_timeout_option
    ],
    esm_entry_point = "proven:handler",
    esm = [ dir "src/extensions/handler", "proven:handler" = "handler-options-parser.js" ],
    docs = "Functions for defining how exports should be run"
);

extension!(
    handler_runtime_ext,
    esm_entry_point = "proven:handler",
    esm = [ dir "src/extensions/handler", "proven:handler" = "handler-runtime.js" ],
    docs = "Functions for defining how exports should be run"
);
