#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use crate::options::{
    HandlerOptions, HttpHandlerOptions, ModuleHandlerOptions, RadixEventHandlerOptions,
    RpcHandlerOptions,
};

use deno_core::{extension, op2};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandlerOutput {
    pub output: Option<serde_json::Value>,

    pub uint8_array_json_paths: Vec<String>,
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
        "radix_event" => HandlerOptions::RadixEvent(RadixEventHandlerOptions::default()),
        "rpc" => HandlerOptions::Rpc(RpcHandlerOptions::default()),
        _ => unreachable!(),
    });

    match options {
        HandlerOptions::Http(options) => options
            .allowed_web_origins
            .insert(origin.to_ascii_lowercase()),
        HandlerOptions::RadixEvent(options) => options
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
        "radix_event" => HandlerOptions::RadixEvent(RadixEventHandlerOptions::default()),
        "rpc" => HandlerOptions::Rpc(RpcHandlerOptions::default()),
        _ => unreachable!(),
    });

    match options {
        HandlerOptions::Http(options) => options.max_heap_mbs.replace(value),
        HandlerOptions::RadixEvent(options) => options.max_heap_mbs.replace(value),
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
        _ => unreachable!(),
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
        "radix_event" => HandlerOptions::RadixEvent(RadixEventHandlerOptions::default()),
        "rpc" => HandlerOptions::Rpc(RpcHandlerOptions::default()),
        _ => unreachable!(),
    });

    match options {
        HandlerOptions::Http(options) => options.timeout_millis.replace(value),
        HandlerOptions::RadixEvent(options) => options.timeout_millis.replace(value),
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

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    use bytes::Bytes;
    use serde::Deserialize;

    #[tokio::test]
    async fn test_fetch_with_allowed_origins() {
        let runtime_options =
            create_runtime_options("handler/test_fetch_with_allowed_origins", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_fetch_with_disallowed_origins() {
        let runtime_options =
            create_runtime_options("handler/test_fetch_with_disallowed_origins", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_handler() {
        let runtime_options = create_runtime_options("handler/test_http_handler", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Http {
            body: Some(Bytes::from_static(b"Hello, world!")),
            dapp_definition_address: "dapp_definition_address".to_string(),
            method: http::Method::GET,
            path: "/test/420".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();

        assert_eq!(execution_result.output, "420");
    }

    #[tokio::test]
    async fn test_return_bytes() {
        let runtime_options = create_runtime_options("handler/test_return_bytes", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();
        let output = execution_result.deserialize_output::<Bytes>().unwrap();

        assert_eq!(output, Bytes::from_static(b"Hello, world!"));

        // Should have correct JSONPath
        assert_eq!(execution_result.paths_to_uint8_arrays, vec!["$"]);
    }

    #[tokio::test]
    async fn test_return_nested_bytes() {
        #[derive(Deserialize)]
        struct Nested {
            nested: Bytes,
        }

        let runtime_options = create_runtime_options("handler/test_return_bytes", "testNested");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();

        let output = execution_result.deserialize_output::<Nested>().unwrap();

        assert_eq!(output.nested, Bytes::from_static(b"Hello, world!"));

        // Should have correct JSONPath
        assert_eq!(execution_result.paths_to_uint8_arrays, vec!["$.nested"]);
    }
}
