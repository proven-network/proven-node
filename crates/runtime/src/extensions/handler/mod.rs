#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use crate::options::{HandlerOptions, ModuleHandlerOptions};

use std::collections::HashSet;

use deno_core::{extension, op2};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandlerOutput {
    pub output: Option<serde_json::Value>,

    pub uint8_array_json_paths: Vec<String>,
}

fn create_new_handler_options(handler_type: &str) -> HandlerOptions {
    match handler_type {
        "http" => HandlerOptions::Http {
            allowed_web_origins: HashSet::new(),
            max_heap_mbs: None,
            path: None,
            timeout_millis: None,
        },
        "radix_event" => HandlerOptions::RadixEvent {
            allowed_web_origins: HashSet::new(),
            event_binding: None,
            max_heap_mbs: None,
            timeout_millis: None,
        },
        "rpc" => HandlerOptions::Rpc {
            allowed_web_origins: HashSet::new(),
            max_heap_mbs: None,
            timeout_millis: None,
        },
        _ => unreachable!(),
    }
}

#[op2(fast)]
pub fn op_add_allowed_origin(
    #[state] state: &mut ModuleHandlerOptions,
    #[string] handler_type: &str,
    #[string] handler_name: String,
    #[string] origin: &str,
) {
    let options = state
        .entry(handler_name)
        .or_insert_with(|| create_new_handler_options(handler_type));

    match options {
        HandlerOptions::Http {
            allowed_web_origins,
            ..
        }
        | HandlerOptions::RadixEvent {
            allowed_web_origins,
            ..
        }
        | HandlerOptions::Rpc {
            allowed_web_origins,
            ..
        } => allowed_web_origins.insert(origin.to_ascii_lowercase()),
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
        .or_insert_with(|| create_new_handler_options(handler_type));

    match options {
        HandlerOptions::Http { path, .. } => path.replace(value),
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
    let options = state
        .entry(handler_name)
        .or_insert_with(|| create_new_handler_options(handler_type));

    match options {
        HandlerOptions::Http { timeout_millis, .. }
        | HandlerOptions::RadixEvent { timeout_millis, .. }
        | HandlerOptions::Rpc { timeout_millis, .. } => timeout_millis.replace(value),
    };
}

extension!(
    handler_options_ext,
    ops = [
        op_add_allowed_origin,
        op_set_path_option,
        op_set_timeout_option
    ],
    esm_entry_point = "proven:handler",
    esm = [ dir "src/extensions/handler", "proven:handler" = "handler-options.js" ],
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
    use crate::{ExecutionRequest, ExecutionResult, HandlerSpecifier, RuntimeOptions, Worker};

    use bytes::Bytes;
    use serde::Deserialize;

    #[tokio::test]
    async fn test_fetch_with_allowed_origins() {
        let runtime_options =
            RuntimeOptions::for_test_code("handler/test_fetch_with_allowed_origins");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
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
            RuntimeOptions::for_test_code("handler/test_fetch_with_disallowed_origins");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Error { .. }) => {
                // Expected - origin has not been allowed
            }
            Ok(ExecutionResult::Ok { output, .. }) => {
                panic!("Expected error but got success: {output:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_http_handler() {
        let runtime_options = RuntimeOptions::for_test_code("handler/test_http_handler");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Http {
            body: Some(Bytes::from_static(b"Hello, world!")),
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            method: http::Method::GET,
            path: "/test/420".to_string(),
            query: None,
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_string());
                assert_eq!(output, "420");
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        };
    }

    #[tokio::test]
    async fn test_return_bytes() {
        let runtime_options = RuntimeOptions::for_test_code("handler/test_return_bytes");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok {
                output,
                paths_to_uint8_arrays,
                ..
            }) => {
                let deserialized_output: Bytes = serde_json::from_value(output).unwrap();

                assert_eq!(deserialized_output, Bytes::from_static(b"Hello, world!"));

                // Should have correct JSONPath
                assert_eq!(paths_to_uint8_arrays, vec!["$"]);
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        };
    }

    #[tokio::test]
    async fn test_return_nested_bytes() {
        #[derive(Deserialize)]
        struct Nested {
            nested: Bytes,
        }

        let runtime_options = RuntimeOptions::for_test_code("handler/test_return_bytes");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#testNested").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok {
                output,
                paths_to_uint8_arrays,
                ..
            }) => {
                let deserialized_output: Nested = serde_json::from_value(output).unwrap();

                assert_eq!(
                    deserialized_output.nested,
                    Bytes::from_static(b"Hello, world!")
                );

                // Should have correct JSONPath
                assert_eq!(paths_to_uint8_arrays, vec!["$.nested"]);
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        };
    }
}
