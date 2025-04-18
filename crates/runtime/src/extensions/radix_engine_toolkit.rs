use deno_core::extension;

extension!(
  radix_engine_toolkit_ext,
  esm_entry_point = "proven:radix_engine_toolkit",
  esm = [dir "vendor/@radixdlt/radix-engine-toolkit",
      "proven:radix_engine_toolkit" = "index.mjs"
  ],
);

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, ExecutionResult, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_radix_engine_toolkit() {
        let runtime_options =
            RuntimeOptions::for_test_code("radix_engine_toolkit/test_radix_engine_toolkit");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_string());
                assert_eq!(
                    output,
                    r#"{"version":"2.1.0-dev1","scryptoDependency":{"kind":"Version","value":"1.2.0"}}"#
                );
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
