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
    use crate::test_utils::create_test_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_radix_engine_toolkit() {
        let runtime_options =
            create_test_runtime_options("radix_engine_toolkit/test_radix_engine_toolkit", "test");
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

        assert!(execution_result.output.is_string());
        assert_eq!(
            execution_result.output,
            r#"{"version":"2.1.0-dev1","scryptoDependency":{"kind":"Version","value":"1.2.0"}}"#
        );
    }
}
