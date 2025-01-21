use deno_core::extension;

extension!(
  zod_ext,
  esm_entry_point = "proven:zod",
  esm = [dir "vendor/zod",
      "proven:zod" = "index.mjs"
  ],
);

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_zod() {
        let runtime_options = create_runtime_options("zod/test_zod", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: Some("test_identity".to_string()),
        };

        let result = worker.execute(request).await;

        assert!(result.is_ok());

        let execution_result = result.unwrap();

        assert!(execution_result.output.is_object());
        assert_eq!(execution_result.output.as_object().unwrap().len(), 2);
        assert_eq!(
            execution_result
                .output
                .as_object()
                .unwrap()
                .get("name")
                .unwrap(),
            "Alice"
        );
        assert_eq!(
            execution_result
                .output
                .as_object()
                .unwrap()
                .get("age")
                .unwrap(),
            30
        );
    }
}
