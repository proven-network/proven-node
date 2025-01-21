use deno_core::extension;

extension!(
  uuid_ext,
  esm_entry_point = "proven:uuid",
  esm = [dir "vendor/uuid",
      "proven:uuid" = "index.mjs"
  ],
);

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_uuid() {
        let runtime_options = create_runtime_options("uuid/test_uuid", "test");
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

        assert!(execution_result.output.is_string());
        assert_eq!(execution_result.output.as_str().unwrap().len(), 36);
    }
}
