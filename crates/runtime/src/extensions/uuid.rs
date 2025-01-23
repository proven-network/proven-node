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
    use crate::{ExecutionRequest, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_uuid() {
        let runtime_options = RuntimeOptions::for_test_code("uuid/test_uuid", "test");
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
        assert_eq!(execution_result.output.as_str().unwrap().len(), 36);
    }
}
