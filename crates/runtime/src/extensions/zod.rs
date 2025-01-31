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
    use crate::{ExecutionRequest, ExecutionResult, HandlerSpecifier, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_zod() {
        let runtime_options = RuntimeOptions::for_test_code("zod/test_zod");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_object());
                assert_eq!(output.as_object().unwrap().len(), 2);
                assert_eq!(output.as_object().unwrap().get("name").unwrap(), "Alice");
                assert_eq!(output.as_object().unwrap().get("age").unwrap(), 30);
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
