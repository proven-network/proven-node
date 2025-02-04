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
    use crate::{ExecutionRequest, ExecutionResult, HandlerSpecifier, RuntimeOptions, Worker};

    use proven_sessions::{Identity, RadixIdentityDetails};

    #[tokio::test]
    async fn test_uuid() {
        let runtime_options = RuntimeOptions::for_test_code("uuid/test_uuid");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            application_id: "application_id".to_string(),
            args: vec![],
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identities: vec![Identity::Radix(RadixIdentityDetails {
                account_addresses: vec!["my_account".to_string()],
                dapp_definition_address: "dapp_definition_address".to_string(),
                expected_origin: "origin".to_string(),
                identity_address: "my_identity".to_string(),
            })],
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_string());
                assert_eq!(output.as_str().unwrap().len(), 36);
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
