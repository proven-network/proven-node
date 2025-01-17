#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2};

#[derive(Default)]
pub struct SessionState {
    pub identity: Option<String>,
    pub accounts: Option<Vec<String>>,
}

#[op2]
#[string]
pub fn op_get_current_identity(#[state] state: &SessionState) -> Option<String> {
    state.identity.clone()
}

#[op2]
#[serde]
pub fn op_get_current_accounts(#[state] state: &SessionState) -> Vec<String> {
    state.accounts.clone().unwrap_or_default()
}

extension!(
    session_ext,
    ops = [op_get_current_identity, op_get_current_accounts],
    esm_entry_point = "proven:session",
    esm = [ dir "src/extensions/session", "proven:session" = "session.js" ],
    docs = "Functions for identity management"
);

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_session_identity() {
        let runtime_options = create_runtime_options("session/test_session_identity", "test");
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
        assert_eq!(execution_result.output.as_str().unwrap(), "test_identity");
        assert!(execution_result.duration.as_millis() < 1000);
    }

    #[tokio::test]
    async fn test_session_accounts() {
        let runtime_options = create_runtime_options("session/test_session_accounts", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        };

        let result = worker.execute(request).await;

        assert!(result.is_ok());

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_array());
        assert_eq!(execution_result.output.as_array().unwrap().len(), 2);
        assert_eq!(
            execution_result.output.as_array().unwrap()[0]
                .as_str()
                .unwrap(),
            "account1"
        );
        assert_eq!(
            execution_result.output.as_array().unwrap()[1]
                .as_str()
                .unwrap(),
            "account2"
        );
        assert!(execution_result.duration.as_millis() < 1000);
    }
}
