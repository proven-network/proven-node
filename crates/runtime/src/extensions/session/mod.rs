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

        let request = ExecutionRequest::HttpWithUserContext {
            accounts: vec![],
            body: None,
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
            method: http::Method::GET,
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_string());
        assert_eq!(execution_result.output.as_str().unwrap(), "my_identity");
        assert!(execution_result.duration.as_millis() < 1000);
    }

    #[tokio::test]
    async fn test_session_identity_no_context() {
        let runtime_options = create_runtime_options("session/test_session_identity", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Http {
            body: None,
            dapp_definition_address: "dapp_definition_address".to_string(),
            method: http::Method::GET,
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_null());
    }

    #[tokio::test]
    async fn test_session_accounts() {
        let runtime_options = create_runtime_options("session/test_session_accounts", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::HttpWithUserContext {
            accounts: vec!["my_account_1".to_string(), "my_account_2".to_string()],
            body: None,
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
            method: http::Method::GET,
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_array());
        assert_eq!(execution_result.output.as_array().unwrap().len(), 2);
        assert_eq!(
            execution_result.output.as_array().unwrap()[0]
                .as_str()
                .unwrap(),
            "my_account_1"
        );
        assert_eq!(
            execution_result.output.as_array().unwrap()[1]
                .as_str()
                .unwrap(),
            "my_account_2"
        );
    }

    #[tokio::test]
    async fn test_session_accounts_no_accounts() {
        let runtime_options = create_runtime_options("session/test_session_accounts", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Http {
            body: None,
            dapp_definition_address: "dapp_definition_address".to_string(),
            method: http::Method::GET,
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_array());
        assert_eq!(execution_result.output.as_array().unwrap().len(), 0);
    }
}
