#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2};

pub enum SessionState {
    NoSession,
    Session {
        accounts: Vec<String>,
        identity: String,
    },
}

#[op2]
#[string]
pub fn op_get_current_identity(#[state] state: &SessionState) -> Option<String> {
    match state {
        SessionState::NoSession => None,
        SessionState::Session { identity, .. } => Some(identity.clone()),
    }
}

#[op2]
#[serde]
pub fn op_get_current_accounts(#[state] state: &SessionState) -> Option<Vec<String>> {
    match state {
        SessionState::NoSession => None,
        SessionState::Session { accounts, .. } => Some(accounts.clone()),
    }
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
    use crate::{ExecutionRequest, ExecutionResult, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_session_identity() {
        let runtime_options = RuntimeOptions::for_test_code("session/test_session_identity");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_string());
                assert_eq!(output.as_str().unwrap(), "my_identity");
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_session_identity_no_context() {
        let runtime_options = RuntimeOptions::for_test_code("session/test_session_identity");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_test("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_null());
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_session_accounts() {
        let runtime_options = RuntimeOptions::for_test_code("session/test_session_accounts");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_array());
                assert_eq!(output.as_array().unwrap().len(), 2);
                assert_eq!(
                    output.as_array().unwrap()[0].as_str().unwrap(),
                    "my_account_1"
                );
                assert_eq!(
                    output.as_array().unwrap()[1].as_str().unwrap(),
                    "my_account_2"
                );
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_session_accounts_no_accounts() {
        let runtime_options = RuntimeOptions::for_test_code("session/test_session_accounts");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request =
            ExecutionRequest::for_rpc_with_session_test_no_accounts("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_array());
                assert_eq!(output.as_array().unwrap().len(), 0);
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_session_accounts_no_context() {
        let runtime_options = RuntimeOptions::for_test_code("session/test_session_accounts");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_test("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_null());
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }
}
