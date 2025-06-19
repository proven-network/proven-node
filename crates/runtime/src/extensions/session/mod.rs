#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2};
use uuid::Uuid;

pub enum IdentityState {
    NoIdentity,
    Identity(Uuid),
}

#[op2]
#[string]
pub fn op_get_current_identity(#[state] state: &IdentityState) -> Option<String> {
    match state {
        IdentityState::NoIdentity => None,
        IdentityState::Identity(identity_id) => Some(identity_id.to_string()),
    }
}

extension!(
    session_ext,
    ops = [op_get_current_identity],
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

        let request =
            ExecutionRequest::for_identified_session_rpc_test("file:///main.ts#test", vec![]);

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_string());
                // Test uses Uuid::max() for the identity ID
                assert_eq!(
                    output.as_str().unwrap(),
                    "ffffffff-ffff-ffff-ffff-ffffffffffff"
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
    async fn test_session_identity_no_context() {
        let runtime_options = RuntimeOptions::for_test_code("session/test_session_identity");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request =
            ExecutionRequest::for_anonymous_session_rpc_test("file:///main.ts#test", vec![]);

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
