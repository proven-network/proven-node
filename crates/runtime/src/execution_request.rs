use crate::HandlerSpecifier;

use bytes::Bytes;
use http::Method;
use proven_identity::Session;
use serde_json::Value;

/// Request for a runtime execution.
#[derive(Clone)]
pub enum ExecutionRequest {
    /// A request received from an HTTP endpoint.
    Http {
        /// The application ID.
        application_id: String,

        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The HTTP method.
        method: Method,

        /// The path of the HTTP request.
        path: String,

        /// The query string of the HTTP request.
        query: Option<String>,
    },
    /// A request received from an HTTP endpoint with authenticated user context.
    HttpWithSession {
        /// The application ID.
        application_id: String,

        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The HTTP method.
        method: Method,

        /// The path of the HTTP request.
        path: String,

        /// The active session.
        session: Session,

        /// The query string of the HTTP request.
        query: Option<String>,
    },
    /// A request created to respond to an event from the Radix network.
    RadixEvent {
        /// The application ID.
        application_id: String,
        // TODO: should have Radix transaction data
        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over and RPC session.
    Rpc {
        /// The application ID.
        application_id: String,

        /// The arguments to the handler.
        args: Vec<Value>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The session of the authenticated user.
        session: Session,
    },
}

impl ExecutionRequest {
    // TODO: Use builder pattern for this stuff
    #[cfg(test)]
    pub(crate) fn for_rpc_test(handler_specifier: &str, args: Vec<Value>) -> Self {
        use ed25519_dalek::{SigningKey, VerifyingKey};

        let random_signing_key = SigningKey::generate(&mut rand::thread_rng());
        let random_verifying_key =
            VerifyingKey::from(&SigningKey::generate(&mut rand::thread_rng()));

        Self::Rpc {
            application_id: "application_id".to_string(),
            args,
            handler_specifier: HandlerSpecifier::parse(handler_specifier).unwrap(),
            session: Session::Anonymous {
                origin: "origin".to_string(),
                session_id: "session_id".to_string(),
                signing_key: random_signing_key,
                verifying_key: random_verifying_key,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn for_rpc_with_session_test(handler_specifier: &str, args: Vec<Value>) -> Self {
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use proven_identity::{Identity, LedgerIdentity, RadixIdentityDetails};

        let random_signing_key = SigningKey::generate(&mut rand::thread_rng());
        let random_verifying_key =
            VerifyingKey::from(&SigningKey::generate(&mut rand::thread_rng()));

        Self::Rpc {
            application_id: "application_id".to_string(),
            args,
            handler_specifier: HandlerSpecifier::parse(handler_specifier).unwrap(),
            session: Session::Identified {
                identity: Identity {
                    identity_id: "identity_id".to_string(),
                    ledger_identities: vec![LedgerIdentity::Radix(RadixIdentityDetails {
                        account_addresses: vec![
                            "my_account_1".to_string(),
                            "my_account_2".to_string(),
                        ],
                        dapp_definition_address: "dapp_definition_address".to_string(),
                        expected_origin: "origin".to_string(),
                        identity_address: "my_identity".to_string(),
                    })],
                    passkeys: vec![],
                },
                ledger_identity: LedgerIdentity::Radix(RadixIdentityDetails {
                    account_addresses: vec!["my_account_1".to_string(), "my_account_2".to_string()],
                    dapp_definition_address: "dapp_definition_address".to_string(),
                    expected_origin: "origin".to_string(),
                    identity_address: "my_identity".to_string(),
                }),
                origin: "origin".to_string(),
                session_id: "session_id".to_string(),
                signing_key: random_signing_key,
                verifying_key: random_verifying_key,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn for_rpc_with_session_test_no_accounts(
        handler_specifier: &str,
        args: Vec<Value>,
    ) -> Self {
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use proven_identity::{Identity, LedgerIdentity, RadixIdentityDetails};

        let random_signing_key = SigningKey::generate(&mut rand::thread_rng());
        let random_verifying_key =
            VerifyingKey::from(&SigningKey::generate(&mut rand::thread_rng()));

        Self::Rpc {
            application_id: "application_id".to_string(),
            args,
            handler_specifier: HandlerSpecifier::parse(handler_specifier).unwrap(),
            session: Session::Identified {
                identity: Identity {
                    identity_id: "identity_id".to_string(),
                    ledger_identities: vec![LedgerIdentity::Radix(RadixIdentityDetails {
                        account_addresses: vec![],
                        dapp_definition_address: "dapp_definition_address".to_string(),
                        expected_origin: "origin".to_string(),
                        identity_address: "my_identity".to_string(),
                    })],
                    passkeys: vec![],
                },
                ledger_identity: LedgerIdentity::Radix(RadixIdentityDetails {
                    account_addresses: vec![],
                    dapp_definition_address: "dapp_definition_address".to_string(),
                    expected_origin: "origin".to_string(),
                    identity_address: "my_identity".to_string(),
                }),
                origin: "origin".to_string(),
                session_id: "session_id".to_string(),
                signing_key: random_signing_key,
                verifying_key: random_verifying_key,
            },
        }
    }
}
