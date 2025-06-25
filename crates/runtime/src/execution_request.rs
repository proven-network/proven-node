use crate::HandlerSpecifier;

use bytes::Bytes;
use http::Method;
use proven_identity::Identity;
use serde_json::Value;
use uuid::Uuid;

/// Request for a runtime execution.
#[derive(Clone)]
pub enum ExecutionRequest {
    /// A request received from an HTTP endpoint.
    Http {
        /// The application ID.
        application_id: Uuid,

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
    HttpWithIdentity {
        /// The application ID.
        application_id: Uuid,

        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The active identity.
        identity: Identity,

        /// The HTTP method.
        method: Method,

        /// The path of the HTTP request.
        path: String,

        /// The query string of the HTTP request.
        query: Option<String>,
    },
    /// A request created to respond to an event from the Radix network.
    RadixEvent {
        /// The application ID.
        application_id: Uuid,

        // TODO: should have Radix transaction data
        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over and RPC session.
    Rpc {
        /// The application ID.
        application_id: Uuid,

        /// The arguments to the handler.
        args: Vec<Value>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over and RPC session with authenticated user context.
    RpcWithIdentity {
        /// The application ID.
        application_id: Uuid,

        /// The arguments to the handler.
        args: Vec<Value>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The active identity.
        identity: Identity,
    },
}

impl ExecutionRequest {
    // TODO: Use builder pattern for this stuff
    #[cfg(test)]
    pub(crate) fn for_anonymous_session_rpc_test(
        handler_specifier: &str,
        args: Vec<Value>,
    ) -> Self {
        use uuid::Uuid;

        Self::Rpc {
            application_id: Uuid::max(),
            args,
            handler_specifier: HandlerSpecifier::parse(handler_specifier).unwrap(),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_identified_session_rpc_test(
        handler_specifier: &str,
        args: Vec<Value>,
    ) -> Self {
        use uuid::Uuid;

        Self::RpcWithIdentity {
            application_id: Uuid::max(),
            args,
            handler_specifier: HandlerSpecifier::parse(handler_specifier).unwrap(),
            identity: Identity { id: Uuid::max() },
        }
    }
}
