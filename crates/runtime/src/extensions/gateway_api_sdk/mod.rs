#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2};

#[derive(Debug, Default)]
pub struct GatewayDetailsState {
    pub gateway_origin: String,
    pub network_id: u8,
}

#[op2(fast)]
pub const fn op_get_gateway_network_id(#[state] state: &GatewayDetailsState) -> u8 {
    state.network_id
}

#[op2]
#[string]
pub fn op_get_gateway_origin(#[state] state: &GatewayDetailsState) -> String {
    state.gateway_origin.clone()
}

extension!(
    babylon_gateway_api_ext,
    ops = [op_get_gateway_network_id, op_get_gateway_origin],
    esm_entry_point = "proven:babylon_gateway_api",
    esm = [
        "proven:raw_babylon_gateway_api" = "vendor/@radixdlt/babylon-gateway-api-sdk/index.mjs",
        "proven:babylon_gateway_api" = "src/extensions/gateway_api_sdk/gateway-api-sdk.js",
    ],
);

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, ExecutionResult, HandlerSpecifier, RuntimeOptions, Worker};

    use proven_sessions::{Identity, RadixIdentityDetails};

    #[tokio::test]
    async fn test_gateway_api_sdk() {
        let runtime_options = RuntimeOptions::for_test_code("gateway_api_sdk/test_gateway_api_sdk");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            application_id: "application_id".to_string(),
            args: vec![],
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identities: vec![Identity::Radix(RadixIdentityDetails {
                account_addresses: vec![],
                dapp_definition_address: "dapp_definition_address".to_string(),
                expected_origin: "origin".to_string(),
                identity_address: "my_identity".to_string(),
            })],
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                // RadixNetwork.Mainnet should be 1
                assert_eq!(output, 1);
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
