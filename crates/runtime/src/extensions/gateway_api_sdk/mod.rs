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
    use crate::{ExecutionRequest, ExecutionResult, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_gateway_api_sdk() {
        let runtime_options = RuntimeOptions::for_test_code("gateway_api_sdk/test_gateway_api_sdk");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::for_rpc_with_session_test("file:///main.ts#test", vec![]);

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
