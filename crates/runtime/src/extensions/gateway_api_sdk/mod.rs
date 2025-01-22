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
    use crate::test_utils::create_test_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_gateway_api_sdk() {
        let runtime_options =
            create_test_runtime_options("gateway_api_sdk/test_gateway_api_sdk", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();

        // RadixNetwork.Mainnet should be 1
        assert_eq!(execution_result.output, 1);
    }
}
