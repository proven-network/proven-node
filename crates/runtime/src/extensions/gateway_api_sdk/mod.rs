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
    radixdlt_babylon_gateway_api_ext,
    ops = [op_get_gateway_network_id, op_get_gateway_origin],
    esm_entry_point = "proven:radixdlt_babylon_gateway_api",
    esm = [
        "proven:raw_radixdlt_babylon_gateway_api" =
            "vendor/@radixdlt/babylon-gateway-api-sdk/index.mjs",
        "proven:radixdlt_babylon_gateway_api" = "src/extensions/gateway_api_sdk/gateway-api-sdk.js",
    ],
);
