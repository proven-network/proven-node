use deno_core::extension;

extension!(
    radixdlt_babylon_gateway_api_ext,
    esm_entry_point = "proven:radixdlt_babylon_gateway_api",
    esm = [
        "proven:raw_radixdlt_babylon_gateway_api" =
            "vendor/@radixdlt/babylon-gateway-api-sdk/index.mjs",
        "proven:radixdlt_babylon_gateway_api" = "src/extensions/gateway_api_sdk/gateway-api-sdk.js",
    ],
);
