import {
  GatewayApiClient,
  GatewayApiClientSettings,
} from "@radixdlt/babylon-gateway-api-sdk";

const { op_get_gateway_network_id, op_get_gateway_origin } = Deno.core.ops;

const originalInitialize = GatewayApiClient.initialize;

GatewayApiClient.initialize = function (
  options: GatewayApiClientSettings | undefined
) {
  if (!options) {
    options = {
      applicationName: "Proven", // TODO: Potentially change this to the name of the app
    };
  }

  if (options.basePath) {
    console.warn(
      "Manually setting baseUrl is not supported in this environment. Ignoring."
    );
  }
  options.basePath = op_get_gateway_origin();

  if (options.networkId) {
    console.warn(
      "Manually setting networkId is not supported in this environment. Ignoring."
    );
  }
  options.networkId = op_get_gateway_network_id();

  return originalInitialize(options);
};

export * from "@radixdlt/babylon-gateway-api-sdk";
export { GatewayApiClient };
