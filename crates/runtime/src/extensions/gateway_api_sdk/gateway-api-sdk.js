import { GatewayApiClient } from "proven:raw_radixdlt_babylon_gateway_api";

function getGatewayDetails() {
  const { op_get_gateway_network_id, op_get_gateway_origin } = Deno.core.ops;

  return {
    networkId: op_get_gateway_network_id(),
    origin: op_get_gateway_origin(),
  }
}

const originalInitialize = GatewayApiClient.initialize;

GatewayApiClient.initialize = function (options) {
  if (!options) {
    options = {};
  }

  const { networkId, origin } = getGatewayDetails();

  if (options.baseUrl) {
    console.warn("Manually setting baseUrl is not supported in this environment. Ignoring.");
  }
  options.baseUrl = origin;

  if (options.networkId) {
    console.warn("Manually setting networkId is not supported in this environment. Ignoring.");
  }
  options.networkId = networkId;

  return originalInitialize(options);
};

export * from "proven:raw_radixdlt_babylon_gateway_api";
export { GatewayApiClient };
