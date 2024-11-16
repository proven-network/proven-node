import { GatewayApiClient } from "proven:raw_radixdlt_babylon_gateway_api";

const originalInitialize = GatewayApiClient.initialize;

GatewayApiClient.initialize = function (options) {
  if (!options) {
    options = {};
  }

  if (options.baseUrl) {
    console.warn("Manually setting baseUrl is not supported in this environment. Ignoring.");
  }
  options.baseUrl = "http://127.0.0.1:8081";

  if (options.networkId) {
    console.warn("Manually setting networkId is not supported in this environment. Ignoring.");
  }
  options.networkId = 2;

  return originalInitialize(options);
};

export * from "proven:raw_radixdlt_babylon_gateway_api";
export { GatewayApiClient };
