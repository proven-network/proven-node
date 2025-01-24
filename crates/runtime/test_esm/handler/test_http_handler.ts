import { runOnHttp } from "@proven-network/handler";

export const test = runOnHttp(
  {
    path: "/test/:id",
  },
  async (request) => {
    if (request.path !== "/test/420") {
      throw new Error("Expected path to be /test/420");
    }

    if (request.method !== "GET") {
      throw new Error("Expected method to be GET");
    }

    if (!request.body) {
      throw new Error("Expected body to be defined");
    }

    const expectedBytes = new TextEncoder().encode("Hello, world!");
    if (!request.body.every((byte, i) => byte === expectedBytes[i])) {
      throw new Error("Expected body to be 'Hello, world!'");
    }

    // Check path variable extraction works
    return request.pathVariables.id;
  }
);
