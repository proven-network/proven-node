import { runOnHttp } from "@proven-network/handler";

export const test = runOnHttp(
  async (request) => {
    return request.pathVariables.id;
  },
  {
    path: "/test/:id",
  }
);
