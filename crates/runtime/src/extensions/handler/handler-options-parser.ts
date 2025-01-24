import {
  HttpHandlerOptions,
  RpcHandlerOptions,
  RadixEventHandlerOptions,
} from "@proven-network/handler";
import { CommittedTransactionInfo } from "@radixdlt/babylon-gateway-api-sdk";

type Output =
  | boolean
  | number
  | null
  | string
  | Uint8Array
  | Output[]
  | { [key: string]: Output };

const {
  op_add_allowed_origin,
  op_set_memory_option,
  op_set_path_option,
  op_set_timeout_option,
} = globalThis.Deno.core.ops;

// moduleSpecifier and handlerName is dynamically inserted and should not be part of exported types.
export function run(
  moduleSpecifier: string,
  handlerName: string,
  fn: (...args: any[]) => Promise<Output>
): typeof fn {
  if (typeof moduleSpecifier !== "string" || typeof handlerName !== "string") {
    throw new Error("run must be used in conjunction with the export keyword");
  }

  if (typeof fn !== "function") {
    throw new Error("No function passed to run");
  }

  return fn;
}

// moduleSpecifier and handlerName is dynamically inserted and should not be part of exported types.
export function runWithOptions(
  moduleSpecifier: string,
  handlerName: string,
  options: RpcHandlerOptions,
  fn: (...args: any[]) => Promise<Output>
): typeof fn {
  if (typeof moduleSpecifier !== "string" || typeof handlerName !== "string") {
    throw new Error(
      "runWithOptions must be used in conjunction with the export keyword"
    );
  }

  if (!options || typeof options !== "object") {
    throw new Error("Options must be provided");
  }

  const handlerSpecifier =
    moduleSpecifier + (handlerName === "__default__" ? "" : `#${handlerName}`);

  if (typeof fn !== "function") {
    throw new Error("No function passed to runWithOptions");
  }

  if (options.memory) {
    if (typeof options.memory !== "number") {
      throw new Error("Memory must be a number");
    }

    op_set_memory_option("rpc", handlerSpecifier, options.memory);
  }

  if (options.timeout) {
    if (typeof options.timeout !== "number") {
      throw new Error("Timeout must be a number");
    }

    op_set_timeout_option("rpc", handlerSpecifier, options.timeout);
  }

  if (options.allowedOrigins) {
    if (!Array.isArray(options.allowedOrigins)) {
      throw new Error("allowedOrigins must be an array");
    }

    for (const origin of options.allowedOrigins) {
      if (typeof origin !== "string") {
        throw new Error("allowedOrigins must be an array of strings");
      }

      op_add_allowed_origin("rpc", handlerSpecifier, origin);
    }
  }

  return fn;
}

// moduleSpecifier and handlerName is dynamically inserted and should not be part of exported types.
export function runOnHttp<P extends string>(
  moduleSpecifier: string,
  handlerName: string,
  options: HttpHandlerOptions<P>,
  fn: (...args: any[]) => Promise<Output>
): typeof fn {
  if (typeof moduleSpecifier !== "string" || typeof handlerName !== "string") {
    throw new Error(
      "runOnHttp must be used in conjunction with the export keyword"
    );
  }

  const handlerSpecifier =
    moduleSpecifier + (handlerName === "__default__" ? "" : `#${handlerName}`);

  if (typeof fn !== "function") {
    throw new Error("No function passed to runOnHttp");
  }

  if (!options || typeof options !== "object") {
    throw new Error("Options must be provided");
  }

  if (!options.path) {
    throw new Error("Path must be provided");
  }

  if (typeof options.path !== "string") {
    throw new Error("Path must be a string");
  }

  op_set_path_option("http", handlerSpecifier, options.path);

  if (options.memory) {
    if (typeof options.memory !== "number") {
      throw new Error("Memory must be a number");
    }

    op_set_memory_option("http", handlerSpecifier, options.memory);
  }

  if (options.timeout) {
    if (typeof options.timeout !== "number") {
      throw new Error("Timeout must be a number");
    }

    op_set_timeout_option("http", handlerSpecifier, options.timeout);
  }

  if (options.allowedOrigins) {
    if (!Array.isArray(options.allowedOrigins)) {
      throw new Error("allowedOrigins must be an array");
    }

    for (const origin of options.allowedOrigins) {
      if (typeof origin !== "string") {
        throw new Error("allowedOrigins must be an array of strings");
      }

      op_add_allowed_origin("http", handlerSpecifier, origin);
    }
  }

  return fn;
}

// moduleSpecifier and handlerName is dynamically inserted and should not be part of exported types.
export function runOnRadixEvent(
  moduleSpecifier: string,
  handlerName: string,
  options: RadixEventHandlerOptions,
  fn: (transaction: CommittedTransactionInfo) => void
): void {
  if (typeof moduleSpecifier !== "string" || typeof handlerName !== "string") {
    throw new Error(
      "runOnRadixEvent must be used in conjunction with the export keyword"
    );
  }

  const handlerSpecifier =
    moduleSpecifier + (handlerName === "__default__" ? "" : `#${handlerName}`);

  if (typeof fn !== "function") {
    throw new Error("No function passed to runOnRadixEvent");
  }

  if (!options || typeof options !== "object") {
    throw new Error("Options must be provided");
  }

  if (options.memory) {
    if (typeof options.memory !== "number") {
      throw new Error("Memory must be a number");
    }

    op_set_memory_option("radix_event", handlerSpecifier, options.memory);
  }

  if (options.timeout) {
    if (typeof options.timeout !== "number") {
      throw new Error("Timeout must be a number");
    }

    op_set_timeout_option("radix_event", handlerSpecifier, options.timeout);
  }

  if (options.allowedOrigins) {
    if (!Array.isArray(options.allowedOrigins)) {
      throw new Error("allowedOrigins must be an array");
    }

    for (const origin of options.allowedOrigins) {
      if (typeof origin !== "string") {
        throw new Error("allowedOrigins must be an array of strings");
      }

      op_add_allowed_origin("radix_event", handlerSpecifier, origin);
    }
  }
}
