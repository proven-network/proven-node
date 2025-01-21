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

// Handler name is dynamically inserted and should not be part of exported types.
export function runWithOptions(
  handlerName: string,
  fn: (...args: any[]) => Promise<Output>,
  options: RpcHandlerOptions = {}
): typeof fn {
  if (typeof handlerName !== "string") {
    throw new Error(
      "runWithOptions must be used in conjunction with the export keyword"
    );
  }

  if (typeof fn !== "function") {
    throw new Error("No function passed to runWithOptions");
  }

  if (options.memory) {
    if (typeof options.memory !== "number") {
      throw new Error("Memory must be a number");
    }

    op_set_memory_option("rpc", handlerName, options.memory);
  }

  if (options.timeout) {
    if (typeof options.timeout !== "number") {
      throw new Error("Timeout must be a number");
    }

    op_set_timeout_option("rpc", handlerName, options.timeout);
  }

  if (options.allowedOrigins) {
    if (!Array.isArray(options.allowedOrigins)) {
      throw new Error("allowedOrigins must be an array");
    }

    for (const origin of options.allowedOrigins) {
      if (typeof origin !== "string") {
        throw new Error("allowedOrigins must be an array of strings");
      }

      op_add_allowed_origin("rpc", handlerName, origin);
    }
  }

  return fn;
}

// Handler name is dynamically inserted and should not be part of exported types.
export function runOnHttp<P extends string>(
  handlerName: string,
  fn: (...args: any[]) => Promise<Output>,
  options: HttpHandlerOptions<P>
): typeof fn {
  if (typeof handlerName !== "string") {
    throw new Error(
      "runOnHttp must be used in conjunction with the export keyword"
    );
  }

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

  op_set_path_option("http", handlerName, options.path);

  if (options.memory) {
    if (typeof options.memory !== "number") {
      throw new Error("Memory must be a number");
    }

    op_set_memory_option("http", handlerName, options.memory);
  }

  if (options.timeout) {
    if (typeof options.timeout !== "number") {
      throw new Error("Timeout must be a number");
    }

    op_set_timeout_option("http", handlerName, options.timeout);
  }

  if (options.allowedOrigins) {
    if (!Array.isArray(options.allowedOrigins)) {
      throw new Error("allowedOrigins must be an array");
    }

    for (const origin of options.allowedOrigins) {
      if (typeof origin !== "string") {
        throw new Error("allowedOrigins must be an array of strings");
      }

      op_add_allowed_origin("http", handlerName, origin);
    }
  }

  return fn;
}

export function runOnRadixEvent(
  handlerName: string,
  fn: (transaction: CommittedTransactionInfo) => void,
  options: RadixEventHandlerOptions
): void {
  if (typeof handlerName !== "string") {
    throw new Error(
      "runOnHttp must be used in conjunction with the export keyword"
    );
  }

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

    op_set_memory_option("radix_event", handlerName, options.memory);
  }

  if (options.timeout) {
    if (typeof options.timeout !== "number") {
      throw new Error("Timeout must be a number");
    }

    op_set_timeout_option("radix_event", handlerName, options.timeout);
  }

  if (options.allowedOrigins) {
    if (!Array.isArray(options.allowedOrigins)) {
      throw new Error("allowedOrigins must be an array");
    }

    for (const origin of options.allowedOrigins) {
      if (typeof origin !== "string") {
        throw new Error("allowedOrigins must be an array of strings");
      }

      op_add_allowed_origin("radix_event", handlerName, origin);
    }
  }
}
