import {
  HttpHandlerOptions,
  RpcHandlerOptions,
  HttpRequest,
} from "@proven-network/handler";

type Input =
  | string
  | number
  | boolean
  | null
  | Uint8Array
  | Input[]
  | { [key: string]: Input };

type Output =
  | string
  | number
  | boolean
  | null
  | Uint8Array
  | Output[]
  | { [key: string]: Output };

type EncodedUint8Array = number[];

function encodeUint8Array(data: Uint8Array): EncodedUint8Array {
  return Array.from(data);
}

async function encodeUint8ArrayInObject(
  obj: Output,
  path: string,
  paths: string[],
): Promise<Output> {
  // deno-lint-ignore no-explicit-any
  if (typeof obj === "object" && obj !== null && "allRows" in (obj as any)) {
    // deno-lint-ignore no-explicit-any
    return (await (obj as any).allRows) as Output;
  } else if (obj instanceof Uint8Array) {
    paths.push(path);
    return encodeUint8Array(obj);
  } else if (Array.isArray(obj)) {
    const results = await Promise.all(
      obj.map((item, index) =>
        encodeUint8ArrayInObject(item, `${path}[${index}]`, paths),
      ),
    );
    return results;
  } else if (obj !== null && typeof obj === "object") {
    const entries = await Promise.all(
      Object.entries(obj).map(async ([key, value]) => [
        key,
        await encodeUint8ArrayInObject(value, `${path}.${key}`, paths),
      ]),
    );
    return Object.fromEntries(entries);
  }
  return obj;
}

export const run = (fn: (..._args: Input[]) => Promise<Output>) => {
  return async (...args: Input[]) => {
    const handlerOutput = await fn(...args);

    const uint8ArrayJsonPaths: string[] = [];
    const output = await encodeUint8ArrayInObject(
      handlerOutput,
      "$",
      uint8ArrayJsonPaths,
    );

    return { output, uint8ArrayJsonPaths };
  };
};

export const runWithOptions = (
  options: RpcHandlerOptions,
  fn: (..._args: Input[]) => Promise<Output>,
) => {
  return async (...args: Input[]) => {
    const handlerOutput = await (options.timeout
      ? (Promise.race([
          Promise.resolve(fn(...args)),
          new Promise((_, reject) => {
            setTimeout(() => {
              reject(new Error("Timed out after " + options.timeout + "ms"));
            }, options.timeout);
          }),
        ]) as Promise<Output>)
      : fn(...args));

    const uint8ArrayJsonPaths: string[] = [];
    const output = await encodeUint8ArrayInObject(
      handlerOutput,
      "$",
      uint8ArrayJsonPaths,
    );

    return { output, uint8ArrayJsonPaths };
  };
};

type ExtractPathVariables<Path extends string> =
  Path extends `${infer _Start}:${infer Param}/${infer Rest}`
    ? { [K in Param | keyof ExtractPathVariables<`/${Rest}`>]: string }
    : Path extends `${infer _Start}:${infer Param}`
      ? { [K in Param]: string }
      : Record<string, never>;

export function runOnHttp<P extends string>(
  options: HttpHandlerOptions<P>,
  fn: (request: HttpRequest) => Promise<Output>,
) {
  return async (
    method: "GET" | "POST" | "PUT" | "DELETE" | "PATCH",
    currentPath: string,
    query?: string,
    body?: Uint8Array,
  ) => {
    const optionsPath = options.path;

    // Extract path variables
    const pathParameters: ExtractPathVariables<P> =
      {} as ExtractPathVariables<P>;
    const templateSegments = optionsPath.split("/").filter(Boolean);
    const currentSegments = currentPath.split("/").filter(Boolean);
    templateSegments.forEach((template, index) => {
      if (template.startsWith(":")) {
        const param = template.slice(1);
        pathParameters[param] = currentSegments[index];
      }
    });

    // Extract query parameters
    const queryParams = new URLSearchParams(query);
    const queryParameters: Record<string, string> = {};
    queryParams.forEach((value, key) => {
      queryParameters[key] = value;
    });

    const request: HttpRequest<P> = {
      body: body ? new Uint8Array(body) : undefined,
      headers: {},
      method,
      path: currentPath,
      pathParameters,
      queryParameters,
    };

    const handlerOutput = await (options.timeout
      ? (Promise.race([
          Promise.resolve(fn(request)),
          new Promise((_, reject) => {
            setTimeout(() => {
              reject(new Error("Timed out after " + options.timeout + "ms"));
            }, options.timeout);
          }),
        ]) as Promise<Output>)
      : fn(request));

    const uint8ArrayJsonPaths: string[] = [];
    const output = await encodeUint8ArrayInObject(
      handlerOutput,
      "$",
      uint8ArrayJsonPaths,
    );

    return { output, uint8ArrayJsonPaths };
  };
}
