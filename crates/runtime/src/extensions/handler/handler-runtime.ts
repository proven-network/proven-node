import { HttpHandlerOptions, RpcHandlerOptions } from "@proven-network/handler";

type Input =
  | string
  | number
  | boolean
  | null
  | Uint8Array
  | Output[]
  | { [key: string]: Output };

type Output =
  | string
  | number
  | boolean
  | null
  | Uint8Array
  | Output[]
  | { [key: string]: Output };

type EncodedUint8Array = number[];

type HttpRequest = {
  body?: Uint8Array;
  method: string;
  path: string;
  pathVariables: Record<string, string>;
  queryVariables: Record<string, string>;
};

function encodeUint8Array(data: Uint8Array): EncodedUint8Array {
  return Array.from(data);
}

async function encodeUint8ArrayInObject(
  obj: Output,
  path: string,
  paths: string[]
): Promise<Output> {
  if (typeof obj === "object" && obj !== null && "allRows" in (obj as any)) {
    return (await (obj as any).allRows) as Output;
  } else if (obj instanceof Uint8Array) {
    paths.push(path);
    return encodeUint8Array(obj);
  } else if (Array.isArray(obj)) {
    const results = await Promise.all(
      obj.map((item, index) =>
        encodeUint8ArrayInObject(item, `${path}[${index}]`, paths)
      )
    );
    return results;
  } else if (obj !== null && typeof obj === "object") {
    const entries = await Promise.all(
      Object.entries(obj).map(async ([key, value]) => [
        key,
        await encodeUint8ArrayInObject(value, `${path}.${key}`, paths),
      ])
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
      uint8ArrayJsonPaths
    );

    return { output, uint8ArrayJsonPaths };
  };
};

export const runWithOptions = (
  options: RpcHandlerOptions,
  fn: (..._args: Input[]) => Promise<Output>
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
      uint8ArrayJsonPaths
    );

    return { output, uint8ArrayJsonPaths };
  };
};

export function runOnHttp<P extends string>(
  options: HttpHandlerOptions<P>,
  fn: (...args: any[]) => Promise<Output>
): typeof fn {
  return async (
    method: string,
    currentPath: string,
    query?: string,
    body?: Uint8Array
  ) => {
    const optionsPath = options.path;

    // Extract path variables
    const pathVariables: Record<string, string> = {};
    const templateSegments = optionsPath.split("/").filter(Boolean);
    const currentSegments = currentPath.split("/").filter(Boolean);
    templateSegments.forEach((template, index) => {
      if (template.startsWith(":")) {
        const varName = template.slice(1);
        pathVariables[varName] = currentSegments[index];
      }
    });

    // Extract query parameters
    const queryParams = new URLSearchParams(query);
    const queryVariables: Record<string, string> = {};
    queryParams.forEach((value, key) => {
      queryVariables[key] = value;
    });

    const request: HttpRequest = {
      body: body ? new Uint8Array(body) : undefined,
      method,
      path: currentPath,
      pathVariables,
      queryVariables,
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
      uint8ArrayJsonPaths
    );

    return { output, uint8ArrayJsonPaths };
  };
}
