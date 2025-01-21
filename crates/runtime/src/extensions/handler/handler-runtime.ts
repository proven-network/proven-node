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
  pathVariables: Record<string, string>;
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

export const runWithOptions = (fn: (..._args: Input[]) => Promise<Output>) => {
  return async (...args: Input[]) => {
    return fn(...args).then(async (handlerOutput) => {
      const uint8ArrayJsonPaths: string[] = [];
      const output = await encodeUint8ArrayInObject(
        handlerOutput,
        "$",
        uint8ArrayJsonPaths
      );

      return { output, uint8ArrayJsonPaths };
    });
  };
};

export const runOnHttp = (fn: (request: HttpRequest) => Promise<Output>) => {
  return async (request: HttpRequest) => {
    return fn(request).then(async (handlerOutput) => {
      const uint8ArrayJsonPaths: string[] = [];
      const output = await encodeUint8ArrayInObject(
        handlerOutput,
        "$",
        uint8ArrayJsonPaths
      );

      return { output, uint8ArrayJsonPaths };
    });
  };
};
