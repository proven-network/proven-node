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

type EncodedUint8Array = {
  Uint8Array: {
    hex: string;
  };
};

type HttpRequest = {
  pathVariables: Record<string, string>;
};

function encodeUint8Array(data: Uint8Array): EncodedUint8Array {
  return {
    Uint8Array: {
      hex: Array.from(data)
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join(""),
    },
  };
}

function encodeUint8ArrayInObject(
  obj: Output,
  path = "",
  paths: string[] = []
): Output {
  if (obj instanceof Uint8Array) {
    paths.push(path);
    return encodeUint8Array(obj);
  } else if (Array.isArray(obj)) {
    return obj.map((item, index) =>
      encodeUint8ArrayInObject(item, `${path}[${index}]`, paths)
    );
  } else if (obj !== null && typeof obj === "object") {
    return Object.keys(obj).reduce(
      (acc, key) => {
        acc[key] = encodeUint8ArrayInObject(obj[key], `${path}.${key}`, paths);
        return acc;
      },
      {} as { [key: string]: Output }
    );
  }
  return obj;
}

export const runWithOptions = (fn: (..._args: Input[]) => Promise<Output>) => {
  return async (...args: Input[]) => {
    return fn(...args).then((handlerOutput) => {
      const paths_to_uint8_arrays: string[] = [];
      const output = encodeUint8ArrayInObject(
        handlerOutput,
        "",
        paths_to_uint8_arrays
      );

      return { output, paths_to_uint8_arrays };
    });
  };
};

export const runOnHttp = (fn: (request: HttpRequest) => Promise<Output>) => {
  return async (request: HttpRequest) => {
    return fn(request).then((handlerOutput) => {
      const paths_to_uint8_arrays: string[] = [];
      const output = encodeUint8ArrayInObject(
        handlerOutput,
        "",
        paths_to_uint8_arrays
      );

      return { output, paths_to_uint8_arrays };
    });
  };
};
