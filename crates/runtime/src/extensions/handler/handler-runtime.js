function encodeUint8Array(data) {
  return {
    Uint8Array: {
      hex: Array.from(data).map((byte) => byte.toString(16).padStart(2, "0")).join(""),
    }
  }
}

function encodeUint8ArrayInObject(obj, path = '', paths = []) {
  if (obj instanceof Uint8Array) {
    paths.push(path);
    return encodeUint8Array(obj);
  } else if (Array.isArray(obj)) {
    return obj.map((item, index) => encodeUint8ArrayInObject(item, `${path}[${index}]`, paths));
  } else if (obj !== null && typeof obj === 'object') {
    return Object.keys(obj).reduce((acc, key) => {
      acc[key] = encodeUint8ArrayInObject(obj[key], `${path}.${key}`, paths);
      return acc;
    }, {});
  }
  return obj;
}

export const runWithOptions = (fn) => {
  return async (...args) => {
    return fn(...args).then((handlerOutput) => {
      const paths_to_uint8_arrays = [];
      const output = encodeUint8ArrayInObject(handlerOutput, '', paths_to_uint8_arrays);
      console.log({ output, paths_to_uint8_arrays });
      return { output, paths_to_uint8_arrays };
    })
  }
}

export const runOnHttp = (fn) => {
  return async (...args) => {
    return fn(...args).then((handlerOutput) => {
      const paths_to_uint8_arrays = [];
      const output = encodeUint8ArrayInObject(handlerOutput, '', paths_to_uint8_arrays);
      console.log({ output, paths_to_uint8_arrays });
      return { output, paths_to_uint8_arrays };
    })
  }
}
