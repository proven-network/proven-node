const text = "Hello, world!";
const encoder = new TextEncoder();
const uint8Array = encoder.encode(text);

export const test = () => {
  return uint8Array;
};

export const testNested = () => {
  return { nested: uint8Array };
};
