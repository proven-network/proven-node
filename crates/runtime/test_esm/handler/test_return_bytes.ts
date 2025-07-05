import { run } from '@proven-network/handler';

const text = 'Hello, world!';
const encoder = new TextEncoder();
const uint8Array = encoder.encode(text);

export const test = run(() => {
  return uint8Array;
});

export const testNested = run(() => {
  return { nested: uint8Array };
});
