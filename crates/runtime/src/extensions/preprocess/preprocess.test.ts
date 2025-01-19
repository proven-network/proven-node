import preprocess from './preprocess';
import { describe, expect, it } from '@jest/globals';

describe('preprocess', () => {
  it('should wrap plain exported functions with runWithOptions', () => {
    const code = `
      export function greet(name: string): string {
        return 'Hello, ' + name;
      }

      export async function goodbye(name: string): string {
        return 'Goodbye, ' + name;
      }

      export const wave = (name: string): string => '👋, ' + name;

      export const pointAt = async (name: string): string => '👉, ' + name;

      export function greetNoParam(): string {
        return 'Hello, ';
      }

      export async function goodbyeNoParam(): string {
        return 'Goodbye, ';
      }

      export const waveNoParam = (): string => '👋';

      export const pointAtNoParam = async (): string => '👉';

      export default () => {
          console.log('Hello, world!');
      };

      export default async () => {
          console.log('Hello, world!');
      };

      export const foo = 42;
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(`
import { runWithOptions } from "proven:handler";
export const greet = runWithOptions(async (name: string): Promise<string> => {
    return 'Hello, ' + name;
}, {});
export const goodbye = runWithOptions(async (name: string): Promise<string> => {
    return 'Goodbye, ' + name;
}, {});
export const wave = runWithOptions(async (name: string): Promise<string> => { return '👋, ' + name; }, {});
export const pointAt = runWithOptions(async (name: string): Promise<string> => { return '👉, ' + name; }, {});
export const greetNoParam = runWithOptions(async (): Promise<string> => {
    return 'Hello, ';
}, {});
export const goodbyeNoParam = runWithOptions(async (): Promise<string> => {
    return 'Goodbye, ';
}, {});
export const waveNoParam = runWithOptions(async (): Promise<string> => { return '👋'; }, {});
export const pointAtNoParam = runWithOptions(async (): Promise<string> => { return '👉'; }, {});
export default runWithOptions(async (): Promise<any> => {
    console.log('Hello, world!');
}, {});
export default runWithOptions(async (): Promise<any> => {
    console.log('Hello, world!');
}, {});
export const foo = 42;
    `.trim());
  });

  it('should not import runWithOptions again if already imported', () => {
    const code = `
      import { runWithOptions } from 'proven:handler';

      export function greet(name: string): string {
        return 'Hello, ' + name;
      }
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(`
import { runWithOptions } from 'proven:handler';
export const greet = runWithOptions(async (name: string): Promise<string> => {
    return 'Hello, ' + name;
}, {});
    `.trim());
  });
});
