import preprocess from "./preprocess";
import { describe, expect, it } from "@jest/globals";

describe("preprocess", () => {
  it("should wrap plain exported functions with runWithOptions", () => {
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

    expect(result.trim()).toBe(
      `
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
    `.trim()
    );
  });

  it("should not import runWithOptions again if already imported", () => {
    const code = `
      import { runWithOptions } from 'proven:handler';

      export function greet(name: string): string {
        return 'Hello, ' + name;
      }
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from 'proven:handler';
export const greet = runWithOptions(async (name: string): Promise<string> => {
    return 'Hello, ' + name;
}, {});
    `.trim()
    );
  });

  it("should ensure exported functions are async", () => {
    const code = `
      export function syncFn() {
        return 'hello';
      }

      export const arrowFn = () => 'world';

      export const objFn = function() {
        return '!';
      }

      export default function() {
        return 'default';
      }
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
export const syncFn = runWithOptions(async (): Promise<any> => {
    return 'hello';
}, {});
export const arrowFn = runWithOptions(async (): Promise<any> => { return 'world'; }, {});
export const objFn = runWithOptions(async (): Promise<any> => {
    return '!';
}, {});
export const defaultName = runWithOptions(async (): Promise<any> => {
    return 'default';
}, {});
    `.trim()
    );
  });

  it("should preserve existing async functions", () => {
    const code = `
      export async function alreadyAsync() {
        return 'hello';
      }

      export const asyncArrow = async () => 'world';
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
export const alreadyAsync = runWithOptions(async (): Promise<any> => {
    return 'hello';
}, {});
export const asyncArrow = runWithOptions(async (): Promise<any> => { return 'world'; }, {});
    `.trim()
    );
  });

  it("should handle complex return types", () => {
    const code = `
      export function complex(): Promise<{ data: string[] }> {
        return Promise.resolve({ data: ['hello'] });
      }
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
export const complex = runWithOptions(async (): Promise<{
    data: string[];
}> => {
    return Promise.resolve({ data: ['hello'] });
}, {});
    `.trim()
    );
  });

  it("should not double-wrap Promise types", () => {
    const code = `
      export function returns(): Promise<string> {
        return Promise.resolve("test");
      }
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
export const returns = runWithOptions(async (): Promise<string> => {
    return Promise.resolve("test");
}, {});
    `.trim()
    );
  });

  it("should make runWithOptions fn parameter async", () => {
    const code = `
      import { runWithOptions } from "proven:handler";

      export const handler = runWithOptions((): string => {
        return "test";
      }, {});
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
export const handler = runWithOptions(async (): Promise<string> => {
    return "test";
}, {});
    `.trim()
    );
  });

  it("should make runOnHttp fn parameter async", () => {
    const code = `
      import { runOnHttp } from "proven:handler";

      export const handler = runOnHttp((): string => {
        return "test";
      }, {});
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
import { runOnHttp } from "proven:handler";
export const handler = runOnHttp(async (): Promise<string> => {
    return "test";
}, {});
    `.trim()
    );
  });

  it("should make runOnRadixEvent fn parameter async", () => {
    const code = `
      import { runOnRadixEvent } from "proven:handler";

      export const handler = runOnRadixEvent((): string => {
        return "test";
      }, {});
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
import { runOnRadixEvent } from "proven:handler";
export const handler = runOnRadixEvent(async (): Promise<string> => {
    return "test";
}, {});
    `.trim()
    );
  });

  it("should make runOnSchedule fn parameter async", () => {
    const code = `
      import { runOnSchedule } from "proven:handler";

      export const handler = runOnSchedule((): string => {
        return "test";
      }, {});
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
import { runOnSchedule } from "proven:handler";
export const handler = runOnSchedule(async (): Promise<string> => {
    return "test";
}, {});
    `.trim()
    );
  });

  it("should make runOnProvenEvent fn parameter async", () => {
    const code = `
      import { runOnProvenEvent } from "proven:handler";

      export const handler = runOnProvenEvent((): string => {
        return "test";
      }, {});
    `;

    const result = preprocess(code);

    expect(result.trim()).toBe(
      `
import { runWithOptions } from "proven:handler";
import { runOnProvenEvent } from "proven:handler";
export const handler = runOnProvenEvent(async (): Promise<string> => {
    return "test";
}, {});
    `.trim()
    );
  });
});
