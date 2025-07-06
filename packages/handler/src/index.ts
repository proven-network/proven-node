import { CommittedTransactionInfo } from '@radixdlt/babylon-gateway-api-sdk';

// Allow any serializable output type including complex objects
type Output = any;

export interface RpcHandlerOptions {
  allowedOrigins?: string[];
  timeout?: number;
}

// Type helper to convert a function type to its async version
export type AsyncFunction<T extends (...args: any[]) => any> = T extends (
  ...args: infer P
) => infer R
  ? (...args: P) => Promise<Awaited<R>>
  : never;

// Note: The bundler will transform this function to return AsyncFunction<T>
// For TypeScript, we keep the original type to avoid breaking existing code
export function run<T extends (...args: any[]) => any>(fn: T): T {
  // This function should be replaced by the bundler at build time.
  // If you're seeing this warning, it means the bundler plugin is not configured correctly.
  console.warn(
    '⚠️  @proven-network/handler: The run() function was called directly.\n' +
      '\n' +
      'This usually means the Proven bundler plugin is not configured correctly.\n' +
      '\n' +
      'To fix this, install and configure one of the following bundler plugins:\n' +
      '\n' +
      '  • Rollup: @proven-network/bundler/rollup\n' +
      '  • Webpack: @proven-network/bundler/webpack\n' +
      '  • Vite: @proven-network/bundler/vite\n' +
      '\n' +
      'See https://docs.proven.network/bundler for configuration details.\n' +
      '\n' +
      `Handler function: ${fn.name || '<anonymous>'}\n` +
      `Handler location: ${new Error().stack?.split('\n')[2] || 'unknown'}`
  );

  // Return the original function so code continues to work (just without remote execution)
  return fn;
}

export function runWithOptions<T extends (...args: any[]) => any>(
  options: RpcHandlerOptions,
  fn: T
): T {
  // This function should be replaced by the bundler at build time.
  // If you're seeing this warning, it means the bundler plugin is not configured correctly.
  console.warn(
    '⚠️  @proven-network/handler: The runWithOptions() function was called directly.\n' +
      '\n' +
      'This usually means the Proven bundler plugin is not configured correctly.\n' +
      '\n' +
      'To fix this, install and configure one of the following bundler plugins:\n' +
      '\n' +
      '  • Rollup: @proven-network/bundler/rollup\n' +
      '  • Webpack: @proven-network/bundler/webpack\n' +
      '  • Vite: @proven-network/bundler/vite\n' +
      '\n' +
      'See https://docs.proven.network/bundler for configuration details.\n' +
      '\n' +
      `Handler function: ${fn.name || '<anonymous>'}\n` +
      `Handler options: ${JSON.stringify(options)}\n` +
      `Handler location: ${new Error().stack?.split('\n')[2] || 'unknown'}`
  );

  // Return the original function so code continues to work (just without remote execution)
  return fn;
}

export type ExtractPathVariables<Path extends string> =
  Path extends `${infer _Start}:${infer Param}/${infer Rest}`
    ? { [K in Param | keyof ExtractPathVariables<`/${Rest}`>]: string }
    : Path extends `${infer _Start}:${infer Param}`
      ? { [K in Param]: string }
      : Record<string, never>;

export interface HttpRequest<Path extends string = string> {
  body?: Uint8Array;
  headers: Record<string, string>;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  path: string;
  pathParameters: ExtractPathVariables<Path>;
  queryParameters: Record<string, string>;
}

export interface HttpHandlerOptions<P extends string> {
  allowedOrigins?: string[];
  attestation?: 'always' | 'request';
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  path: P;
  timeout?: number;
}

export function runOnHttp<
  P extends string,
  O extends Output | Promise<Output> | void | Promise<void>,
>(
  options: HttpHandlerOptions<P>,
  fn: (request: HttpRequest<P>) => O
): (request: HttpRequest<P>) => O {
  // This function should be replaced by the bundler at build time.
  console.warn(
    '⚠️  @proven-network/handler: The runOnHttp() function was called directly.\n' +
      '\n' +
      'This usually means the Proven bundler plugin is not configured correctly.\n' +
      '\n' +
      'To fix this, install and configure one of the following bundler plugins:\n' +
      '\n' +
      '  • Rollup: @proven-network/bundler/rollup\n' +
      '  • Webpack: @proven-network/bundler/webpack\n' +
      '  • Vite: @proven-network/bundler/vite\n' +
      '\n' +
      'See https://docs.proven.network/bundler for configuration details.\n' +
      '\n' +
      `HTTP handler path: ${options.path}\n` +
      `HTTP handler method: ${options.method || 'GET'}`
  );

  return fn;
}

export type RadixEventHandlerOptions =
  | {
      allowedOrigins?: string[];
      eventEmitter?: string;
      eventName: string;
      timeout?: number;
    }
  | {
      allowedOrigins?: string[];
      eventEmitter: string;
      eventName?: string;
      timeout?: number;
    };

export function runOnRadixEvent(
  options: RadixEventHandlerOptions,
  _fn: (transaction: CommittedTransactionInfo) => void | Promise<void>
): void {
  // This function should be replaced by the bundler at build time.
  console.warn(
    '⚠️  @proven-network/handler: The runOnRadixEvent() function was called directly.\n' +
      '\n' +
      'This usually means the Proven bundler plugin is not configured correctly.\n' +
      '\n' +
      'To fix this, install and configure one of the following bundler plugins:\n' +
      '\n' +
      '  • Rollup: @proven-network/bundler/rollup\n' +
      '  • Webpack: @proven-network/bundler/webpack\n' +
      '  • Vite: @proven-network/bundler/vite\n' +
      '\n' +
      'See https://docs.proven.network/bundler for configuration details.\n' +
      '\n' +
      `Event options: ${JSON.stringify(options)}`
  );

  return;
}
