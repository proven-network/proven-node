import { CommittedTransactionInfo } from '@radixdlt/babylon-gateway-api-sdk';

// Allow any serializable output type including complex objects
type Output = any;

export interface RpcHandlerOptions {
  allowedOrigins?: string[];
  timeout?: number;
}

export function run<T extends (...args: any[]) => any>(fn: T): T {
  return fn;
}

export function runWithOptions<T extends (...args: any[]) => any>(
  options: RpcHandlerOptions,
  fn: T
): T {
  return fn;
}

type ExtractPathVariables<Path extends string> =
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

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function runOnRadixEvent(
  options: RadixEventHandlerOptions,
  fn: (transaction: CommittedTransactionInfo) => void | Promise<void>
): void {
  return;
}
