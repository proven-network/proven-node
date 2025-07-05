import { CommittedTransactionInfo } from '@radixdlt/babylon-gateway-api-sdk';
type Output = any;
export interface RpcHandlerOptions {
    allowedOrigins?: string[];
    timeout?: number;
}
export declare function run<T extends (...args: any[]) => any>(fn: T): T;
export declare function runWithOptions<T extends (...args: any[]) => any>(options: RpcHandlerOptions, fn: T): T;
type ExtractPathVariables<Path extends string> = Path extends `${infer _Start}:${infer Param}/${infer Rest}` ? {
    [K in Param | keyof ExtractPathVariables<`/${Rest}`>]: string;
} : Path extends `${infer _Start}:${infer Param}` ? {
    [K in Param]: string;
} : Record<string, never>;
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
export declare function runOnHttp<P extends string, O extends Output | Promise<Output> | void | Promise<void>>(options: HttpHandlerOptions<P>, fn: (request: HttpRequest<P>) => O): (request: HttpRequest<P>) => O;
export type RadixEventHandlerOptions = {
    allowedOrigins?: string[];
    eventEmitter?: string;
    eventName: string;
    timeout?: number;
} | {
    allowedOrigins?: string[];
    eventEmitter: string;
    eventName?: string;
    timeout?: number;
};
export declare function runOnRadixEvent(options: RadixEventHandlerOptions, fn: (transaction: CommittedTransactionInfo) => void | Promise<void>): void;
export {};
