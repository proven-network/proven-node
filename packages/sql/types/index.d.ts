import { LexSqlTokens } from './lexer';
import { ParseMigration, ParseQueryType } from './parser';
import { GeneratedSchema } from './state';
import { TokenizeSqlString } from './tokenizer';
export type SqlStatement<S extends string> = {
  readonly params: Record<string, null | number | string | Uint8Array>;
  readonly statement: S;
};
declare class Statement<S extends string> implements SqlStatement<S> {
  readonly params: Record<string, null | number | string | Uint8Array>;
  readonly statement: S;
  constructor(statement: S, params?: Record<string, null | number | string | Uint8Array>);
}
export declare function sql<S extends string>(
  statement: S,
  params: Record<string, null | number | string | Uint8Array>
): SqlStatement<S>;
export type SqlRows<T extends Record<string, null | number | string | Uint8Array>> = {
  readonly [index: number]: T | undefined;
  readonly columnNames: Readonly<string[]>;
  readonly length: Promise<number>;
  [Symbol.iterator](): Iterator<T>;
  map<U>(callbackfn: (value: T, index: number, array: T[]) => U): U[];
  filter(predicate: (value: T, index: number, array: T[]) => boolean): T[];
  forEach(callbackfn: (value: T, index: number, array: T[]) => void): void;
};
export type SqlDatabase<
  Schema extends GeneratedSchema = {
    tables: {};
  },
> = {
  readonly name: string;
  execute<S extends string>(query: S | Statement<S>): Promise<number>;
  migrate<S extends string>(
    query: S
  ): SqlDatabase<ParseMigration<LexSqlTokens<TokenizeSqlString<S>>, Schema>>;
  query<S extends string>(
    query: S | Statement<S>
  ): Promise<SqlRows<ParseQueryType<LexSqlTokens<TokenizeSqlString<S>>, Schema>>>;
};
export declare function getApplicationDb(name: string): SqlDatabase;
export declare function getPersonalDb(name: string): SqlDatabase;
export type SqlNftDatabase<
  Schema extends GeneratedSchema = {
    tables: {};
  },
> = {
  readonly name: string;
  execute<S extends string>(
    resourceAddress: string,
    nftId: string | number | Uint8Array,
    query: S | Statement<S>
  ): Promise<number>;
  migrate<S extends string>(
    query: S
  ): SqlNftDatabase<ParseMigration<LexSqlTokens<TokenizeSqlString<S>>, Schema>>;
  query<S extends string>(
    resourceAddress: string,
    nftId: string | number | Uint8Array,
    query: S | Statement<S>
  ): Promise<SqlRows<ParseQueryType<LexSqlTokens<TokenizeSqlString<S>>, Schema>>>;
};
export declare function getNftDb(name: string): SqlNftDatabase;
export {};
