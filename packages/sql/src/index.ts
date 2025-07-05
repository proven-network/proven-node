import { LexSqlTokens } from "./lexer";
import { ParseMigration, ParseQueryType } from "./parser";
import { GeneratedSchema } from "./state";
import { TokenizeSqlString } from "./tokenizer";

export type SqlStatement<S extends string> = {
  readonly params: Record<string, null | number | string | Uint8Array>;
  readonly statement: S;
};

class Statement<S extends string> implements SqlStatement<S> {
  readonly params: Record<string, null | number | string | Uint8Array>;
  readonly statement: S;

  constructor(
    statement: S,
    params?: Record<string, null | number | string | Uint8Array>
  ) {
    this.statement = statement;
    this.params = params || {};
  }
}

export function sql<S extends string>(
  statement: S,
  params: Record<string, null | number | string | Uint8Array>
): SqlStatement<S> {
  return new Statement<S>(statement, params);
}

type FirstRowValue =
  | { BlobWithName: [string, Uint8Array] }
  | { IntegerWithName: [string, number] }
  | { NullWithName: string }
  | { RealWithName: [string, number] }
  | { TextWithName: [string, string] };

type FirstRow = FirstRowValue[];

export type SqlRows<
  T extends Record<string, null | number | string | Uint8Array>,
> = {
  readonly [index: number]: T | undefined;
  readonly columnNames: Readonly<string[]>;
  readonly length: Promise<number>;
  [Symbol.iterator](): Iterator<T>;
  map<U>(callbackfn: (value: T, index: number, array: T[]) => U): U[];
  filter(predicate: (value: T, index: number, array: T[]) => boolean): T[];
  forEach(callbackfn: (value: T, index: number, array: T[]) => void): void;
};

class Rows<T extends Record<string, null | number | string | Uint8Array>>
  implements SqlRows<T>
{
  [index: number]: T | undefined;
  columnNames: Readonly<string[]>;

  get length(): Promise<number> {
    return Promise.resolve(0);
  }

  constructor(firstRow: FirstRow, rowStreamId: number) {
    this.columnNames = [];
  }

  [Symbol.iterator](): Iterator<T> {
    return [][Symbol.iterator]();
  }

  map<U>(callbackfn: (value: T, index: number, array: T[]) => U): U[] {
    return [].map(callbackfn);
  }

  filter(predicate: (value: T, index: number, array: T[]) => boolean): T[] {
    return [].filter(predicate);
  }

  forEach(callbackfn: (value: T, index: number, array: T[]) => void): void {
    [].forEach(callbackfn);
  }
}

export type SqlDatabase<Schema extends GeneratedSchema = { tables: {} }> = {
  readonly name: string;
  execute<S extends string>(query: S | Statement<S>): Promise<number>;
  migrate<S extends string>(
    query: S
  ): SqlDatabase<ParseMigration<LexSqlTokens<TokenizeSqlString<S>>, Schema>>;
  query<S extends string>(
    query: S | Statement<S>
  ): Promise<
    SqlRows<ParseQueryType<LexSqlTokens<TokenizeSqlString<S>>, Schema>>
  >;
};

class Database<Schema extends GeneratedSchema = { tables: {} }>
  implements SqlDatabase<Schema>
{
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute<S extends string>(query: S | Statement<S>): Promise<number> {
    return Promise.resolve(0);
  }

  migrate<S extends string>(
    query: S
  ): Database<ParseMigration<LexSqlTokens<TokenizeSqlString<S>>, Schema>> {
    return this;
  }

  query<S extends string>(
    query: S | Statement<S>
  ): Promise<
    SqlRows<ParseQueryType<LexSqlTokens<TokenizeSqlString<S>>, Schema>>
  > {
    return [] as any;
  }
}

export function getApplicationDb(name: string): SqlDatabase {
  return new Database(name);
}

export function getPersonalDb(name: string): SqlDatabase {
  return new Database(name);
}

export type SqlNftDatabase<Schema extends GeneratedSchema = { tables: {} }> = {
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
  ): Promise<
    SqlRows<ParseQueryType<LexSqlTokens<TokenizeSqlString<S>>, Schema>>
  >;
};

class NftDatabase<Schema extends GeneratedSchema = { tables: {} }>
  implements SqlNftDatabase<Schema>
{
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute<S extends string>(
    resourceAddress: string,
    nftId: string | number | Uint8Array,
    query: S | Statement<S>
  ): Promise<number> {
    return Promise.resolve(0);
  }

  migrate<S extends string>(
    query: S
  ): NftDatabase<ParseMigration<LexSqlTokens<TokenizeSqlString<S>>, Schema>> {
    return this;
  }

  query<S extends string>(
    resourceAddress: string,
    nftId: string | number | Uint8Array,
    query: S | Statement<S>
  ): Promise<
    SqlRows<ParseQueryType<LexSqlTokens<TokenizeSqlString<S>>, Schema>>
  > {
    return [] as any;
  }
}

export function getNftDb(name: string): SqlNftDatabase {
  return new NftDatabase(name);
}
