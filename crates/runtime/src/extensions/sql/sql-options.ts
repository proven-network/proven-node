import { SqlRows, SqlDatabase, SqlNftDatabase, SqlStatement } from '@proven-network/sql';

const { op_migrate_application_sql, op_migrate_nft_sql, op_migrate_personal_sql } =
  globalThis.Deno.core.ops;

class Sql implements SqlStatement<any> {
  readonly statement: string;
  readonly params: Record<string, null | number | string | Uint8Array>;

  constructor(statement: string, params?: Record<string, null | number | string | Uint8Array>) {
    this.statement = statement;
    this.params = params || {};
  }
}

export const sql = (
  statement: string,
  params?: Record<string, null | number | string | Uint8Array>
) => {
  return new Sql(statement, params);
};

class ApplicationSqlStore implements SqlDatabase {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute(): Promise<number> {
    throw new Error('`execute` must be run inside a handler function');
  }

  migrate(sql: string) {
    if (typeof sql === 'string') {
      op_migrate_application_sql(this.name, sql);

      return this;
    } else {
      throw new TypeError('Expected `sql` to be a string');
    }
  }

  query(): Promise<SqlRows<any>> {
    throw new Error('`query` must be run inside a handler function');
  }
}

export const getApplicationDb = (name: string) => {
  if (!name) {
    throw new Error('Name must be provided');
  }

  return new ApplicationSqlStore(name);
};

class NftSqlStore implements SqlNftDatabase {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _sql: string | Sql
  ): Promise<number> {
    throw new Error('`execute` must be run inside a handler function');
  }

  migrate(sql: string) {
    if (typeof sql === 'string') {
      op_migrate_nft_sql(this.name, sql);

      return this;
    } else {
      throw new TypeError('Expected `sql` to be a string');
    }
  }

  query(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _sql: string | Sql
  ): Promise<SqlRows<any>> {
    throw new Error('`query` must be run inside a handler function');
  }
}

export const getNftDb = (name: string) => {
  if (!name) {
    throw new Error('Name must be provided');
  }

  return new NftSqlStore(name);
};

class PersonalSqlStore implements SqlDatabase {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute(_sql: string | Sql): Promise<number> {
    throw new Error('`execute` must be run inside a handler function');
  }

  migrate(sql: string) {
    if (typeof sql === 'string') {
      op_migrate_personal_sql(this.name, sql);

      return this;
    } else {
      throw new TypeError('Expected `sql` to be a string');
    }
  }

  query(_sql: string | Sql): Promise<SqlRows<any>> {
    throw new Error('`query` must be run inside a handler function');
  }
}

export const getPersonalDb = (name: string) => {
  if (!name) {
    throw new Error('Name must be provided');
  }

  return new PersonalSqlStore(name);
};
