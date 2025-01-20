import {
  getApplicationDb as _getApplicationDb,
  getNftDb as _getNftDb,
  getPersonalDb as _getPersonalDb,
  sql as _sql,
} from "@proven-network/sql";

type IApplicationDb = ReturnType<typeof _getApplicationDb>;
type INftDb = ReturnType<typeof _getNftDb>;
type IPersonalDb = ReturnType<typeof _getPersonalDb>;
type ISql = ReturnType<typeof _sql>;

const {
  op_migrate_application_sql,
  op_migrate_nft_sql,
  op_migrate_personal_sql,
} = globalThis.Deno.core.ops;

class Sql implements ISql {
  readonly statement: string;
  readonly params: Record<string, null | number | string | Uint8Array>;

  constructor(
    statement: string,
    params?: Record<string, null | number | string | Uint8Array>
  ) {
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

class ApplicationSqlStore implements IApplicationDb {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute(): ReturnType<IApplicationDb["execute"]> {
    throw new Error("`execute` must be run inside a handler function");
  }

  migrate(sql: string): IApplicationDb {
    if (typeof sql === "string") {
      op_migrate_application_sql(this.name, sql);

      return this;
    } else {
      throw new TypeError("Expected `sql` to be a string");
    }
  }

  query(): ReturnType<IApplicationDb["query"]> {
    throw new Error("`query` must be run inside a handler function");
  }
}

export const getApplicationDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new ApplicationSqlStore(name);
};

class NftSqlStore implements INftDb {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _sql: string | Sql
  ): ReturnType<INftDb["execute"]> {
    throw new Error("`execute` must be run inside a handler function");
  }

  migrate(sql: string): INftDb {
    if (typeof sql === "string") {
      op_migrate_nft_sql(this.name, sql);

      return this;
    } else {
      throw new TypeError("Expected `sql` to be a string");
    }
  }

  query(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _sql: string | Sql
  ): ReturnType<INftDb["query"]> {
    throw new Error("`query` must be run inside a handler function");
  }
}

export const getNftDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new NftSqlStore(name);
};

class PersonalSqlStore implements IPersonalDb {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  execute(_sql: string | Sql): ReturnType<IPersonalDb["execute"]> {
    throw new Error("`execute` must be run inside a handler function");
  }

  migrate(sql: string): IPersonalDb {
    if (typeof sql === "string") {
      op_migrate_personal_sql(this.name, sql);

      return this;
    } else {
      throw new TypeError("Expected `sql` to be a string");
    }
  }

  query(_sql: string | Sql): ReturnType<IPersonalDb["query"]> {
    throw new Error("`query` must be run inside a handler function");
  }
}

export const getPersonalDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new PersonalSqlStore(name);
};
