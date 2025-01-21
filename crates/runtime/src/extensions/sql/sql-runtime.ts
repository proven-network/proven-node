import {
  SqlRows,
  SqlDatabase,
  SqlNftDatabase,
  SqlStatement,
} from "@proven-network/sql";

const {
  op_create_params_list,
  op_add_blob_param,
  op_add_integer_param,
  op_add_null_param,
  op_add_real_param,
  op_add_text_param,
  op_get_row_batch,
  op_execute_application_sql,
  op_query_application_sql,
  op_execute_nft_sql,
  op_query_nft_sql,
  op_execute_personal_sql,
  op_query_personal_sql,
} = globalThis.Deno.core.ops;

class Statement implements SqlStatement<any> {
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

export const sql = (statement: string, params?: Record<string, any>) => {
  return new Statement(statement, params);
};

class Rows
  implements SqlRows<Record<string, null | number | string | Uint8Array>>
{
  [key: number]: never;
  columnNames: Readonly<string[]>;
  rows: never[];
  rowStreamId: number;
  private loadedAll: boolean = false;

  get allRows(): Promise<never[]> {
    return this.ensureAllRowsLoaded().then(() => {
      return this.rows;
    });
  }

  get length(): Promise<number> {
    return this.ensureAllRowsLoaded().then(() => {
      return this.rows.length;
    });
  }

  constructor(firstRow: SqlValue[], rowStreamId: number) {
    this.rowStreamId = rowStreamId;
    const columnNames: string[] = [];
    this.rows = [];

    // First row also contains column names
    const processedFirstRow: Record<
      string,
      null | number | string | Uint8Array
    > = {};
    for (let i = 0; i < firstRow.length; i++) {
      const sqlValue = firstRow[i];
      if (typeof sqlValue === "object" && "IntegerWithName" in sqlValue) {
        const [name, value] = sqlValue.IntegerWithName;
        columnNames.push(name);
        processedFirstRow[name] = value;
      } else if (typeof sqlValue === "object" && "RealWithName" in sqlValue) {
        const [name, value] = sqlValue.RealWithName;
        columnNames.push(name);
        processedFirstRow[name] = value;
      } else if (typeof sqlValue === "object" && "TextWithName" in sqlValue) {
        const [name, value] = sqlValue.TextWithName;
        columnNames.push(name);
        processedFirstRow[name] = value;
      } else if (typeof sqlValue === "object" && "BlobWithName" in sqlValue) {
        const [name, value] = sqlValue.BlobWithName;
        columnNames.push(name);
        processedFirstRow[name] = value;
      } else if (typeof sqlValue === "object" && "NullWithName" in sqlValue) {
        const name = sqlValue.NullWithName;
        columnNames.push(name);
        processedFirstRow[name] = null;
      } else {
        throw new TypeError("Expected first row to contain column names");
      }
    }

    const finalColumnNames = Object.freeze(columnNames);
    this.columnNames = finalColumnNames;

    this.rows.push(processedFirstRow as never);
  }

  [Symbol.iterator](): Iterator<never> {
    return this.rows[Symbol.iterator]() as Iterator<never>;
  }

  getAtIndex(index: number): never {
    return this.rows[index];
  }

  map<U>(callbackfn: (value: never, index: number, array: never[]) => U): U[] {
    return this.rows.map(callbackfn);
  }

  filter(
    predicate: (value: never, index: number, array: never[]) => boolean
  ): never[] {
    return this.rows.filter(predicate);
  }

  forEach(
    callbackfn: (value: never, index: number, array: never[]) => void
  ): void {
    this.rows.forEach(callbackfn);
  }

  private async ensureAllRowsLoaded() {
    while (!this.loadedAll) {
      await this.loadMoreRows();
    }
  }

  private async loadMoreRows() {
    const rows = await op_get_row_batch(this.rowStreamId);

    if (rows.length === 0) {
      this.loadedAll = true;
    }

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const result: Record<string, null | number | string | Uint8Array> = {};
      for (let j = 0; j < row.length; j++) {
        const sqlValue = row[j];

        if (typeof sqlValue === "object" && "Integer" in sqlValue) {
          result[this.columnNames[j]] = sqlValue.Integer;
        } else if (typeof sqlValue === "object" && "Real" in sqlValue) {
          result[this.columnNames[j]] = sqlValue.Real;
        } else if (typeof sqlValue === "object" && "Text" in sqlValue) {
          result[this.columnNames[j]] = sqlValue.Text;
        } else if (typeof sqlValue === "object" && "Blob" in sqlValue) {
          result[this.columnNames[j]] = sqlValue.Blob;
        } else if (sqlValue === "Null") {
          result[this.columnNames[j]] = null;
        } else {
          throw new TypeError(
            "Only expected Integer, Real, Text, Blob, or Null"
          );
        }
      }
      this.rows.push(result as never);
    }
  }
}

function prepareParamList(values: any[]) {
  const paramListId = op_create_params_list();

  for (const value of values) {
    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        op_add_integer_param(paramListId, value);
      } else {
        op_add_real_param(paramListId, value);
      }
    } else if (typeof value === "string") {
      op_add_text_param(paramListId, value);
    } else if (value === null) {
      op_add_null_param(paramListId);
    } else if (typeof value === "object" && value instanceof Uint8Array) {
      op_add_blob_param(paramListId, value);
    } else {
      throw new TypeError(
        "Expected all values to be null, numbers, strings, or blobs"
      );
    }
  }

  return paramListId;
}

class ApplicationSqlStore implements SqlDatabase {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async execute(sql: string | Statement) {
    if (typeof sql === "string") {
      return await op_execute_application_sql(this.name, sql);
    } else if (sql instanceof Statement) {
      let affectedRows;
      if (Object.keys(sql.params).length === 0) {
        affectedRows = await op_execute_application_sql(
          this.name,
          sql.statement
        );
      } else {
        // Sort the keys by length in descending order to avoid partial matches
        const paramKeys = Object.keys(sql.params)
          .sort((a, b) => a.length - b.length)
          .reverse();

        // Replace all named parameters with positional parameters
        let preparedStatement = sql.statement;
        for (const key of paramKeys) {
          const paramIndex = paramKeys.indexOf(key) + 1;

          preparedStatement = preparedStatement.replace(
            new RegExp(`:${key}\\b`, "g"),
            `?${paramIndex}`
          );
        }

        const paramListId = prepareParamList(
          paramKeys.map((key) => sql.params[key])
        );

        affectedRows = await op_execute_application_sql(
          this.name,
          preparedStatement,
          paramListId
        );
      }

      return affectedRows;
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }
  }

  migrate(_sql: string) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this;
  }

  async query(sql: string | Statement): Promise<Rows> {
    let result;

    if (typeof sql === "string") {
      result = await op_query_application_sql(this.name, sql);
    } else if (sql instanceof Statement) {
      if (Object.keys(sql.params).length === 0) {
        result = await op_query_application_sql(this.name, sql.statement);
      } else {
        // Sort the keys by length in descending order to avoid partial matches
        const paramKeys = Object.keys(sql.params)
          .sort((a, b) => a.length - b.length)
          .reverse();

        // Replace all named parameters with positional parameters
        let preparedStatement = sql.statement;
        for (const key of paramKeys) {
          const paramIndex = paramKeys.indexOf(key) + 1;

          preparedStatement = preparedStatement.replace(
            new RegExp(`:${key}\\b`, "g"),
            `?${paramIndex}`
          );
        }

        const paramListId = prepareParamList(
          paramKeys.map((key) => sql.params[key])
        );

        result = await op_query_application_sql(
          this.name,
          preparedStatement,
          paramListId
        );
      }
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }

    if (!result) {
      throw new Error("TODO: This shouldn't error... fix later");
    }

    const [firstRow, rowStreamId] = result;

    let rows = new Rows(firstRow, rowStreamId);

    let rowsProxy = new Proxy(rows, {
      get: (target, prop) => {
        if (typeof prop === "string") {
          const index = parseInt(prop);

          if (!isNaN(index)) {
            return rows.getAtIndex(index);
          }
        }

        return Reflect.get(target, prop);
      },
    });

    return rowsProxy;
  }
}

export const getApplicationDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new ApplicationSqlStore(name);
};

const RUID_CHECK =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function prepareNftId(nftId: number | string | Uint8Array) {
  if (typeof nftId === "number") {
    return `#${nftId}#`;
  } else if (typeof nftId === "string") {
    if (RUID_CHECK.test(nftId)) {
      return `{${nftId}}`;
    } else {
      return `<${nftId}>`;
    }
  } else if (nftId instanceof Uint8Array) {
    const hex = Array.from(nftId)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");

    return `[${hex}]`;
  } else {
    throw new Error("nftId must be a number, string, or Uint8Array");
  }
}

class NftSqlStore implements SqlNftDatabase {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async execute(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    sql: string | Statement
  ) {
    let result;

    if (typeof sql === "string") {
      result = await op_execute_nft_sql(
        this.name,
        resourceAddress,
        prepareNftId(nftId),
        sql
      );
    } else if (sql instanceof Statement) {
      if (Object.keys(sql.params).length === 0) {
        result = await op_execute_nft_sql(
          this.name,
          resourceAddress,
          prepareNftId(nftId),
          sql.statement
        );
      } else {
        // Sort the keys by length in descending order to avoid partial matches
        const paramKeys = Object.keys(sql.params)
          .sort((a, b) => a.length - b.length)
          .reverse();

        // Replace all named parameters with positional parameters
        let preparedStatement = sql.statement;
        for (const key of paramKeys) {
          const paramIndex = paramKeys.indexOf(key) + 1;

          preparedStatement = preparedStatement.replace(
            new RegExp(`:${key}\\b`, "g"),
            `?${paramIndex}`
          );
        }

        const paramListId = prepareParamList(
          paramKeys.map((key) => sql.params[key])
        );

        result = await op_execute_nft_sql(
          this.name,
          resourceAddress,
          prepareNftId(nftId),
          preparedStatement,
          paramListId
        );
      }
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }

    if (result === "NftDoesNotExist") {
      throw new Error("NFT does not exist");
    } else if (result === "NoAccountsInContext") {
      throw new Error("No accounts in context");
    } else if (typeof result === "object" && "OwnershipInvalid" in result) {
      throw new Error(
        `NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`
      );
    }

    return result.Ok;
  }

  migrate(_sql: string) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this;
  }

  async query(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    sql: string | Statement
  ): Promise<Rows> {
    let result;

    if (typeof sql === "string") {
      result = await op_query_nft_sql(
        this.name,
        resourceAddress,
        prepareNftId(nftId),
        sql
      );
    } else if (sql instanceof Statement) {
      if (Object.keys(sql.params).length === 0) {
        result = await op_query_nft_sql(
          this.name,
          resourceAddress,
          prepareNftId(nftId),
          sql.statement
        );
      } else {
        // Sort the keys by length in descending order to avoid partial matches
        const paramKeys = Object.keys(sql.params)
          .sort((a, b) => a.length - b.length)
          .reverse();

        // Replace all named parameters with positional parameters
        let preparedStatement = sql.statement;
        for (const key of paramKeys) {
          const paramIndex = paramKeys.indexOf(key) + 1;

          preparedStatement = preparedStatement.replace(
            new RegExp(`:${key}\\b`, "g"),
            `?${paramIndex}`
          );
        }

        const paramListId = prepareParamList(
          paramKeys.map((key) => sql.params[key])
        );

        result = await op_query_nft_sql(
          this.name,
          resourceAddress,
          prepareNftId(nftId),
          preparedStatement,
          paramListId
        );
      }
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }

    if (result === "NftDoesNotExist") {
      throw new Error("NFT does not exist");
    } else if (result === "NoAccountsInContext") {
      throw new Error("No accounts in context");
    } else if (typeof result === "object" && "OwnershipInvalid" in result) {
      throw new Error(
        `NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`
      );
    }

    if (!result.Ok) {
      throw new Error("TODO: This shouldn't error... fix later");
    }

    const [firstRow, rowStreamId] = result.Ok;

    let rows = new Rows(firstRow, rowStreamId);

    let rowsProxy = new Proxy(rows, {
      get: (target, prop) => {
        if (typeof prop === "string") {
          const index = parseInt(prop);

          if (!isNaN(index)) {
            return rows.getAtIndex(index);
          }
        }

        return Reflect.get(target, prop);
      },
    });

    return rowsProxy;
  }
}

export const getNftDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new NftSqlStore(name);
};

class PersonalSqlStore implements SqlDatabase {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async execute(sql: string | Statement) {
    let result;

    if (typeof sql === "string") {
      result = await op_execute_personal_sql(this.name, sql);
    } else if (sql instanceof Statement) {
      if (Object.keys(sql.params).length === 0) {
        result = await op_execute_personal_sql(this.name, sql.statement);
      } else {
        // Sort the keys by length in descending order to avoid partial matches
        const paramKeys = Object.keys(sql.params)
          .sort((a, b) => a.length - b.length)
          .reverse();

        // Replace all named parameters with positional parameters
        let preparedStatement = sql.statement;
        for (const key of paramKeys) {
          const paramIndex = paramKeys.indexOf(key) + 1;

          preparedStatement = preparedStatement.replace(
            new RegExp(`:${key}\\b`, "g"),
            `?${paramIndex}`
          );
        }

        const paramListId = prepareParamList(
          paramKeys.map((key) => sql.params[key])
        );

        result = await op_execute_personal_sql(
          this.name,
          preparedStatement,
          paramListId
        );
      }
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }

    if (result === "NoPersonalContext") {
      throw new Error("No personal context");
    }

    return result.Ok;
  }

  migrate(_sql: string) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this;
  }

  async query(sql: string | Statement): Promise<Rows> {
    let result;

    if (typeof sql === "string") {
      result = await op_query_personal_sql(this.name, sql);
    } else if (sql instanceof Statement) {
      if (Object.keys(sql.params).length === 0) {
        result = await op_query_personal_sql(this.name, sql.statement);
      } else {
        // Sort the keys by length in descending order to avoid partial matches
        const paramKeys = Object.keys(sql.params)
          .sort((a, b) => a.length - b.length)
          .reverse();

        // Replace all named parameters with positional parameters
        let preparedStatement = sql.statement;
        for (const key of paramKeys) {
          const paramIndex = paramKeys.indexOf(key) + 1;

          preparedStatement = preparedStatement.replace(
            new RegExp(`:${key}\\b`, "g"),
            `?${paramIndex}`
          );
        }

        const paramListId = prepareParamList(
          paramKeys.map((key) => sql.params[key])
        );

        result = await op_query_personal_sql(
          this.name,
          preparedStatement,
          paramListId
        );
      }
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }

    if (result === "NoPersonalContext") {
      throw new Error("No personal context");
    }

    if (!result.Ok) {
      throw new Error("No results returned");
    }

    const [firstRow, rowStreamId] = result.Ok;

    let rows = new Rows(firstRow, rowStreamId);

    let rowsProxy = new Proxy(rows, {
      get: (target, prop) => {
        if (typeof prop === "string") {
          const index = parseInt(prop);

          if (!isNaN(index)) {
            return rows.getAtIndex(index);
          }
        }

        return Reflect.get(target, prop);
      },
    });

    return rowsProxy;
  }
}

export const getPersonalDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new PersonalSqlStore(name);
};
