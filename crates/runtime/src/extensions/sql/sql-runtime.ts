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
  op_create_application_params_list,
  op_add_application_blob_param,
  op_add_application_integer_param,
  op_add_application_null_param,
  op_add_application_real_param,
  op_add_application_text_param,
  op_execute_application_sql,
  op_query_application_sql,
  op_create_nft_params_list,
  op_add_nft_blob_param,
  op_add_nft_integer_param,
  op_add_nft_null_param,
  op_add_nft_real_param,
  op_add_nft_text_param,
  op_execute_nft_sql,
  op_query_nft_sql,
  op_create_personal_params_list,
  op_add_personal_blob_param,
  op_add_personal_integer_param,
  op_add_personal_null_param,
  op_add_personal_real_param,
  op_add_personal_text_param,
  op_execute_personal_sql,
  op_query_personal_sql,
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

export const sql = (statement: string, params?: Record<string, any>) => {
  return new Sql(statement, params);
};

function prepareApplicationParamList(values: any[]) {
  const paramListId = op_create_application_params_list();

  for (const value of values) {
    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        op_add_application_integer_param(paramListId, value);
      } else {
        op_add_application_real_param(paramListId, value);
      }
    } else if (typeof value === "string") {
      op_add_application_text_param(paramListId, value);
    } else if (value === null) {
      op_add_application_null_param(paramListId);
    } else if (typeof value === "object" && value instanceof Uint8Array) {
      op_add_application_blob_param(paramListId, value);
    } else {
      throw new TypeError(
        "Expected all values to be null, numbers, strings, or blobs"
      );
    }
  }

  return paramListId;
}

class ApplicationSqlStore implements IApplicationDb {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async execute(sql: string | Sql): ReturnType<IApplicationDb["execute"]> {
    if (typeof sql === "string") {
      return await op_execute_application_sql(this.name, sql);
    } else if (sql instanceof Sql) {
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

        const paramListId = prepareApplicationParamList(
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

  migrate(_sql: string): IApplicationDb {
    // No-op at runtime (migrations are collated at options-parse time)
    return this as unknown as IApplicationDb;
  }

  async query(sql: string | Sql): ReturnType<IApplicationDb["query"]> {
    let rows;

    if (typeof sql === "string") {
      rows = await op_query_application_sql(this.name, sql);
    } else if (sql instanceof Sql) {
      if (Object.keys(sql.params).length === 0) {
        rows = await op_query_application_sql(this.name, sql.statement);
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

        const paramListId = prepareApplicationParamList(
          paramKeys.map((key) => sql.params[key])
        );

        rows = await op_query_application_sql(
          this.name,
          preparedStatement,
          paramListId
        );
      }
    } else {
      throw new TypeError("Expected `sql` to be a string or Sql object");
    }

    const columnNames: string[] = [];

    // First row also contains column names
    const firstRow: Record<string, null | number | string | Uint8Array> = {};
    for (let i = 0; i < rows[0].length; i++) {
      const sqlValue = rows[0][i];
      if (typeof sqlValue === "object" && "IntegerWithName" in sqlValue) {
        const [name, value] = sqlValue.IntegerWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "RealWithName" in sqlValue) {
        const [name, value] = sqlValue.RealWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "TextWithName" in sqlValue) {
        const [name, value] = sqlValue.TextWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "BlobWithName" in sqlValue) {
        const [name, value] = sqlValue.BlobWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "NullWithName" in sqlValue) {
        const name = sqlValue.NullWithName;
        columnNames.push(name);
        firstRow[name] = null;
      } else {
        throw new TypeError("Expected first row to contain column names");
      }
    }

    const results: Record<string, null | number | string | Uint8Array>[] = [];
    results.push(firstRow);

    // TODO: Reworks rows code to use generators
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const result: Record<string, null | number | string | Uint8Array> = {};
      for (let j = 0; j < row.length; j++) {
        const sqlValue = row[j];

        if (typeof sqlValue === "object" && "Integer" in sqlValue) {
          result[columnNames[j]] = sqlValue.Integer;
        } else if (typeof sqlValue === "object" && "Real" in sqlValue) {
          result[columnNames[j]] = sqlValue.Real;
        } else if (typeof sqlValue === "object" && "Text" in sqlValue) {
          result[columnNames[j]] = sqlValue.Text;
        } else if (typeof sqlValue === "object" && "Blob" in sqlValue) {
          result[columnNames[j]] = sqlValue.Blob;
        } else if (sqlValue === "Null") {
          result[columnNames[j]] = null;
        } else {
          throw new TypeError(
            "Only expected Integer, Real, Text, Blob, or Null"
          );
        }
      }
      results.push(result);
    }

    return results as never;
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

function prepareNftParamList(values: any[]) {
  const paramListId = op_create_nft_params_list();

  for (const value of values) {
    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        op_add_nft_integer_param(paramListId, value);
      } else {
        op_add_nft_real_param(paramListId, value);
      }
    } else if (typeof value === "string") {
      op_add_nft_text_param(paramListId, value);
    } else if (value === null) {
      op_add_nft_null_param(paramListId);
    } else if (typeof value === "object" && value instanceof Uint8Array) {
      op_add_nft_blob_param(paramListId, value);
    } else {
      throw new TypeError(
        "Expected all values to be null, numbers, strings, or blobs"
      );
    }
  }

  return paramListId;
}

class NftSqlStore implements INftDb {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async execute(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    sql: string | Sql
  ): ReturnType<INftDb["execute"]> {
    let result;

    if (typeof sql === "string") {
      result = await op_execute_nft_sql(
        this.name,
        resourceAddress,
        prepareNftId(nftId),
        sql
      );
    } else if (sql instanceof Sql) {
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

        const paramListId = prepareNftParamList(
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

  migrate(_sql: string): INftDb {
    // No-op at runtime (migrations are collated at options-parse time)
    return this;
  }

  async query(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    sql: string | Sql
  ): ReturnType<INftDb["query"]> {
    let result;

    if (typeof sql === "string") {
      result = await op_query_nft_sql(
        this.name,
        resourceAddress,
        prepareNftId(nftId),
        sql
      );
    } else if (sql instanceof Sql) {
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

        const paramListId = prepareNftParamList(
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

    let rows;

    if (result === "NftDoesNotExist") {
      throw new Error("NFT does not exist");
    } else if (result === "NoAccountsInContext") {
      throw new Error("No accounts in context");
    } else if (typeof result === "object" && "OwnershipInvalid" in result) {
      throw new Error(
        `NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`
      );
    } else {
      rows = result.Ok;
    }

    const columnNames: string[] = [];

    // First row also contains column names
    const firstRow: Record<string, null | number | string | Uint8Array> = {};
    for (let i = 0; i < rows[0].length; i++) {
      const sqlValue = rows[0][i];
      if (typeof sqlValue === "object" && "IntegerWithName" in sqlValue) {
        const [name, value] = sqlValue.IntegerWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "RealWithName" in sqlValue) {
        const [name, value] = sqlValue.RealWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "TextWithName" in sqlValue) {
        const [name, value] = sqlValue.TextWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "BlobWithName" in sqlValue) {
        const [name, value] = sqlValue.BlobWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "NullWithName" in sqlValue) {
        const name = sqlValue.NullWithName;
        columnNames.push(name);
        firstRow[name] = null;
      } else {
        throw new TypeError("Expected first row to contain column names");
      }
    }

    const results: Record<string, null | number | string | Uint8Array>[] = [];
    results.push(firstRow);

    // TODO: Reworks rows code to use generators
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const result: Record<string, null | number | string | Uint8Array> = {};
      for (let j = 0; j < row.length; j++) {
        const sqlValue = row[j];

        if (typeof sqlValue === "object" && "Integer" in sqlValue) {
          result[columnNames[j]] = sqlValue.Integer;
        } else if (typeof sqlValue === "object" && "Real" in sqlValue) {
          result[columnNames[j]] = sqlValue.Real;
        } else if (typeof sqlValue === "object" && "Text" in sqlValue) {
          result[columnNames[j]] = sqlValue.Text;
        } else if (typeof sqlValue === "object" && "Blob" in sqlValue) {
          result[columnNames[j]] = sqlValue.Blob;
        } else if (sqlValue === "Null") {
          result[columnNames[j]] = null;
        } else {
          throw new TypeError(
            "Only expected Integer, Real, Text, Blob, or Null"
          );
        }
      }
      results.push(result);
    }

    return results as never;
  }
}

export const getNftDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new NftSqlStore(name);
};

function preparePersonalParamList(values: any[]) {
  const paramListId = op_create_personal_params_list();

  for (const value of values) {
    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        op_add_personal_integer_param(paramListId, value);
      } else {
        op_add_personal_real_param(paramListId, value);
      }
    } else if (typeof value === "string") {
      op_add_personal_text_param(paramListId, value);
    } else if (value === null) {
      op_add_personal_null_param(paramListId);
    } else if (typeof value === "object" && value instanceof Uint8Array) {
      op_add_personal_blob_param(paramListId, value);
    } else {
      throw new TypeError(
        "Expected all values to be null, numbers, strings, or blobs"
      );
    }
  }

  return paramListId;
}

class PersonalSqlStore implements IPersonalDb {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async execute(sql: string | Sql): ReturnType<IApplicationDb["execute"]> {
    let result;

    if (typeof sql === "string") {
      result = await op_execute_personal_sql(this.name, sql);
    } else if (sql instanceof Sql) {
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

        const paramListId = preparePersonalParamList(
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

  migrate(_sql: string): IPersonalDb {
    // No-op at runtime (migrations are collated at options-parse time)
    return this as unknown as IPersonalDb;
  }

  async query(sql: string | Sql): ReturnType<IApplicationDb["query"]> {
    let result;

    if (typeof sql === "string") {
      result = await op_query_personal_sql(this.name, sql);
    } else if (sql instanceof Sql) {
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

        const paramListId = preparePersonalParamList(
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

    let rows;

    if (result === "NoPersonalContext") {
      throw new Error("No personal context");
    } else {
      rows = result.Ok;
    }

    const columnNames: string[] = [];

    // First row also contains column names
    const firstRow: Record<string, null | number | string | Uint8Array> = {};
    for (let i = 0; i < rows[0].length; i++) {
      const sqlValue = rows[0][i];
      if (typeof sqlValue === "object" && "IntegerWithName" in sqlValue) {
        const [name, value] = sqlValue.IntegerWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "RealWithName" in sqlValue) {
        const [name, value] = sqlValue.RealWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "TextWithName" in sqlValue) {
        const [name, value] = sqlValue.TextWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "BlobWithName" in sqlValue) {
        const [name, value] = sqlValue.BlobWithName;
        columnNames.push(name);
        firstRow[name] = value;
      } else if (typeof sqlValue === "object" && "NullWithName" in sqlValue) {
        const name = sqlValue.NullWithName;
        columnNames.push(name);
        firstRow[name] = null;
      } else {
        throw new TypeError("Expected first row to contain column names");
      }
    }

    const results: Record<string, null | number | string | Uint8Array>[] = [];
    results.push(firstRow);

    // TODO: Reworks rows code to use generators
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const result: Record<string, null | number | string | Uint8Array> = {};
      for (let j = 0; j < row.length; j++) {
        const sqlValue = row[j];

        if (typeof sqlValue === "object" && "Integer" in sqlValue) {
          result[columnNames[j]] = sqlValue.Integer;
        } else if (typeof sqlValue === "object" && "Real" in sqlValue) {
          result[columnNames[j]] = sqlValue.Real;
        } else if (typeof sqlValue === "object" && "Text" in sqlValue) {
          result[columnNames[j]] = sqlValue.Text;
        } else if (typeof sqlValue === "object" && "Blob" in sqlValue) {
          result[columnNames[j]] = sqlValue.Blob;
        } else if (sqlValue === "Null") {
          result[columnNames[j]] = null;
        } else {
          throw new TypeError(
            "Only expected Integer, Real, Text, Blob, or Null"
          );
        }
      }
      results.push(result);
    }

    return results as never;
  }
}

export const getPersonalDb = (name: string) => {
  if (!name) {
    throw new Error("Name must be provided");
  }

  return new PersonalSqlStore(name);
};
