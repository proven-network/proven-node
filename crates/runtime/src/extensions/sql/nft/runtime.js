import { Sql } from "proven:sql-template-tag"

function createNftParamList () {
  const { op_create_nft_params_list } = globalThis.Deno.core.ops;
  return op_create_nft_params_list()
}

function addNftBlobParam (paramListId, value) {
  const { op_add_nft_blob_param } = globalThis.Deno.core.ops;
  return op_add_nft_blob_param(paramListId, value)
}

function addNftIntegerParam (paramListId, value) {
  const { op_add_nft_integer_param } = globalThis.Deno.core.ops;
  return op_add_nft_integer_param(paramListId, value)
}

function addNftNullParam (paramListId, value) {
  const { op_add_nft_null_param } = globalThis.Deno.core.ops;
  return op_add_nft_null_param(paramListId)
}

function addNftRealParam (paramListId, value) {
  const { op_add_nft_real_param } = globalThis.Deno.core.ops;
  return op_add_nft_real_param(paramListId, value)
}

function addNftTextParam (paramListId, value) {
  const { op_add_nft_text_param } = globalThis.Deno.core.ops;
  return op_add_nft_text_param(paramListId, value)
}

function executeNftSql (sqlStoreName, resourceAddress, nftId, sqlStatement, paramListId) {
  const { op_execute_nft_sql } = globalThis.Deno.core.ops;
  return op_execute_nft_sql(sqlStoreName, resourceAddress, nftId, sqlStatement, paramListId)
}

function queryNftSql (sqlStoreName, resourceAddress, nftId, sqlStatement, paramListId) {
  const { op_query_nft_sql } = globalThis.Deno.core.ops;
  return op_query_nft_sql(sqlStoreName, resourceAddress, nftId, sqlStatement, paramListId)
}

function prepareNftId (nftId) {
  if (typeof nftId === "number") {
    return `#${nftId}#`
  } else if (typeof nftId === "string") {
    if (RUID_CHECK.test(nftId)) {
      return `{${nftId}}`
    } else {
      return `<${nftId}>`
    }
  } else if (nftId instanceof Uint8Array) {
    const hex = Array.from(nftId)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");

    return `[${hex}]`
  } else {
    throw new Error("nftId must be a number, string, or Uint8Array");
  }
}

function prepareNftParamList (values) {
  const paramListId = createNftParamList();

  for (const value of values) {
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        addNftIntegerParam(paramListId, value);
      } else {
        addNftRealParam(paramListId, value);
      }
    } else if (typeof value === 'string') {
      addNftTextParam(paramListId, value);
    } else if (value === null) {
      addNftNullParam(paramListId);
    } else if (typeof value === 'object' && value instanceof Uint8Array) {
      addNftBlobParam(paramListId, value);
    } else {
      throw new TypeError('Expected all values to be null, numbers, strings, or blobs')
    }
  }

  return paramListId
}

class NftSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  async execute (resourceAddress, nftId, sql) {
    let sqlStatement;
    let sqlValues = [];

    if (typeof sql === 'string') {
      sqlStatement = sql;
    } else if (sql instanceof Sql) {
      sqlStatement = sql.statement;
      sqlValues = sql.values;
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object');
    }

    let result;
    if (sqlValues.length === 0) {
      result = await executeNftSql(this.sqlStoreName, resourceAddress, prepareNftId(nftId), sqlStatement);
    } else {
      const paramListId = prepareNftParamList(sqlValues);

      result = await executeNftSql(this.sqlStoreName, resourceAddress, prepareNftId(nftId), sqlStatement, paramListId);
    }

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }

    return result.Ok;
  }

  migrate (sql) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this
  }

  async query (resourceAddress, nftId, sql) {
    let sqlStatement;
    let sqlValues = [];

    if (typeof sql === 'string') {
      sqlStatement = sql;
    } else if (sql instanceof Sql) {
      sqlStatement = sql.statement;
      sqlValues = sql.values;
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }

    let rows;

    if (sqlValues.length === 0) {
      const result = await queryNftSql(this.sqlStoreName, resourceAddress, prepareNftId(nftId), sqlStatement)

      if (result === "NftDoesNotExist") {
        throw new Error('NFT does not exist');
      } else if (result === "NoAccountsInContext") {
        throw new Error('No accounts in context');
      } else if (result.OwnershipInvalid) {
        throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
      } else {
        rows = result.Ok
      }
    } else {
      const paramListId = prepareNftParamList(sqlValues);

      const result = await queryNftSql(this.sqlStoreName, resourceAddress, prepareNftId(nftId), sqlStatement, paramListId)

      if (result === "NftDoesNotExist") {
        throw new Error('NFT does not exist');
      } else if (result === "NoAccountsInContext") {
        throw new Error('No accounts in context');
      } else if (result.OwnershipInvalid) {
        throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
      } else {
        rows = result.Ok
      }
    }

    const columnNames = []

    // First row also contains column names
    const firstRow = {}
    for (let i = 0; i < rows[0].length; i++) {
      if (rows[0][i].IntegerWithName) {
        const [ name, value ] = rows[0][i].IntegerWithName
        columnNames.push(name)
        firstRow[name] = value
      } else if (rows[0][i].RealWithName) {
        const [ name, value ] = row[0][i].RealWithName
        columnNames.push(name)
        firstRow[name] = value
      } else if (rows[0][i].TextWithName) {
        const [ name, value ] = rows[0][i].TextWithName
        columnNames.push(name)
        firstRow[name] = value
      } else if (rows[0][i].BlobWithName) {
        const [ name, value ] = rows[0][i].BlobWithName
        columnNames.push(name)
        firstRow[name] = value
      } else if (rows[0][i].NullWithName) {
        const name = rows[0][i].NullWithName
        columnNames.push(name)
        firstRow[name] = null
      } else {
        throw new TypeError('Expected first row to contain column names')
      }
    }

    const results = []
    results.push(firstRow)

    // TODO: Reworks rows code to use generators
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]
      const result = {}
      for (let j = 0; j < row.length; j++) {
        if (row[j].Integer) {
          result[columnNames[j]] = row[j].Integer
        } else if (row[j].Real) {
          result[columnNames[j]] = row[j].Real
        } else if (row[j].Text) {
          result[columnNames[j]] = row[j].Text
        } else if (row[j].Blob) {
          result[columnNames[j]] = row[j].Blob
        } else if (row[j].Null) {
          result[columnNames[j]] = null
        }
      }
      results.push(result)
    }

    return results
  }
}

export const getNftDb = (sqlStoreName) => {
  if (sqlStoreName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!sqlStoreName) {
    sqlStoreName = 'DEFAULT'
  }

  return new NftSqlStore(sqlStoreName)
}

export { sql } from "proven:sql-template-tag"
