import { Sql } from "proven:sql-template-tag"

function createPersonalParamList () {
  const { op_create_personal_params_list } = globalThis.Deno.core.ops;
  return op_create_personal_params_list()
}

function addPersonalBlobParam (paramListId, value) {
  const { op_add_personal_blob_param } = globalThis.Deno.core.ops;
  return op_add_personal_blob_param(paramListId, value)
}

function addPersonalIntegerParam (paramListId, value) {
  const { op_add_personal_integer_param } = globalThis.Deno.core.ops;
  return op_add_personal_integer_param(paramListId, value)
}

function addPersonalNullParam (paramListId, value) {
  const { op_add_personal_null_param } = globalThis.Deno.core.ops;
  return op_add_personal_null_param(paramListId)
}

function addPersonalRealParam (paramListId, value) {
  const { op_add_personal_real_param } = globalThis.Deno.core.ops;
  return op_add_personal_real_param(paramListId, value)
}

function addPersonalTextParam (paramListId, value) {
  const { op_add_personal_text_param } = globalThis.Deno.core.ops;
  return op_add_personal_text_param(paramListId, value)
}

function executePersonalSql (sqlStoreName, sqlStatement, paramListId) {
  const { op_execute_personal_sql } = globalThis.Deno.core.ops;
  return op_execute_personal_sql(sqlStoreName, sqlStatement, paramListId)
}

function queryPersonalSql (sqlStoreName, sqlStatement, paramListId) {
  const { op_query_personal_sql } = globalThis.Deno.core.ops;
  return op_query_personal_sql(sqlStoreName, sqlStatement, paramListId)
}

function preparePersonalParamList (values) {
  const paramListId = createPersonalParamList();

  for (const value of values) {
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        addPersonalIntegerParam(paramListId, value);
      } else {
        addPersonalRealParam(paramListId, value);
      }
    } else if (typeof value === 'string') {
      addPersonalTextParam(paramListId, value);
    } else if (value === null) {
      addPersonalNullParam(paramListId);
    } else if (typeof value === 'object' && value instanceof Uint8Array) {
      addPersonalBlobParam(paramListId, value);
    } else {
      throw new TypeError('Expected all values to be null, numbers, strings, or blobs')
    }
  }

  return paramListId
}

class PersonalSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  async execute (sql) {
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

    let affectedRows;
    if (sqlValues.length === 0) {
      affectedRows = await executePersonalSql(this.sqlStoreName, sqlStatement);
    } else {
      const paramListId = preparePersonalParamList(sqlValues);

      affectedRows = await executePersonalSql(this.sqlStoreName, sqlStatement, paramListId);
    }

    return affectedRows;
  }

  migrate (sql) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this
  }

  async query (sql) {
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
      const result = await queryPersonalSql(this.sqlStoreName, sqlStatement)

      if (result === "NoPersonalContext") {
        throw new Error('No personal context');
      } else {
        rows = result.Ok;
      }
    } else {
      const paramListId = preparePersonalParamList(sqlValues);

      const result = await queryPersonalSql(this.sqlStoreName, sqlStatement, paramListId)

      if (result === "NoPersonalContext") {
        throw new Error('No personal context');
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

export const getPersonalDb = (sqlStoreName) => {
  if (sqlStoreName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!sqlStoreName) {
    sqlStoreName = 'DEFAULT'
  }

  return new PersonalSqlStore(sqlStoreName)
}

export { sql } from "proven:sql-template-tag"
