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
    if (sql instanceof Sql) {
      let affectedRows
      if (sql.values.length === 0) {
        affectedRows = await executePersonalSql(this.sqlStoreName, sql.statement)
      } else {
        const paramListId = preparePersonalParamList(sql.values);

        affectedRows = await executePersonalSql(this.sqlStoreName, sql.statement, paramListId)
      }

      return affectedRows
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }
  }

  migrate (sql) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this
  }

  async query (sql) {
    if (sql instanceof Sql) {
      let rows
      if (sql.values.length === 0) {
        rows = await queryPersonalSql(this.sqlStoreName, sql.statement)
      } else {
        const paramListId = preparePersonalParamList(sql.values);

        rows = await queryPersonalSql(this.sqlStoreName, sql.statement, paramListId)
      }

      // TODO: Reworks rows code to use generators
      const results = []
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i]
        const result = []
        for (let j = 0; j < row.length; j++) {
          if (row[j].Integer) {
            result.push(row[j].Integer)
          } else if (row[j].Real) {
            result.push(row[j].Real)
          } else if (row[j].Text) {
            result.push(row[j].Text)
          } else if (row[j].Blob) {
            result.push(row[j].Blob)
          } else if (row[j].Null) {
            result.push(null)
          }
        }
        results.push(result)
      }

      return results
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }
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
