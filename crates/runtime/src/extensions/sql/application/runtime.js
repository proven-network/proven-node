import { Sql } from "proven:sql-template-tag"

function createApplicationParamList () {
  const { op_create_application_params_list } = globalThis.Deno.core.ops;
  return op_create_application_params_list()
}

function addApplicationBlobParam (paramListId, value) {
  const { op_add_application_blob_param } = globalThis.Deno.core.ops;
  return op_add_application_blob_param(paramListId, value)
}

function addApplicationIntegerParam (paramListId, value) {
  const { op_add_application_integer_param } = globalThis.Deno.core.ops;
  return op_add_application_integer_param(paramListId, value)
}

function addApplicationNullParam (paramListId, value) {
  const { op_add_application_null_param } = globalThis.Deno.core.ops;
  return op_add_application_null_param(paramListId)
}

function addApplicationRealParam (paramListId, value) {
  const { op_add_application_real_param } = globalThis.Deno.core.ops;
  return op_add_application_real_param(paramListId, value)
}

function addApplicationTextParam (paramListId, value) {
  const { op_add_application_text_param } = globalThis.Deno.core.ops;
  return op_add_application_text_param(paramListId, value)
}

function executeApplicationSql (sqlStoreName, sqlStatement, paramListId) {
  const { op_execute_application_sql } = globalThis.Deno.core.ops;
  return op_execute_application_sql(sqlStoreName, sqlStatement, paramListId)
}

function queryApplicationSql (sqlStoreName, sqlStatement, paramListId) {
  const { op_query_application_sql } = globalThis.Deno.core.ops;
  return op_query_application_sql(sqlStoreName, sqlStatement, paramListId)
}

function prepareApplicationParamList (values) {
  const paramListId = createApplicationParamList();

  for (const value of values) {
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        addApplicationIntegerParam(paramListId, value);
      } else {
        addApplicationRealParam(paramListId, value);
      }
    } else if (typeof value === 'string') {
      addApplicationTextParam(paramListId, value);
    } else if (value === null) {
      addApplicationNullParam(paramListId);
    } else if (typeof value === 'object' && value instanceof Uint8Array) {
      addApplicationBlobParam(paramListId, value);
    } else {
      throw new TypeError('Expected all values to be null, numbers, strings, or blobs')
    }
  }

  return paramListId
}

class ApplicationSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  async execute (sql) {
    if (sql instanceof Sql) {
      let affectedRows
      if (sql.values.length === 0) {
        affectedRows = await executeApplicationSql(this.sqlStoreName, sql.statement)
      } else {
        const paramListId = prepareApplicationParamList(sql.values);

        affectedRows = await executeApplicationSql(this.sqlStoreName, sql.statement, paramListId)
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
        rows = await queryApplicationSql(this.sqlStoreName, sql.statement)
      } else {
        const paramListId = prepareApplicationParamList(sql.values);

        rows = await queryApplicationSql(this.sqlStoreName, sql.statement, paramListId)
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

export const getApplicationDb = (sqlStoreName) => {
  if (sqlStoreName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!sqlStoreName) {
    sqlStoreName = 'DEFAULT'
  }

  return new ApplicationSqlStore(sqlStoreName)
}

export { sql } from "proven:sql-template-tag"
