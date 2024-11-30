import { Sql } from "proven:sql-template-tag"

function createApplicationParamList () {
  const { op_create_application_params_list } = globalThis.Deno.core.ops;
  return op_create_application_params_list()
}

function addApplicationIntegerParam (paramListId, value) {
  const { op_add_application_integer_param } = globalThis.Deno.core.ops;
  return op_add_application_integer_param(paramListId, value)
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

class ApplicationSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  async execute (sql) {
    if (sql instanceof Sql) {
      const paramListId = createApplicationParamList();

      for (const value of sql.values) {
        if (typeof value === 'number') {
          addApplicationIntegerParam(paramListId, value);
        } else if (typeof value === 'string') {
          addApplicationTextParam(paramListId, value);
        } else {
          throw new TypeError('Expected all values to be numbers or strings')
        }
      }

      const affectedRows = await executeApplicationSql(this.sqlStoreName, sql.statement, paramListId)

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
      // TODO: No need to create param list unless sql.values is non-empty
      const paramListId = createApplicationParamList();

      for (const value of sql.values) {
        if (typeof value === 'number') {
          addApplicationIntegerParam(paramListId, value);
        } else if (typeof value === 'string') {
          addApplicationTextParam(paramListId, value);
        } else {
          throw new TypeError('Expected all values to be numbers or strings')
        }
      }

      const rowData = await queryApplicationSql(this.sqlStoreName, sql.statement, paramListId)

      const { column_count, column_names, column_types, rows } = rowData

      // TODO: Reworks rows code to use generators
      const results = []
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i]
        const result = {}
        for (let j = 0; j < column_count; j++) {
          const columnName = column_names[j]
          const columnType = column_types[j]
          const columnValue = row[j]
          if (columnType === 'INTEGER') {
            result[columnName] = Number(columnValue['Integer'])
          } else {
            result[columnName] = columnValue['Text']
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
