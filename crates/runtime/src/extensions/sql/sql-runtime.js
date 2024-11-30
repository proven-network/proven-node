import { Sql } from "proven:sql-template-tag"

function executeApplicationSql (store_name, sql) {
  const { op_execute_application_sql } = globalThis.Deno.core.ops;
  return op_execute_application_sql(store_name, sql)
}

class ApplicationSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  execute (sql) {
    if (sql instanceof Sql) {
      throw new Error(JSON.stringify(sql.values))
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }
  }

  migrate (sql) {
    // No-op at runtime (migrations are collated at options-parse time)
    return this
  }

  query (sql) {
    if (sql instanceof Sql) {
      throw new Error(JSON.stringify(sql.values))
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
