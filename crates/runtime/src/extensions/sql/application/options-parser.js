import { Sql } from "proven:sql-template-tag"

function migrateApplicationSqlStore (store_name, sql) {
  const { op_migrate_application_sql } = globalThis.Deno.core.ops;
  return op_migrate_application_sql(store_name, sql)
}

class ApplicationSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  execute () {
    throw new Error('`execute` must be run inside a handler function')
  }

  migrate (sql) {
    if (sql instanceof Sql) {
      migrateApplicationSqlStore(this.sqlStoreName, sql.sql)

      return this
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }
  }

  query () {
    throw new Error('`query` must be run inside a handler function')
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
