import { Sql } from "proven:sql-template-tag"

function migratePersonalSqlStore (storeName, sql) {
  const { op_migrate_personal_sql } = globalThis.Deno.core.ops;
  return op_migrate_personal_sql(storeName, sql)
}

class PersonalSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  execute () {
    throw new Error('`execute` must be run inside a handler function')
  }

  migrate (sql) {
    if (typeof sql === 'string') {
      migratePersonalSqlStore(this.sqlStoreName, sql)

      return this
    } else if (sql instanceof Sql) {
      migratePersonalSqlStore(this.sqlStoreName, sql.sql)

      return this
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }
  }

  query () {
    throw new Error('`query` must be run inside a handler function')
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
