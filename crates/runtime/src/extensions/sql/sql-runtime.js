import { Sql } from "proven:sql-template-tag"

class ApplicationSqlStore {
  constructor (sqlStoreName) {
    this.sqlStoreName = sqlStoreName
  }

  execute (sql) {
    if (sql instanceof Sql) {
      throw new Error('unimplemented')
    } else {
      throw new TypeError('Expected `sql` to be a string or Sql object')
    }
  }

  migrate (sql) {
    throw new Error('`migrate` must be run at the top level of your module')
  }

  query (sql) {
    if (sql instanceof Sql) {
      throw new Error('unimplemented')
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
