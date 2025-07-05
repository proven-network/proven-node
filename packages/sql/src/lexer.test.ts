type TestLowercase = LexSqlTokens<['create', 'table', 'users']>;

type TestTripleKeyword = LexSqlTokens<
  ['create', 'table', 'if', 'not', 'exists', 'users', '(', 'id', 'integer', ')']
>;

type TestLowercaseType = LexSqlTokens<
  ['create', 'table', 'users', '(', 'id', 'integer', 'primary', 'key', 'not', 'null', ')']
>;

type TestAlterTable = LexSqlTokens<
  TokenizeSqlString<`
  ALTER TABLE users
  ADD COLUMN email TEXT NOT NULL;
`>
>;

type TestBig = LexSqlTokens<
  TokenizeSqlString<`
  CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    creator TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`>
>;
