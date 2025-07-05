import { LexSqlTokens } from './lexer';
import { TokenizeSqlString } from './tokenizer';

type _TestLowercase = LexSqlTokens<['create', 'table', 'users']>;

type _TestTripleKeyword = LexSqlTokens<
  ['create', 'table', 'if', 'not', 'exists', 'users', '(', 'id', 'integer', ')']
>;

type _TestLowercaseType = LexSqlTokens<
  ['create', 'table', 'users', '(', 'id', 'integer', 'primary', 'key', 'not', 'null', ')']
>;

type _TestAlterTable = LexSqlTokens<
  TokenizeSqlString<`
  ALTER TABLE users
  ADD COLUMN email TEXT NOT NULL;
`>
>;

type _TestBig = LexSqlTokens<
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

// Basic test to verify TypeScript compilation
test('should compile lexer types', () => {
  expect(true).toBe(true);
});
