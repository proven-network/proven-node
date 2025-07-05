// Debug type to check intermediate results
type DebugSplit = SplitToWordsAndSymbols<'CREATE TABLE'>;
// this gives ["CREATE", "TABLE"]

type DebugSplit2 = SplitToWordsAndSymbols<'CREATE TABLE (hello)'>;
// this gives ["CREATE", "TABLE", "(", "hello", ")"]

type DebugSplit3 = SplitToWordsAndSymbols<'CREATE TABLE (hello world)'>;
// this gives ["CREATE", "TABLE", "(", "hello", "world", ")"]

type DebugSplit4 = TokenizeSqlString<`
  CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL DEFAULT "Untitled Post",
    content TEXT NOT NULL,
    creator TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
`>;
