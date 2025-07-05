type ColumnType = 'INTEGER' | 'TEXT' | 'REAL' | 'BLOB';
type SqliteSymbol = '(' | ')' | ',' | ';';
type SqliteKeyword =
  | 'ADD'
  | 'ADD COLUMN'
  | 'ALTER TABLE'
  | 'CREATE TABLE'
  | 'DROP'
  | 'DROP COLUMN'
  | 'DROP TABLE'
  | 'FROM'
  | 'IF EXISTS'
  | 'IF NOT EXISTS'
  | 'NOT NULL'
  | 'PRIMARY KEY'
  | 'RENAME'
  | 'RENAME COLUMN'
  | 'SELECT'
  | 'TO';
type Token =
  | {
      type: 'TYPE';
      value: ColumnType;
    }
  | {
      type: 'KEYWORD';
      value: SqliteKeyword;
    }
  | {
      type: 'IDENTIFIER';
      value: string;
    }
  | {
      type: 'SYMBOL';
      value: SqliteSymbol;
    };
type LookaheadKeywordMap = {
  ADD: {
    COLUMN: {
      type: 'KEYWORD';
      value: 'ADD COLUMN';
    };
  };
  ALTER: {
    TABLE: {
      type: 'KEYWORD';
      value: 'ALTER TABLE';
    };
  };
  CREATE: {
    TABLE: {
      type: 'KEYWORD';
      value: 'CREATE TABLE';
    };
  };
  DROP: {
    COLUMN: {
      type: 'KEYWORD';
      value: 'DROP COLUMN';
    };
    TABLE: {
      type: 'KEYWORD';
      value: 'DROP TABLE';
    };
  };
  IF: {
    EXISTS: {
      type: 'KEYWORD';
      value: 'IF EXISTS';
    };
    NOT: {
      EXISTS: {
        type: 'KEYWORD';
        value: 'IF NOT EXISTS';
      };
    };
  };
  NOT: {
    NULL: {
      type: 'KEYWORD';
      value: 'NOT NULL';
    };
  };
  PRIMARY: {
    KEY: {
      type: 'KEYWORD';
      value: 'PRIMARY KEY';
    };
  };
  RENAME: {
    COLUMN: {
      type: 'KEYWORD';
      value: 'RENAME COLUMN';
    };
    TO: {
      type: 'KEYWORD';
      value: 'RENAME TO';
    };
  };
};
type ProcessWord<Word extends string> = Word extends '(' | ')' | ',' | ';'
  ? {
      type: 'SYMBOL';
      value: Word;
    }
  : ToUpper<Word> extends SqliteKeyword
    ? {
        type: 'KEYWORD';
        value: ToUpper<Word>;
      }
    : ToUpper<Word> extends ColumnType
      ? {
          type: 'TYPE';
          value: ToUpper<Word>;
        }
      : {
          type: 'IDENTIFIER';
          value: Word;
        };
type ToUpper<S extends string> = Uppercase<S>;
export type LexSqlTokens<
  Words extends readonly string[],
  Acc extends Token[] = [],
> = Words extends readonly [
  infer First extends string,
  infer Second extends string,
  infer Third extends string,
  ...infer Rest extends string[],
]
  ? ToUpper<First> extends keyof LookaheadKeywordMap
    ? ToUpper<Second> extends keyof LookaheadKeywordMap[ToUpper<First>]
      ? ToUpper<Third> extends keyof LookaheadKeywordMap[ToUpper<First>][ToUpper<Second>]
        ? LexSqlTokens<
            Rest,
            [...Acc, LookaheadKeywordMap[ToUpper<First>][ToUpper<Second>][ToUpper<Third>] & Token]
          >
        : LexSqlTokens<
            [Third, ...Rest],
            [...Acc, LookaheadKeywordMap[ToUpper<First>][ToUpper<Second>] & Token]
          >
      : LexSqlTokens<[Second, Third, ...Rest], [...Acc, ProcessWord<First>]>
    : LexSqlTokens<[Second, Third, ...Rest], [...Acc, ProcessWord<First>]>
  : Words extends readonly [infer First extends string, infer Second extends string]
    ? ToUpper<First> extends keyof LookaheadKeywordMap
      ? ToUpper<Second> extends keyof LookaheadKeywordMap[ToUpper<First>]
        ? [...Acc, LookaheadKeywordMap[ToUpper<First>][ToUpper<Second>]]
        : [...Acc, ProcessWord<First>, ProcessWord<Second>]
      : [...Acc, ProcessWord<First>, ProcessWord<Second>]
    : Words extends readonly [infer Single extends string]
      ? [...Acc, ProcessWord<Single>]
      : Acc;
export {};
