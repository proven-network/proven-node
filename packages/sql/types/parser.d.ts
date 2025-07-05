import {
  AlterTableState,
  ColumnDetails,
  CreateTableState,
  DropTableState,
  GeneratedSchema,
  InitialAlterTableState,
  InitialCreateTableState,
  InitialDropTableState,
  InitialGeneratedSchema,
  InitialQueryState,
  QueryState,
} from './state';
import { AddColumn } from './helpers/add-column';
import { AddTable, EvaluateAddTable } from './helpers/add-table';
import { UpdateLastColumn } from './helpers/merge-column';
import { RemoveColumn } from './helpers/remove-column';
import { RemoveTable } from './helpers/remove-table';
import { RenameColumn } from './helpers/rename-column';
import { ColumnToType, GetTableColumns } from './helpers/table-columns';
type ParseColumnDefinitions<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends CreateTableState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'KEYWORD';
      value: 'CREATE TABLE';
    }
    ? ParseColumnDefinitions<Rest, Schema, State>
    : First extends {
          type: 'IDENTIFIER';
          value: string;
        }
      ? ParseColumnDefinitions<
          Rest,
          Schema,
          {
            currentTableName: State['currentTableName'];
            currentColumns: [
              ...State['currentColumns'],
              {
                name: First['value'];
                type: '';
                isNullable: true;
              },
            ];
          }
        >
      : First extends {
            type: 'TYPE';
            value: string;
          }
        ? ParseColumnDefinitions<
            Rest,
            Schema,
            {
              currentTableName: State['currentTableName'];
              currentColumns: UpdateLastColumn<
                State['currentColumns'],
                {
                  type: First['value'];
                }
              >;
            }
          >
        : First extends {
              type: 'KEYWORD';
              value: 'PRIMARY KEY' | 'NOT NULL';
            }
          ? ParseColumnDefinitions<
              Rest,
              Schema,
              {
                currentTableName: State['currentTableName'];
                currentColumns: UpdateLastColumn<
                  State['currentColumns'],
                  {
                    isNullable: false;
                  }
                >;
              }
            >
          : First extends {
                type: 'SYMBOL';
                value: ')';
              }
            ? {
                tables: EvaluateAddTable<
                  AddTable<Schema['tables'], State['currentTableName'], State['currentColumns']>
                >;
              }
            : ParseColumnDefinitions<Rest, Schema, State>
  : never;
type ParseCreateTable<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema = InitialGeneratedSchema,
  State extends CreateTableState = InitialCreateTableState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'IDENTIFIER';
      value: infer Table extends string;
    }
    ? ParseCreateTable<
        Rest,
        Schema,
        {
          currentTableName: Table;
          currentColumns: [];
        }
      >
    : First extends {
          type: 'SYMBOL';
          value: '(';
        }
      ? ParseColumnDefinitions<Rest, Schema, State>
      : never
  : never;
type ParseAlterTable<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema = InitialGeneratedSchema,
  State extends AlterTableState = InitialAlterTableState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'IDENTIFIER';
      value: infer Table extends string;
    }
    ? ParseAlterTableAction<
        Rest,
        Schema,
        {
          currentTableName: Table;
        }
      >
    : ParseAlterTable<Rest, Schema, State>
  : Schema;
type ParseAlterTableAction<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends AlterTableState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'KEYWORD';
      value: 'ADD COLUMN';
    }
    ? ParseAddColumn<Rest, Schema, State>
    : First extends {
          type: 'KEYWORD';
          value: 'ADD';
        }
      ? ParseAddColumn<Rest, Schema, State>
      : First extends {
            type: 'KEYWORD';
            value: 'DROP COLUMN';
          }
        ? ParseDropColumn<Rest, Schema, State>
        : First extends {
              type: 'KEYWORD';
              value: 'DROP';
            }
          ? ParseDropColumn<Rest, Schema, State>
          : First extends {
                type: 'KEYWORD';
                value: 'RENAME COLUMN';
              }
            ? ParseRenameColumn<Rest, Schema, State>
            : First extends {
                  type: 'KEYWORD';
                  value: 'RENAME';
                }
              ? ParseRenameColumn<Rest, Schema, State>
              : ParseAlterTableAction<Rest, Schema, State>
  : Schema;
type ParseAddColumn<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends AlterTableState,
> = Tokens extends [
  {
    type: 'IDENTIFIER';
    value: infer Column extends string;
  },
  {
    type: 'TYPE';
    value: infer Type extends string;
  },
  ...infer Rest,
]
  ? Rest extends [
      {
        type: 'KEYWORD';
        value: 'PRIMARY KEY' | 'NOT NULL';
      },
      ...infer RestAfterConstraint,
    ]
    ? {
        tables: AddColumn<
          Schema['tables'],
          State['currentTableName'],
          Column,
          {
            type: Type;
            isNullable: false;
          }
        >;
      }
    : {
        tables: AddColumn<
          Schema['tables'],
          State['currentTableName'],
          Column,
          {
            type: Type;
            isNullable: true;
          }
        >;
      }
  : Schema;
type ParseDropColumn<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends AlterTableState,
> = Tokens extends [
  {
    type: 'IDENTIFIER';
    value: infer Column extends string;
  },
  ...infer Rest,
]
  ? {
      tables: RemoveColumn<Schema['tables'], State['currentTableName'], Column>;
    }
  : Schema;
type ParseRenameColumn<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends AlterTableState,
> = Tokens extends [
  {
    type: 'IDENTIFIER';
    value: infer OldColumn;
  },
  {
    type: 'KEYWORD';
    value: 'TO';
  },
  {
    type: 'IDENTIFIER';
    value: infer NewColumn;
  },
  ...infer Rest,
]
  ? {
      tables: RenameColumn<
        Schema['tables'],
        State['currentTableName'],
        OldColumn & string,
        NewColumn & string
      >;
    }
  : Schema;
type ParseDropTable<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema = InitialGeneratedSchema,
  State extends DropTableState = InitialDropTableState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'IDENTIFIER';
      value: infer Table extends string;
    }
    ? {
        tables: RemoveTable<Schema['tables'], Table>;
      }
    : ParseDropTable<Rest, Schema, State>
  : Schema;
export type ParseMigration<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema = InitialGeneratedSchema,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'KEYWORD';
      value: 'CREATE TABLE';
    }
    ? ParseCreateTable<Rest, Schema>
    : First extends {
          type: 'KEYWORD';
          value: 'ALTER TABLE';
        }
      ? ParseAlterTable<Rest, Schema>
      : First extends {
            type: 'KEYWORD';
            value: 'DROP TABLE';
          }
        ? ParseDropTable<Rest, Schema>
        : ParseMigration<Rest, Schema>
  : Schema;
export type ParseQueryType<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends QueryState = InitialQueryState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'KEYWORD';
      value: 'SELECT';
    }
    ? ParseSelectColumns<Rest, Schema, State> extends Record<string, ColumnDetails>
      ? {
          [K in keyof ParseSelectColumns<Rest, Schema, State>]: ColumnToType<
            ParseSelectColumns<Rest, Schema, State>[K]
          >;
        }
      : never
    : ParseQueryType<Rest, Schema, State>
  : never;
type ParseSelectColumns<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends QueryState,
> = Tokens extends [infer First, ...infer Rest]
  ? First extends {
      type: 'IDENTIFIER';
      value: '*';
    }
    ? ParseFromClause<
        Rest,
        Schema,
        {
          currentTableName: State['currentTableName'];
          allColumns: true;
          specificColumns: [];
        }
      >
    : First extends {
          type: 'IDENTIFIER';
          value: string;
        }
      ? ParseSelectColumns<
          Rest,
          Schema,
          {
            currentTableName: State['currentTableName'];
            allColumns: false;
            specificColumns: [...State['specificColumns'], First['value']];
          }
        >
      : First extends {
            type: 'KEYWORD';
            value: 'FROM';
          }
        ? ParseFromClause<Rest, Schema, State>
        : ParseSelectColumns<Rest, Schema, State>
  : never;
type ParseFromClause<
  Tokens extends readonly any[],
  Schema extends GeneratedSchema,
  State extends QueryState,
> = Tokens extends [
  {
    type: 'KEYWORD';
    value: 'FROM';
  },
  {
    type: 'IDENTIFIER';
    value: infer Table extends string;
  },
  ...infer Rest,
]
  ? State['allColumns'] extends true
    ? GetTableColumns<Schema, Table>
    : Pick<GetTableColumns<Schema, Table>, State['specificColumns'][number]>
  : Tokens extends [
        {
          type: 'KEYWORD';
          value: 'FROM';
        },
        ...infer Rest,
      ]
    ? ParseFromClause<Rest, Schema, State>
    : Tokens extends [infer _, ...infer Rest]
      ? ParseFromClause<Rest, Schema, State>
      : never;
export {};
