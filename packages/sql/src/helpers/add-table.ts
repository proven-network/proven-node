import { CurrentColumn, TableRecord } from '../state';

export type ColumnsToRecord<Cols extends CurrentColumn[]> = {
  [K in Cols[number]['name']]: Extract<Cols[number], { name: K }>;
};

export type EvaluateColumnsToRecord<T> =
  T extends ColumnsToRecord<infer Cols>
    ? {
        [K in Cols[number]['name']]: Extract<Cols[number], { name: K }> extends infer Col
          ? Col extends CurrentColumn
            ? {
                type: Col['type'];
                isNullable: Col['isNullable'];
              }
            : never
          : never;
      }
    : never;

export type AddTable<
  Tables extends TableRecord,
  TableName extends string,
  Columns extends CurrentColumn[],
> = {
  [K in keyof Tables | TableName]: K extends keyof Tables
    ? Tables[K]
    : K extends TableName
      ? ColumnsToRecord<Columns>
      : never;
};

export type EvaluateAddTable<T> =
  T extends AddTable<infer Tables, infer Table, infer Columns>
    ? {
        [K in keyof Tables | Table]: K extends keyof Tables
          ? Tables[K]
          : K extends Table
            ? EvaluateColumnsToRecord<ColumnsToRecord<Columns>>
            : never;
      }
    : never;
