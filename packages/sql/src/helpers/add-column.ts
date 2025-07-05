import { ColumnDetails, TableRecord } from '../state';

export type AddColumn<
  T extends TableRecord,
  Table extends string,
  Column extends string,
  Details extends ColumnDetails,
> = {
  [K in keyof T]: K extends Table
    ? { [Col in keyof T[K] | Column]: Col extends Column ? Details : T[K][Col] }
    : T[K];
} extends infer Result
  ? { [K in keyof Result]: Result[K] }
  : never;
