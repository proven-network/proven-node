import { TableRecord } from '../state';
export type RenameColumn<
  T extends TableRecord,
  Table extends string,
  OldCol extends string,
  NewCol extends string,
> = {
  [K in keyof T]: K extends Table
    ? {
        [Col in keyof T[K] as Col extends OldCol ? NewCol : Col]: T[K][Col];
      }
    : T[K];
} extends infer Result
  ? {
      [K in keyof Result]: Result[K];
    }
  : never;
