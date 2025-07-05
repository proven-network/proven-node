import { ColumnDetails, GeneratedSchema } from '../state';
export type GetTableColumns<Schema extends GeneratedSchema, Table extends string> = Schema['tables'][Table];
export type ColumnToType<T extends ColumnDetails> = T['type'] extends 'INTEGER' ? T['isNullable'] extends true ? number | null : number : T['type'] extends 'TEXT' ? T['isNullable'] extends true ? string | null : string : T['type'] extends 'REAL' ? T['isNullable'] extends true ? number | null : number : T['type'] extends 'BLOB' ? T['isNullable'] extends true ? Uint8Array | null : Uint8Array : never;
