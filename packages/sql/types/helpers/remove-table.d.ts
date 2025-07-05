import { TableRecord } from '../state';
export type RemoveTable<T extends TableRecord, Table extends string> = {
    [K in keyof T as K extends Table ? never : K]: T[K];
} extends infer Result ? {
    [K in keyof Result]: Result[K];
} : never;
