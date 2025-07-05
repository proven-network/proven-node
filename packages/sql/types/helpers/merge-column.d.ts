import { CurrentColumn } from '../state';
type LastElement<T extends any[]> = T extends [...any[], infer Last] ? Last extends CurrentColumn ? Last : never : never;
type AllButLast<T extends CurrentColumn[]> = T extends [...infer Rest extends CurrentColumn[], any] ? Rest : T;
type MergeColumn<Col extends CurrentColumn, Updates extends Partial<CurrentColumn>> = Col extends {
    name: infer N extends string;
    type: infer T extends string;
    isNullable: infer I extends boolean;
} ? {
    name: N;
    type: Updates['type'] extends string ? Updates['type'] : T;
    isNullable: Updates['isNullable'] extends boolean ? Updates['isNullable'] : I;
} : never;
export type UpdateLastColumn<Columns extends CurrentColumn[], Updates extends Partial<CurrentColumn>> = Columns['length'] extends 0 ? Columns : [...AllButLast<Columns>, MergeColumn<LastElement<Columns>, Updates>];
export {};
